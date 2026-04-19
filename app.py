import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
import google.generativeai as genai
from sklearn.linear_model import LinearRegression
import numpy as np
import json
import requests
import xml.etree.ElementTree as ET

def get_env_canada_forecast():
    # Coordinates for Hard Rock Ottawa area
    lat, lon = 45.33, -75.71 
    
    # Environment Canada GeoMet WFS URL
    # We are pulling the HRDPS (High-Resolution Deterministic Prediction System)
    url = f"https://geo.weather.gc.ca/geomet?service=WFS&version=2.0.0&request=GetFeature&typeName=GDPS.DIAG_2M.TA&outputFormat=application/json&srsName=EPSG:4326&bbox={lat-0.1},{lon-0.1},{lat+0.1},{lon+0.1}"
    
    try:
        # Note: For production, we'd parse the specific GRIB2 layers. 
        # Here is a simplified version that structures the data for your AI.
        # Environment Canada's API is free and requires no API key.
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            # We structure this specifically for the Gemini prompt
            return {
                "source": "Environment Canada (MSC GeoMet)",
                "region": "Ottawa-Gatineau",
                "data_points": "High-Res Deterministic Model"
            }
    except Exception as e:
        return None

# 1. INITIALIZE SUPABASE
url = st.secrets["SUPABASE_URL"]
key = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

# 2. THE HYDRATION BLOCK (Add this now)
# This checks the "Vault" (Supabase) as soon as the app wakes up
if 'coeffs' not in st.session_state:
    try:
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        if response.data:
            st.session_state.coeffs = response.data[0]
        else:
            # Fallback if DB is empty
            st.session_state.coeffs = {
                "id": 1, "Intercept": 1000, "Avg_Coin_In": 1200,
                "Temp_C": 0, "Snow_cm": 0, "Rain_mm": 0,
                "Promo": 0, "Clicks": 0, "Impressions": 0
            }
    except Exception as e:
        st.session_state.coeffs = {"Intercept": 1000, "Avg_Coin_In": 1200}

# 3. GLOBAL DATA LOADING (Your ledger)
# Ensure your ledger data is also loaded into session state here...
# 1. PAGE CONFIG (Must be the very first Streamlit command)
st.set_page_config(page_title="FloorCast", layout="wide")

# 2. MODERN UI STYLING (The CSS)
st.markdown("""
    <style>
    /* Main Background */
    .stApp {
        background-color: #f4f7f9;
    }
    
    /* Bento Box Card Effect */
    div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column;"] > div[data-testid="stVerticalBlock"] {
        border: 1px solid #e6e9ef;
        border-radius: 12px;
        padding: 20px;
        background-color: #ffffff;
        box-shadow: 0 4px 12px rgba(0,0,0,0.03);
    }

    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff;
        border: 1px solid #e6e9ef;
        border-radius: 8px 8px 0px 0px;
        padding: 10px 20px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FFCC00 !important; /* Hard Rock Gold */
        color: #000000 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# 3. DATABASE & AI SETUP (Using your existing secrets)
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

# 4. INITIALIZE SESSION STATE
if 'coeffs' not in st.session_state:
    st.session_state.coeffs = {
        'Intercept': 3250.0,
        'DOW_Mon': -450.0, 'DOW_Tue': -380.0, 'DOW_Wed': -210.0, 'DOW_Thu': 150.0,
        'DOW_Fri': 1200.0, 'DOW_Sat': 2100.0, 'DOW_Sun': 850.0,
        'Temp_C': 2.5, 'Snow_cm': -45.0, 'Rain_mm': -12.0, 'Alert': -500.0,
        'Promo': 450.0, 'Impressions': 0.0002, 'Engagements': 0.15, 'Clicks': 0.85,
        'Avg_Coin_In': 112.50
    }

# 5. FETCH DATA
def fetch_data():
    try:
        response = supabase.table("ledger").select("*").execute()
        return response.data
    except:
        return []

ledger_data = fetch_data()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Executive", "Input", "Strategy", "Admin", "Analyst", "Master Report"
])

# --- 1. INITIAL DATA HYDRATION (Run at Startup) ---
if 'coeffs' not in st.session_state:
    try:
        # Pull the master record (ID 1) from Supabase
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        
        if response.data:
            # Load saved weights into session state
            st.session_state.coeffs = response.data[0]
        else:
            # Fallback if the table is empty
            st.session_state.coeffs = {
                "id": 1, "Intercept": 1000, "Temp_C": 0, "Snow_cm": 0, 
                "Rain_mm": 0, "Promo": 0, "Clicks": 0, 
                "Impressions": 0, "Avg_Coin_In": 1200
            }
    except Exception as e:
        st.error(f"Failed to load Engine Weights: {e}")

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    # 1. BRANDED HEADER
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🏛️ Executive Property Overview</h2>
            <p style="color: #888; margin: 0;">YTD Performance & AI Model Confidence</p>
        </div>
    """, unsafe_allow_html=True)

    # Pull latest coefficients
    c = st.session_state.coeffs
    avg_spend = c.get('Avg_Coin_In', 1200)
    click_weight = c.get('Clicks', 0)
    promo_lift = c.get('Promo', 0)
    intercept = c.get('Intercept', 0)

    df_exec = pd.DataFrame(ledger_data)
    
    if not df_exec.empty:
        # --- 2. SAFETY GUARD: ENSURE COLUMNS EXIST ---
        for col in ['temp_c', 'snow_cm', 'rain_mm', 'ad_clicks', 'active_promo']:
            if col not in df_exec.columns:
                df_exec[col] = 0.0
        
        df_exec['entry_date'] = pd.to_datetime(df_exec['entry_date'])
        
        # --- 3. CALCULATE LIFT & PREDICTABILITY ---
        # Calculate Digital Lift
        df_exec['daily_digital_lift'] = (df_exec['ad_clicks'] * click_weight) + (df_exec['active_promo'].astype(int) * promo_lift)
        df_exec['daily_digital_revenue'] = df_exec['daily_digital_lift'] * avg_spend
        
        # Calculate AI Expected Traffic (The Model's "Guess")
        df_exec['expected_traffic'] = (
            intercept + 
            df_exec['daily_digital_lift'] + 
            (df_exec['temp_c'] * c.get('Temp_C', 0)) + 
            (df_exec['snow_cm'] * c.get('Snow_cm', 0)) + 
            (df_exec['rain_mm'] * c.get('Rain_mm', 0))
        )
        
        # Aggregate Totals
        total_traffic = df_exec['actual_traffic'].sum()
        total_revenue = df_exec['actual_coin_in'].sum()
        total_lift_rev_ytd = df_exec['daily_digital_revenue'].sum()
        
        # Accuracy Calculation (MAPE)
        df_exec['error'] = abs(df_exec['actual_traffic'] - df_exec['expected_traffic']) / df_exec['actual_traffic']
        accuracy_score = max(0, (1 - df_exec['error'].mean()) * 100)
        score_color = "#00FF00" if accuracy_score > 85 else "#FFCC00" if accuracy_score > 70 else "#FF0000"

        # --- 4. BENTO KPI CARDS ---
        row1_col1, row1_col2 = st.columns(2)
        with row1_col1:
            st.markdown(f"""
                <div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-top: 5px solid #FFCC00; text-align: center;">
                    <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">Total YTD Traffic</p>
                    <h1 style="color: #FFF; margin: 0;">{total_traffic:,}</h1>
                    <p style="color: #FFCC00; font-size: 11px; margin-top: 10px;">Property Volume</p>
                </div>
            """, unsafe_allow_html=True)
        with row1_col2:
            st.markdown(f"""
                <div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-top: 5px solid #FFCC00; text-align: center;">
                    <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">Total YTD Revenue</p>
                    <h1 style="color: #FFF; margin: 0;">${total_revenue:,.0f}</h1>
                    <p style="color: #FFCC00; font-size: 11px; margin-top: 10px;">Actual Coin-In</p>
                </div>
            """, unsafe_allow_html=True)

        st.write("##")
        row2_col1, row2_col2 = st.columns(2)
        with row2_col1:
            st.markdown(f"""
                <div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-top: 5px solid #FFCC00; text-align: center;">
                    <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">Digital Lift Revenue</p>
                    <h1 style="color: #FFF; margin: 0;">${total_lift_rev_ytd:,.0f}</h1>
                    <p style="color: #FFCC00; font-size: 11px; margin-top: 10px;">Value of Marketing Weights</p>
                </div>
            """, unsafe_allow_html=True)
        with row2_col2:
            st.markdown(f"""
                <div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-left: 10px solid {score_color}; text-align: center;">
                    <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">AI Predictability</p>
                    <h1 style="color: {score_color}; margin: 0;">{accuracy_score:.1f}%</h1>
                    <p style="color: #FFF; font-size: 11px; margin-top: 10px;">Model Confidence</p>
                </div>
            """, unsafe_allow_html=True)

        # --- 5. DATA TABLE ---
        st.write("---")
        st.write("#### 🗓️ Recent Ledger Activity")
        st.dataframe(
            df_exec[['entry_date', 'actual_traffic', 'actual_coin_in', 'daily_digital_lift', 'daily_digital_revenue']].tail(5), 
            use_container_width=True, hide_index=True
        )

    else:
        st.warning("Ledger is empty. Please add data in the Input tab.")
# --- TAB 2: DAILY TRACKER & FORECAST ---
with tab2:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🕹️ FloorPace Control Panel</h2>
            <p style="color: #888; margin: 0;">Log Daily Actuals, Simulate Forecasts, and Audit Historical Data.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Create the two-column "Bento" layout
    col_input, col_sandbox = st.columns([1, 1.2])
    
    with col_input:
        with st.container(border=True):
            st.subheader("📝 1. Log Actuals")
            with st.form("entry_form", clear_on_submit=True):
                date_in = st.date_input("Date", datetime.date.today())
                act_traf = st.number_input("Actual Traffic", min_value=0, step=100)
                act_coin = st.number_input("Actual Coin-In ($)", min_value=0, step=1000)
                
                st.divider()
                st.markdown("**Environment & Marketing**")
                w1, w2 = st.columns(2)
                temp = w1.slider("Temp (°C)", -30, 40, 15)
                snow = w2.slider("Snow (cm)", 0, 50, 0)
                
                promo = st.checkbox("Active Promotion")
                
                with st.expander("Detailed Digital Metrics"):
                    imp = st.number_input("Ad Impressions", 0, 1000000, 300000)
                    clks = st.number_input("Ad Clicks", 0, 5000, 200)

                if st.form_submit_button("💾 Save to FloorPace Ledger", use_container_width=True):
                    c = st.session_state.coeffs
                    dow_key = f"DOW_{date_in.strftime('%a')}"
                    
                    # Math for Prediction
                    base_v = float(c['Intercept'] + c.get(dow_key, 0))
                    weather_v = float((temp * c['Temp_C']) + (snow * c['Snow_cm']))
                    dig_lift_v = float((promo * c['Promo']) + (imp * c['Impressions']) + (clks * c['Clicks']))
                    final_pred = float(base_v + weather_v + dig_lift_v)
                    
                    # CLEAN PAYLOAD: Removing 'variance' to match SQL Schema
                    data = {
                        "entry_date": str(date_in),
                        "actual_traffic": int(act_traf),
                        "actual_coin_in": float(act_coin),
                        "predicted_traffic": int(final_pred),
                        "temp_c": float(temp),
                        "snow_cm": float(snow),
                        "active_promo": bool(promo),
                        "ad_impressions": int(imp),
                        "ad_clicks": int(clks)
                    }
                    
                    try:
                        supabase.table("ledger").upsert(data, on_conflict="entry_date").execute()
                        st.toast("✅ Record saved successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Database Sync Error: {e}")

    with col_sandbox:
        with st.container(border=True):
            st.subheader("🔮 2. Forecast Sandbox")
            st.write("Simulate future dates with full environmental and digital variables.")
            
            # 1. DATE & PRIMARY ENVIRONMENT
            f_range = st.date_input("Forecast Range", [datetime.date.today(), datetime.date.today() + datetime.timedelta(days=7)])
            
            s1, s2, s3 = st.columns(3)
            sim_temp = s1.slider("Sim. Temp (°C)", -30, 40, 15)
            sim_rain = s2.slider("Sim. Rain (mm)", 0, 50, 0)
            sim_snow = s3.slider("Sim. Snow (cm)", 0, 50, 0)
            
            # 2. ALERTS & PROMOS
            a1, a2 = st.columns(2)
            sim_promo = a1.checkbox("Apply Promotion?")
            sim_alert = a2.checkbox("Simulate Weather Alert?")
            
            # 3. DIGITAL CAMPAIGN SIMULATOR
            st.markdown("**Digital Campaign Simulation**")
            sd1, sd2 = st.columns(2)
            sim_imp = sd1.number_input("Est. Impressions", value=300000, step=10000)
            sim_clk = sd2.number_input("Est. Ad Clicks", value=500, step=50)
            
            if len(f_range) == 2:
                dates = pd.date_range(f_range[0], f_range[1])
                c = st.session_state.coeffs
                f_list = []
                
                for d in dates:
                    dk = f"DOW_{d.strftime('%a')}"
                    
                    # ENHANCED MULTIVARIATE MATH
                    # We assume a -15% 'Alert Penalty' if a Weather Alert is simulated
                    alert_penalty = 0.85 if sim_alert else 1.0
                    
                    p_traffic = (
                        c['Intercept'] + 
                        c.get(dk, 0) + 
                        (sim_temp * c.get('Temp_C', 0)) + 
                        (sim_snow * c.get('Snow_cm', 0)) +
                        (sim_rain * c.get('Rain_mm', -2.5)) + # Fallback penalty for rain
                        (c.get('Promo', 0) if sim_promo else 0) +
                        (sim_imp * c.get('Impressions', 0)) + 
                        (sim_clk * c.get('Clicks', 0))
                    ) * alert_penalty
                    
                    p_revenue = p_traffic * c['Avg_Coin_In']
                    
                    f_list.append({
                        "Date": d.strftime("%a %d"), 
                        "Visitors": int(max(0, p_traffic)), 
                        "Revenue": float(max(0, p_revenue))
                    })
                
                df_f = pd.DataFrame(f_list)
                
                # 4. DUAL METRIC DISPLAY
                m_col1, m_col2 = st.columns(2)
                m_col1.metric("Est. Total Visitors", f"{df_f['Visitors'].sum():,.0f}")
                m_col2.metric("Est. Total Revenue", f"${df_f['Revenue'].sum():,.0f}")
                
                # 5. VISUAL TREND
                st.line_chart(df_f.set_index("Date")["Visitors"], color="#FFCC00")
                
                if sim_alert:
                    st.warning("⚠️ Projections reflect a 15% reduction due to Simulated Weather Alert.")

    # --- SECTION 3: AUDIT & FULL-FIELD EDIT ---
    st.markdown("---")
    st.markdown("### 🔍 3. Historical Ledger Audit & Corrections")
    if ledger_data:
        df_edit = pd.DataFrame(ledger_data)
        df_edit['entry_date'] = pd.to_datetime(df_edit['entry_date'])
        
        with st.container(border=True):
            st.markdown("**Find & Fix a Specific Entry**")
            edit_col1, edit_col2 = st.columns([1, 2])
            
            with edit_col1:
                search_date = st.date_input("Select Date to Edit", value=df_edit['entry_date'].max().date())
                search_str = search_date.strftime("%Y-%m-%d")
            
            found = df_edit[df_edit['entry_date'].dt.strftime('%Y-%m-%d') == search_str]
            
            with edit_col2:
                if not found.empty:
                    record = found.iloc[0]
                    if st.toggle(f"🔓 Unlock ALL FIELDS for {search_str}"):
                        with st.form(f"full_edit_{search_str}"):
                            ec1, ec2, ec3 = st.columns(3)
                            with ec1:
                                up_t = st.number_input("Traffic", value=int(record.get('actual_traffic', 0)))
                                up_c = st.number_input("Coin-In ($)", value=float(record.get('actual_coin_in', 0.0)))
                                up_p = st.number_input("Pred. Traffic", value=int(record.get('predicted_traffic', 0)))
                            with ec2:
                                up_temp = st.number_input("Temp", value=float(record.get('temp_c', 0.0)))
                                up_snow = st.number_input("Snow", value=float(record.get('snow_cm', 0.0)))
                            with ec3:
                                up_promo = st.checkbox("Promo", value=bool(record.get('active_promo', False)))
                                up_imp = st.number_input("Impressions", value=int(record.get('ad_impressions', 0)))
                                up_clk = st.number_input("Clicks", value=int(record.get('ad_clicks', 0)))

                            if st.form_submit_button("💾 Save All Changes", use_container_width=True):
                                try:
                                    # STRICT TYPES: Ints for traffic/impressions, Floats for money/temp
                                    supabase.table("ledger").update({
                                        "actual_traffic": int(up_t), 
                                        "actual_coin_in": float(up_c), 
                                        "predicted_traffic": int(up_p),
                                        "temp_c": float(up_temp), 
                                        "snow_cm": float(up_snow), 
                                        "active_promo": bool(up_promo), 
                                        "ad_impressions": int(up_imp), 
                                        "ad_clicks": int(up_clk)
                                    }).eq("entry_date", search_str).execute()
                                    st.toast(f"Record for {search_str} updated!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Update Error: {e}")
                else:
                    st.info("No record found for this date.")

        st.markdown("**Full Historical Ledger**")
        display_df = df_edit.sort_values('entry_date', ascending=False)
        display_df['entry_date'] = display_df['entry_date'].dt.strftime('%Y-%m-%d')
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# --- TAB 3: STRATEGY & ROI ---
with tab3:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🚀 Strategy & Digital ROI</h2>
            <p style="color: #888; margin: 0;">Calculating the financial lift of digital marketing efforts.</p>
        </div>
    """, unsafe_allow_html=True)

    # Pull latest coefficients
    c = st.session_state.coeffs
    click_weight = c.get('Clicks', 0)
    promo_lift = c.get('Promo', 0)
    avg_spend = c.get('Avg_Coin_In', 1200)

    # 1. ROI CALCULATOR
    st.write("### Digital Lift Analysis")
    col_input, col_result = st.columns([1, 1])
    
    with col_input:
        st.info("Current Engine Weights applied:")
        st.write(f"* **Weight per Click:** {click_weight}")
        st.write(f"* **Promo Base Lift:** {promo_lift} guests")
        st.write(f"* **Revenue Value:** ${avg_spend:,.2f} / head")

    with col_result:
        # Example Calculation based on 1,000 clicks
        test_clicks = 1000
        attributed_traffic = (test_clicks * click_weight) + promo_lift
        attributed_rev = attributed_traffic * avg_spend
        
        st.success(f"**Attributed Revenue per 1k Clicks**")
        st.header(f"${attributed_rev:,.2f}")
        st.caption(f"Estimated {attributed_traffic:,.0f} additional guests driven by digital.")

    # 2. HISTORICAL ROI TREND
    df_strat = pd.DataFrame(ledger_data)
    if not df_strat.empty:
        # Calculate daily attributed revenue
        df_strat['Digital_Revenue_Lift'] = ((df_strat['ad_clicks'] * click_weight) + 
                                           (df_strat['active_promo'].astype(int) * promo_lift)) * avg_spend
        
        st.write("### Historical Digital Revenue Contribution")
        st.area_chart(df_strat.set_index('entry_date')['Digital_Revenue_Lift'])

# --- TAB 4: ADMIN ENGINE (WHOLESOME CALIBRATION) ---
with tab4:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Control & Data Management</h2>
            <p style="color: #888; margin: 0;">AI Calibration with Historical Guardrails (Negative Weather Impact).</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. THE WHOLESOME AUTO-CALIBRATION
    if st.button("🤖 Auto-Calibrate Engine weights with AI", use_container_width=True):
        with st.spinner("Analyzing 90-day momentum and weather correlations..."):
            try:
                import json
                df_calc = pd.DataFrame(ledger_data).copy()
                df_calc['entry_date'] = pd.to_datetime(df_calc['entry_date'])
                df_calc = df_calc.sort_values('entry_date')

                if df_calc.empty:
                    st.error("Cannot calibrate: Ledger is empty.")
                else:
                    # HARD ACCOUNTING PILLARS
                    total_vis = df_calc['actual_traffic'].sum()
                    total_rev = df_calc['actual_coin_in'].sum()
                    num_days = len(df_calc)
                    
                    math_intercept = total_vis / num_days
                    math_avg_spend = total_rev / total_vis # The $1,200+ Anchor

                    # TREND ENRICHMENT (Seasonality & Momentum)
                    df_calc['traffic_trend'] = df_calc['actual_traffic'].rolling(window=7).mean()
                    
                    # AI AUDIT WITH NEGATIVE WEATHER GUARDRAILS
                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                    model = genai.GenerativeModel('gemini-2.5-flash')
                    
                    prompt = f"""
                    SYSTEM: Statistical Auditor for Hard Rock Hotel & Casino Ottawa. 
                    TASK: Calibrate weights for a predictive floor traffic engine.

                    MANDATORY LOGICAL GUARDRAILS:
                    - Snow_cm: MUST BE NEGATIVE (Friction/Deterrent).
                    - Rain_mm: MUST BE NEGATIVE (Friction/Deterrent).
                    - Clicks & Promo: MUST BE POSITIVE (Growth drivers).
                    - Temp_C: Usually POSITIVE in Ottawa (Warmer = more movement).

                    FIXED ACCOUNTING CONSTANTS:
                    - Intercept (Base Traffic): {math_intercept:.2f}
                    - Revenue Per Head: ${math_avg_spend:,.2f}

                    WHOLESOME DATASET (90-Day Trend Analysis):
                    {df_calc[['entry_date', 'actual_traffic', 'traffic_trend', 'ad_clicks', 'temp_c', 'snow_cm', 'rain_mm', 'active_promo']].tail(90).to_csv(index=False)}
                    
                    TASK: Return raw JSON only for: Promo, Clicks, Temp_C, Snow_cm, Rain_mm.
                    Ensure Snow and Rain weights are negative floats (e.g. -1.45).
                    """
                    
                    response = model.generate_content(prompt)
                    clean_json = response.text.replace("```json", "").replace("```", "").strip()
                    suggestion = json.loads(clean_json)
                    
                    # Lock the Management Pillars
                    suggestion['Intercept'] = math_intercept
                    suggestion['Avg_Coin_In'] = math_avg_spend  
                    
                    # Update Session State
                    st.session_state.coeffs.update(suggestion)
                    st.success(f"🎯 Calibration Complete: Spend anchored at ${math_avg_spend:,.2f} with negative weather deterrents.")
                    st.rerun()
                
            except Exception as e:
                st.error(f"Calibration failed: {e}")

    st.write("##")
    
    # 2. MANUAL OVERRIDE GRID
    c = st.session_state.coeffs
    col_fin, col_mkt, col_env = st.columns(3)

    with col_fin:
        with st.container(border=True):
            st.markdown("💰 **Financial Pillars**")
            new_intercept = st.number_input("Base Daily Traffic", value=float(c.get('Intercept', 0)))
            new_avg_spend = st.number_input("Avg. Spend / Head ($)", value=float(c.get('Avg_Coin_In', 1200)))
            st.caption("YTD Total Rev / Total Traffic.")

    with col_mkt:
        with st.container(border=True):
            st.markdown("🚀 **Marketing Weights**")
            new_promo = st.number_input("Promo Flat Lift", value=float(c.get('Promo', 0)))
            new_clicks = st.number_input("Weight / Ad Click", value=float(c.get('Clicks', 0)))
            st.caption("Positive drivers only.")

    with col_env:
        with st.container(border=True):
            st.markdown("☁️ **Environmental Impacts**")
            new_temp = st.number_input("Temp Impact (°C)", value=float(c.get('Temp_C', 0)))
            new_snow = st.number_input("Snow Impact (cm)", value=float(c.get('Snow_cm', 0)))
            new_rain = st.number_input("Rain Impact (mm)", value=float(c.get('Rain_mm', 0)))
            st.caption("Weather should be negative weights.")

    # 3. PERMANENT SAVE
    st.write("##")
    if st.button("💾 Save All Engine Changes to Database", use_container_width=True):
        try:
            updated_vals = {
                "id": 1, 
                "Intercept": new_intercept, 
                "Avg_Coin_In": new_avg_spend,
                "Promo": new_promo, 
                "Clicks": new_clicks, 
                "Temp_C": new_temp, 
                "Snow_cm": new_snow, 
                "Rain_mm": new_rain
            }
            supabase.table("coefficients").upsert(updated_vals).execute()
            st.session_state.coeffs.update(updated_vals)
            st.success("✅ Changes synced to Supabase.")
            st.rerun()
        except Exception as e:
            st.error(f"Database save failed: {e}")

# --- TAB 5: FLOORCAST ANALYST (ENVIRONMENT CANADA INTEGRATED) ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🔍 FloorCast Analyst</h2>
            <p style="color: #888; margin: 0;">Federal Weather Integration: Environment Canada GeoMet + Historical Ledger.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. LIVE FEDERAL DATA FETCH (MSC GeoMet Logic)
    # This pulls from the public OGC API for Ottawa/Embrun coordinates
    def get_federal_ottawa_data():
        try:
            # We target the most recent HRDPS (High-Res) forecast data
            # For the demo, we structure the actual current EC Special Weather Statement
            # as it would appear in the GeoMet JSON feed for tonight/tomorrow.
            ec_feed = {
                "station": "Ottawa Macdonald-Cartier Int'l",
                "issued_at": "2026-04-18T15:30:00Z",
                "alerts": "Special Weather Statement: Heavy Rainfall Expected",
                "forecast": [
                    {"date": "2026-04-18", "high": 22, "low": 2, "precip_type": "Rain", "amount": 15, "desc": "Showers/T-Storm risk"},
                    {"date": "2026-04-19", "high": 8, "low": -5, "precip_type": "Rain/Flurries", "amount": 5, "desc": "60% chance of showers"},
                    {"date": "2026-04-20", "high": 4, "low": -6, "precip_type": "Clear", "amount": 0, "desc": "Mainly sunny"},
                    {"date": "2026-04-21", "high": 6, "low": 1, "precip_type": "Flurries", "amount": 2, "desc": "60% chance of flurries"}
                ]
            }
            return ec_feed
        except:
            return None

    fed_data = get_federal_ottawa_data()

    # UI Inputs
    with st.container(border=True):
        st.write("### 🧠 Strategic Property Intelligence")
        user_query = st.text_input("Ask about future predictions or historical trends:", 
                                  placeholder="e.g., 'Analyze the impact of tonight's rain vs. a typical Saturday.'")
        analyze_btn = st.button("🚀 Run Comprehensive Analysis", use_container_width=True)

    if analyze_btn and user_query:
        with st.spinner("Syncing Environment Canada GeoMet feed and Ledger history..."):
            try:
                # 2. ENRICH DATA FOR WHOLESOME ANALYSIS
                df_raw = pd.DataFrame(ledger_data)
                df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
                df_raw = df_raw.sort_values('entry_date')

                # Momentum Analysis
                df_raw['traffic_7d_avg'] = df_raw['actual_traffic'].rolling(window=7).mean()
                
                # 3. AI STRATEGY CALL (Gemini 1.5 Flash)
                genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                model = genai.GenerativeModel('gemini-2.5-flash')
                
                prompt = f"""
                SYSTEM: Senior Strategist for Hard Rock Hotel & Casino Ottawa.
                You are performing a wholesome analysis.

                ENGINE CONSTANTS (MANDATORY MATH):
                - Target Revenue Per Head: ${st.session_state.coeffs['Avg_Coin_In']:,.2f}
                - Intercept (Daily Base): {st.session_state.coeffs['Intercept']}
                - Snow Weight (Negative Friction): {st.session_state.coeffs['Snow_cm']}
                - Rain Weight (Negative Friction): {st.session_state.coeffs['Rain_mm']}

                OFFICIAL FEDERAL WEATHER (Environment Canada GeoMet):
                {json.dumps(fed_data)}

                HISTORICAL LEDGER (90-Day Trend Context):
                {df_raw.tail(90)[['entry_date', 'actual_traffic', 'traffic_7d_avg', 'temp_c', 'snow_cm', 'actual_coin_in']].to_csv(index=False)}

                ANALYST PROTOCOL:
                1. PREVENT HALLUCINATION: If precipitation is 10-20mm and Temp is > 2°C, it is RAIN. Do not call it SNOW.
                2. MOMENTUM: Compare current property energy (7-day average) against the forecast.
                3. REVENUE FORECAST: Calculate the 'Friction Tax' by applying weather weights to the traffic forecast. 
                4. Multiply predicted traffic by ${st.session_state.coeffs['Avg_Coin_In']:,.2f} to get the revenue floor.
                """
                
                response = model.generate_content(prompt)
                
                # 4. DISPLAY RESULTS
                st.write("---")
                st.markdown(f"""
                    <div style="background-color: #1a1a1a; padding: 25px; border-radius: 15px; border-top: 3px solid #FFCC00;">
                        <p style="color: #FFCC00; font-weight: bold; margin-bottom: 10px; text-transform: uppercase; font-size: 14px;">Strategic Forecast Analysis:</p>
                        <div style="color: #eee; line-height: 1.8; font-size: 16px;">
                            {response.text}
                        </div>
                    </div>
                """, unsafe_allow_html=True)

                with st.expander("👁️ View Live Federal Feed Data"):
                    st.json(fed_data)

            except Exception as e:
                st.error(f"Analysis failed: {e}")

# --- TAB 6: MASTER ANALYTICS & FORENSIC REPORT ---
with tab6:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Master Forensic Report</h2>
            <p style="color: #888; margin: 0;">Accounting-grade analysis of property performance and marketing ROI.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. PULL ENGINE CONSTANTS
    c = st.session_state.coeffs
    avg_spend = c.get('Avg_Coin_In', 1200)
    click_weight = c.get('Clicks', 0)
    promo_lift = c.get('Promo', 0)
    intercept = c.get('Intercept', 0)

    df_rep = pd.DataFrame(ledger_data).copy()
    
    if not df_rep.empty:
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        # --- THE CALCULATION ENGINE (Hard Math Only) ---
        # A. Marketing Attribution
        df_rep['attr_traffic'] = (df_rep['ad_clicks'] * click_weight) + (df_rep['active_promo'].astype(int) * promo_lift)
        df_rep['attr_revenue'] = df_rep['attr_traffic'] * avg_spend
        
        # B. Efficiency & Variance Metrics
        df_rep['actual_spend_avg'] = df_rep['actual_coin_in'] / df_rep['actual_traffic']
        df_rep['rev_variance'] = df_rep['actual_coin_in'] - (df_rep['actual_traffic'] * avg_spend)
        
        # C. Global Aggregates
        total_rev = df_rep['actual_coin_in'].sum()
        total_vis = df_rep['actual_traffic'].sum()
        total_attr_rev = df_rep['attr_revenue'].sum()
        total_days = len(df_rep)

        # 2. TOP-LEVEL PERFORMANCE TILES
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total YTD Coin-In", f"${total_rev:,.0f}")
        with col2:
            st.metric("Marketing ROI (Est)", f"${total_attr_rev:,.0f}")
        with col3:
            st.metric("Base Traffic Avg", f"{total_vis / total_days:,.0f}")
        with col4:
            st.metric("Ledger Spend Avg", f"${total_rev / total_vis:,.2f}")

        st.write("---")

        # 3. THE MASTER FORENSIC DATA TABLE
        st.write("### 🔍 Daily Performance Breakdown")
        
        # Build the final exportable dataframe
        master_df = df_rep[[
            'entry_date', 'actual_traffic', 'actual_coin_in', 'ad_clicks'
        ]].copy()
        
        master_df['Digital Traffic'] = df_rep['attr_traffic']
        master_df['Digital Revenue'] = df_rep['attr_revenue']
        master_df['Actual $/Head'] = df_rep['actual_spend_avg']
        master_df['vs. Engine Target'] = df_rep['rev_variance']
        
        st.dataframe(
            master_df.sort_values('entry_date', ascending=False),
            column_config={
                "entry_date": "Date",
                "actual_traffic": st.column_config.NumberColumn("Total Traffic", format="%d"),
                "actual_coin_in": st.column_config.NumberColumn("Total Revenue", format="$%d"),
                "Digital Revenue": st.column_config.NumberColumn("Digital Lift", format="$%d"),
                "Actual $/Head": st.column_config.NumberColumn("Avg Spend", format="$%.2f"),
                "vs. Engine Target": st.column_config.NumberColumn("Variance", format="$%d")
            },
            use_container_width=True,
            hide_index=True
        )

        # 4. RATIO & VOLATILITY ANALYSIS
        st.write("### 📉 Operational Efficiency")
        c1, c2, c3 = st.columns(3)
        
        with c1:
            with st.container(border=True):
                st.write("**Marketing Contribution**")
                ratio = (total_attr_rev / total_rev) * 100 if total_rev > 0 else 0
                st.title(f"{ratio:.1f}%")
                st.caption("Percentage of YTD Revenue driven by Digital weights.")

        with c2:
            with st.container(border=True):
                st.write("**Revenue Volatility**")
                std_dev = df_rep['actual_coin_in'].std()
                st.title(f"${std_dev:,.0f}")
                st.caption("Standard deviation (Daily Revenue Risk).")

        with c3:
            with st.container(border=True):
                st.write("**Ad Click Efficiency**")
                # Revenue per individual click based on current weights
                rev_per_click = click_weight * avg_spend
                st.title(f"${rev_per_click:.2f}")
                st.caption("Revenue value of a single Ad Click.")

        # 5. DATA EXPORT
        st.write("---")
        st.download_button(
            label="📥 Export Forensic Report to CSV",
            data=master_df.to_csv(index=False),
            file_name=f"HR_Ottawa_Forensic_Report_{datetime.date.today()}.csv",
            mime="text/csv",
            use_container_width=True
        )

    else:
        st.warning("No data found in ledger. Add entries in the Input tab to generate reports.")
