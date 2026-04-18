import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
import google.generativeai as genai
from sklearn.linear_model import LinearRegression
import numpy as np
import json

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
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🏛️ Executive Property Overview</h2>
            <p style="color: #888; margin: 0;">YTD Performance & Brand Equity Value.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. Pull Engine Constants
    c = st.session_state.coeffs
    avg_spend = c.get('Avg_Coin_In', 1200)
    click_weight = c.get('Clicks', 0)
    imps_weight = c.get('Impressions', 0) 
    promo_lift = c.get('Promo', 0)
    intercept = c.get('Intercept', 0)

    # 2. Safety Prep
    df_exec = pd.DataFrame(ledger_data)
    
    if not df_exec.empty:
        # Safety Column Injection
        for col in ['temp_c', 'snow_cm', 'rain_mm', 'ad_clicks', 'impressions', 'active_promo']:
            if col not in df_exec.columns: df_exec[col] = 0.0
        
        df_exec['entry_date'] = pd.to_datetime(df_exec['entry_date'])
        
        # Calculations
        df_exec['direct_lift'] = df_exec['ad_clicks'] * click_weight
        df_exec['brand_lift'] = (df_exec['impressions'] / 1000) * imps_weight
        df_exec['total_digital_lift'] = df_exec['direct_lift'] + df_exec['brand_lift'] + (df_exec['active_promo'].astype(int) * promo_lift)
        df_exec['brand_rev_ytd'] = df_exec['brand_lift'] * avg_spend
        df_exec['total_digital_rev'] = df_exec['total_digital_lift'] * avg_spend

        # AI Predictability
        df_exec['expected_traffic'] = intercept + df_exec['total_digital_lift'] + (df_exec['temp_c'] * c.get('Temp_C', 0))
        df_exec['error'] = abs(df_exec['actual_traffic'] - df_exec['expected_traffic']) / df_exec['actual_traffic']
        accuracy_score = max(0, (1 - df_exec['error'].mean()) * 100)

        # 3. UI BENTO CARDS
        k1, k2 = st.columns(2)
        with k1:
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-top: 5px solid #FFCC00; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase;">Total YTD Revenue</p>
                <h1 style="color: #FFF; margin: 0;">${df_exec['actual_coin_in'].sum():,.0f}</h1>
            </div>""", unsafe_allow_html=True)
        with k2:
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-top: 5px solid #FFCC00; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase;">Total Digital ROI</p>
                <h1 style="color: #FFF; margin: 0;">${df_exec['total_digital_rev'].sum():,.0f}</h1>
            </div>""", unsafe_allow_html=True)

        st.write("##")
        k3, k4 = st.columns(2)
        with k3:
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-left: 10px solid #FFCC00; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase;">Brand Equity Value</p>
                <h1 style="color: #FFCC00; margin: 0;">${df_exec['brand_rev_ytd'].sum():,.0f}</h1>
                <p style="color: #888; font-size: 11px; margin-top:5px;">Revenue from Impression Lift</p>
            </div>""", unsafe_allow_html=True)
        with k4:
            score_color = "#00FF00" if accuracy_score > 85 else "#FFCC00"
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-left: 10px solid {score_color}; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase;">Predictability</p>
                <h1 style="color: {score_color}; margin: 0;">{accuracy_score:.1f}%</h1>
                <p style="color: #888; font-size: 11px; margin-top:5px;">Model Confidence</p>
            </div>""", unsafe_allow_html=True)
    else:
        st.info("Awaiting ledger data to populate executive overview.")
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

# --- TAB 4: ADMIN ENGINE & DATA MANAGEMENT ---
with tab4:
    # 1. BRANDED HEADER
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Control & Data Management</h2>
            <p style="color: #888; margin: 0;">Accounting-Verified YTD Calibration (Revenue ÷ Traffic).</p>
        </div>
    """, unsafe_allow_html=True)

    # 2. THE ACCOUNTING-FIRST AUTO-CALIBRATION
    if st.button("🤖 Auto-Calibrate Engine weights with AI", use_container_width=True):
        with st.spinner("Calculating YTD Financial Reality..."):
            try:
                import json
                # Load the full ledger context
                df_calc = pd.DataFrame(ledger_data).copy()
                
                if df_calc.empty:
                    st.error("Cannot calibrate: Ledger is empty.")
                else:
                    # --- THE HARD ACCOUNTING MATH ---
                    total_ytd_vis = df_calc['actual_traffic'].sum()
                    total_ytd_rev = df_calc['actual_coin_in'].sum()
                    num_days = len(df_calc)

                    # Pillar 1: Base Traffic = Total YTD Traffic / Total Days
                    math_intercept = total_ytd_vis / num_days if num_days > 0 else 0
                    
                    # Pillar 2: Avg Spend = Total YTD Revenue / Total YTD Traffic
                    # This ensures the $1,200+ reality is the anchor of the model
                    math_avg_spend = total_ytd_rev / total_ytd_vis if total_ytd_vis > 0 else 0

                    # --- THE AI VARIANCE ENGINE ---
                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    
                    prompt = f"""
                    SYSTEM: Statistical Auditor for Hard Rock Ottawa. 
                    FIXED PILLARS: Base_Traffic={math_intercept:.2f}, Avg_Spend_Per_Head={math_avg_spend:.2f}.
                    DATA: {df_calc.tail(150).to_csv(index=False)}
                    
                    TASK: Given these FIXED accounting pillars, calculate the optimal weights for the following 
                    variables to minimize prediction error against 'actual_traffic':
                    - Promo, Clicks, Impressions, Temp_C, Snow_cm, Rain_mm.
                    
                    RETURN: A single raw JSON object. NO MARKDOWN.
                    """
                    
                    response = model.generate_content(prompt)
                    clean_json = response.text.replace("```json", "").replace("```", "").strip()
                    suggestion = json.loads(clean_json)
                    
                    # --- ENFORCE THE ACCOUNTING PILLARS ---
                    suggestion['Intercept'] = math_intercept
                    suggestion['Avg_Coin_In'] = math_avg_spend  
                    
                    # Update the live session state
                    st.session_state.coeffs.update(suggestion)
                    st.success(f"🎯 YTD Reality Locked: Average Spend is ${math_avg_spend:,.2f}")
                
            except Exception as e:
                st.error(f"Calibration failed: {e}")

    st.write("##")
    
    # 3. BENTO CONTROL CENTER (Review & Manual Overrides)
    # Helper to prevent crashes if values are None
    def safe_float(val):
        try: return float(val) if val is not None else 0.0
        except: return 0.0

    c = st.session_state.coeffs
    col_fin, col_dig, col_env = st.columns(3)

    with col_fin:
        with st.container(border=True):
            st.markdown("💰 **Financial & Baseline**")
            new_intercept = st.number_input("Base Daily Traffic", value=safe_float(c.get('Intercept', 0)))
            new_avg_spend = st.number_input("Avg. Spend per Head ($)", value=safe_float(c.get('Avg_Coin_In', 0)))
            st.caption("Locked to: Total YTD Revenue / Total YTD Traffic.")

    with col_dig:
        with st.container(border=True):
            st.markdown("🚀 **Digital Marketing Weights**")
            new_promo = st.number_input("Promo Flat Lift", value=safe_float(c.get('Promo', 0)))
            new_clicks = st.number_input("Weight / Ad Click", value=safe_float(c.get('Clicks', 0)))
            new_imps = st.number_input("Weight / 1k Imps", value=safe_float(c.get('Impressions', 0)), format="%.4f")
            st.caption("Marketing ROI multipliers.")

    with col_env:
        with st.container(border=True):
            st.markdown("☁️ **Environmental Impact**")
            new_temp = st.number_input("Temp Impact (°C)", value=safe_float(c.get('Temp_C', 0)))
            new_snow = st.number_input("Snow Impact (cm)", value=safe_float(c.get('Snow_cm', 0)))
            new_rain = st.number_input("Rain Impact (mm)", value=safe_float(c.get('Rain_mm', 0)))
            st.caption("Ottawa weather adjustments.")

    # 4. PERMANENT DATABASE SYNC
    st.write("##")
    if st.button("💾 Save All Engine Changes", use_container_width=True):
        try:
            updated_values = {
                "id": 1, 
                "Intercept": new_intercept, 
                "Temp_C": new_temp, 
                "Snow_cm": new_snow, 
                "Rain_mm": new_rain, 
                "Promo": new_promo, 
                "Clicks": new_clicks, 
                "Impressions": new_imps, 
                "Avg_Coin_In": new_avg_spend
            }
            # Permanent write to Supabase
            supabase.table("coefficients").upsert(updated_values).execute()
            st.session_state.coeffs.update(updated_values)
            st.success("✅ Engine Tuned: All changes are now permanent in the database.")
            st.rerun()
        except Exception as e:
            st.error(f"Database save failed: {e}")

    # 5. MAINTENANCE
    st.write("---")
    if st.button("🚀 Force Global Promo: TRUE", use_container_width=True):
        try:
            supabase.table("ledger").update({"active_promo": True}).neq("active_promo", True).execute()
            st.success("Ledger Synchronized: All records reflect active promotion.")
            st.rerun()
        except Exception as e:
            st.error(f"Global sync failed: {e}")

# --- TAB 5: FloorCast Analyst ---
with tab5:
    # 1. BRANDED HEADER
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🔍 FloorCast Analyst</h2>
            <p style="color: #888; margin: 0;">Natural language insights based on your property ledger and digital weights.</p>
        </div>
    """, unsafe_allow_html=True)

    # 2. CHAT INTERFACE CONTAINER
    with st.container(border=True):
        st.write("### FloorCast AI")
        
        # FIX: The User Input box
        user_query = st.text_input("Ask about traffic trends, high-revenue days, or ROI:", 
                                  placeholder="e.g., 'What was our highest traffic day in the last 6 months?'")
        
        col_btn, col_spacer = st.columns([1, 2])
        with col_btn:
            analyze_button = st.button("🚀 Ask FloorCast", use_container_width=True)

    # 3. ANALYSIS LOGIC
    if analyze_button and user_query:
        with st.spinner("Consulting the ledger and calculating variances..."):
            try:
                # A. PREPARE THE DATA (The "Hard Math" Context)
                df_full = pd.DataFrame(ledger_data)
                
                # We give the AI the top 20 "Best" days and the 30 "Latest" days
                df_highlights = df_full.sort_values('actual_traffic', ascending=False).head(20)
                df_recent = df_full.tail(30)
                context_df = pd.concat([df_highlights, df_recent]).drop_duplicates()
                
                # B. CALL GEMINI
                genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                model = genai.GenerativeModel('gemini-2.5-flash')
                
                prompt = f"""
                SYSTEM: You are the Lead Data Analyst for Hard Rock Hotel & Casino Ottawa.
                PROPERTY COEFFICIENTS: {st.session_state.coeffs}
                
                LEDGER CONTEXT (Top Performers & Recent Records):
                {context_df.to_csv(index=False)}
                
                QUESTION: {user_query}
                
                INSTRUCTION: Use the actual_traffic and actual_coin_in columns to provide 
                specific, data-backed answers. If a user asks for the 'highest' or 'best' day, 
                scan the provided LEDGER CONTEXT for the maximum value.
                """
                
                response = model.generate_content(prompt)
                
                # C. DISPLAY STYLED RESPONSE
                st.write("---")
                st.markdown(f"""
                    <div style="background-color: #1a1a1a; padding: 20px; border-radius: 10px; border-top: 3px solid #FFCC00;">
                        <p style="color: #FFCC00; font-weight: bold; margin-bottom: 10px;">ANALYSIS RESULT:</p>
                        <div style="color: #eee; line-height: 1.6;">
                            {response.text}
                        </div>
                    </div>
                """, unsafe_allow_html=True)

            except Exception as e:
                st.error(f"Analyst Error: {e}")

# --- TAB 6: MASTER FORENSIC REPORT ---
with tab6:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Master Forensic Report</h2>
            <p style="color: #888; margin: 0;">Detailed attribution including Direct vs. Brand Lift.</p>
        </div>
    """, unsafe_allow_html=True)

    df_rep = pd.DataFrame(ledger_data).copy()
    
    if not df_rep.empty:
        # Safety Column Injection
        for col in ['temp_c', 'snow_cm', 'rain_mm', 'ad_clicks', 'impressions', 'active_promo']:
            if col not in df_rep.columns: df_rep[col] = 0.0

        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        # Attribution Calculations
        df_rep['direct_lift'] = df_rep['ad_clicks'] * click_weight
        df_rep['brand_lift'] = (df_rep['impressions'] / 1000) * imps_weight
        df_rep['total_lift'] = df_rep['direct_lift'] + df_rep['brand_lift'] + (df_rep['active_promo'].astype(int) * promo_lift)
        df_rep['lift_rev'] = df_rep['total_lift'] * avg_spend
        df_rep['actual_avg'] = df_rep['actual_coin_in'] / df_rep['actual_traffic']

        # Table Display
        st.dataframe(
            df_rep.sort_values('entry_date', ascending=False),
            column_config={
                "entry_date": "Date",
                "actual_traffic": "Total Traffic",
                "actual_coin_in": st.column_config.NumberColumn("Actual Revenue", format="$%d"),
                "direct_lift": st.column_config.NumberColumn("Click Lift", format="%.1f"),
                "brand_lift": st.column_config.NumberColumn("Brand Lift", format="%.1f"),
                "lift_rev": st.column_config.NumberColumn("Digital ROI", format="$%d"),
                "actual_avg": st.column_config.NumberColumn("Actual $/Head", format="$%.2f")
            },
            use_container_width=True, hide_index=True
        )

        # Efficiency Metrics
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("YTD Rev", f"${df_rep['actual_coin_in'].sum():,.0f}")
        with c2: 
            brand_pct = (df_rep['brand_lift'].sum() / df_rep['total_lift'].sum()) * 100 if df_rep['total_lift'].sum() > 0 else 0
            st.metric("Brand Equity Share", f"{brand_pct:.1f}%")
        with c3: 
            ytd_avg = df_rep['actual_coin_in'].sum() / df_rep['actual_traffic'].sum() if df_rep['actual_traffic'].sum() > 0 else 0
            st.metric("YTD Ledger Avg", f"${ytd_avg:,.2f}")

        # Download
        st.download_button("📥 Export Forensic Data", df_rep.to_csv(index=False), "HR_Ottawa_Forensic.csv", "text/csv", use_container_width=True)
    else:
        st.info("No ledger records found to analyze.")
