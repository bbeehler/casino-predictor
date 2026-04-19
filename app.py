import streamlit as st
import pandas as pd
import datetime
import json
import asyncio
from env_canada import ECWeather
import google.generativeai as genai
from supabase import create_client

# --- CORE WEATHER FUNCTION ---
async def fetch_live_ec_data():
    """Fetches real-time data from Environment Canada"""
    try:
        # Hard Rock Ottawa / Embrun Coordinates
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {
            "current": ec.conditions,
            "forecast": ec.daily_forecasts,
            "alerts": ec.alerts
        }
    except Exception as e:
        return {"error": str(e)}

# --- APP INITIALIZATION ---
if 'weather_data' not in st.session_state:
    # Run the weather fetcher once so it's ready for Tab 5
    st.session_state.weather_data = asyncio.run(fetch_live_ec_data())

live_data = st.session_state.weather_data

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

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 Executive Overview", 
    "📑 Ledger Management", 
    "📊 Property Analytics", 
    "⚙️ Engine Control", 
    "🧠 FloorCast Analyst",
    "📋 Master Report",
    "🧪 Forecast Sandbox"
])
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

# --- TAB 4: ULTIMATE CONSOLIDATED ENGINE ---
with tab4:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Control</h2>
            <p style="color: #888; margin: 0;">Global Anchoring: Heartbeat, Weather Guardrails, and Social Conversion Standards.</p>
        </div>
    """, unsafe_allow_html=True)

    # Use the FULL ledger for coefficients
    df_global = pd.DataFrame(ledger_data).copy()
    
    # 1. DATA INTEGRITY SHIELD
    # Include every variable we've built: Weather, Marketing, and Social
    social_cols = ['social_impressions', 'social_engagement']
    numeric_cols = ['actual_traffic', 'snow_cm', 'rain_mm', 'active_promo', 'temp_c', 'ad_clicks', 'actual_coin_in'] + social_cols
    
    for col in numeric_cols:
        if col in df_global.columns:
            df_global[col] = pd.to_numeric(df_global[col], errors='coerce').fillna(0.0)
        else:
            df_global[col] = 0.0 # Create column with 0 if missing from CSV

    ledger_signature = hash(pd.util.hash_pandas_object(df_global).sum())

    if st.button("🤖 Auto-Calibrate Engine weights with AI", use_container_width=True):
        if st.session_state.get('last_calib_hash') == ledger_signature:
            st.info("⚖️ **Weights Locked**: Global history is unchanged.")
        else:
            with st.spinner("Executing Global Forensic Calibration..."):
                try:
                    from sklearn.linear_model import Ridge
                    
                    # STEP 1: SEGMENTED HEARTBEAT (60-Day DOW Averages)
                    df_global['entry_date'] = pd.to_datetime(df_global['entry_date'])
                    df_global['day_name'] = df_global['entry_date'].dt.day_name()
                    dow_profiles = df_global.groupby('day_name')['actual_traffic'].mean().to_dict()
                    df_global['residual'] = df_global.apply(lambda x: x['actual_traffic'] - dow_profiles[x['day_name']], axis=1)

                    # STEP 2: MULTI-CHANNEL REGRESSION
                    features = ['ad_clicks', 'temp_c', 'snow_cm', 'rain_mm', 'active_promo', 'social_impressions', 'social_engagement']
                    X = df_global[features]
                    y = df_global['residual']

                    model = Ridge(alpha=0.1) # Sensitive to rare events (like snow)
                    model.fit(X, y)
                    raw_weights = dict(zip(features, model.coef_))

                    # STEP 3: INDUSTRY & OTTAWA GUARDRAILS (Permanent Memory)
                    avg_traffic = float(df_global['actual_traffic'].mean())
                    
                    # Hard-coded Floors (Industry Standards)
                    promo_floor = avg_traffic * 0.05
                    imp_floor   = 0.0002 # 2 guests per 10k impressions
                    eng_floor   = 0.0100 # 1 guest per 100 engagements
                    snow_floor  = -4.00   # 4 guests lost per cm
                    rain_floor  = -2.00   # 2 guests lost per mm

                    final_weights = {
                        "Intercept": avg_traffic, 
                        "Avg_Coin_In": float(df_global['actual_coin_in'].sum() / df_global['actual_traffic'].sum()) if df_global['actual_traffic'].sum() > 0 else 1200.0,
                        "Clicks": max(0.001, float(raw_weights.get('ad_clicks', 0))),
                        "Promo": max(float(raw_weights.get('active_promo', 0)), promo_floor),
                        "Social_Imp": max(float(raw_weights.get('social_impressions', 0)), imp_floor),
                        "Social_Eng": max(float(raw_weights.get('social_engagement', 0)), eng_floor),
                        "Temp_C": float(raw_weights.get('temp_c', 0)),
                        "Snow_cm": min(-abs(float(raw_weights.get('snow_cm', 0))), snow_floor),
                        "Rain_mm": min(-abs(float(raw_weights.get('rain_mm', 0))), rain_floor)
                    }

                    st.session_state.coeffs.update(final_weights)
                    st.session_state.last_calib_hash = ledger_signature
                    st.success("🎯 Global Calibration Complete: Weather & Social Integrated.")
                    st.rerun()

                except Exception as e:
                    st.error(f"Calibration Error: {e}")

    # --- 2. LIVE COEFFICIENT MONITOR ---
    st.write("### 📊 Active Engine Coefficients")
    c = st.session_state.coeffs
    d_coeffs = {k: v for k, v in c.items() if k not in ['id', 'created_at']}
    
    # Use metrics for high-level visibility
    r1, r2 = st.columns(2), st.columns(3)
    # Financials
    r1[0].metric("Base Daily Traffic", f"{float(d_coeffs.get('Intercept',0)):.2f}")
    r1[1].metric("Avg Spend", f"${float(d_coeffs.get('Avg_Coin_In',0)):.2f}")
    
    # Friction/Lift Details (Second Row)
    st.write("**Friction & Lift Factors**")
    f_cols = st.columns(len(d_coeffs) - 2)
    friction_keys = [k for k in d_coeffs if k not in ['Intercept', 'Avg_Coin_In']]
    for i, k in enumerate(friction_keys):
        f_cols[i].metric(label=k, value=f"{float(d_coeffs.get(k,0)):.4f}")

    st.write("---")

    # --- 3. MANUAL OVERRIDE & DB SYNC ---
    col_fin, col_mkt, col_env = st.columns(3)

    with col_fin:
        with st.container(border=True):
            st.write("**💰 Financials**")
            new_intercept = st.number_input("Base Daily Traffic", value=float(c.get('Intercept', 0)))
            new_avg_spend = st.number_input("Spend / Head ($)", value=float(c.get('Avg_Coin_In', 1200)))

    with col_mkt:
        with st.container(border=True):
            st.write("**🚀 Marketing & Social**")
            new_promo = st.number_input("Promo Flat Lift", value=float(c.get('Promo', 0)))
            new_imp = st.number_input("Weight / Impression", value=float(c.get('Social_Imp', 0.0002)), format="%.4f")
            new_eng = st.number_input("Weight / Engagement", value=float(c.get('Social_Eng', 0.0100)), format="%.4f")

    with col_env:
        with st.container(border=True):
            st.write("**☁️ Environment**")
            new_temp = st.number_input("Temp Weight", value=float(c.get('Temp_C', 0)), format="%.4f")
            new_snow = st.number_input("Snow Weight (cm)", value=float(c.get('Snow_cm', 0)), format="%.4f")
            new_rain = st.number_input("Rain Weight (mm)", value=float(c.get('Rain_mm', 0)), format="%.4f")

    if st.button("💾 Save All Engine Changes to Database", use_container_width=True):
        try:
            # We must include the NEW Social keys in the Supabase update
            updated_vals = {
                "id": 1, 
                "Intercept": new_intercept, "Avg_Coin_In": new_avg_spend,
                "Promo": new_promo, "Social_Imp": new_imp, "Social_Eng": new_eng,
                "Temp_C": new_temp, "Snow_cm": new_snow, "Rain_mm": new_rain
            }
            supabase.table("coefficients").upsert(updated_vals).execute()
            st.session_state.coeffs.update(updated_vals)
            st.success("✅ Engine settings synced to Database.")
        except Exception as e:
            st.error(f"Sync failed: {e}")

# --- TAB 5: DYNAMIC "DATE-AWARE" ANALYST ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🔍 FloorCast Analyst</h2>
            <p style="color: #888; margin: 0;">Date-Specific Intelligence: Scanning Federal Forecasts vs. 60-Day Historical Baselines.</p>
        </div>
    """, unsafe_allow_html=True)

    # Access the live federal data from the top of app.py
    live_data = st.session_state.get('weather_data', {})

    with st.container(border=True):
        st.write("### 🧠 Strategic Intelligence Query")
        user_query = st.text_input("Ask for a prediction (e.g., 'Monday April 20' or 'Next Friday'):", 
                                  placeholder="Predict revenue for Monday April 20 based on the forecast.")
        analyze_btn = st.button("🚀 Run Date-Specific Analysis", use_container_width=True)

    if analyze_btn and user_query:
        if not live_data or "error" in live_data:
            st.error("Cannot run analysis: Federal weather feed is currently unavailable.")
        else:
            with st.spinner("Scanning historical ledger and future weather layers..."):
                try:
                    # 1. 60-DAY HEARTBEAT & LEDGER CONTEXT
                    df_raw = pd.DataFrame(ledger_data)
                    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
                    df_raw['day_name'] = df_raw['entry_date'].dt.day_name()
                    
                    sixty_days_ago = datetime.datetime.now() - datetime.timedelta(days=60)
                    df_60 = df_raw[df_raw['entry_date'] >= sixty_days_ago].copy()

                    # Calculate Averages for EVERY day of the week
                    dow_stats = df_60.groupby('day_name')['actual_traffic'].mean().to_dict()

                    # 2. AI TRIANGULATION (GEMINI 2.5 FLASH)
                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                    model = genai.GenerativeModel('gemini-2.5-flash')
                    
                    prompt = f"""
                    SYSTEM: Senior Strategist for Hard Rock Hotel & Casino Ottawa. 
                    Your task is to provide a SPOT ON prediction for a SPECIFIC DATE.

                    USER QUERY: "{user_query}"

                    DATASET 1: LIVE ENVIRONMENT CANADA FORECAST (7-DAY)
                    {json.dumps(live_data.get('forecast', []), default=str)}

                    DATASET 2: 60-DAY WEEKLY HEARTBEAT (Averages)
                    {json.dumps(dow_stats, default=str)}

                    DATASET 3: ENGINE COEFFICIENTS
                    - Spend Anchor: ${st.session_state.coeffs['Avg_Coin_In']:,.2f}
                    - Snow Friction: {st.session_state.coeffs['Snow_cm']}
                    - Rain Friction: {st.session_state.coeffs['Rain_mm']}

                    ANALYSIS STEPS:
                    1. IDENTIFY TARGET: Scan the user query for a specific day/date (e.g. Monday).
                    2. EXTRACT WEATHER: Find the exact weather for that day in the Environment Canada feed. 
                    3. CALCULATE BASE: Start with the 60-day average for that specific day of the week.
                    4. APPLY FRICTION: Subtract for rain or snow based on the weights.
                    5. REVENUE: Predicted Traffic * ${st.session_state.coeffs['Avg_Coin_In']:,.2f}.
                    6. EXPLAIN: Clearly state 'Based on the average Monday over the last 60 days ({dow_stats.get('Monday', 0):,.0f} guests)...'
                    """
                    
                    response = model.generate_content(prompt)
                    
                    st.write("---")
                    st.markdown(f"""
                        <div style="background-color: #1a1a1a; padding: 25px; border-radius: 15px; border-top: 3px solid #FFCC00;">
                            <p style="color: #FFCC00; font-weight: bold; text-transform: uppercase; font-size: 14px;">Strategic Forecast Response:</p>
                            <div style="color: #eee; line-height: 1.8; font-size: 16px;">
                                {response.text}
                            </div>
                        </div>
                    """, unsafe_allow_html=True)

                except Exception as e:
                    st.error(f"Analysis Error: {e}")

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

        # --- TAB 7: FORECAST SANDBOX ---
with tab7:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧪 Forecast Sandbox</h2>
            <p style="color: #888; margin: 0;">Manual Scenario Simulator: Use the sliders to stress-test your strategy.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. DATE SELECTION
    # This identifies the "Heartbeat" (DOW Average) for the simulation
    target_date = st.date_input("Target Simulation Date:", datetime.date(2026, 4, 20))
    target_day = target_date.strftime("%A")

    # Pull the 60-day DOW Average from the Ledger
    df_sb = pd.DataFrame(ledger_data)
    df_sb['entry_date'] = pd.to_datetime(df_sb['entry_date'])
    df_sb['day_name'] = df_sb['entry_date'].dt.day_name()
    dow_avg = df_sb[df_sb['day_name'] == target_day]['actual_traffic'].mean() if not df_sb.empty else 1000

    # 2. SCENARIO INPUTS
    st.write(f"### 🎛️ Adjust Factors for {target_day}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Marketing & Social**")
        s_promo = st.checkbox("Active Promotion?", value=False)
        s_clicks = st.number_input("Ad Clicks", value=500)
        s_imp = st.number_input("Social Impressions", value=10000)
        s_eng = st.number_input("Social Engagement", value=500)
    
    with col2:
        st.write("**Environment**")
        s_temp = st.slider("Temperature (°C)", -30, 40, 15)
        s_rain = st.slider("Rainfall (mm)", 0, 50, 0)
        s_snow = st.slider("Snowfall (cm)", 0, 50, 0)

    with col3:
        st.write("**Baseline Anchor**")
        # This defaults to the 60-day average we calculated above
        s_base = st.number_input("Starting Traffic (DOW Avg)", value=int(dow_avg))
        st.info(f"The 60-day average for {target_day} is {int(dow_avg)} guests.")

    # 3. THE UNIFIED CALCULATION
    # This uses the exact same coefficients from Tab 4
    c = st.session_state.coeffs
    
    promo_lift = c['Promo'] if s_promo else 0
    social_lift = (s_imp * c.get('Social_Imp', 0.0002)) + (s_eng * c.get('Social_Eng', 0.01))
    marketing_lift = (s_clicks * c.get('Clicks', 0.001))
    weather_friction = (s_temp * c.get('Temp_C', 0)) + (s_rain * c.get('Rain_mm', -2.0)) + (s_snow * c.get('Snow_cm', -4.0))
    
    # Final Math
    total_traffic = s_base + promo_lift + social_lift + marketing_lift + weather_friction
    total_revenue = total_traffic * c.get('Avg_Coin_In', 1200.0)

    # 4. RESULTS DISPLAY
    st.write("---")
    res1, res2, res3 = st.columns(3)
    res1.metric("Predicted Traffic", f"{int(total_traffic)} Guests")
    res2.metric("Predicted Revenue", f"${total_revenue:,.2f}")
    res3.metric("Revenue Variance", f"{((total_revenue/(dow_avg * c['Avg_Coin_In']))-1)*100:.1f}% vs. Avg")

    st.markdown(f"""
        <div style="background-color: #1a1a1a; padding: 15px; border-radius: 10px; border-left: 3px solid #FFCC00;">
            <p style="color: #888; font-size: 13px; margin: 0;">
                <b>Scenario Note:</b> This simulation uses your <b>Global Anchor Weights</b>. 
                Any change here should reflect the strategic output provided by the AI Analyst in Tab 5.
            </p>
        </div>
    """, unsafe_allow_html=True)
