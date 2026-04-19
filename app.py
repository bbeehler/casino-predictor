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

# --- REPLACEMENT NAV BLOCK ---
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 Executive Overview", 
    "📑 Ledger Management", 
    "📊 Property Analytics", 
    "⚙️ Engine Control", 
    "🧠 FloorCast Analyst",
    "📋 Master Report",
    "🧪 Forecast Sandbox"
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
# --- TAB 2: LEDGER MANAGEMENT (RESTYLED & FUNCTIONAL) ---
with tab2:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📑 Ledger Management</h2>
            <p style="color: #888; margin: 0;">Data Integrity & History: Manage your historical property performance records.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. TOP ACTIONS: BULK IMPORT & MANUAL ENTRY
    col_upload, col_entry = st.columns([1, 1], gap="large")

    with col_upload:
        with st.container(border=True):
            st.write("### 📤 Bulk Import")
            st.info("Upload your historical CSV. Ensure columns match the standard ledger schema.")
            uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
            if uploaded_file:
                try:
                    import_df = pd.read_csv(uploaded_file)
                    # Force numeric conversion for known columns during import
                    num_cols = ['actual_traffic', 'actual_coin_in', 'social_impressions', 'social_engagement', 'snow_cm', 'rain_mm']
                    for col in num_cols:
                        if col in import_df.columns:
                            import_df[col] = pd.to_numeric(import_df[col], errors='coerce').fillna(0)
                    
                    # Convert to list of dicts for Supabase
                    data_to_insert = import_df.to_dict(orient='records')
                    supabase.table("ledger_data").insert(data_to_insert).execute()
                    st.success(f"Successfully imported {len(import_df)} records!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Import failed: {e}")

    with col_entry:
        with st.container(border=True):
            st.write("### ✍️ Manual Entry")
            st.write("Add a single day's performance directly to the database.")
            
            # THE FORM: Now inside a popover for a clean restyled UI
            with st.popover("➕ Open Entry Form", use_container_width=True):
                with st.form("manual_entry_form", clear_on_submit=True):
                    st.write("#### Daily Performance Details")
                    
                    f_date = st.date_input("Entry Date", datetime.date.today())
                    f_traffic = st.number_input("Actual Traffic (Heads)", min_value=0, step=1)
                    f_coin = st.number_input("Actual Coin-In ($)", min_value=0.0, step=100.0)
                    
                    st.divider()
                    st.write("#### Marketing & Social")
                    m_col1, m_col2 = st.columns(2)
                    f_promo = m_col1.checkbox("Active Promotion?")
                    f_clicks = m_col2.number_input("Ad Clicks", min_value=0, step=1)
                    f_imp = m_col1.number_input("Social Impressions", min_value=0, step=100)
                    f_eng = m_col2.number_input("Social Engagement", min_value=0, step=10)
                    
                    st.divider()
                    st.write("#### Weather Data")
                    w_col1, w_col2, w_col3 = st.columns(3)
                    f_temp = w_col1.number_input("Temp (°C)", value=15.0)
                    f_rain = w_col2.number_input("Rain (mm)", min_value=0.0)
                    f_snow = w_col3.number_input("Snow (cm)", min_value=0.0)
                    
                    submitted = st.form_submit_button("🚀 Commit to Ledger", use_container_width=True)
                    
                    if submitted:
                        try:
                            new_entry = {
                                "entry_date": str(f_date),
                                "actual_traffic": f_traffic,
                                "actual_coin_in": f_coin,
                                "active_promo": f_promo,
                                "ad_clicks": f_clicks,
                                "social_impressions": f_imp,
                                "social_engagement": f_eng,
                                "temp_c": f_temp,
                                "rain_mm": f_rain,
                                "snow_cm": f_snow
                            }
                            supabase.table("ledger_data").insert(new_entry).execute()
                            st.success("Entry Saved!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Save failed: {e}")

    st.write("---")

    # 2. DATA EXPLORER (The Ledger View)
    st.write("### 🔍 Historical Records Explorer")
    
    if ledger_data:
        df_display = pd.DataFrame(ledger_data).copy()
        
        # Ensure proper sorting by date
        df_display['entry_date'] = pd.to_datetime(df_display['entry_date'])
        df_display = df_display.sort_values(by='entry_date', ascending=False)
        
        # Premium Styled Dataframe
        st.dataframe(
            df_display.style.format({
                "actual_traffic": "{:,.0f}",
                "actual_coin_in": "${:,.2f}",
                "social_impressions": "{:,.0f}",
                "social_engagement": "{:,.0f}",
                "snow_cm": "{:.1f} cm",
                "rain_mm": "{:.1f} mm",
                "temp_c": "{:.1f}°C"
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Export Option
        csv_data = df_display.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Export Ledger to CSV",
            data=csv_data,
            file_name=f'Hard_Rock_Ledger_Export_{datetime.date.today()}.csv',
            mime='text/csv',
            use_container_width=True
        )
    else:
        st.warning("Ledger is empty. Please upload data or use the Manual Entry form.")

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

# --- TAB 5: STRATEGIC CONSULTANT (FINAL STABILIZED) ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧠 Strategic Consultant</h2>
            <p style="color: #888; margin: 0;">Full-Spectrum Intelligence: Ledger History, Live Federal Feeds, and Global Industry Benchmarks.</p>
        </div>
    """, unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 1. THE DATA VAULT (Pre-processing with safety)
    vault_metrics = {}
    if ledger_data:
        df_vault = pd.DataFrame(ledger_data).copy()
        df_vault['entry_date'] = pd.to_datetime(df_vault['entry_date'])
        
        # --- COLUMN NORMALIZATION (Ensures AI can always find your data) ---
        col_map = {
            'social_impressions': ['social_impressions', 'Impressions', 'Social_Imp'],
            'social_engagement': ['social_engagement', 'Engagement', 'Social_Eng'],
            'actual_traffic': ['actual_traffic', 'Traffic', 'Attendance'],
            'actual_coin_in': ['actual_coin_in', 'Revenue', 'Coin_In', 'Coin In']
        }
        for target, aliases in col_map.items():
            if target not in df_vault.columns:
                for alias in aliases:
                    if alias in df_vault.columns:
                        df_vault.rename(columns={alias: target}, inplace=True)
                        break
            if target not in df_vault.columns:
                df_vault[target] = 0 # Fallback so math doesn't break
            df_vault[target] = pd.to_numeric(df_vault[target], errors='coerce').fillna(0)

        # Calculate Rolling Totals
        last_30 = df_vault[df_vault['entry_date'] > (df_vault['entry_date'].max() - datetime.timedelta(days=30))]
        
        vault_metrics = {
            "30d_revenue": float(last_30['actual_coin_in'].sum()),
            "30d_traffic": int(last_30['actual_traffic'].sum()),
            "avg_social_imp": float(df_vault['social_impressions'].mean()),
            "avg_social_eng": float(df_vault['social_engagement'].mean()),
            "heartbeats": df_vault.groupby(df_vault['entry_date'].dt.day_name())['actual_traffic'].mean().to_dict()
        }

    # 2. CHAT INTERFACE
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Ask me anything: Trends, Forecasts, Social Strategy, or Industry Benchmarks..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Triangulating property data and global industry intelligence..."):
                try:
                    import google.generativeai as genai
                    import json
                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                    model = genai.GenerativeModel('gemini-2.5-flash')
                    
                    live_forecast = st.session_state.get('weather_data', {}).get('forecast', [])
                    
                    sys_context = f"""
                    SYSTEM ROLE: Chief Strategy Officer at Hard Rock Hotel & Casino Ottawa.
                    
                    YOUR ASSETS:
                    - INTERNAL LEDGER: {json.dumps(vault_metrics)}
                    - LIVE WEATHER: {json.dumps(live_forecast, default=str)}
                    - CALIBRATED WEIGHTS: {json.dumps(st.session_state.coeffs)}
                    
                    MANDATE: 
                    Answer ANY query. Use Ledger for history/trends, Forecast for future dates, and your internal AI knowledge for general casino industry strategy.
                    Proactively identify anomalies in the data.
                    """

                    history = [{"role": m["role"], "parts": [m["content"]]} for m in st.session_state.messages[:-1]]
                    chat = model.start_chat(history=history)
                    
                    response = chat.send_message(f"{sys_context}\n\nUSER MESSAGE: {prompt}")
                    
                    st.markdown(response.text)
                    st.session_state.messages.append({"role": "assistant", "content": response.text})

                except Exception as e:
                    st.error(f"Consultation Error: {e}")

    if st.button("🗑️ Reset Consultation Space", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

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

# --- TAB 7: SYNCHRONIZED FORECAST SANDBOX ---
with tab7:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧪 Forecast Sandbox</h2>
            <p style="color: #888; margin: 0;">Fully Synchronized: Aligned with Tab 5 Strategy & Live Environment Canada Data.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. DATE RANGE SELECTION
    today = datetime.date.today()
    date_range = st.date_input(
        "Select Simulation Window:",
        value=(today, today + datetime.timedelta(days=2)),
        help="The Sandbox will pull specific daily forecasts for this entire window."
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        num_days = (end_date - start_date).days + 1
        
        # Pull live data fetched at top of app
        live_forecast = st.session_state.get('weather_data', {}).get('forecast', [])

        # 2. SCENARIO INPUTS
        st.write(f"### 🎛️ Simulation Parameters ({num_days} Days)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Marketing & Social**")
            s_promo = st.checkbox("Active Promotion?", value=False)
            s_clicks = st.number_input("Daily Ad Clicks", value=500)
            s_imp = st.number_input("Daily Social Impressions", value=10000)
            s_eng = st.number_input("Daily Social Engagement", value=500)
        
        with col2:
            st.write("**Scenario Toggles**")
            weather_mode = st.radio("Weather Source:", ["Live EC Forecast", "Manual Overrides"])
            m_temp = st.slider("Manual Temp (°C)", -30, 40, 15, disabled=(weather_mode == "Live EC Forecast"))
            m_rain = st.slider("Manual Rain (mm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))
            m_snow = st.slider("Manual Snow (cm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))

        with col3:
            st.write("**Engine Baseline**")
            c = st.session_state.coeffs
            st.metric("Spend Anchor", f"${c.get('Avg_Coin_In', 1200):,.2f}")
            st.info("The Sandbox is now triangulating historical DOW averages per day.")

        # 3. UNIFIED CALCULATION LOOP (Crucial for closing the disconnect)
        total_range_traffic = 0
        total_range_revenue = 0
        
        # Load Ledger for DOW Averages
        df_sb = pd.DataFrame(ledger_data)
        df_sb['entry_date'] = pd.to_datetime(df_sb['entry_date'])
        df_sb['day_name'] = df_sb['entry_date'].dt.day_name()
        dow_profiles = df_sb.groupby('day_name')['actual_traffic'].mean().to_dict()

        current_date = start_date
        while current_date <= end_date:
            day_str = current_date.strftime("%Y-%m-%d")
            day_name = current_date.strftime("%A")
            
            # Identify Weather for THIS specific day
            if weather_mode == "Live EC Forecast":
                ec_day = next((item for item in live_forecast if day_str in str(item.get('datetime'))), None)
                day_temp = ec_day.get('temperature', 15.0) if ec_day else m_temp
                day_rain = m_rain # Fallback or map from EC
                day_snow = m_snow
            else:
                day_temp, day_rain, day_snow = m_temp, m_rain, m_snow

            # THE MATH (Aligned with Tab 4 & 5)
            # 1. Start with the historical heartbeat for THIS specific day
            base_traffic = dow_profiles.get(day_name, c.get('Intercept', 1000))
            
            # 2. Add Marketing Lifts
            promo_lift = c['Promo'] if s_promo else 0
            social_lift = (s_imp * c.get('Social_Imp', 0.0002)) + (s_eng * c.get('Social_Eng', 0.01))
            marketing_lift = (s_clicks * c.get('Clicks', 0.001))
            
            # 3. Apply Weather Friction
            weather_friction = (day_temp * c.get('Temp_C', 0)) + (day_rain * c.get('Rain_mm', -2.0)) + (day_snow * c.get('Snow_cm', -4.0))
            
            # 4. Aggregate
            daily_total = base_traffic + promo_lift + social_lift + marketing_lift + weather_friction
            total_range_traffic += daily_total
            total_range_revenue += (daily_total * c.get('Avg_Coin_In', 1200.0))
            
            current_date += datetime.timedelta(days=1)

        # 4. OUTPUTS
        st.write("---")
        res1, res2, res3 = st.columns(3)
        res1.metric("Predicted Traffic", f"{int(total_range_traffic):,} Guests")
        res2.metric("Total Window Revenue", f"${total_range_revenue:,.2f}")
        res3.metric("Daily Avg Volume", f"{int(total_range_traffic / num_days)} / day")
