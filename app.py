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

# --- AUTHENTICATION LOGIC ---
if 'user_authenticated' not in st.session_state:
    st.session_state.user_authenticated = False

def login_user(email, password):
    try:
        # Supabase handles the secure hashing and verification
        response = supabase.auth.sign_in_with_password({"email": email, "password": password})
        if response.user:
            st.session_state.user_authenticated = True
            st.rerun()
    except Exception as e:
        st.error("Invalid credentials. Please try again.")

# The Gatekeeper: If not logged in, show the login UI and STOP execution
if not st.session_state.user_authenticated:
    st.markdown("""
        <div style="text-align: center; padding: 50px;">
            <h1 style="color: #FFCC00;">🎰 Hard Rock Ottawa</h1>
            <h3>Strategic Predictor Login</h3>
        </div>
    """, unsafe_allow_html=True)
    
    with st.container(border=True):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        if st.button("Login", use_container_width=True):
            login_user(email, password)
    
    st.stop() # Prevents the rest of the app/tabs from loading

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
# --- TAB 2: LEDGER MANAGEMENT (INTEGRATED) ---
with tab2:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📑 Ledger Management</h2>
            <p style="color: #888; margin: 0;">Data Integrity: Upload history or record daily performance manually.</p>
        </div>
    """, unsafe_allow_html=True)

    col_upload, col_entry = st.columns([1, 1], gap="large")

    # 1. BULK IMPORT CARD
    with col_upload:
        with st.container(border=True):
            st.write("### 📤 Bulk Import")
            
            # --- INTEGRATED HELP GUIDE ---
            with st.expander("❓ View Required CSV Columns"):
                st.markdown("""
                | Column Name | Description |
                | :--- | :--- |
                | `entry_date` | YYYY-MM-DD |
                | `actual_traffic` | Daily Headcount |
                | `actual_coin_in` | Daily Revenue ($) |
                | `social_impressions` | Digital Reach |
                | `social_engagement` | Digital Interactions |
                | `active_promo` | 1 (Yes) or 0 (No) |
                """)
                st.caption("Note: Headers are case-sensitive. Use 'actual_traffic' for best results.")

            uploaded_file = st.file_uploader("Upload Ledger CSV", type="csv", label_visibility="collapsed")
            
            if uploaded_file:
                try:
                    import_df = pd.read_csv(uploaded_file)
                    # Normalize and Convert
                    num_cols = ['actual_traffic', 'actual_coin_in', 'social_impressions', 'social_engagement']
                    for col in num_cols:
                        if col in import_df.columns:
                            import_df[col] = pd.to_numeric(import_df[col], errors='coerce').fillna(0)
                    
                    data_to_insert = import_df.to_dict(orient='records')
                    supabase.table("ledger_data").insert(data_to_insert).execute()
                    st.success(f"Successfully imported {len(import_df)} records!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Import failed: {e}")

    # 2. MANUAL ENTRY CARD
    with col_entry:
        with st.container(border=True):
            st.write("### ✍️ Manual Entry")
            with st.popover("➕ Open Daily Entry Form", use_container_width=True):
                with st.form("manual_entry_form", clear_on_submit=True):
                    st.write("#### Performance Data")
                    f_date = st.date_input("Entry Date", datetime.date.today())
                    f_traffic = st.number_input("Actual Traffic", min_value=0)
                    f_coin = st.number_input("Actual Coin-In ($)", min_value=0.0)
                    
                    st.divider()
                    st.write("#### Marketing Metrics")
                    m1, m2 = st.columns(2)
                    f_imp = m1.number_input("Social Impressions", min_value=0)
                    f_eng = m2.number_input("Social Engagement", min_value=0)
                    f_promo = m1.checkbox("Active Promotion?")
                    
                    submitted = st.form_submit_button("🚀 Commit to Ledger", use_container_width=True)
                    if submitted:
                        entry_data = {
                            "entry_date": str(f_date),
                            "actual_traffic": f_traffic,
                            "actual_coin_in": f_coin,
                            "social_impressions": f_imp,
                            "social_engagement": f_eng,
                            "active_promo": 1 if f_promo else 0
                        }
                        supabase.table("ledger_data").insert([entry_data]).execute()
                        st.success("Entry Saved!")
                        st.rerun()

    st.write("---")

    # 3. DATA EXPLORER
    st.write("### 🔍 Historical Records Explorer")
    if ledger_data:
        df_display = pd.DataFrame(ledger_data).copy()
        df_display['entry_date'] = pd.to_datetime(df_display['entry_date'])
        df_display = df_display.sort_values(by='entry_date', ascending=False)
        
        st.dataframe(
            df_display.style.format({
                "actual_traffic": "{:,.0f}",
                "actual_coin_in": "${:,.2f}",
                "social_impressions": "{:,.0f}",
                "social_engagement": "{:,.0f}"
            }),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("The ledger is currently empty. Use the tools above to add data.")
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

# --- TAB 4: THE FULLY INTEGRATED ENGINE CONTROL ---
with tab4:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Control</h2>
            <p style="color: #888; margin: 0;">Dynamic DOW Heartbeats + Industry-Standard Guardrails.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.warning("⚠️ No ledger data detected. Please upload a CSV in Tab 2 to begin calibration.")
    else:
        df_global = pd.DataFrame(ledger_data).copy()
        
        # 1. DYNAMIC COLUMN NORMALIZATION
        target_schema = {
            'actual_traffic': ['actual_traffic', 'Traffic', 'Attendance', 'Daily_Traffic'],
            'actual_coin_in': ['actual_coin_in', 'Revenue', 'Coin_In', 'Daily_Revenue'],
            'social_impressions': ['social_impressions', 'Impressions', 'Social_Imp'],
            'social_engagement': ['social_engagement', 'Engagement', 'Social_Eng'],
            'ad_clicks': ['ad_clicks', 'Clicks', 'Ad_Clicks'],
            'temp_c': ['temp_c', 'Temp', 'Temperature'],
            'snow_cm': ['snow_cm', 'Snow', 'Snowfall'],
            'rain_mm': ['rain_mm', 'Rain', 'Rainfall'],
            'active_promo': ['active_promo', 'Promo', 'Promotion']
        }

        for target, aliases in target_schema.items():
            if target not in df_global.columns:
                for alias in aliases:
                    if alias in df_global.columns:
                        df_global.rename(columns={alias: target}, inplace=True); break
            if target not in df_global.columns: df_global[target] = 0
            df_global[target] = pd.to_numeric(df_global[target], errors='coerce').fillna(0)

        ledger_signature = hash(pd.util.hash_pandas_object(df_global).sum())

        # 2. CALIBRATION BUTTON
        if st.button("🤖 Auto-Calibrate Engine weights with AI", use_container_width=True):
            with st.spinner("Executing DOW-Segmented Regression with Industry Guardrails..."):
                try:
                    from sklearn.linear_model import Ridge
                    import numpy as np
                    
                    df_global['entry_date'] = pd.to_datetime(df_global['entry_date'])
                    df_global['day_name'] = df_global['entry_date'].dt.day_name()
                    dow_profiles = df_global.groupby('day_name')['actual_traffic'].mean().to_dict()
                    df_global['residual'] = df_global.apply(lambda x: x['actual_traffic'] - dow_profiles[x['day_name']], axis=1)

                    features = ['ad_clicks', 'temp_c', 'snow_cm', 'rain_mm', 'active_promo', 'social_impressions', 'social_engagement']
                    X = df_global[features]
                    y = df_global['residual']

                    model = Ridge(alpha=1.0)
                    model.fit(X, y)
                    raw_weights = dict(zip(features, model.coef_))

                    final_weights = {
                        "Intercept": float(df_global['actual_traffic'].mean()), 
                        "Avg_Coin_In": float(df_global['actual_coin_in'].sum() / df_global['actual_traffic'].sum()) if df_global['actual_traffic'].sum() > 0 else 1200.0,
                        "Clicks": np.clip(float(raw_weights.get('ad_clicks', 0.02)), 0.01, 0.08),
                        "Social_Imp": np.clip(float(raw_weights.get('social_impressions', 0.0002)), 0.0001, 0.0005),
                        "Promo": max(float(raw_weights.get('active_promo', 0)), 150.0),
                        "Social_Eng": max(float(raw_weights.get('social_engagement', 0)), 0.0100),
                        "Temp_C": float(raw_weights.get('temp_c', 0)),
                        "Snow_cm": min(-abs(float(raw_weights.get('snow_cm', -5.0))), -2.00),
                        "Rain_mm": min(-abs(float(raw_weights.get('rain_mm', -2.0))), -1.00),
                        "DOW_Profiles": dow_profiles 
                    }

                    st.session_state.coeffs.update(final_weights)
                    st.session_state.last_calib_hash = ledger_signature
                    st.success("🎯 Dynamic Calibration Complete.")
                    st.rerun()

                except Exception as e:
                    st.error(f"Calibration Error: {e}")

        # 3. LIVE MONITORING & MANUAL OVERRIDES
        st.write("### 📊 Active Engine Weights")
        c = st.session_state.coeffs
        
        # Display key metrics in a clean row
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("Global Intercept", f"{float(c.get('Intercept',0)):.0f}")
        col_m2.metric("Avg Spend", f"${float(c.get('Avg_Coin_In',0)):.2f}")
        col_m3.metric("Click Weight", f"{float(c.get('Clicks',0)):.4f}")

        st.divider()

        # Input grid for manual tweaking
        col_fin, col_mkt, col_env = st.columns(3)
        
        with col_fin:
            st.write("**💰 Financials**")
            n_intercept = st.number_input("Global Intercept", value=float(c.get('Intercept', 0)))
            n_spend = st.number_input("Spend/Head ($)", value=float(c.get('Avg_Coin_In', 1200)))

        with col_mkt:
            st.write("**🚀 Marketing**")
            n_promo = st.number_input("Promo Flat Lift", value=float(c.get('Promo', 0)))
            n_clicks = st.number_input("Click Weight", value=float(c.get('Clicks', 0.02)), format="%.4f")
            n_imp = st.number_input("Social Imp Weight", value=float(c.get('Social_Imp', 0.0002)), format="%.4f")

        with col_env:
            st.write("**☁️ Environment**")
            n_temp = st.number_input("Temp Weight", value=float(c.get('Temp_C', 0)), format="%.4f")
            n_snow = st.number_input("Snow Weight (cm)", value=float(c.get('Snow_cm', 0)), format="%.4f")
            n_rain = st.number_input("Rain Weight (mm)", value=float(c.get('Rain_mm', 0)), format="%.4f")

        if st.button("💾 Sync Engine to Database", use_container_width=True):
            try:
                update_data = {
                    "id": 1, "Intercept": n_intercept, "Avg_Coin_In": n_spend,
                    "Promo": n_promo, "Social_Imp": n_imp, "Clicks": n_clicks,
                    "Temp_C": n_temp, "Snow_cm": n_snow, "Rain_mm": n_rain
                }
                supabase.table("coefficients").upsert(update_data).execute()
                st.session_state.coeffs.update(update_data)
                st.success("✅ Database Synced.")
            except Exception as e:
                st.error(f"Sync failed: {e}")
# --- TAB 5: EXECUTIVE STRATEGIC CONSULTANT (FINAL KICK-ASS VERSION) ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧠 Strategic Consultant</h2>
            <p style="color: #888; margin: 0;">Real-Time Intelligence: Analyzing history, live weather, and social benchmarks.</p>
        </div>
    """, unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 1. THE REAL-TIME DATA VAULT
    vault_metrics = {}
    if ledger_data:
        df_vault = pd.DataFrame(ledger_data).copy()
        df_vault['entry_date'] = pd.to_datetime(df_vault['entry_date'])
        
        # --- DYNAMIC COLUMN NORMALIZATION ---
        col_map = {
            'social_impressions': ['social_impressions', 'Impressions', 'Social_Imp', 'Reach'],
            'social_engagement': ['social_engagement', 'Engagement', 'Social_Eng', 'Interactions'],
            'actual_traffic': ['actual_traffic', 'Traffic', 'Attendance'],
            'actual_coin_in': ['actual_coin_in', 'Revenue', 'Coin_In']
        }
        
        for target, aliases in col_map.items():
            existing_col = next((c for c in aliases if c in df_vault.columns), None)
            if existing_col:
                df_vault.rename(columns={existing_col: target}, inplace=True)
                df_vault[target] = pd.to_numeric(df_vault[target], errors='coerce').fillna(0)
            else:
                df_vault[target] = 0

        # Calculate Real-Time Benchmarks
        avg_imp = float(df_vault['social_impressions'].mean())
        avg_eng = float(df_vault['social_engagement'].mean())
        df_vault['day_name'] = df_vault['entry_date'].dt.day_name()
        heartbeats = df_vault.groupby('day_name')['actual_traffic'].mean().to_dict()

        vault_metrics = {
            "avg_social_imp": avg_imp,
            "avg_social_eng": avg_eng,
            "heartbeats": heartbeats
        }

    # 2. CHAT INPUT (AT THE TOP)
    prompt = st.chat_input("Analyze Monday, discuss social trends, or ask for a strategy...")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            import google.generativeai as genai
            import json
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            # Filter coefficients to prevent double-counting
            coeffs = st.session_state.coeffs
            clean_weights = {k: v for k, v in coeffs.items() if k not in ['Intercept', 'DOW_Profiles']}
            
            live_forecast = st.session_state.get('weather_data', {}).get('forecast', [])
            
            # ROLE MAPPER: Streamlit 'assistant' -> Gemini 'model'
            history_payload = []
            for m in st.session_state.messages[:-1]:
                role = "model" if m["role"] == "assistant" else "user"
                history_payload.append({"role": role, "parts": [m["content"]]})
            
            # THE EXECUTIVE BRAIN
            sys_context = f"""
            SYSTEM ROLE: Chief Strategy Officer at Hard Rock Hotel & Casino Ottawa.
            
            CRITICAL MATHEMATICAL MANDATE:
            1. Use the 'Heartbeat' (DOW Average) as your ONLY baseline for predictions.
            2. DO NOT ADD THE GLOBAL INTERCEPT (4365) TO THE HEARTBEAT. 
            3. Prediction = [Specific Day Heartbeat] + (Weather Friction) + (Marketing Lifts).
            4. USE REAL-TIME BENCHMARKS: For social media, use {vault_metrics.get('avg_social_imp', 0)} impressions and {vault_metrics.get('avg_social_eng', 0)} engagement as your standard reference.

            DATA ASSETS:
            - DOW HEARTBEATS (Your Start Points): {json.dumps(vault_metrics.get('heartbeats', {}))}
            - ENGINE WEIGHTS: {json.dumps(clean_weights)}
            - LIVE WEATHER: {json.dumps(live_forecast, default=str)}
            
            COMMUNICATION STYLE:
            - Executive Summary first (2-3 sentences).
            - Hide the math unless the user asks "How?" or "Show me the math".
            - End with 1 strategic question.
            """

            chat = model.start_chat(history=history_payload)
            response = chat.send_message(f"{sys_context}\n\nUSER MESSAGE: {prompt}")
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()

        except Exception as e:
            st.error(f"Consultation Error: {e}")

    # 3. REVERSED FEED: LATEST RESPONSE AT THE TOP
    for message in reversed(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    st.write("---")
    if st.button("🗑️ Reset Strategy Session", use_container_width=True):
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
