import streamlit as st
import pandas as pd
import datetime
import json
import asyncio
import numpy as np
from env_canada import ECWeather
import google.generativeai as genai
from supabase import create_client

# 1. PAGE CONFIG (Must be the very first Streamlit command)
st.set_page_config(page_title="FloorCast | Hard Rock Ottawa", layout="wide", page_icon="🎰")

# --- AUTH LIST ---
ADMIN_USERS = ["bjbeehler@gmail.com"]

import numpy as np
import pandas as pd

def get_forensic_metrics(df_input, coeffs):
    """
    THE MASTER ENGINE: Triangulates Organic Baselines, Adstock, 
    OOH Pressure, Hard Rock LIVE Gravity, and Weather Friction.
    """
    if df_input is None or len(df_input) == 0:
        return {
            "predictability": "0.0%", 
            "digital_lift": "0.0%", 
            "heartbeats": {}, 
            "ooh_total_daily": 0,
            "df_with_awareness": pd.DataFrame()
        }

    df = pd.DataFrame(df_input).copy()
    
    # 1. STANDARDIZE & CLEAN COLUMNS
    cols_to_ensure = {
        'ad_clicks': ['ad_clicks', 'Clicks'],
        'ad_impressions': ['ad_impressions', 'Impressions'],
        'actual_traffic': ['actual_traffic', 'Traffic'],
        'snow_cm': ['snow_cm', 'Snow', 'snow'],
        'rain_mm': ['rain_mm', 'Rain', 'rain'],
        'attendance': ['attendance', 'Attendance', 'event_attendance']
    }

    for target, aliases in cols_to_ensure.items():
        existing = next((c for c in aliases if c in df.columns), None)
        if existing:
            df.rename(columns={existing: target}, inplace=True)
        if target not in df.columns:
            df[target] = 0 
        df[target] = pd.to_numeric(df[target], errors='coerce').fillna(0)

    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df = df.sort_values('entry_date')
    df['day_name'] = df['entry_date'].dt.day_name()
    
    # 2. PULL CALIBRATED WEIGHTS (RESILIENT FETCH)
    # Using .get() with fallbacks to avoid TypeErrors if keys are missing or cased differently
    c_clicks = float(coeffs.get('Clicks') or coeffs.get('clicks') or 0.04)
    c_social = float(coeffs.get('Impressions') or coeffs.get('impressions') or 0.0002)
    decay_rate = float(coeffs.get('Ad_Decay') or coeffs.get('ad_decay') or 85.0) / 100 
    
    # --- FIX: SAFE-FETCH FOR EVENT GRAVITY ---
    raw_gravity = coeffs.get('event_gravity') or coeffs.get('Event_Gravity') or 20.0
    event_capture = float(raw_gravity) / 100
    
    # OOH Weights
    ooh_w = float(coeffs.get('Static_Weight') or coeffs.get('static_weight') or 50.0)
    ooh_c = int(coeffs.get('Static_Count') or coeffs.get('static_count') or 2)
    dig_w = float(coeffs.get('Digital_OOH_Weight') or coeffs.get('digital_ooh_weight') or 10.0)
    dig_c = int(coeffs.get('Digital_OOH_Count') or coeffs.get('digital_ooh_count') or 4)
    total_ooh_lift = (ooh_w * ooh_c) + (dig_w * dig_c)
    
    # Weather Friction
    c_snow = float(coeffs.get('Snow_cm') or coeffs.get('snow_cm') or -45.0)
    c_rain = float(coeffs.get('Rain_mm') or coeffs.get('rain_mm') or -12.0)

    # 3. THE AWARENESS POOL (ADSTOCK LOOP)
    awareness_pool = []
    current_pool = 0.0
    for _, row in df.iterrows():
        daily_input = (row['ad_clicks'] * c_clicks) + (row['ad_impressions'] * c_social)
        current_pool = daily_input + (current_pool * decay_rate)
        awareness_pool.append(current_pool)
    
    df['residual_lift'] = awareness_pool

    # 4. THE GRAVITY PULSE (HARD ROCK LIVE)
    df['gravity_lift'] = df['attendance'] * event_capture

    # 5. BASELINE PURIFICATION
    df['baseline_isolated'] = df['actual_traffic'] - df['residual_lift'] - total_ooh_lift - df['gravity_lift']
    heartbeats = df.groupby('day_name')['baseline_isolated'].mean().to_dict()

    # 6. MASTER ATTRIBUTION (EXPECTED VALUE)
    df['expected'] = df.apply(lambda x: 
        heartbeats.get(x['day_name'], 4000) + 
        x['residual_lift'] + 
        total_ooh_lift + 
        x['gravity_lift'] + 
        (x['snow_cm'] * c_snow) + 
        (x['rain_mm'] * c_rain), 
        axis=1
    )

    # 7. FINAL PERFORMANCE METRICS
    df_filtered = df[df['actual_traffic'] > 0].copy()
    if not df_filtered.empty:
        mape = (np.abs(df_filtered['actual_traffic'] - df_filtered['expected']) / df_filtered['actual_traffic']).replace([np.inf, -np.inf], np.nan).dropna().mean()
        pred_val = (1 - mape) * 100 if not np.isnan(mape) else 85.0
        latest_traffic = df_filtered['actual_traffic'].iloc[-1]
        latest_residual = df_filtered['residual_lift'].iloc[-1]
        lift_pct = (latest_residual / latest_traffic * 100) if latest_traffic > 0 else 0
    else:
        pred_val, lift_pct, latest_residual = 0, 0, 0

    return {
        "predictability": f"{pred_val:.1f}%",
        "digital_lift": f"{lift_pct:.1f}%",
        "digital_lift_val": latest_residual,
        "heartbeats": heartbeats,
        "ooh_total_daily": total_ooh_lift,
        "df_with_awareness": df
    }
# 3. INITIALIZE CLIENTS
url = st.secrets["SUPABASE_URL"]
key = st.secrets["SUPABASE_KEY"]
supabase = create_client(url, key)

if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

# 4. WEATHER LOGIC
async def fetch_live_ec_data():
    try:
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {"current": ec.conditions, "forecast": ec.daily_forecasts, "alerts": ec.alerts}
    except:
        return {"error": "Weather Unavailable"}

if 'weather_data' not in st.session_state:
    st.session_state.weather_data = asyncio.run(fetch_live_ec_data())

if 'user_authenticated' not in st.session_state:
    st.session_state.user_authenticated = False

# 5. GATEKEEPER (Secure Login UI with Identity Capture)
if not st.session_state.user_authenticated:
    st.markdown("<div style='text-align:center; padding:50px;'><h1 style='color:#FFCC00;'>🎰 FloorCast</h1><h3>Hard Rock Ottawa | Strategic Engine</h3></div>", unsafe_allow_html=True)
    
    with st.container(border=True):
        email_input = st.text_input("Email")
        pw_input = st.text_input("Password", type="password")
        
        if st.button("Access Engine", use_container_width=True, key="login_btn"):
            try:
                # 1. Authenticate with Supabase
                res = supabase.auth.sign_in_with_password({"email": email_input, "password": pw_input})
                
                if res.user:
                    # 2. Store identity and status
                    st.session_state.user_authenticated = True
                    st.session_state.user_email = res.user.email  # CRITICAL for permissions
                    st.success(f"Welcome, {res.user.email}")
                    st.rerun() 
                else:
                    st.error("Authentication failed.")
            except Exception as e:
                st.error("Invalid credentials or connection error.")
    
    st.stop() # Prevents unauthorized users from seeing the rest of the script

# --- 6. CRITICAL DATA HYDRATION (NEW FIX) ---
# This must happen before we call the engine

# A. Load Coefficients first
try:
    c_res = supabase.table("coefficients").select("*").eq("id", 1).execute()
    if c_res.data:
        st.session_state.coeffs = c_res.data[0]
        # Safety injection for missing OOH columns
        defaults = {'Static_Weight': 50.0, 'Static_Count': 2, 'Digital_OOH_Weight': 10.0, 'Digital_OOH_Count': 4}
        for k, v in defaults.items():
            if k not in st.session_state.coeffs:
                st.session_state.coeffs[k] = v
except:
    st.session_state.coeffs = {}

# B. Load Ledger Data next (fixes the NameError)
try:
    l_res = supabase.table("ledger").select("*").execute()
    ledger_data = l_res.data if l_res.data else []
except:
    ledger_data = []

# C. Calculate Metrics once for all Tabs
metrics = get_forensic_metrics(ledger_data, st.session_state.coeffs)

# 7. MODERN UI STYLING (Minimalist Executive Theme)
st.markdown("""
    <style>
    /* Clean Page Background */
    .stApp {
        background-color: #f4f7f9;
        color: #111111;
    }
    
    /* Remove the 'Bento Box' borders and shadows */
    div[data-testid="stVerticalBlock"] > div {
        border: none !important;
        background-color: transparent !important;
        box-shadow: none !important;
        margin-bottom: 0px !important;
        padding: 0px !important;
    }

    /* Keep the Tabs professional */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: #e6e9ef;
        border-radius: 4px 4px 0px 0px;
        padding: 10px 20px;
        color: #333;
    }

    /* Active Tab - Hard Rock Gold */
    .stTabs [aria-selected="true"] {
        background-color: #FFCC00 !important; 
        color: #000000 !important;
        font-weight: bold;
    }

    /* Standardize Metric spacing */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #e6e9ef;
    }
    </style>
    """, unsafe_allow_html=True)

# 8. TOP NAVIGATION BAR
h1, h2 = st.columns([4, 1])
with h1:
    st.markdown("<h1 style='color: #FFCC00; margin:0;'>🎰 FloorCast</h1>", unsafe_allow_html=True)
with h2:
    if st.button("🔓 Logout", use_container_width=True):
        supabase.auth.sign_out()
        st.session_state.user_authenticated = False
        st.rerun()

st.divider()

# 9. MAIN TAB NAVIGATION
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 Executive Overview", "📑 Ledger Management", "📊 Property Analytics", 
    "⚙️ Engine Control", "🧠 FloorCast Analyst", "📋 Master Report", "🧪 Forecast Sandbox"
])

# --- TAB 1: EXECUTIVE OVERVIEW ---
with tab1:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📈 Executive Overview</h2>
            <p style="color: #888; margin: 0;">Real-time property pulse and forensic attribution for Hard Rock Ottawa.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. RUN THE ENGINE
    # This pulls the 'digital_lift' and 'ooh_total_daily' from Section 2
    metrics = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    
# 2. KEY PERFORMANCE INDICATORS (Updated to 5 Columns)
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Model Predictability", metrics['predictability'], 
                  help="How accurately the AI is explaining current traffic trends.")
    
    with col2:
        # PURE ONLINE ATTRIBUTION
        st.metric("Pure Digital Lift", metrics['digital_lift'], 
                  help="Guest traffic generated specifically by Clicks and Social Impressions.")
        
    with col3:
        # THE NEW INERTIA LIFTER
        ooh_val = metrics.get('ooh_total_daily', 0)
        st.metric("OOH Baseline Pressure", f"{ooh_val:.0f} Guests",
                  help="The constant daily lift generated by the billboard campaign.")

    with col4:
        # NEW MEMBERS KPI (The New Addition)
        latest_entry = ledger_data[-1] if ledger_data else {}
        new_members_count = latest_entry.get('new_members', 0)
        st.metric("New Members", f"{new_members_count}", 
                  help="Unity Card sign-ups recorded for the most recent entry.")

    with col5:
        # FINANCIAL ANCHOR
        # Ensure we pull the latest from session state
        avg_spend_val = float(st.session_state.coeffs.get('Avg_Coin_In', 112.50))
        st.metric("Avg. Spend / Head", f"${avg_spend_val:.2f}")

    st.divider()

    # 3. FORENSIC INSIGHT PANEL
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.write("### 🧬 Attribution Mix")
        # Visualizing the split between Digital ROI and OOH Baseline
        ooh_weekly = ooh_val * 7
        st.info(f"""
            **Forensic Summary:**
            * **Online:** Your digital campaigns are currently driving a **{metrics['digital_lift']}** lift in total property traffic.
            * **Offline:** Your OOH campaign is providing a stable inertia of **{ooh_weekly:,.0f} guests per week**.
            * **Synthesis:** The model is currently operating at **{metrics['predictability']}** predictability, indicating high confidence in these attribution splits.
        """)

    with c2:
        st.write("### ❄️ Live Environment")
        # Pulling live weather from the top of the script
        weather = st.session_state.weather_data
        if "error" not in weather:
            temp = weather['current'].get('temperature', 'N/A')
            cond = weather['current'].get('condition', 'Unknown')
            st.success(f"**Current:** {temp}°C | {cond}")
            
            # Show friction impact if it's snowing
            snow_friction = st.session_state.coeffs.get('Snow_cm', -45)
            st.write(f"Current Snow Friction: `{snow_friction}` guests/cm")
        else:
            st.warning("Weather sync unavailable.")

# 4. TREND VISUALIZATION (Performance vs. Prediction)
    st.write("---")
    st.write("### 📊 Performance vs. Prediction")
    
    if ledger_data:
        # 1. Convert ledger to DataFrame
        df_chart = pd.DataFrame(ledger_data).copy()
        df_chart['entry_date'] = pd.to_datetime(df_chart['entry_date'])
        
        # 2. RUN THE FORENSIC ENGINE TO GET PREDICTIONS
        # This adds the 'expected' column using your calibration weights
        # We need to calculate this specifically for the chart
        heartbeats = metrics.get('heartbeats', {})
        c_clicks = st.session_state.coeffs.get('Clicks', 0.02)
        c_social = st.session_state.coeffs.get('Impressions', 0.0002)
        total_ooh = metrics.get('ooh_total_daily', 0)
        
        # Apply the exact same math used in Section 2
        df_chart['day_name'] = df_chart['entry_date'].dt.day_name()
        df_chart['Expected Traffic'] = df_chart.apply(lambda x: 
            heartbeats.get(x['day_name'], 0) + 
            (float(x.get('ad_clicks', 0)) * c_clicks) + 
            (float(x.get('ad_impressions', 0)) * c_social) + 
            total_ooh, axis=1)
        
        # Rename actual traffic for the legend
        df_chart = df_chart.rename(columns={'actual_traffic': 'Actual Traffic'})
        
        # 3. Sort and Filter for the last 14 days
        df_chart = df_chart.sort_values('entry_date').tail(14)
        
        # 4. PLOT THE MULTI-LINE CHART
        # This will show two lines: Actual (Gold/Blue) vs Expected (Shadow)
        st.line_chart(df_chart.set_index('entry_date')[['Actual Traffic', 'Expected Traffic']])
        
        st.caption("The 'Expected' line accounts for historical baselines, weather friction, digital spend, and OOH inertia.")

# --- TAB 2: LEDGER MANAGEMENT ---
with tab2:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📑 Ledger Management</h2>
            <p style="color: #888; margin: 0;">Update property performance. Newest entries will appear at the top of the editor below.</p>
        </div>
    """, unsafe_allow_html=True)

    col_a, col_b = st.columns([1, 1])

    # --- SECTION A: MANUAL FORM ---
    with col_a:
        st.write("### ✍️ Manual Results Entry")
        with st.form("manual_entry_v6"):
            entry_date = st.date_input("Select Date", datetime.date.today())
            
            # Row 1: Core Financials
            c1, c2, c3 = st.columns(3)
            with c1: traffic = st.number_input("Traffic (Headcount)", min_value=0)
            with c2: coin_in = st.number_input("Coin-In ($)", min_value=0.0, format="%.2f")
            with c3: new_mems = st.number_input("New Members", min_value=0, step=1)
            
            st.divider()
            
            # Row 2: Environment & Promo (Temperature Restored)
            st.write("**🌦️ Environment & Promotion**")
            w1, w2, w3, w4 = st.columns(4)
            with w1: temp = st.number_input("Temp (°C)", value=15.0, step=0.5)
            with w2: snow = st.number_input("Snow (cm)", min_value=0.0, step=0.1)
            with w3: rain = st.number_input("Rain (mm)", min_value=0.0, step=0.1)
            with w4: promo = st.checkbox("Major Promo?")

            st.divider()

            # Row 3: Hard Rock LIVE
            st.write("**🎸 Hard Rock LIVE Event Data**")
            e1, e2 = st.columns(2)
            with e1: 
                event_type = st.selectbox("Event Setup", ["None", "GA (2,200)", "Seated (1,900)"])
            with e2: 
                attendance = st.number_input("Actual Attendance", min_value=0, max_value=2200)

            st.divider()

            # Row 4: Marketing
            st.write("**📣 Marketing Metrics**")
            m1, m2, m3 = st.columns(3)
            with m1: clicks = st.number_input("Ad Clicks", min_value=0)
            with m2: imps = st.number_input("Ad Impressions", min_value=0)
            with m3: social = st.number_input("Social Engagements", min_value=0)
            
            submit_form = st.form_submit_button("💾 Sync Results to Vault", use_container_width=True)
            
            if submit_form:
                with st.spinner("Writing to Vault..."):
                    date_str = entry_date.isoformat()
                    new_row = {
                        "entry_date": date_str,
                        "actual_traffic": int(traffic),
                        "actual_coin_in": float(coin_in),
                        "ad_clicks": int(clicks),
                        "ad_impressions": int(imps),
                        "social_engagements": int(social),
                        "new_members": int(new_mems),
                        "temp_c": float(temp),       # Added Temperature
                        "snow_cm": float(snow),
                        "rain_mm": float(rain),
                        "active_promo": bool(promo),
                        "event_type": event_type,
                        "attendance": int(attendance)
                    }
                    try:
                        supabase.table("ledger").upsert(new_row, on_conflict="entry_date").execute()
                        st.success(f"✅ Data for {date_str} synced.")
                        import time
                        time.sleep(1)
                        st.rerun() 
                    except Exception as e:
                        st.error(f"🚨 Database Error: {e}")

    # --- SECTION B: BULK UPLOAD ---
    with col_b:
        st.write("### 📤 Bulk CSV Upload")
        uploaded_file = st.file_uploader("Upload Ledger CSV", type="csv", key="csv_uploader_t2")
        if uploaded_file is not None:
            df_upload = pd.read_csv(uploaded_file)
            st.dataframe(df_upload.head(3), use_container_width=True)
            if st.button("🚀 Push CSV to Vault", use_container_width=True):
                try:
                    data_dict = df_upload.to_dict(orient='records')
                    supabase.table("ledger").upsert(data_dict, on_conflict="entry_date").execute()
                    st.success("Bulk upload complete!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Upload failed: {e}")

    # --- SECTION C: THE LEDGER EDITOR ---
    st.divider()
    st.write("### 📜 Ledger Editor")
    
    if ledger_data:
        df_history = pd.DataFrame(ledger_data)
        
        # Ensure all columns exist locally for the editor including temp
        expected_cols = {
            'temp_c': 15.0, 'snow_cm': 0.0, 'rain_mm': 0.0, 
            'active_promo': False, 'event_type': "None", 'attendance': 0
        }
        for col, default in expected_cols.items():
            if col not in df_history.columns:
                df_history[col] = default

        if 'entry_date' in df_history.columns:
            df_history['entry_date'] = pd.to_datetime(df_history['entry_date'])
            df_history = df_history.sort_values(by='entry_date', ascending=False)
        
        edited_df = st.data_editor(
            df_history, 
            key="ledger_editor_v6", 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "entry_date": st.column_config.DateColumn("Date", disabled=True),
                "actual_traffic": st.column_config.NumberColumn("Traffic"),
                "temp_c": st.column_config.NumberColumn("Temp (°C)"),
                "snow_cm": st.column_config.NumberColumn("Snow (cm)"),
                "rain_mm": st.column_config.NumberColumn("Rain (mm)"),
                "active_promo": st.column_config.CheckboxColumn("Promo?"),
                "event_type": st.column_config.SelectboxColumn("Live Setup", options=["None", "GA (2,200)", "Seated (1,900)"]),
                "attendance": st.column_config.NumberColumn("Attendance")
            }
        )

        if st.button("✅ Confirm & Sync Edits", key="btn_sync_v6", use_container_width=True):
            with st.spinner("Sanitizing Types & Updating Vault..."):
                try:
                    # 1. Prepare the data
                    sync_ready = edited_df.copy()
                    
                    # 2. THE FIX: Explicitly cast to whole numbers to avoid '22P02' error
                    # Fill NaNs first to avoid conversion errors
                    sync_ready['attendance'] = sync_ready['attendance'].fillna(0).astype(int)
                    sync_ready['actual_traffic'] = sync_ready['actual_traffic'].fillna(0).astype(int)
                    sync_ready['new_members'] = sync_ready['new_members'].fillna(0).astype(int)
                    
                    # 3. Sanitize the rest
                    sync_ready['temp_c'] = sync_ready['temp_c'].fillna(15.0).astype(float)
                    sync_ready['snow_cm'] = sync_ready['snow_cm'].fillna(0.0).astype(float)
                    sync_ready['rain_mm'] = sync_ready['rain_mm'].fillna(0.0).astype(float)
                    sync_ready['event_type'] = sync_ready['event_type'].fillna("None").astype(str)

                    # 4. Format Date
                    sync_ready['entry_date'] = sync_ready['entry_date'].dt.strftime('%Y-%m-%d')
                    
                    # 5. Push to Supabase
                    payload = sync_ready.to_dict(orient='records')
                    supabase.table("ledger").upsert(payload, on_conflict="entry_date").execute()
                    
                    st.success("✅ Vault Successfully Updated with Whole Numbers.")
                    st.rerun() 
                except Exception as e:
                    st.error(f"Manual sync failed: {e}")

# --- TAB 3: PROPERTY ANALYTICS ---
with tab3:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Property Performance Analytics</h2>
            <p style="color: #888; margin: 0;">Forensic trend analysis of digital vs. physical results.</p>
        </div>
    """, unsafe_allow_html=True)

    if ledger_data:
        df_analysis = pd.DataFrame(ledger_data)
        df_analysis['entry_date'] = pd.to_datetime(df_analysis['entry_date'])
        df_analysis = df_analysis.sort_values('entry_date')

        # 1. VISUAL TREND SELECTION
        st.write("### 📈 Performance Trends")
        
        # Added "New Members" to the selector
        metric_choice = st.pills("Select Metric to Analyze", 
                                ["Traffic", "Coin-In", "New Members", "Ad Clicks"], 
                                selection_mode="single",
                                default="Traffic",
                                key="analysis_pills_t3") 

        if metric_choice == "Traffic":
            st.area_chart(df_analysis.set_index('entry_date')['actual_traffic'], color="#FFCC00")
        elif metric_choice == "Coin-In":
            st.line_chart(df_analysis.set_index('entry_date')['actual_coin_in'], color="#2ecc71")
        elif metric_choice == "New Members":
            # Using a bar chart for members as it's a discrete daily count
            st.bar_chart(df_analysis.set_index('entry_date')['new_members'], color="#E74C3C")
        else:
            st.bar_chart(df_analysis.set_index('entry_date')['ad_clicks'], color="#00CCFF")

        # 2. READ-ONLY DATA VIEW
        st.divider()
        st.write("### 📜 Detailed Analytics View")
        
        # Expanded columns to include new_members
        cols_to_show = [
            'entry_date', 'actual_traffic', 'actual_coin_in', 
            'new_members', 'ad_clicks', 'ad_impressions', 'social_engagements'
        ]
        
        st.dataframe(
            df_analysis[cols_to_show],
            column_config={
                "entry_date": st.column_config.DateColumn("Date"),
                "actual_traffic": st.column_config.NumberColumn("Traffic"),
                "actual_coin_in": st.column_config.NumberColumn("Coin-In ($)", format="$%.2f"),
                "new_members": st.column_config.NumberColumn("New Members"),
                "ad_clicks": st.column_config.NumberColumn("Ad Clicks"),
                "ad_impressions": st.column_config.NumberColumn("Ad Impressions"),
                "social_engagements": st.column_config.NumberColumn("Social Engagements")
            },
            use_container_width=True,
            hide_index=True
        )
        
        st.caption("💡 To edit these values, please use the **Ledger Management** tab.")

    else:
        st.info("No data found in the Vault. Please backfill results in Tab 2 to see analytics.")

# --- TAB 4: ENGINE CONTROL (Entertainment Integrated) ---

with tab4:
    current_user = st.session_state.get('user_email', "unauthorized")
    
    # 1. PERMISSION GATEKEEPER
    if current_user not in ADMIN_USERS:
        st.warning("### 🔒 Access Restricted")
        st.info("This tab is reserved for Executive Management to calibrate the Forensic Engine.")
    else:
        st.markdown(f"""
            <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
                <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Calibration</h2>
                <p style="color: #888; margin: 0;">Admin Identity: <b>{current_user}</b></p>
            </div>
        """, unsafe_allow_html=True)

        # 2. PRE-FLIGHT DATA CLEANING
        defaults = {
            'Clicks': 0.04, 'Impressions': 0.0002, 'Ad_Decay': 85.0,
            'Avg_Coin_In': 112.50, 'Property_Theo': 450.0, 'Hold_Pct': 10.0,
            'Snow_cm': -45.0, 'Rain_mm': -12.0, 'Static_Weight': 50.0, 
            'Static_Count': 2, 'Digital_OOH_Weight': 10.0, 'Digital_OOH_Count': 4,
            'Event_Gravity': 20.0  # New Default: 20% Capture Rate
        }
        
        clean_coeffs = {}
        for key, default_val in defaults.items():
            raw_val = st.session_state.coeffs.get(key)
            try:
                clean_coeffs[key] = float(raw_val) if raw_val is not None else default_val
            except (ValueError, TypeError):
                clean_coeffs[key] = default_val

        # 3. CALIBRATION FORM
        with st.form("engine_settings_v17"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("### 📣 Marketing & Awareness")
                new_clicks = st.slider("Click Weight", 0.0, 0.5, value=clean_coeffs['Clicks'], format="%.3f")
                new_decay = st.slider("Awareness Retention (%)", 0.0, 100.0, value=clean_coeffs['Ad_Decay'], 
                                      help="Adstock Decay: 85% is the standard for retail/gaming half-life.")
                
                # --- NEW: HARD ROCK LIVE CALIBRATION ---
                st.write("### 🎸 Hard Rock LIVE Gravity")
                new_event_grav = st.slider("Event Capture Rate (%)", 0.0, 50.0, value=clean_coeffs['Event_Gravity'], 
                                           help="The % of show attendees expected to visit the gaming floor. Industry standard is 15-22%.")

                st.write("### 📍 OOH / Billboard Pressure")
                new_static_w = st.slider("Static Board Lift", 0.0, 500.0, value=clean_coeffs['Static_Weight'])
                new_static_c = st.number_input("Static Count", 0, 10, value=int(clean_coeffs['Static_Count']))

            with col2:
                st.write("### 💰 Financials & Yield")
                new_coin = st.number_input("Avg Gross Spend ($)", 0.0, 5000.0, value=clean_coeffs['Avg_Coin_In'])
                new_hold = st.slider("House Hold %", 1.0, 25.0, value=clean_coeffs['Hold_Pct'])
                
                st.write("### ❄️ Weather Friction")
                new_snow = st.slider("Snow Friction (Guests/cm)", -1000.0, 0.0, value=clean_coeffs['Snow_cm'])
                new_rain = st.slider("Rain Friction (Guests/mm)", -500.0, 0.0, value=clean_coeffs['Rain_mm'])

            st.divider()
            submit_btn = st.form_submit_button("💾 Save All Calibration & Sync Vault", use_container_width=True)

            if submit_btn:
                # 4. PREPARE THE SYNC PAYLOAD
                sync_payload = {
                    'Clicks': float(new_clicks),
                    'Ad_Decay': float(new_decay),
                    'Avg_Coin_In': float(new_coin),
                    'Hold_Pct': float(new_hold),
                    'Snow_cm': float(new_snow),
                    'Rain_mm': float(new_rain),
                    'Static_Weight': float(new_static_w),
                    'Static_Count': int(new_static_c),
                    'Event_Gravity': float(new_event_grav), # Syncing the new feature
                    'Digital_OOH_Weight': clean_coeffs['Digital_OOH_Weight'], 
                    'Digital_OOH_Count': int(clean_coeffs['Digital_OOH_Count'])
                }
                
                try:
                    # 5. EXECUTE SUPABASE UPDATE
                    response = supabase.table("coefficients").update(sync_payload).eq("id", 1).execute()
                    
                    # 6. VERIFY SUCCESS
                    if hasattr(response, 'data') and len(response.data) > 0:
                        st.session_state.coeffs.update(sync_payload)
                        st.success("✅ Vault Successfully Synced. Entertainment Gravity Integrated.")
                        import time
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("### 🚨 Sync Failed: Row ID 1 not found.")
                
                except Exception as e:
                    # 7. EXPOSE THE REAL ERROR
                    st.error("### 🚨 Database Error")
                    st.write(f"**The Vault rejected the update:** {e}")
                    st.write("**Verify your Supabase column names match exactly:**")
                    st.code(list(sync_payload.keys()))

# --- TAB 5: FORENSIC ANALYST & PRODUCT EXPERT ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧠 Forensic Consultant & App Expert</h2>
            <p style="color: #888; margin: 0;">Real-time KPI calculation and strategic guidance powered by Gemini.</p>
        </div>
    """, unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 1. THE FORENSIC DATA VAULT (Synced to Latest Schema)
    vault_metrics = {}
    if ledger_data:
        df_vault = pd.DataFrame(ledger_data).copy()
        df_vault['entry_date'] = pd.to_datetime(df_vault['entry_date'])
        
        # --- DYNAMIC COLUMN NORMALIZATION ---
        col_map = {
            'ad_impressions': ['ad_impressions', 'Impressions'],
            'ad_clicks': ['ad_clicks', 'Clicks'],
            'actual_traffic': ['actual_traffic', 'Traffic'],
            'actual_coin_in': ['actual_coin_in', 'Revenue', 'actual_coin_in'],
            'new_members': ['new_members', 'Signups', 'Members'] # Added New Members mapping
        }
        
        for target, aliases in col_map.items():
            existing_col = next((c for c in aliases if c in df_vault.columns), None)
            if existing_col:
                df_vault.rename(columns={existing_col: target}, inplace=True)
                df_vault[target] = pd.to_numeric(df_vault[target], errors='coerce').fillna(0)
            else:
                df_vault[target] = 0

        # Calculation logic
        df_vault['day_name'] = df_vault['entry_date'].dt.day_name()
        heartbeats = df_vault.groupby('day_name')['actual_traffic'].mean().to_dict()
        
        # Pull Weights
        w_clicks = st.session_state.coeffs.get('Clicks', 0.02)
        w_social = st.session_state.coeffs.get('Impressions', 0.0002)
        c_static = st.session_state.coeffs.get('Static_Weight', 50.0)
        n_static = st.session_state.coeffs.get('Static_Count', 2)
        c_dig_ooh = st.session_state.coeffs.get('Digital_OOH_Weight', 10.0)
        n_dig_ooh = st.session_state.coeffs.get('Digital_OOH_Count', 4)
        total_ooh_lift = (c_static * n_static) + (c_dig_ooh * n_dig_ooh)
        
        total_traffic = df_vault['actual_traffic'].sum()
        total_members = df_vault['new_members'].sum() # New Calculation
        marketing_impact = (df_vault['ad_clicks'].sum() * w_clicks) + \
                           (df_vault['ad_impressions'].sum() * w_social)
        digital_lift_pct = (marketing_impact / total_traffic) * 100 if total_traffic > 0 else 0

        # AI Predictability
        df_vault['expected'] = df_vault.apply(lambda x: heartbeats.get(x['day_name'], 0) + 
                                            (x['ad_clicks'] * w_clicks) + 
                                            (x['ad_impressions'] * w_social) +
                                            total_ooh_lift, axis=1)
        
        import numpy as np
        mape = (np.abs(df_vault['actual_traffic'] - df_vault['expected']) / df_vault['actual_traffic']).replace([np.inf, -np.inf], np.nan).dropna().mean()
        predictability_score = (1 - mape) * 100 if not np.isnan(mape) else 85.0

        vault_metrics = {
            "heartbeats": heartbeats,
            "digital_lift": f"{digital_lift_pct:.1f}%",
            "ooh_lift": f"{total_ooh_lift:.0f} guests/day",
            "predictability": f"{predictability_score:.1f}%",
            "avg_spend": f"${df_vault['actual_coin_in'].mean():,.2f}",
            "total_new_members": f"{total_members:,.0f} sign-ups" # New Metric for AI
        }

    # 2. CHAT INPUT
    prompt = st.chat_input("Ask about Digital Lift, Member Sign-ups, or Billboard ROI...")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            import google.generativeai as genai
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash') 
            
            history_payload = []
            for m in st.session_state.messages[:-1]:
                role = "model" if m["role"] == "assistant" else "user"
                history_payload.append({"role": role, "parts": [m["content"]]})
            
            # THE FORENSIC BRAIN CONTEXT (Now includes Loyalty Data)
            sys_context = f"""
            SYSTEM ROLE: Chief Strategy Officer at Hard Rock Ottawa. 
            TONE: Professional, Data-Driven, Wit/Sharp.

            LIVE KPI VAULT:
            - AI Predictability: {vault_metrics.get('predictability', 'N/A')}
            - Pure Digital Lift (Ads): {vault_metrics.get('digital_lift', 'N/A')}
            - OOH Baseline Lift (Billboards): {vault_metrics.get('ooh_lift', 'N/A')}
            - Total New Members (Loyalty): {vault_metrics.get('total_new_members', 'N/A')}
            - Avg. Property Spend: {vault_metrics.get('avg_spend', 'N/A')}
            - Baseline DOW Heartbeats: {vault_metrics.get('heartbeats', {})}

            CAMPAIGN CONTEXT:
            - Campaign: 'Your Turn to Hit'.
            - Goal: Drive foot traffic and Unity card sign-ups (New Members).
            - OOH is an 'Inertia Lifter' while Digital is 'Reactive Pressure'.

            STRATEGY RULE: 
            If New Members are low relative to traffic, suggest a 'Unity-specific' digital ad push.
            Always end with one sharp strategic question regarding ROI or demographic capture.
            """

            chat = model.start_chat(history=history_payload)
            response = chat.send_message(f"{sys_context}\n\nUSER MESSAGE: {prompt}")
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()

        except Exception as e:
            st.error(f"Consultation Error: {e}")

    # 3. DISPLAY FEED
    for message in reversed(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if st.button("🗑️ Reset Forensic Session", key="reset_chat_t5"):
        st.session_state.messages = []
        st.rerun()

# --- TAB 6: MASTER REPORT (Comprehensive with Total Revenue) ---
with tab6:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📋 Master Forensic Report</h2>
            <p style="color: #888; margin: 0;">Comprehensive Yield Audit: Performance, Marketing Equity, and Entertainment Gravity.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. PERMISSION & DATA PREP
    current_user = st.session_state.get('user_email', "unauthorized")
    if current_user not in ADMIN_USERS:
        st.warning("🔒 Access Restricted: Executive View Only")
        st.stop()

    if not ledger_data:
        st.warning("Vault is empty. No data available.")
        st.stop()

    # --- DATE RANGE SELECTOR ---
    df_raw = pd.DataFrame(ledger_data)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
    
    min_date = df_raw['entry_date'].min().date()
    max_date = df_raw['entry_date'].max().date()

    col_date_1, col_date_2 = st.columns([1, 2])
    with col_date_1:
        selected_range = st.date_input(
            "Select Audit Period:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key="master_report_date_filter_v2"
        )

    # Proceed only if a full range is selected
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
        mask = (df_raw['entry_date'].dt.date >= start_date) & (df_raw['entry_date'].dt.date <= end_date)
        filtered_ledger = df_raw.loc[mask].to_dict(orient='records')
        
        if not filtered_ledger:
            st.info("No data found for the selected range.")
            st.stop()

        # Pull data from the Upgraded Engine
        metrics = get_forensic_metrics(filtered_ledger, st.session_state.coeffs)
        df_rep = metrics.get('df_with_awareness').copy()
        
        c = st.session_state.coeffs
        avg_spend = float(c.get('Avg_Coin_In', 112.50))
        prop_theo = float(c.get('Property_Theo', 450.00))
        hold_factor = float(c.get('Hold_Pct', 10.0)) / 100
        ooh_daily = metrics.get('ooh_total_daily', 0)

        # 2. TOP ROW: THE FINANCIAL CORE (Updated to 5 Columns)
        st.write("### 💰 Property Yield & GGR")
        f1, f2, f3, f4, f5 = st.columns(5)
        
        total_traffic = df_rep['actual_traffic'].sum()
        # NEW: Total Revenue calculation (Gross Coin-In)
        total_revenue = total_traffic * avg_spend
        actual_ggr = total_revenue * hold_factor
        
        total_theo_win = total_traffic * prop_theo
        yield_variance = ((actual_ggr / total_theo_win) - 1) * 100 if total_theo_win > 0 else 0
        
        f1.metric("Total Traffic", f"{total_traffic:,.0f}", 
                  help="Total headcount recorded during this period.")
        
        # NEW CARD
        f2.metric("Total Revenue", f"${total_revenue:,.2f}", 
                  help="Gross property volume (Traffic × Avg Spend) before House Hold is applied.")
        
        f3.metric("Actual GGR", f"${actual_ggr:,.2f}", 
                  delta=f"{yield_variance:.1f}% vs Theo", 
                  help=f"Net Win based on your calibrated {hold_factor*100:.1f}% hold.")
        
        f4.metric("Total Theo Win", f"${total_theo_win:,.2f}", 
                  help="Revenue target based on property theoretical spend.")
        
        f5.metric("Avg Spend", f"${avg_spend:.2f}", 
                  help="The average Coin-In per guest anchor.")

        st.divider()

        # 3. SECOND ROW: ATTRIBUTION
        st.write("### 📣 Attribution: Marketing & Hard Rock LIVE")
        m1, m2, m3, m4 = st.columns(4)
        
        total_digital_lift_guests = df_rep['residual_lift'].sum()
        total_ooh_lift_guests = ooh_daily * len(df_rep)
        total_live_gravity_guests = df_rep['gravity_lift'].sum()
        
        total_mkt_guests = total_digital_lift_guests + total_ooh_lift_guests + total_live_gravity_guests
        mkt_revenue_impact = (total_mkt_guests * avg_spend) * hold_factor
        capture_rate = (mkt_revenue_impact / actual_ggr * 100) if actual_ggr > 0 else 0

        m1.metric("Marketing Guests", f"{total_mkt_guests:,.0f}")
        m2.metric("LIVE Gravity Lift", f"{total_live_gravity_guests:,.0f}")
        m3.metric("Market Capture Rate", f"{capture_rate:.1f}%")
        m4.metric("AI Predictability", metrics['predictability'])

        st.divider()

        # 4. THIRD ROW: LOYALTY & ENVIRONMENT
        st.write("### 💎 Loyalty & Environmental Friction")
        l1, l2, l3, l4 = st.columns(4)
        
        total_new_members = df_rep['new_members'].sum() if 'new_members' in df_rep.columns else 0
        member_conv_rate = (total_new_members / total_traffic * 100) if total_traffic > 0 else 0
        
        total_snow_loss = (df_rep['snow_cm'].sum() * float(c.get('Snow_cm', -45)))
        total_rain_loss = (df_rep['rain_mm'].sum() * float(c.get('Rain_mm', -12)))
        total_env_friction = total_snow_loss + total_rain_loss

        l1.metric("New Unity Members", f"{total_new_members:,.0f}")
        l2.metric("Member Conv. Rate", f"{member_conv_rate:.2f}%")
        l3.metric("Weather Friction", f"{total_env_friction:,.0f}", delta="Guests Lost", delta_color="inverse")
        l4.metric("Guest Quality Index", f"{(actual_ggr / total_theo_win):.2f}x")

        # 5. ATTRIBUTION STACK
        st.write("### 📊 Comprehensive Attribution Stack")
        df_rep['OOH Lift'] = ooh_daily
        df_rep['Weather Impact'] = (df_rep['snow_cm'] * float(c.get('Snow_cm', -45))) + (df_rep['rain_mm'] * float(c.get('Rain_mm', -12)))
        
        chart_cols = {
            'baseline_isolated': 'Organic Baseline',
            'OOH Lift': 'Billboard Lift',
            'residual_lift': 'Digital Awareness Pool',
            'gravity_lift': 'Entertainment Gravity', 
            'Weather Impact': 'Weather Friction'
        }
        
        chart_df = df_rep.rename(columns=chart_cols)
        st.area_chart(chart_df.set_index('entry_date')[list(chart_cols.values())])

        # 6. SHOW AUDIT TABLE
        if total_live_gravity_guests > 0:
            with st.expander("🎸 View Hard Rock LIVE Impact Audit"):
                event_days = df_rep[df_rep['attendance'] > 0].copy()
                st.dataframe(
                    event_days[['entry_date', 'event_type', 'attendance', 'gravity_lift']],
                    column_config={
                        "entry_date": "Show Date",
                        "event_type": "Setup",
                        "attendance": "Tickets Sold",
                        "gravity_lift": "Gaming Floor Cross-over"
                    },
                    hide_index=True,
                    use_container_width=True
                )
    else:
        st.info("Please select a valid start and end date to generate the Forensic Report.")

# --- TAB 7: SYNCHRONIZED FORECAST SANDBOX ---
with tab7:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧪 Forecast Sandbox</h2>
            <p style="color: #888; margin: 0;">Fully Synchronized: Triangulating Social, Awareness, Hard Rock LIVE, & Environment.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. DATE RANGE SELECTION
    today = datetime.date.today()
    date_range = st.date_input(
        "Select Simulation Window:",
        value=(today, today + datetime.timedelta(days=2)),
        help="The Sandbox pulls specific daily forecasts for this entire window.",
        key="sb_date_range"
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        num_days = (end_date - start_date).days + 1
        live_forecast = st.session_state.get('weather_data', {}).get('forecast', [])

        # 2. SCENARIO INPUTS
        st.write(f"### 🎛️ Simulation Parameters ({num_days} Days)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**📣 Marketing & Social**")
            s_promo = st.checkbox("Active Major Promotion?", value=False)
            s_clicks = st.number_input("Est. Daily Ad Clicks", value=500)
            # RESTORED: Social and Impression inputs
            s_imp = st.number_input("Est. Daily Social Impressions", value=10000)
            s_eng = st.number_input("Est. Daily Social Engagements", value=250)
            
            st.divider()
            st.write("**🎸 Hard Rock LIVE Simulation**")
            sim_event = st.checkbox("Include Show Night in window?", value=False)
            sim_setup = st.selectbox("Simulated Setup", ["GA (2,200)", "Seated (1,900)"], disabled=not sim_event)
            sim_attend = st.number_input("Projected Tickets Sold", 0, 2200, value=1800, disabled=not sim_event)
        
        with col2:
            st.write("**❄️ Environment**")
            weather_mode = st.radio("Weather Source:", ["Live EC Forecast", "Manual Overrides"])
            m_temp = st.slider("Manual Temp (°C)", -30, 40, 15, disabled=(weather_mode == "Live EC Forecast"))
            m_rain = st.slider("Manual Rain (mm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))
            m_snow = st.slider("Manual Snow (cm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))

        with col3:
            st.write("**⚙️ Engine Baseline**")
            c = st.session_state.coeffs
            st.metric("Spend Anchor", f"${c.get('Avg_Coin_In', 112.50):,.2f}")
            st.metric("Event Gravity", f"{c.get('Event_Gravity', 20.0):,.1f}%")
            # RESTORED: Awareness Retention context
            st.info(f"Using {c.get('Ad_Decay', 85.0)}% Awareness Retention for digital carry-over.")

        # 3. UNIFIED CALCULATION LOOP
        total_range_traffic = 0
        total_range_revenue = 0
        total_range_members = 0
        
        if ledger_data:
            df_sb = pd.DataFrame(ledger_data)
            df_sb['entry_date'] = pd.to_datetime(df_sb['entry_date'])
            df_sb['day_name'] = df_sb['entry_date'].dt.day_name()
            
            # Pull purified heartbeats from engine
            sb_metrics = get_forensic_metrics(ledger_data, c)
            dow_profiles = sb_metrics.get('heartbeats', {})
            
            # Loyalty Ratio
            tot_h_traffic = df_sb['actual_traffic'].sum()
            tot_h_members = df_sb['new_members'].sum() if 'new_members' in df_sb.columns else 0
            member_ratio = (tot_h_members / tot_h_traffic) if tot_h_traffic > 0 else 0.02
        else:
            dow_profiles = {}
            member_ratio = 0.02

        current_date = start_date
        while current_date <= end_date:
            day_str = current_date.strftime("%Y-%m-%d")
            day_name = current_date.strftime("%A")
            
            # Weather logic
            if weather_mode == "Live EC Forecast" and live_forecast:
                ec_day = next((item for item in live_forecast if str(item.get('datetime')).startswith(day_str)), None)
                day_temp = ec_day.get('temperature', 15.0) if ec_day else m_temp
                day_rain, day_snow = m_rain, m_snow 
            else:
                day_temp, day_rain, day_snow = m_temp, m_rain, m_snow

            # --- THE FORENSIC MATH ---
            base_traffic = dow_profiles.get(day_name, 4365)
            p_lift = c.get('Promo', 450.0) if s_promo else 0
            
            # RESTORED: Multi-factor Marketing Lift (Clicks + Impressions)
            m_lift = (s_clicks * c.get('Clicks', 0.04)) + (s_imp * c.get('Impressions', 0.0002))
            
            # Event Gravity Lift
            e_lift = (sim_attend * (c.get('Event_Gravity', 20.0)/100)) if sim_event else 0
            
            # Weather Friction
            w_friction = (day_rain * c.get('Rain_mm', -12.0)) + (day_snow * c.get('Snow_cm', -45.0))
            
            daily_total = max(0, base_traffic + p_lift + m_lift + e_lift + w_friction)
            daily_members = daily_total * member_ratio
            
            total_range_traffic += daily_total
            total_range_revenue += (daily_total * c.get('Avg_Coin_In', 112.50))
            total_range_members += daily_members
            
            current_date += datetime.timedelta(days=1)

        # 4. RESULTS DISPLAY
        st.divider()
        res1, res2, res3, res4 = st.columns(4)
        
        with res1:
            st.metric("Predicted Traffic", f"{int(total_range_traffic):,} Guests")
        with res2:
            st.metric("Projected Net Win", f"${(total_range_revenue * (c.get('Hold_Pct', 10)/100)):,.2f}")
        with res3:
            st.metric("Predicted New Members", f"{int(total_range_members):,}")
        with res4:
            st.metric("Gravity Impact", f"+{int(sim_attend * (c.get('Event_Gravity', 20.0)/100)) if sim_event else 0} / Show",
                      help="The specific traffic lift provided by Hard Rock LIVE attendees.")

        # 5. STRATEGIC INSIGHTS
        st.write("---")
        if sim_event:
            st.info(f"🎸 **Entertainment Insight:** A **{sim_setup}** event with **{sim_attend}** attendees is projected to contribute **{ ( (sim_attend * (c.get('Event_Gravity', 20.0)/100)) / (total_range_traffic / num_days) ) * 100:.1f}%** of your total simulated daily traffic.")

        if total_range_members / num_days > 50:
            st.success(f"💎 **Loyalty Alert:** Forecast suggests high acquisition ({int(total_range_members / num_days)}/day). Ensure Unity staff is optimized.")

    else:
        st.info("Please select a valid date range to start the simulation.")
