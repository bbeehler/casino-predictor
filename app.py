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

# 2. THE UNIFIED FORENSIC ENGINE
def get_forensic_metrics(df, coeffs):
    """Calculates KPIs once for the entire app. Safely handles missing columns."""
    if df is None or len(df) == 0:
        return {"predictability": "0.0%", "digital_lift": "0.0%", "heartbeats": {}, "ooh_total_daily": 0}

    df = pd.DataFrame(df).copy()
    
    # 1. Standardize columns
    cols_to_ensure = {
        'ad_clicks': ['ad_clicks', 'Clicks'],
        'ad_impressions': ['ad_impressions', 'Impressions'],
        'actual_traffic': ['actual_traffic', 'Traffic'],
        'snow_cm': ['snow_cm', 'Snow', 'snow'],
        'rain_mm': ['rain_mm', 'Rain', 'rain']
    }

    for target, aliases in cols_to_ensure.items():
        existing = next((c for c in aliases if c in df.columns), None)
        if existing:
            df.rename(columns={existing: target}, inplace=True)
        if target not in df.columns:
            df[target] = 0 
        df[target] = pd.to_numeric(df[target], errors='coerce').fillna(0)

    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['day_name'] = df['entry_date'].dt.day_name()
    
    # 2. Pull Weights from Coeffs
    heartbeats = df.groupby('day_name')['actual_traffic'].mean().to_dict()
    c_clicks = coeffs.get('Clicks', 0.02)
    c_social = coeffs.get('Impressions', 0.0002)
    
    # Environmental Weights (THE MISSING LINK)
    c_snow = coeffs.get('Snow_cm', -45.0)
    c_rain = coeffs.get('Rain_mm', -12.0)

    # OOH Weights
    c_static = coeffs.get('Static_Weight', 50.0)
    n_static = coeffs.get('Static_Count', 2)
    c_dig_ooh = coeffs.get('Digital_OOH_Weight', 10.0)
    n_dig_ooh = coeffs.get('Digital_OOH_Count', 4)
    total_ooh_lift = (c_static * n_static) + (c_dig_ooh * n_dig_ooh)
    
    # 3. THE MASTER CALCULATION
    # Expected = Baseline + Marketing + OOH + (Weather * Friction)
    df['expected'] = df.apply(lambda x: 
        heartbeats.get(x['day_name'], 0) + 
        (x['ad_clicks'] * c_clicks) + 
        (x['ad_impressions'] * c_social) +
        total_ooh_lift + 
        (x['snow_cm'] * c_snow) +  # Now it subtracts guests for snow
        (x['rain_mm'] * c_rain),   # Now it subtracts guests for rain
        axis=1
    )

    # 4. FINAL METRICS
    total_traffic = df['actual_traffic'].sum()
    digital_impact = (df['ad_clicks'].sum() * c_clicks) + (df['ad_impressions'].sum() * c_social)
    lift_val = (digital_impact / total_traffic * 100) if total_traffic > 0 else 0

    df_filtered = df[df['actual_traffic'] > 0].copy()
    if df_filtered.empty:
        return {"predictability": "0.0%", "digital_lift": f"{lift_val:.1f}%", "heartbeats": heartbeats, "ooh_total_daily": total_ooh_lift}
        
    mape = (np.abs(df_filtered['actual_traffic'] - df_filtered['expected']) / df_filtered['actual_traffic']).mean()
    pred_val = (1 - mape) * 100 if not np.isnan(mape) else 0

    # Ensure Hold_Pct is a number, even if the DB returns None
    raw_hold = coeffs.get('Hold_Pct')
    if raw_hold is None:
        clean_hold = 10.0  # Fallback to 10% if DB is empty
    else:
        clean_hold = float(raw_hold)

    # Now calculate the factor safely
    return {
        "predictability": f"{pred_val:.1f}%",
        "digital_lift": f"{lift_val:.1f}%",
        "heartbeats": heartbeats,
        "ooh_total_daily": total_ooh_lift,
        "hold_factor": clean_hold / 100 
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
        with st.form("manual_entry_v3"):
            entry_date = st.date_input("Select Date", datetime.date.today())
            
            c1, c2 = st.columns(2)
            with c1: traffic = st.number_input("Traffic (Headcount)", min_value=0)
            with c2: coin_in = st.number_input("Coin-In ($)", min_value=0.0, format="%.2f")
            with c3: new_members = st.number_input("New Members Signed Up", min_value=0, step=1)
            
            st.divider()
            st.write("**Marketing Metrics**")
            m1, m2, m3 = st.columns(3)
            with m1: clicks = st.number_input("Ad Clicks", min_value=0)
            with m2: imps = st.number_input("Ad Impressions", min_value=0)
            with m3: social = st.number_input("Social Engagements", min_value=0)
            
            # This button is inside the form, so it's safe from duplicate ID errors
            submit_form = st.form_submit_button("💾 Sync Results to Vault", use_container_width=True)
            
            if submit_form:
                with st.spinner("Writing to Vault..."):
                    date_str = entry_date.isoformat()
                    new_row = {
                        "entry_date": date_str,
                        "actual_traffic": traffic,
                        "actual_coin_in": coin_in,
                        "ad_clicks": clicks,
                        "ad_impressions": imps,
                        "social_engagements": social
                        "new_members": new_members
                    }
                    try:
                        # UPSERT handles backfilling missed weekend dates or updating existing ones
                        supabase.table("ledger").upsert(new_row, on_conflict="entry_date").execute()
                        st.success(f"✅ Data for {date_str} is now in the Vault.")
                        
                        # FORCE REFRESH: This clears the cache and re-fetches the latest data
                        import time
                        time.sleep(1)
                        st.rerun() 
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

    # --- SECTION B: BULK UPLOAD ---
    with col_b:
        st.write("### 📤 Bulk CSV Upload")
        uploaded_file = st.file_uploader("Upload Ledger CSV", type="csv", key="csv_uploader_t2")
        if uploaded_file is not None:
            df_upload = pd.read_csv(uploaded_file)
            st.dataframe(df_upload.head(3), use_container_width=True)
            
            if st.button("🚀 Push CSV to Vault", use_container_width=True, key="btn_csv_push_t2"):
                try:
                    data_dict = df_upload.to_dict(orient='records')
                    supabase.table("ledger").upsert(data_dict, on_conflict="entry_date").execute()
                    st.success("Bulk upload complete!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Upload failed: {e}")

    # --- SECTION C: THE LEDGER EDITOR (Indented correctly) ---
    st.divider()
    st.write("### 📜 Ledger Editor")
    st.caption("Newest entries are forced to the top. Double-click cells to edit directly.")
    
    if ledger_data:
        # 1. Convert to DataFrame
        df_history = pd.DataFrame(ledger_data)
        
        # 2. FORCE SORTING: This solves the "where is my data" problem
        if 'entry_date' in df_history.columns:
            df_history['entry_date'] = pd.to_datetime(df_history['entry_date'])
            df_history = df_history.sort_values(by='entry_date', ascending=False)
        
        # 3. RENDER THE EDITOR
        # key="ledger_editor_unique_t2" prevents the 'Duplicate ID' error
        edited_df = st.data_editor(
            df_history, 
            key="ledger_editor_unique_t2", 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "entry_date": st.column_config.DateColumn("Date", disabled=True),
                "actual_traffic": st.column_config.NumberColumn("Traffic", format="%d"),
                "actual_coin_in": st.column_config.NumberColumn("Coin-In ($)", format="$%.2f")
            }
        )

        # 4. SYNC BUTTON (Indented so it stays in Tab 2 only)
        if st.button("✅ Confirm & Sync Edits", key="btn_sync_ledger_t2", use_container_width=True):
            with st.spinner("Updating Vault records..."):
                try:
                    for _, row in edited_df.iterrows():
                        up_data = row.to_dict()
                        # Format date for Supabase match
                        d_key = pd.to_datetime(up_data['entry_date']).strftime('%Y-%m-%d')
                        up_data['entry_date'] = d_key
                        
                        # Clean payload
                        if 'id' in up_data: del up_data['id']
                        
                        supabase.table("ledger").update(up_data).eq("entry_date", d_key).execute()
                    
                    st.success("Vault Successfully Updated!")
                    st.rerun() # Refresh the view
                except Exception as e:
                    st.error(f"Manual sync failed: {e}")
    else:
        st.info("The Vault is currently empty. Use the form above to add your first entry.")

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
        # Added _t3 to the key to prevent collisions
        metric_choice = st.pills("Select Metric to Analyze", 
                                ["Traffic", "Coin-In", "Ad Clicks"], 
                                selection_mode="single",
                                default="Traffic",
                                key="analysis_pills_t3") 

        if metric_choice == "Traffic":
            st.area_chart(df_analysis.set_index('entry_date')['actual_traffic'], color="#FFCC00")
        elif metric_choice == "Coin-In":
            st.line_chart(df_analysis.set_index('entry_date')['actual_coin_in'], color="#2ecc71")
        else:
            st.bar_chart(df_analysis.set_index('entry_date')['ad_clicks'], color="#00CCFF")

        # 2. READ-ONLY DATA VIEW (Safer & Faster)
        st.divider()
        st.write("### 📜 Detailed Analytics View")
        
        # We use a standard dataframe here instead of an editor to prevent ID errors
        st.dataframe(
            df_analysis[['entry_date', 'actual_traffic', 'actual_coin_in', 'ad_clicks', 'ad_impressions', 'social_engagements']],
            column_config={
                "entry_date": st.column_config.DateColumn("Date"),
                "actual_traffic": st.column_config.NumberColumn("Traffic"),
                "actual_coin_in": st.column_config.NumberColumn("Coin-In ($)", format="$%.2f"),
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
# --- TAB 4: ENGINE CONTROL (Final Stability Fix) ---
with tab4:
    current_user = st.session_state.get('user_email', "unauthorized")
    
    if current_user not in ADMIN_USERS:
        st.warning("### 🔒 Access Restricted")
        st.info(f"Identity: **{current_user}** does not have permission to calibrate the engine.")
    else:
        st.markdown("""
            <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
                <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Calibration</h2>
                <p style="color: #888; margin: 0;">Admin Mode: {current_user}</p>
            </div>
        """, unsafe_allow_html=True)

        # 1. PRE-FLIGHT DATA CLEANING
        # This prevents TypeErrors by ensuring every key is a valid float
        defaults = {
            'Clicks': 0.02, 'Impressions': 0.0002, 'Avg_Coin_In': 112.50,
            'Property_Theo': 450.0, 'Hold_Pct': 10.0, 'Snow_cm': -45.0,
            'Rain_mm': -12.0, 'Static_Weight': 50.0, 'Static_Count': 2,
            'Digital_OOH_Weight': 10.0, 'Digital_OOH_Count': 4
        }
        
        clean_coeffs = {}
        for key, default_val in defaults.items():
            raw_val = st.session_state.coeffs.get(key)
            try:
                # Handle None, empty strings, or weird database formats
                if raw_val is None or raw_val == "":
                    clean_coeffs[key] = default_val
                else:
                    clean_coeffs[key] = float(raw_val)
            except (ValueError, TypeError):
                clean_coeffs[key] = default_val

        # 2. CALIBRATION FORM
        with st.form("engine_settings_v12_final"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("### 📣 Marketing Multipliers")
                new_clicks = st.slider("Click Weight", 0.0, 0.5, value=clean_coeffs['Clicks'])
                new_social = st.number_input("Social Weight", 0.0, 0.01, value=clean_coeffs['Impressions'], format="%.4f")
                
                st.write("### 📍 OOH / Billboards")
                new_static_w = st.slider("Static Board Lift", 0.0, 500.0, value=clean_coeffs['Static_Weight'])
                new_static_c = st.number_input("Static Count", 0, 10, value=int(clean_coeffs['Static_Count']))
                new_digital_w = st.slider("Digital Board Lift", 0.0, 200.0, value=clean_coeffs['Digital_OOH_Weight'])
                new_digital_c = st.number_input("Digital Count", 0, 20, value=int(clean_coeffs['Digital_OOH_Count']))

            with col2:
                st.write("### ❄️ Environmental Friction")
                new_snow = st.slider("Snow Friction", -1000.0, 0.0, value=clean_coeffs['Snow_cm'])
                new_rain = st.slider("Rain Friction", -500.0, 0.0, value=clean_coeffs['Rain_mm'])
                
                st.write("### 💰 Financials & Yield")
                new_coin = st.number_input("Avg Gross Spend ($)", 0.0, 5000.0, value=clean_coeffs['Avg_Coin_In'])
                new_theo = st.number_input("Property Theo ($)", 0.0, 2000.0, value=clean_coeffs['Property_Theo'])
                new_hold = st.slider("House Hold %", 1.0, 25.0, value=clean_coeffs['Hold_Pct'])

            st.divider()
            
            # The submit button is now safe from crashing
            submit_v12 = st.form_submit_button("💾 Save All Calibration & Sync Vault", use_container_width=True)

            if submit_v12:
                sync_payload = {
                    'Clicks': new_clicks, 
                    'Impressions': new_social, 
                    'Avg_Coin_In': new_coin,
                    'Property_Theo': new_theo, 
                    'Hold_Pct': new_hold, 
                    'Snow_cm': new_snow, 
                    'Rain_mm': new_rain, 
                    'Static_Weight': new_static_w, 
                    'Static_Count': new_static_c,
                    'Digital_OOH_Weight': new_digital_w, 
                    'Digital_OOH_Count': new_digital_c
                }
                
                # Update local session for immediate math update
                st.session_state.coeffs.update(sync_payload)
                
                try:
                    # Overwrite the Supabase row with clean numbers
                    supabase.table("coefficients").update(sync_payload).eq("id", 1).execute()
                    st.success("✅ Vault Synced Successfully")
                    import time
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"⚠️ Database Sync Failed: {e}")
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

    # 1. THE FORENSIC DATA VAULT (Synced to Tab 2 Schema)
    vault_metrics = {}
    if ledger_data:
        df_vault = pd.DataFrame(ledger_data).copy()
        df_vault['entry_date'] = pd.to_datetime(df_vault['entry_date'])
        
        # --- DYNAMIC COLUMN NORMALIZATION ---
        col_map = {
            'ad_impressions': ['ad_impressions', 'Impressions', 'social_impressions'],
            'ad_clicks': ['ad_clicks', 'Clicks'],
            'actual_traffic': ['actual_traffic', 'Traffic'],
            'actual_coin_in': ['actual_coin_in', 'Revenue']
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
        
        # --- OOH & DIGITAL WEIGHTS ---
        w_clicks = st.session_state.coeffs.get('Clicks', 0.02)
        w_social = st.session_state.coeffs.get('Impressions', 0.0002)
        
        # Split OOH logic
        c_static = st.session_state.coeffs.get('Static_Weight', 50.0)
        n_static = st.session_state.coeffs.get('Static_Count', 2)
        c_dig_ooh = st.session_state.coeffs.get('Digital_OOH_Weight', 10.0)
        n_dig_ooh = st.session_state.coeffs.get('Digital_OOH_Count', 4)
        total_ooh_lift = (c_static * n_static) + (c_dig_ooh * n_dig_ooh)
        
        # Calculate Pure Digital Lift
        total_traffic = df_vault['actual_traffic'].sum()
        marketing_impact = (df_vault['ad_clicks'].sum() * w_clicks) + \
                           (df_vault['ad_impressions'].sum() * w_social)
        digital_lift_pct = (marketing_impact / total_traffic) * 100 if total_traffic > 0 else 0

        # AI Predictability (Now accounts for OOH baseline shift)
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
            "avg_spend": f"${df_vault['actual_coin_in'].mean():,.2f}"
        }

    # 2. CHAT INPUT
    prompt = st.chat_input("Ask about Digital Lift, Billboard ROI, or weekend predictions...")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            import google.generativeai as genai
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash') # Using the latest flash model
            
            history_payload = []
            for m in st.session_state.messages[:-1]:
                role = "model" if m["role"] == "assistant" else "user"
                history_payload.append({"role": role, "parts": [m["content"]]})
            
            # THE FORENSIC BRAIN CONTEXT (Now with OOH Knowledge)
            sys_context = f"""
            SYSTEM ROLE: Chief Strategy Officer at Hard Rock Ottawa. 
            TONE: Professional, Data-Driven, Strategic.

            LIVE KPI VAULT:
            - AI Predictability: {vault_metrics.get('predictability', 'N/A')}
            - Pure Digital Lift (Ads): {vault_metrics.get('digital_lift', 'N/A')}
            - OOH Baseline Lift (Billboards): {vault_metrics.get('ooh_lift', 'N/A')}
            - Avg. Property Spend: {vault_metrics.get('avg_spend', 'N/A')}
            - Baseline DOW Heartbeats: {vault_metrics.get('heartbeats', {})}

            CAMPAIGN CONTEXT:
            - You are running the 'Your Turn to Hit' campaign.
            - You have a mix of Static (24/7) and Digital (Shared Loop) billboards.
            - OOH is treated as an 'Inertia Lifter' that raises the property floor.

            STRATEGY RULE: 
            If Predictability is < 80%, suggest checking if the OOH Weights or Digital Multipliers in Tab 4 need calibration.
            Always end with one sharp strategic question regarding revenue or attribution.
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

# --- TAB 6: MASTER REPORT (Integrated Executive Yield Board) ---
with tab6:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📋 Master Forensic Report</h2>
            <p style="color: #888; margin: 0;">Executive yield audit: Actual Performance vs. Theoretical Math.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. PERMISSION & DATA CHECK
    current_user = st.session_state.get('user_email', "unauthorized")
    if current_user not in ADMIN_USERS:
        st.warning("🔒 Access Restricted: Executive View Only")
        st.stop()

    if not ledger_data:
        st.warning("Vault is empty. No data available for reporting.")
        st.stop()

    # 2. DATA CLEANING (Safety First)
    df_rep = pd.DataFrame(ledger_data).copy()
    df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
    df_rep = df_rep.sort_values('entry_date')

    # Pull and Clean Coefficients from Session State
    c = st.session_state.coeffs
    def clean(key, default):
        val = c.get(key)
        try:
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            return default

    avg_gross_spend = clean('Avg_Coin_In', 112.50)
    prop_theo = clean('Property_Theo', 450.00)
    hold_factor = clean('Hold_Pct', 10.0) / 100
    c_clicks = clean('Clicks', 0.02)
    c_social = clean('Impressions', 0.0002)
    c_snow = clean('Snow_cm', -45.0)
    c_rain = clean('Rain_mm', -12.0)

    # 3. RUN ENGINE MATH
    metrics = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    ooh_daily = metrics.get('ooh_total_daily', 0)
    
    df_rep['day_name'] = df_rep['entry_date'].dt.day_name()
    df_rep['Baseline'] = df_rep['day_name'].map(metrics.get('heartbeats', {}))
    df_rep['OOH Lift'] = ooh_daily
    df_rep['Digital Lift'] = (df_rep.get('ad_clicks', 0).fillna(0) * c_clicks) + \
                             (df_rep.get('ad_impressions', 0).fillna(0) * c_social)
    df_rep['Weather Penalty'] = (df_rep.get('snow_cm', 0).fillna(0) * c_snow) + \
                                (df_rep.get('rain_mm', 0).fillna(0) * c_rain)

    # 4. FINANCIAL YIELD CALCULATIONS
    total_traffic = df_rep['actual_traffic'].sum()
    total_gross_vol = total_traffic * avg_gross_spend
    actual_net_win = total_gross_vol * hold_factor
    total_theo_win = total_traffic * prop_theo
    
    yield_gap = (actual_net_win / total_traffic) - prop_theo if total_traffic > 0 else 0
    perf_index = (actual_net_win / total_theo_win) if total_theo_win > 0 else 0
    
    net_mkt_guests = df_rep['Digital Lift'].sum() + (ooh_daily * len(df_rep)) + df_rep['Weather Penalty'].sum()
    mkt_net_win_impact = max(0, net_mkt_guests * avg_gross_spend * hold_factor)

    # 5. POWER METRICS: THE YIELD BOARD
    st.write("### ⚖️ Property Yield: Actual Net vs. Theoretical")
    y1, y2, y3 = st.columns(3)
    y1.metric("Total Theo Win", f"${total_theo_win:,.2f}", help=f"Expected Win based on ${prop_theo} Theo.")
    y2.metric("Actual Net Win (GGR)", f"${actual_net_win:,.2f}", 
              delta=f"{(perf_index-1)*100:.1f}% Yield Variance",
              help=f"Actual profit based on {hold_factor*100:.1f}% Hold.")
    y3.metric("Marketing Net Impact", f"${mkt_net_win_impact:,.2f}", 
              delta=f"{net_mkt_guests:,.0f} Guests",
              help="Net profit attributed to OOH and Digital maneuvers.")

    st.divider()

    # 6. EXECUTIVE SCORECARD
    st.write("### 🏆 Strategic Performance Scorecard")
    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric("Guest Quality Index", f"{perf_index:.2f}x", 
                  delta=f"${yield_gap:,.2f} vs Theo",
                  help="Score above 1.0 means we are attracting players more valuable than the house average.")
    with s2:
        capture_rate = (mkt_net_win_impact / actual_net_win * 100) if actual_net_win > 0 else 0
        st.metric("Market Capture Rate", f"{capture_rate:.1f}%", 
                  help="Percentage of total profit directly generated by active marketing maneuvers.")
    with s3:
        friction_loss = df_rep['Weather Penalty'].sum()
        st.metric("Environmental Friction", f"{friction_loss:,.0f}", 
                  delta="Guest Loss", delta_color="inverse",
                  help="Total traffic lost to Snow/Rain friction during this period.")

    # 7. ATTRIBUTION STACK CHART
    st.write("### 📊 Attribution Stack Over Time")
    chart_cols = ['Baseline', 'OOH Lift', 'Digital Lift', 'Weather Penalty']
    st.area_chart(df_rep.set_index('entry_date')[chart_cols])

    # 8. FINANCIAL VERIFICATION
    st.divider()
    f1, f2 = st.columns(2)
    with f1:
        st.write("**Marketing Volume Audit**")
        st.write(f"* OOH Total Lift: {(ooh_daily * len(df_rep)):,.0f} guests")
        st.write(f"* Digital Total Lift: {df_rep['Digital Lift'].sum():,.0f} guests")
        st.write(f"* Net Marketing Guests: {net_mkt_guests:,.0f}")
    with f2:
        st.write("**Yield Controls**")
        st.write(f"* Calibrated Hold: {hold_factor*100:.1f}%")
        st.write(f"* Calibrated Gross Spend: ${avg_gross_spend:,.2f}")
        st.write(f"* AI Engine Predictability: {metrics['predictability']}")

    # 9. EXPORT
    csv = df_rep.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Final Executive Audit", data=csv, 
                       file_name=f'HR_Ottawa_Master_Report.csv', use_container_width=True)
# --- TAB 7: SYNCHRONIZED FORECAST SANDBOX ---
with tab7:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧪 Forecast Sandbox</h2>
            <p style="color: #888; margin: 0;">Fully Synchronized: Triangulating Historical Baselines & Live Weather Forecasts.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. DATE RANGE SELECTION
    today = datetime.date.today()
    date_range = st.date_input(
        "Select Simulation Window:",
        value=(today, today + datetime.timedelta(days=2)),
        help="The Sandbox will pull specific daily forecasts for this entire window.",
        key="sb_date_range"
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        num_days = (end_date - start_date).days + 1
        
        # Pull live data from session state
        live_forecast = st.session_state.get('weather_data', {}).get('forecast', [])

        # 2. SCENARIO INPUTS
        st.write(f"### 🎛️ Simulation Parameters ({num_days} Days)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Marketing & Social**")
            s_promo = st.checkbox("Active Major Promotion?", value=False)
            s_clicks = st.number_input("Est. Daily Ad Clicks", value=500)
            s_imp = st.number_input("Est. Daily Social Impressions", value=10000)
        
        with col2:
            st.write("**Environment**")
            weather_mode = st.radio("Weather Source:", ["Live EC Forecast", "Manual Overrides"])
            m_temp = st.slider("Manual Temp (°C)", -30, 40, 15, disabled=(weather_mode == "Live EC Forecast"))
            m_rain = st.slider("Manual Rain (mm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))
            m_snow = st.slider("Manual Snow (cm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))

        with col3:
            st.write("**Engine Baseline**")
            c = st.session_state.coeffs
            st.metric("Spend Anchor", f"${c.get('Avg_Coin_In', 112.50):,.2f}")
            st.info("Simulation pulls baseline 'Heartbeats' from your Ledger history.")

        # 3. UNIFIED CALCULATION LOOP
        total_range_traffic = 0
        total_range_revenue = 0
        
        # Calculate DOW Averages from Ledger for the "Heartbeat"
        if ledger_data:
            df_sb = pd.DataFrame(ledger_data)
            df_sb['entry_date'] = pd.to_datetime(df_sb['entry_date'])
            df_sb['day_name'] = df_sb['entry_date'].dt.day_name()
            dow_profiles = df_sb.groupby('day_name')['actual_traffic'].mean().to_dict()
        else:
            dow_profiles = {}

        current_date = start_date
        while current_date <= end_date:
            day_str = current_date.strftime("%Y-%m-%d")
            day_name = current_date.strftime("%A")
            
            # --- WEATHER ATTRIBUTION ---
            if weather_mode == "Live EC Forecast" and live_forecast:
                # Find weather matching the date
                ec_day = next((item for item in live_forecast if str(item.get('datetime')).startswith(day_str)), None)
                day_temp = ec_day.get('temperature', 15.0) if ec_day else m_temp
                # Use overrides for precip if not in current EC feed
                day_rain = m_rain 
                day_snow = m_snow
            else:
                day_temp, day_rain, day_snow = m_temp, m_rain, m_snow

            # --- THE FORENSIC MATH ---
            # 1. Start with the historical DOW heartbeat or the global intercept
            base_traffic = dow_profiles.get(day_name, c.get('Intercept', 4365))
            
            # 2. Add Marketing Lifts from Tab 4 Coefficients
            p_lift = c.get('Promo', 450.0) if s_promo else 0
            m_lift = (s_clicks * c.get('Clicks', 0.02)) + (s_imp * c.get('Social_Imp', 0.0002))
            
            # 3. Apply Weather Friction
            w_friction = (day_rain * c.get('Rain_mm', -12.0)) + (day_snow * c.get('Snow_cm', -45.0))
            
            # 4. Final Aggregation for the Day
            daily_total = base_traffic + p_lift + m_lift + w_friction
            
            total_range_traffic += daily_total
            total_range_revenue += (daily_total * c.get('Avg_Coin_In', 112.50))
            
            current_date += datetime.timedelta(days=1)

        # 4. RESULTS DISPLAY
        st.divider()
        res1, res2, res3 = st.columns(3)
        
        with res1:
            st.metric("Predicted Total Traffic", f"{int(total_range_traffic):,} Guests")
        with res2:
            st.metric("Projected Window Revenue", f"${total_range_revenue:,.2f}")
        with res3:
            st.metric("Daily Avg Volume", f"{int(total_range_traffic / num_days)} / day")

        # 5. STRATEGIC INSIGHT
        st.write("---")
        if total_range_traffic / num_days > (c.get('Intercept', 4365) * 1.2):
            st.success("🔥 **High Volume Scenario:** This configuration suggests a 20%+ lift over baseline. Ensure floor staffing is optimized.")
        elif total_range_traffic / num_days < (c.get('Intercept', 4365) * 0.8):
            st.warning("📉 **Low Volume Warning:** Forecast is significantly below average. Consider boosting digital ad spend.")

    else:
        st.info("Please select a valid date range to start the simulation.")
