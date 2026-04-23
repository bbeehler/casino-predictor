import streamlit as st
import pandas as pd
import datetime
import json
import asyncio
import numpy as np
from env_canada import ECWeather
import google.generativeai as genai
from supabase import create_client

# --- THE PERMANENT INITIALIZATION LOCK ---
# This MUST be at the top of your script, after imports
if 'coeffs' not in st.session_state:
    # These are only used if the database/vault is completely empty
    st.session_state.coeffs = {
        'Static_Count': 10,
        'Static_Weight': 15.0,
        'Digital_OOH_Count': 5,
        'Digital_OOH_Weight': 25.0,
        'Clicks': 0.05,
        'Social_Imp': 0.0002,
        'Social_Eng': 0.01,
        'Event_Gravity': 25.0,
        'Avg_Coin_In': 112.50,
        'Property_Theo': 450.00,
        'Hold_Pct': 10.0,
        'Snow_cm': -45,
        'Rain_mm': -12,
        'Ad_Decay': 85.0
    }

# 1. PAGE CONFIG (Must be the very first Streamlit command)
st.set_page_config(page_title="FloorCast | Hard Rock Ottawa", layout="wide", page_icon="🎰")

# 2. LIGHT BLUE & GREY THEME INJECTION
st.markdown("""
    <style>
    /* Global Background: Light Grey */
    .stApp {
        background-color: #F0F2F6 !important;
    }

    /* Force ALL text to Black for legibility */
    * {
        color: #000000 !important;
    }

    /* Target Widget Labels & Markdown specifically */
    label p, .stMarkdown p, [data-testid="stWidgetLabel"] {
        color: #000000 !important;
    }

    /* Metric Card Styling: Light Blue with Darker Blue Border */
    div[data-testid="metric-container"] {
        background-color: #E1E8F0 !important; /* Light Blue-Grey */
        border: 1px solid #B0C4DE !important;
        border-left: 5px solid #0047AB !important; /* Cobalt Blue Accent */
        padding: 15px !important;
        border-radius: 10px !important;
    }

    /* Metric Labels */
    [data-testid="stMetricLabel"] p {
        color: #0047AB !important; /* Cobalt Blue for Metric titles */
        font-weight: bold !important;
    }

    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #F8F9FA !important;
        border-right: 1px solid #DEE2E6 !important;
    }

    /* Input Fields: White boxes with Black text */
    input, textarea, select {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #CED4DA !important;
    }

    /* Analyst Status Bar */
    [data-testid="stStatus"] {
        background-color: #E7F3FF !important;
        border: 1px solid #0047AB !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- AUTH LIST ---
ADMIN_USERS = ["bjbeehler@gmail.com"]

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
    
    # 2. PULL CALIBRATED WEIGHTS
    c_clicks = float(coeffs.get('Clicks') or coeffs.get('clicks') or 0.04)
    c_social = float(coeffs.get('Impressions') or coeffs.get('impressions') or 0.0002)
    decay_rate = float(coeffs.get('Ad_Decay') or coeffs.get('ad_decay') or 85.0) / 100 
    
    raw_gravity = coeffs.get('event_gravity') or coeffs.get('Event_Gravity') or 20.0
    event_capture = float(raw_gravity) / 100
    
    ooh_w = float(coeffs.get('Static_Weight') or coeffs.get('static_weight') or 50.0)
    ooh_c = int(coeffs.get('Static_Count') or coeffs.get('static_count') or 2)
    dig_w = float(coeffs.get('Digital_OOH_Weight') or coeffs.get('digital_ooh_weight') or 10.0)
    dig_c = int(coeffs.get('Digital_OOH_Count') or coeffs.get('digital_ooh_count') or 4)
    total_ooh_lift = (ooh_w * ooh_c) + (dig_w * dig_c)
    
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

# 5. GATEKEEPER
if not st.session_state.user_authenticated:
    st.markdown("<div style='text-align:center; padding:50px;'><h1 style='color:#0047AB;'>🎰 FloorCast</h1><h3>Hard Rock Ottawa | Strategic Engine</h3></div>", unsafe_allow_html=True)
    with st.container(border=True):
        email_input = st.text_input("Email")
        pw_input = st.text_input("Password", type="password")
        if st.button("Access Engine", use_container_width=True, key="login_btn"):
            try:
                res = supabase.auth.sign_in_with_password({"email": email_input, "password": pw_input})
                if res.user:
                    st.session_state.user_authenticated = True
                    st.session_state.user_email = res.user.email
                    st.success(f"Welcome, {res.user.email}")
                    st.rerun() 
                else:
                    st.error("Authentication failed.")
            except Exception as e:
                st.error("Invalid credentials or connection error.")
    st.stop()

# --- 6. CRITICAL DATA HYDRATION ---
try:
    c_res = supabase.table("coefficients").select("*").eq("id", 1).execute()
    if c_res.data:
        st.session_state.coeffs = c_res.data[0]
        defaults = {'Static_Weight': 50.0, 'Static_Count': 2, 'Digital_OOH_Weight': 10.0, 'Digital_OOH_Count': 4}
        for k, v in defaults.items():
            if k not in st.session_state.coeffs:
                st.session_state.coeffs[k] = v
except:
    st.session_state.coeffs = {}

try:
    l_res = supabase.table("ledger").select("*").execute()
    ledger_data = l_res.data if l_res.data else []
except:
    ledger_data = []

metrics = get_forensic_metrics(ledger_data, st.session_state.coeffs)

# --- 7. SIDEBAR NAVIGATION (THE SIDECAR) ---
st.sidebar.markdown("<h2 style='color:#0047AB; margin-bottom:0;'>🎰 FloorCast</h2>", unsafe_allow_html=True)
st.sidebar.write(f"User: {st.session_state.get('user_email', 'Brian')}")
st.sidebar.divider()

page = st.sidebar.radio("Select Workspace:", [
    "📈 Executive Overview", 
    "📑 Ledger Management", 
    "📊 Property Analytics", 
    "⚙️ Engine Control", 
    "🧠 FloorCast Analyst", 
    "📋 Master Report", 
    "🧪 Forecast Sandbox"
])

if st.sidebar.button("🔓 Logout", use_container_width=True):
    supabase.auth.sign_out()
    st.session_state.user_authenticated = False
    st.rerun()

# =================================================================
# 8. WORKSPACE LOGIC (IF/ELIF BLOCKS)
# =================================================================

if page == "📈 Executive Overview":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 10px; border-left: 5px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📈 Executive Overview</h2>
            <p style="color: #444; margin: 0;">Real-time property pulse and forensic attribution for Hard Rock Ottawa.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.warning("Vault is empty. No data available.")
    else:
        df_raw_exec = pd.DataFrame(ledger_data)
        df_raw_exec['entry_date'] = pd.to_datetime(df_raw_exec['entry_date'])
        min_d_exec = df_raw_exec['entry_date'].min().date()
        max_d_exec = df_raw_exec['entry_date'].max().date()

        col_d1, col_d2 = st.columns([1, 2])
        with col_d1:
            d_start_exec = max(min_d_exec, max_d_exec - datetime.timedelta(days=14))
            exec_range = st.date_input("Executive View Period:", value=(d_start_exec, max_d_exec), key="exec_overview_calendar_vfinal")

        if isinstance(exec_range, tuple) and len(exec_range) == 2:
            start_exec, end_exec = exec_range
            mask_exec = (df_raw_exec['entry_date'].dt.date >= start_exec) & (df_raw_exec['entry_date'].dt.date <= end_exec)
            filtered_exec = df_raw_exec.loc[mask_exec].to_dict(orient='records')
            
            metrics = get_forensic_metrics(filtered_exec, st.session_state.coeffs)
            df_chart = metrics.get('df_with_awareness').copy()
            
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Predictability", metrics['predictability'])
            total_digital = df_chart['residual_lift'].sum()
            col2.metric("Digital Lift", f"{total_digital:,.0f}")
            ooh_val = metrics.get('ooh_total_daily', 0)
            col3.metric("OOH Pressure", f"{ooh_val:.0f} Guests")
            total_new_mems = df_chart['new_members'].sum()
            col4.metric("New Members", f"{total_new_mems:,.0f}")
            avg_spend_val = float(st.session_state.coeffs.get('Avg_Coin_In', 112.50))
            col5.metric("Avg. Spend", f"${avg_spend_val:.2f}")

            st.divider()
            st.write("### 📊 Performance vs. Prediction")
            df_plot = df_chart.copy().rename(columns={'actual_traffic': 'Actual Traffic', 'expected': 'Expected Traffic'})
            st.line_chart(df_plot.set_index('entry_date')[['Actual Traffic', 'Expected Traffic']])

elif page == "📑 Ledger Management":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 10px; border-left: 5px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📑 Ledger Management</h2>
            <p style="color: #444; margin: 0;">Update property performance records.</p>
        </div>
    """, unsafe_allow_html=True)

    col_a, col_b = st.columns([1, 1])
    with col_a:
        st.write("### ✍️ Manual Results Entry")
        with st.form("manual_entry_v6"):
            entry_date = st.date_input("Select Date", datetime.date.today())
            c1, c2, c3 = st.columns(3)
            traffic = c1.number_input("Traffic", min_value=0)
            coin_in = c2.number_input("Coin-In ($)", min_value=0.0)
            new_mems = c3.number_input("New Members", min_value=0)
            w1, w2, w3, w4 = st.columns(4)
            temp = w1.number_input("Temp (°C)", value=15.0)
            snow = w2.number_input("Snow (cm)", 0.0)
            rain = w3.number_input("Rain (mm)", 0.0)
            promo = w4.checkbox("Major Promo?")
            
            if st.form_submit_button("💾 Sync to Vault"):
                new_row = {"entry_date": entry_date.isoformat(), "actual_traffic": int(traffic), "actual_coin_in": float(coin_in), "new_members": int(new_mems), "temp_c": float(temp), "snow_cm": float(snow), "rain_mm": float(rain), "active_promo": bool(promo)}
                supabase.table("ledger").upsert(new_row, on_conflict="entry_date").execute()
                st.success("Synced!")
                st.rerun()

    with col_b:
        st.write("### 📤 Bulk CSV Upload")
        uploaded_file = st.file_uploader("Upload Ledger CSV", type="csv")
        if uploaded_file and st.button("🚀 Push CSV"):
            df_upload = pd.read_csv(uploaded_file)
            supabase.table("ledger").upsert(df_upload.to_dict(orient='records')).execute()
            st.success("Bulk Upload Complete")
            st.rerun()

    st.divider()
    st.write("### 📜 Ledger Editor")
    if ledger_data:
        df_history = pd.DataFrame(ledger_data).sort_values(by='entry_date', ascending=False)
        edited_df = st.data_editor(df_history, key="ledger_editor_v6", use_container_width=True, hide_index=True)
        if st.button("✅ Confirm & Sync Edits"):
            sync_ready = edited_df.copy()
            sync_ready['entry_date'] = pd.to_datetime(sync_ready['entry_date']).dt.strftime('%Y-%m-%d')
            supabase.table("ledger").upsert(sync_ready.to_dict(orient='records')).execute()
            st.success("Vault Updated.")
            st.rerun()

elif page == "📊 Property Analytics":
    st.markdown("<h2 style='color:#0047AB;'>📊 Property Performance Analytics</h2>", unsafe_allow_html=True)
    if ledger_data:
        df_analysis = pd.DataFrame(ledger_data).sort_values('entry_date')
        df_analysis['entry_date'] = pd.to_datetime(df_analysis['entry_date'])
        metric_choice = st.pills("Metric:", ["Traffic", "Coin-In", "New Members"], default="Traffic")
        if metric_choice == "Traffic": st.area_chart(df_analysis.set_index('entry_date')['actual_traffic'], color="#0047AB")
        elif metric_choice == "Coin-In": st.line_chart(df_analysis.set_index('entry_date')['actual_coin_in'], color="#2ecc71")
        elif metric_choice == "New Members": st.bar_chart(df_analysis.set_index('entry_date')['new_members'], color="#E74C3C")

elif page == "⚙️ Engine Control":
    st.header("⚙️ Engine Calibration")
    with st.form("db_calib_form_v14_uncapped"):
        st.write("### 🏢 OOH Weighted Logic")
        c1, c2 = st.columns(2)
        sc = c1.number_input("Static Count", value=int(st.session_state.coeffs.get('Static_Count', 10)))
        sw = c1.slider("Static Weight", 0.0, 100.0, float(st.session_state.coeffs.get('Static_Weight', 15.0)))
        dc = c2.number_input("Digital Count", value=int(st.session_state.coeffs.get('Digital_OOH_Count', 5)))
        dw = c2.slider("Digital Weight", 0.0, 200.0, float(st.session_state.coeffs.get('Digital_OOH_Weight', 25.0)))
        
        st.divider()
        st.write("### 💰 Financial DNA")
        f1, f2, f3 = st.columns(3)
        spend = f1.number_input("Avg_Coin_In", value=float(st.session_state.coeffs.get('Avg_Coin_In', 112.50)))
        hold = f2.slider("Hold %", 0.0, 100.0, float(st.session_state.coeffs.get('Hold_Pct', 10.0)))
        grav = f3.slider("Event Gravity", 0.0, 100.0, float(st.session_state.coeffs.get('Event_Gravity', 25.0)))
        
        if st.form_submit_button("🚀 Commit Weights"):
            st.session_state.coeffs.update({'Static_Count': sc, 'Static_Weight': sw, 'Digital_OOH_Count': dc, 'Digital_OOH_Weight': dw, 'Avg_Coin_In': spend, 'Hold_Pct': hold, 'Event_Gravity': grav})
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            st.success("Vault Calibrated.")

elif page == "🧠 FloorCast Analyst":
    st.header("🧠 FloorCast Analyst")
    df_raw = pd.DataFrame(ledger_data)
    dossier = "".join([f"Date: {r.get('entry_date')} | Traffic: {r.get('actual_traffic')} | Members: {r.get('new_members')} | Promo: {r.get('promo_active')} | Clicks: {r.get('ad_clicks')} | Temp: {r.get('temp_c')}C\n" for _, r in df_raw.iterrows()])
    
    if "messages" not in st.session_state: st.session_state.messages = []
    prompt = st.chat_input("Ask about ROI or trends...")
    
    if prompt:
        hist = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in st.session_state.messages[-10:]])
        st.session_state.messages.append({"role": "user", "content": prompt})
        try:
            model = genai.GenerativeModel('gemini-2.0-flash')
            with st.status("🕵️ Analyst is thinking...", expanded=True) as status:
                ctx = f"Role: Casino Analyst. Vault:\n{dossier}\nHistory:\n{hist}\nQuestion: {prompt}"
                resp = model.generate_content(ctx)
                status.update(label="✅ Analysis Complete!", state="complete")
            st.session_state.messages.append({"role": "assistant", "content": resp.text})
            st.rerun()
        except Exception as e: st.error(f"Error: {e}")

    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]): st.markdown(m["content"])

elif page == "📋 Master Report":
    st.header("📋 Master Forensic Report")
    df_raw = pd.DataFrame(ledger_data)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
    sel_range = st.date_input("Audit Period:", value=(df_raw['entry_date'].min().date(), df_raw['entry_date'].max().date()), key="master_report_final_filter_v8")
    
    if isinstance(sel_range, tuple) and len(sel_range) == 2:
        s_d, e_d = sel_range
        df_f = df_raw[(df_raw['entry_date'].dt.date >= s_d) & (df_raw['entry_date'].dt.date <= e_d)].to_dict(orient='records')
        m = get_forensic_metrics(df_f, st.session_state.coeffs)
        df_rep = m['df_with_awareness']
        
        st.write("### 💰 Property Yield")
        f1, f2, f3, f4, f5 = st.columns(5)
        total_t = df_rep['actual_traffic'].sum()
        rev = total_t * float(st.session_state.coeffs['Avg_Coin_In'])
        f1.metric("Traffic", f"{total_t:,}")
        f2.metric("Revenue", f"${rev:,.2f}")
        f3.metric("GGR", f"${(rev * float(st.session_state.coeffs['Hold_Pct'])/100):,.2f}")
        f4.metric("Theo Win", f"${(total_t * float(st.session_state.coeffs['Property_Theo'])):,.2f}")
        f5.metric("Predictability", m['predictability'])

elif page == "🧪 Forecast Sandbox":
    st.header("🧪 Forecast Sandbox")
    c = st.session_state.coeffs
    ooh = (float(c['Static_Count']) * float(c['Static_Weight'])) + (float(c['Digital_OOH_Count']) * float(c['Digital_OOH_Weight']))
    
    col_l, col_r = st.columns(2)
    s_clicks = col_l.number_input("Ad Clicks", 500)
    s_attend = col_l.number_input("Event Attendance", 1800)
    s_snow = col_r.slider("Snow (cm)", 0, 50, 0)
    
    pred = max(0, 4365 + ooh + (s_clicks * c['Clicks']) + (s_attend * (c['Event_Gravity']/100)) + (s_snow * c['Snow_cm']))
    st.divider()
    res1, res2 = st.columns(2)
    res1.metric("Predicted Traffic", f"{int(pred):,}")
    res2.metric("Net Win", f"${(pred * c['Avg_Coin_In'] * (c['Hold_Pct']/100)):,.2f}")

st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies")
