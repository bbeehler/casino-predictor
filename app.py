import streamlit as st
import pandas as pd
import datetime
import json
import asyncio
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from env_canada import ECWeather
import google.generativeai as genai
from supabase import create_client
from io import BytesIO

# =================================================================
# 1. PERMANENT INITIALIZATION & STATE LOCK
# =================================================================
if 'coeffs' not in st.session_state:
    st.session_state.coeffs = {
        'Static_Count': 10, 'Static_Weight': 15.0, 
        'Digital_OOH_Count': 5, 'Digital_OOH_Weight': 25.0, 
        'Clicks': 0.05, 'Social_Imp': 0.0002, 'Social_Eng': 0.01, 
        'Event_Gravity': 25.0, 'Avg_Coin_In': 112.50, 
        'Property_Theo': 450.00, 'Hold_Pct': 10.0, 
        'Snow_cm': -45, 'Rain_mm': -12, 'Ad_Decay': 85.0
    }

if 'messages' not in st.session_state:
    st.session_state.messages = []

# =================================================================
# 2. GLOBAL PAGE CONFIG & EXECUTIVE THEME
# =================================================================
st.set_page_config(
    page_title="FloorCast Pro | Hard Rock Ottawa", 
    layout="wide", 
    page_icon="🎰",
    initial_sidebar_state="expanded"
)

def apply_corporate_styling():
    st.markdown("""
        <style>
        .stApp { background-color: #F0F2F6 !important; }
        h1, h2, h3, h4, h5, h6, p, span, label, div, [data-testid="stMarkdownContainer"] p {
            color: #1A1A1B !important;
            font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        }
        section[data-testid="stSidebar"] {
            background-color: #FFFFFF !important;
            border-right: 2px solid #DEE2E6 !important;
            padding-top: 2rem;
        }
        div[data-testid="metric-container"] {
            background-color: #E1E8F0 !important;
            border: 1px solid #B0C4DE !important;
            border-left: 6px solid #0047AB !important;
            padding: 20px !important;
            border-radius: 12px !important;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }
        [data-testid="stMetricLabel"] p {
            color: #0047AB !important;
            font-weight: 700 !important;
            text-transform: uppercase;
        }
        .stButton>button {
            background-color: #0047AB !important;
            color: white !important;
            border-radius: 8px !important;
        }
        </style>
    """, unsafe_allow_html=True)

apply_corporate_styling()

# =================================================================
# 3. MASTER FORENSIC ENGINE
# =================================================================
def get_forensic_metrics(df_input, coeffs):
    if not df_input:
        return {"predictability": "0.0%", "heartbeats": {}, "ooh_total_daily": 0, "df": pd.DataFrame()}

    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df = df.sort_values('entry_date')
    df['day_name'] = df['entry_date'].dt.day_name()
    
    c_clicks = float(coeffs.get('Clicks', 0.05))
    c_social = float(coeffs.get('Social_Imp', 0.0002))
    c_eng = float(coeffs.get('Social_Eng', 0.01))
    decay = float(coeffs.get('Ad_Decay', 85.0)) / 100 
    gravity = float(coeffs.get('Event_Gravity', 25.0)) / 100
    
    ooh_daily = (float(coeffs.get('Static_Weight', 15)) * int(coeffs.get('Static_Count', 10))) + \
                 (float(coeffs.get('Digital_OOH_Weight', 25)) * int(coeffs.get('Digital_OOH_Count', 5)))

    # Recursive Adstock Loop
    awareness_pool, current_pool = [], 0.0
    for _, row in df.iterrows():
        daily_in = (row.get('ad_clicks', 0) * c_clicks) + \
                   (row.get('ad_impressions', 0) * c_social) + \
                   (row.get('social_engagements', 0) * c_eng)
        current_pool = daily_in + (current_pool * decay)
        awareness_pool.append(current_pool)
    
    df['residual_lift'] = awareness_pool
    df['gravity_lift'] = df.get('attendance', 0) * gravity
    df['baseline_isolated'] = df['actual_traffic'] - df['residual_lift'] - ooh_daily - df['gravity_lift']
    heartbeats = df.groupby('day_name')['baseline_isolated'].mean().to_dict()
    df['expected'] = df.apply(lambda x: heartbeats.get(x['day_name'], 4365) + x['residual_lift'] + ooh_daily + x['gravity_lift'], axis=1)
    
    mape = (np.abs(df['actual_traffic'] - df['expected']) / df['actual_traffic']).replace([np.inf, -np.inf], np.nan).dropna().mean()
    pred_score = (1 - mape) * 100 if not np.isnan(mape) else 85.0

    return {"predictability": f"{pred_score:.1f}%", "heartbeats": heartbeats, "ooh_total_daily": ooh_daily, "df": df}

# =================================================================
# 4. DATA INFRASTRUCTURE & AUTH
# =================================================================
try:
    supabase = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
except:
    st.error("🚨 Critical Error: Supabase connection failed.")

async def fetch_weather():
    try:
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {"current": ec.conditions, "forecast": ec.daily_forecasts}
    except:
        return {"error": "Station Unavailable"}

if 'weather_data' not in st.session_state:
    st.session_state.weather_data = asyncio.run(fetch_weather())

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.sidebar.subheader("Executive Access")
    e_mail = st.sidebar.text_input("Email")
    p_word = st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Unlock Engine", use_container_width=True):
        try:
            res = supabase.auth.sign_in_with_password({"email": e_mail, "password": p_word})
            if res.user:
                st.session_state.authenticated, st.session_state.user_email = True, res.user.email
                st.rerun()
        except: st.error("Invalid Credentials")
    st.stop()

# =================================================================
# 5. HYDRATION & NAVIGATION
# =================================================================
try:
    c_res = supabase.table("coefficients").select("*").eq("id", 1).execute()
    if c_res.data: st.session_state.coeffs = c_res.data[0]
    l_res = supabase.table("ledger").select("*").execute()
    ledger_data = l_res.data if l_res.data else []
except: ledger_data = []

st.sidebar.markdown("<h1 style='color:#0047AB;'>🎰 FloorCast</h1>", unsafe_allow_html=True)
page = st.sidebar.radio("Navigation Workspace", [
    "📈 Executive Dashboard", "📑 Daily Ledger Vault", 
    "📊 Attribution Analytics", "📋 Master Audit Report",
    "🧠 FloorCast AI Analyst", "⚙️ Engine Calibration", "🧪 Forecast Sandbox"
])

if st.sidebar.button("🔓 Logout", use_container_width=True):
    st.session_state.authenticated = False
    st.rerun()

# =================================================================
# 6. WORKSPACE CONTENT
# =================================================================

if page == "📈 Executive Dashboard":
    st.header("📈 Executive Dashboard")
    if ledger_data:
        df_full = pd.DataFrame(ledger_data)
        df_full['entry_date'] = pd.to_datetime(df_full['entry_date'])
        max_d, min_d = df_full['entry_date'].max().date(), df_full['entry_date'].min().date()
        d_range = st.date_input("Audit Window:", value=(max_d - datetime.timedelta(days=14), max_d))
        
        if isinstance(d_range, tuple) and len(d_range) == 2:
            df_f = df_full[(df_full['entry_date'].dt.date >= d_range[0]) & (df_full['entry_date'].dt.date <= d_range[1])].to_dict(orient='records')
            m = get_forensic_metrics(df_f, st.session_state.coeffs)
            df_v = m['df']
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Predictability", m['predictability'])
            k2.metric("OOH Daily Lift", f"{m['ooh_total_daily']:.0f} Guests")
            k3.metric("Total Signups", f"{df_v['new_members'].sum():,}")
            k4.metric("Est. Floor GGR", f"${(df_v['actual_traffic'].sum() * float(st.session_state.coeffs['Avg_Coin_In']) * 0.10):,.0f}")
            
            st.write("### 📊 Actual vs. Prediction")
            st.line_chart(df_v.set_index('entry_date')[['actual_traffic', 'expected']])

elif page == "📑 Daily Ledger Vault":
    st.header("📑 Forensic Ledger Management")
    col_l, col_r = st.columns(2)
    with col_l:
        with st.form("vault_entry"):
            st.subheader("✍️ Add Daily Entry")
            date = st.date_input("Date", datetime.date.today())
            t = st.number_input("Traffic", min_value=0)
            c = st.number_input("Coin-In", min_value=0.0)
            m = st.number_input("Signups", min_value=0)
            a = st.number_input("Attendance", min_value=0)
            if st.form_submit_button("Sync to Supabase"):
                payload = {"entry_date": date.isoformat(), "actual_traffic": int(t), "actual_coin_in": float(c), "new_members": int(m), "attendance": int(a)}
                supabase.table("ledger").upsert(payload, on_conflict="entry_date").execute()
                st.success("Verified and Stored.")
                st.rerun()
    with col_r:
        st.subheader("📤 Bulk Systems Import")
        csv = st.file_uploader("Upload Ledger CSV", type="csv")
        if csv and st.button("🚀 Execute Sync"):
            df_u = pd.read_csv(csv)
            supabase.table("ledger").upsert(df_u.to_dict(orient='records')).execute()
            st.success("Bulk Sync Complete.")

    st.divider()
    if ledger_data:
        st.subheader("📜 Universal Ledger History")
        edited = st.data_editor(pd.DataFrame(ledger_data).sort_values('entry_date', ascending=False), use_container_width=True, hide_index=True)
        if st.button("✅ Confirm Overwrites"):
            supabase.table("ledger").upsert(edited.to_dict(orient='records')).execute()
            st.success("Vault state updated.")

elif page == "🧠 FloorCast AI Analyst":
    st.header("🧠 FloorCast Strategic AI")
    df_ai = pd.DataFrame(ledger_data)
    dossier = "".join([f"Date: {r.get('entry_date')} | Traffic: {r.get('actual_traffic')} | Signups: {r.get('new_members')}\n" for _, r in df_ai.iterrows()])
    prompt = st.chat_input("Chief, what do you need to know about our floor performance?")
    
    if prompt:
        hist = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in st.session_state.messages[-8:]])
        st.session_state.messages.append({"role": "user", "content": prompt})
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.0-flash')
            with st.status("🕵️ Auditing Ledger...", expanded=True):
                ctx = f"You are the Chief Analyst for HR Ottawa. Data:\n{dossier}\nHistory:\n{hist}\nQuestion: {prompt}"
                resp = model.generate_content(ctx)
            st.session_state.messages.append({"role": "assistant", "content": resp.text})
            st.rerun()
        except Exception as e: st.error(f"Error: {e}")
    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]): st.markdown(m["content"])

elif page == "📋 Master Audit Report":
    st.header("📋 Comprehensive Forensic Audit")
    df_ad = pd.DataFrame(ledger_data)
    df_ad['entry_date'] = pd.to_datetime(df_ad['entry_date'])
    sel = st.date_input("Audit Selection:", value=(df_ad['entry_date'].min().date(), df_ad['entry_date'].max().date()))
    
    if isinstance(sel, tuple) and len(sel) == 2:
        df_s = df_ad[(df_ad['entry_date'].dt.date >= sel[0]) & (df_ad['entry_date'].dt.date <= sel[1])].to_dict(orient='records')
        m_a = get_forensic_metrics(df_s, st.session_state.coeffs)
        df_r = m_a['df']
        
        st.write("### 💰 Financial Integrity Analysis")
        a1, a2, a3, a4 = st.columns(4)
        t_t = df_r['actual_traffic'].sum()
        a1.metric("Traffic", f"{t_t:,}")
        a2.metric("Audited GGR", f"${(t_t * float(st.session_state.coeffs['Avg_Coin_In']) * (float(st.session_state.coeffs['Hold_Pct'])/100)):,.2f}")
        a3.metric("Digital ROI Lift", f"{df_r['residual_lift'].sum():,.0f}")
        a4.metric("Model Confidence", m_a['predictability'])
        
        st.write("### 📊 Component Breakdown")
        df_r['OOH'] = m_a['ooh_total_daily']
        st.area_chart(df_r.set_index('entry_date')[['baseline_isolated', 'OOH', 'residual_lift', 'gravity_lift']])
        st.download_button("📂 Export to Excel", data=df_r.to_csv(index=False).encode('utf-8'), file_name="Audit.csv")

elif page == "📊 Attribution Analytics":
    st.header("📊 Attribution Analytics")
    df_an = pd.DataFrame(ledger_data)
    st.write("### 🧬 Variable Correlation Matrix")
    fig = px.scatter_matrix(df_an, dimensions=['actual_traffic', 'new_members', 'actual_coin_in'], color='new_members')
    st.plotly_chart(fig, use_container_width=True)
    st.write("### 🕒 Day-of-Week Performance")
    df_an['day'] = pd.to_datetime(df_an['entry_date']).dt.day_name()
    st.bar_chart(df_an.groupby('day')['actual_traffic'].mean().reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']))

elif page == "⚙️ Engine Calibration":
    st.header("⚙️ Engine Calibration")
    with st.form("calib"):
        c1, c2 = st.columns(2)
        with c1:
            sc = st.number_input("Static Count", value=int(st.session_state.coeffs['Static_Count']))
            sw = st.slider("Static Weight", 0.0, 100.0, float(st.session_state.coeffs['Static_Weight']))
        with c2:
            dc = st.number_input("Digital Count", value=int(st.session_state.coeffs['Digital_OOH_Count']))
            dw = st.slider("Digital Weight", 0.0, 100.0, float(st.session_state.coeffs['Digital_OOH_Weight']))
        f1, f2, f3 = st.columns(3)
        with f1: sp = st.number_input("Avg Spend", value=float(st.session_state.coeffs['Avg_Coin_In']))
        with f2: ho = st.slider("Hold %", 0.0, 100.0, float(st.session_state.coeffs['Hold_Pct']))
        with f3: gr = st.slider("Gravity %", 0.0, 100.0, float(st.session_state.coeffs['Event_Gravity']))
        if st.form_submit_button("🚀 Commit Weights"):
            st.session_state.coeffs.update({"Static_Count": sc, "Static_Weight": sw, "Digital_OOH_Count": dc, "Digital_OOH_Weight": dw, "Avg_Coin_In": sp, "Hold_Pct": ho, "Event_Gravity": gr})
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            st.success("Vault recalibrated.")

elif page == "🧪 Forecast Sandbox":
    st.header("🧪 Strategic Simulator")
    c = st.session_state.coeffs
    ooh = (float(c['Static_Count']) * float(c['Static_Weight'])) + (float(c['Digital_OOH_Count']) * float(c['Digital_OOH_Weight']))
    cl, cr = st.columns(2)
    s_c = cl.number_input("Planned Ad Clicks", 500)
    s_a = cl.number_input("Concert Attendance", 1800)
    s_s = cr.slider("Snow Forecast (cm)", 0, 50, 0)
    pred = max(0, 4365 + ooh + (s_c * c['Clicks']) + (s_a * (c['Event_Gravity']/100)) + (s_s * c['Snow_cm']))
    st.divider()
    r1, r2 = st.columns(2)
    r1.metric("Predicted Daily Traffic", f"{int(pred):,}")
    r2.metric("Projected Daily Win", f"${(pred * c['Avg_Coin_In'] * (c['Hold_Pct']/100)):,.2f}")
