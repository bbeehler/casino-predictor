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
        /* Global Foundations */
        .stApp { background-color: #F0F2F6 !important; }
        
        /* Typography Force-Black */
        h1, h2, h3, h4, h5, h6, p, span, label, div, [data-testid="stMarkdownContainer"] p {
            color: #1A1A1B !important;
            font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        }

        /* Sidebar: Clean Drawer Style */
        section[data-testid="stSidebar"] {
            background-color: #FFFFFF !important;
            border-right: 2px solid #DEE2E6 !important;
            padding-top: 2rem;
        }
        
        /* Metric Card: Executive Blue */
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
            letter-spacing: 1px;
            font-size: 0.85rem !important;
        }

        /* Inputs & Buttons */
        .stButton>button {
            background-color: #0047AB !important;
            color: white !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
            border: none !important;
            transition: all 0.3s ease;
        }
        .stButton>button:hover {
            background-color: #002D6B !important;
            box-shadow: 0 4px 12px rgba(0,71,171,0.3);
        }
        input, textarea, select {
            background-color: #FFFFFF !important;
            border-radius: 8px !important;
        }
        
        /* Analyst Status Bar */
        [data-testid="stStatus"] {
            background-color: #E7F3FF !important;
            border: 1px solid #0047AB !important;
            border-radius: 10px !important;
        }
        </style>
    """, unsafe_allow_html=True)

apply_corporate_styling()

# =================================================================
# 3. MASTER FORENSIC ENGINE (THE HEARTBEAT)
# =================================================================
def get_forensic_metrics(df_input, coeffs):
    """
    Triangulates Daily Traffic by isolating Organic, Digital, OOH, and LIVE factors.
    """
    if not df_input:
        return {"predictability": "0.0%", "heartbeats": {}, "ooh_total_daily": 0, "df": pd.DataFrame()}

    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df = df.sort_values('entry_date')
    df['day_name'] = df['entry_date'].dt.day_name()
    
    # 3.1 Extract Weighted Coefficients
    c_clicks = float(coeffs.get('Clicks', 0.05))
    c_social = float(coeffs.get('Social_Imp', 0.0002))
    c_eng = float(coeffs.get('Social_Eng', 0.01))
    decay = float(coeffs.get('Ad_Decay', 85.0)) / 100 
    gravity = float(coeffs.get('Event_Gravity', 25.0)) / 100
    
    ooh_daily = (float(coeffs.get('Static_Weight', 15)) * int(coeffs.get('Static_Count', 10))) + \
                 (float(coeffs.get('Digital_OOH_Weight', 25)) * int(coeffs.get('Digital_OOH_Count', 5)))

    # 3.2 Recursive Adstock Loop (Awareness Persistence)
    awareness_pool, current_pool = [], 0.0
    for _, row in df.iterrows():
        daily_in = (row.get('ad_clicks', 0) * c_clicks) + \
                   (row.get('ad_impressions', 0) * c_social) + \
                   (row.get('social_engagements', 0) * c_eng)
        current_pool = daily_in + (current_pool * decay)
        awareness_pool.append(current_pool)
    
    df['residual_lift'] = awareness_pool
    df['gravity_lift'] = df.get('attendance', 0) * gravity
    
    # 3.3 Baseline Purification
    df['baseline_isolated'] = df['actual_traffic'] - df['residual_lift'] - ooh_daily - df['gravity_lift']
    heartbeats = df.groupby('day_name')['baseline_isolated'].mean().to_dict()
    
    # 3.4 Predictive Modeling
    df['expected'] = df.apply(lambda x: heartbeats.get(x['day_name'], 4365) + x['residual_lift'] + ooh_daily + x['gravity_lift'], axis=1)
    
    # Forecast Error Variance
    mape = (np.abs(df['actual_traffic'] - df['expected']) / df['actual_traffic']).replace([np.inf, -np.inf], np.nan).dropna().mean()
    pred_score = (1 - mape) * 100 if not np.isnan(mape) else 85.0

    return {
        "predictability": f"{pred_score:.1f}%",
        "heartbeats": heartbeats,
        "ooh_total_daily": ooh_daily,
        "df": df
    }

# =================================================================
# 4. DATA INFRASTRUCTURE (SUPABASE & WEATHER)
# =================================================================
try:
    supabase = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
except:
    st.error("🚨 Critical Error: Supabase connection failed. Check your secrets.toml.")

async def fetch_weather():
    try:
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {"current": ec.conditions, "forecast": ec.daily_forecasts}
    except:
        return {"error": "Station Unavailable"}

if 'weather_data' not in st.session_state:
    st.session_state.weather_data = asyncio.run(fetch_weather())

# =================================================================
# 5. HYDRATION & RECOVERY
# =================================================================
try:
    # Hydrate Coefficients (Vault)
    c_res = supabase.table("coefficients").select("*").eq("id", 1).execute()
    if c_res.data:
        st.session_state.coeffs = c_res.data[0]
    
    # Hydrate Daily Ledger
    l_res = supabase.table("ledger").select("*").execute()
    ledger_data = l_res.data if l_res.data else []
except:
    ledger_data = []

# =================================================================
# 6. SIDEBAR NAVIGATION & AUTH
# =================================================================
st.sidebar.markdown("<h1 style='color:#0047AB; font-size: 28px; margin-bottom: 0;'>🎰 FloorCast</h1><p style='color:#888;'>Hard Rock Ottawa v4.0</p>", unsafe_allow_html=True)
st.sidebar.divider()

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    with st.sidebar:
        st.subheader("Executive Access")
        e_mail = st.text_input("Email")
        p_word = st.text_input("Password", type="password")
        if st.button("Unlock Engine", use_container_width=True):
            try:
                res = supabase.auth.sign_in_with_password({"email": e_mail, "password": p_word})
                if res.user:
                    st.session_state.authenticated = True
                    st.session_state.user_email = res.user.email
                    st.rerun()
            except:
                st.error("Invalid Credentials")
    st.stop()

# Persistent Menu
page = st.sidebar.radio("Navigation Workspace", [
    "📈 Executive Dashboard", 
    "📑 Daily Ledger Vault", 
    "📊 Attribution Analytics", 
    "📋 Master Audit Report",
    "🧠 FloorCast AI Analyst", 
    "⚙️ Engine Calibration", 
    "🧪 Forecast Sandbox"
])

st.sidebar.divider()
if st.sidebar.button("🔓 Logout", use_container_width=True):
    st.session_state.authenticated = False
    st.rerun()

# =================================================================
# 7. PAGE 1: EXECUTIVE DASHBOARD
# =================================================================
if page == "Executive Dashboard":
    st.header("📈 Executive Performance Pulse")
    
    if not ledger_data:
        st.info("The Forensic Vault is currently empty. Please populate the Ledger.")
        st.stop()

    df_full = pd.DataFrame(ledger_data)
    df_full['entry_date'] = pd.to_datetime(df_full['entry_date'])
    
    # Date Filter
    c_start, c_end = st.columns([1, 3])
    with c_start:
        d_range = st.date_input("Audit Window:", value=(df_full['entry_date'].min().date(), df_full['entry_date'].max().date()))

    if isinstance(d_range, tuple) and len(d_range) == 2:
        start_d, end_d = d_range
        df_f = df_full[(df_full['entry_date'].dt.date >= start_d) & (df_full['entry_date'].dt.date <= end_d)].to_dict(orient='records')
        m_results = get_forensic_metrics(df_f, st.session_state.coeffs)
        df_viz = m_results['df']

        # KPI Layer
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Predictability Score", m_results['predictability'])
        k2.metric("Avg OOH Inertia", f"{m_results['ooh_total_daily']:.0f} Guests")
        k3.metric("Total Signups", f"{df_viz['new_members'].sum():,}")
        
        # Spend Logic
        total_head = df_viz['actual_traffic'].sum()
        spend_head = float(st.session_state.coeffs.get('Avg_Coin_In', 112.50))
        k4.metric("Est. Floor GGR", f"${(total_head * spend_head * 0.10):,.0f}")

        # Primary Chart
        st.write("### 🎰 Actual Traffic vs. Engine Prediction")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_viz['entry_date'], y=df_viz['actual_traffic'], name="Actual Headcount", line=dict(color='#0047AB', width=4)))
        fig.add_trace(go.Scatter(x=df_viz['entry_date'], y=df_viz['expected'], name="AI Prediction", line=dict(color='#FFCC00', width=2, dash='dot')))
        fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

# =================================================================
# 8. PAGE 2: DAILY LEDGER VAULT
# =================================================================
elif page == "📑 Daily Ledger Vault":
    st.header("📑 Forensic Ledger Management")
    
    col_l, col_r = st.columns(2)
    with col_l:
        with st.form("vault_entry_form"):
            st.subheader("✍️ Add Daily Metrics")
            d_entry = st.date_input("Entry Date", datetime.date.today())
            f1, f2 = st.columns(2)
            with f1:
                t_in = st.number_input("Traffic (Turnstile)", min_value=0)
                m_in = st.number_input("Unity Signups", min_value=0)
            with f2:
                c_in = st.number_input("Total Coin-In ($)", min_value=0.0)
                a_in = st.number_input("Event Attendance", min_value=0)
            
            st.write("**Environmental & Ad Data**")
            w1, w2 = st.columns(2)
            with w1:
                temp = st.number_input("Temp (°C)", value=15.0)
                clicks = st.number_input("Ad Clicks", min_value=0)
            with w2:
                snow = st.number_input("Snow (cm)", min_value=0.0)
                imps = st.number_input("Impressions", min_value=0)
            
            if st.form_submit_button("🔒 Sync to Supabase", use_container_width=True):
                payload = {
                    "entry_date": d_entry.isoformat(), "actual_traffic": int(t_in),
                    "actual_coin_in": float(c_in), "new_members": int(m_in),
                    "attendance": int(a_in), "temp_c": float(temp),
                    "snow_cm": float(snow), "ad_clicks": int(clicks), "ad_impressions": int(imps)
                }
                supabase.table("ledger").upsert(payload, on_conflict="entry_date").execute()
                st.success(f"Data for {d_entry} verified and stored.")
                st.rerun()

    with col_r:
        st.subheader("📤 Bulk Systems Import")
        st.caption("Upload your daily .csv export from the Marketing Hub.")
        csv_file = st.file_uploader("Drop Ledger CSV here", type="csv")
        if csv_file and st.button("🚀 Execute Bulk Sync"):
            df_up = pd.read_csv(csv_file)
            supabase.table("ledger").upsert(df_up.to_dict(orient='records')).execute()
            st.success("Bulk synchronization complete.")
            st.rerun()

    st.divider()
    st.subheader("📜 Universal Ledger History")
    if ledger_data:
        df_edit = pd.DataFrame(ledger_data).sort_values('entry_date', ascending=False)
        edited_df = st.data_editor(df_edit, use_container_width=True, hide_index=True)
        if st.button("✅ Confirm Manual Overwrites"):
            final_p = edited_df.to_dict(orient='records')
            supabase.table("ledger").upsert(final_p).execute()
            st.success("Vault state updated.")

# =================================================================
# 9. PAGE 5: AI STRATEGIC ANALYST (MEMORY INTEGRATED)
# =================================================================
elif page == "🧠 FloorCast AI Analyst":
    st.header("🧠 FloorCast Strategic AI")
    
    # Prep the data for the AI Brain
    df_ai = pd.DataFrame(ledger_data)
    dossier = "".join([f"Date: {r.get('entry_date')} | Traffic: {r.get('actual_traffic')} | Signups: {r.get('new_members')} | Promo: {r.get('active_promo')} | Weather: {r.get('temp_c')}C\n" for _, r in df_ai.iterrows()])

    # Memory UI Flow
    prompt = st.chat_input("Chief, what do you need to know about our floor performance?")
    
    if prompt:
        # History Context Bridge
        history_str = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in st.session_state.messages[-8:]])
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            with st.status("🕵️ Analyst is Auditing Daily Ledger...", expanded=True) as status:
                st.write("🔍 Accessing Forensic Vault...")
                full_prompt = f"""
                You are the Chief Strategic Analyst for Hard Rock Casino Ottawa.
                You have full access to the daily ledger data below.
                
                LEDGER DATA:
                {dossier}
                
                CONVERSATION HISTORY:
                {history_str}
                
                MISSION:
                Answer the user's question with precise data. Use correlations between 
                weather, events, and signups. Be direct.
                
                QUESTION: {prompt}
                """
                response = model.generate_content(full_prompt)
                status.update(label="✅ Strategic Insight Finalized!", state="complete", expanded=False)
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()
        except Exception as e:
            st.error(f"Brain Sync Error: {e}")

    # Display Response (Newest to Oldest)
    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

# =================================================================
# 10. PAGE 6: ENGINE CALIBRATION
# =================================================================
elif page == "⚙️ Engine Calibration":
    st.header("⚙️ Forensic Engine Weight Calibration")
    st.info("These weights drive the AI's predictive logic. Adjust based on audited marketing ROI.")
    
    with st.form("calibration_form"):
        st.subheader("🏢 OOH Inertia Weights")
        c1, c2 = st.columns(2)
        with c1:
            n_sc = st.number_input("Static Board Count", value=int(st.session_state.coeffs.get('Static_Count', 10)))
            n_sw = st.slider("Weight per Static Board", 0.0, 100.0, float(st.session_state.coeffs.get('Static_Weight', 15.0)))
        with c2:
            n_dc = st.number_input("Digital Face Count", value=int(st.session_state.coeffs.get('Digital_OOH_Count', 5)))
            n_dw = st.slider("Weight per Digital Face", 0.0, 200.0, float(st.session_state.coeffs.get('Digital_OOH_Weight', 25.0)))

        st.divider()
        st.subheader("💰 Financial & Gravity Anchors")
        f1, f2, f3 = st.columns(3)
        with f1: 
            n_spend = st.number_input("Avg Spend / Head ($)", value=float(st.session_state.coeffs.get('Avg_Coin_In', 112.50)))
        with f2:
            n_hold = st.slider("Property Hold %", 0.0, 100.0, float(st.session_state.coeffs.get('Hold_Pct', 10.0)))
        with f3:
            n_grav = st.slider("Event Gravity %", 0.0, 100.0, float(st.session_state.coeffs.get('Event_Gravity', 25.0)))
        
        if st.form_submit_button("🚀 Commit Calibrated Weights to Vault", use_container_width=True):
            st.session_state.coeffs.update({
                "Static_Count": n_sc, "Static_Weight": n_sw,
                "Digital_OOH_Count": n_dc, "Digital_OOH_Weight": n_dw,
                "Avg_Coin_In": n_spend, "Hold_Pct": n_hold, "Event_Gravity": n_grav
            })
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            st.success("Engine recalibrated successfully.")

# =================================================================
# 11. PAGE 7: FORECAST SANDBOX (SIMULATION)
# =================================================================
elif page == "🧪 Forecast Sandbox":
    st.header("🧪 Strategic Forecast Simulator")
    
    # Bridge Current Inertia
    c = st.session_state.coeffs
    ooh_inertia = (float(c['Static_Count']) * float(c['Static_Weight'])) + \
                  (float(c['Digital_OOH_Count']) * float(c['Digital_OOH_Weight']))

    col_l, col_r = st.columns([1, 1])
    with col_l:
        st.subheader("🎛️ Market Inputs")
        s_clicks = st.number_input("Planned Daily Ad Clicks", 500)
        s_imp = st.number_input("Planned Impressions", 10000)
        s_attend = st.number_input("Concert Projected Attendance", 1800)
    
    with col_r:
        st.subheader("❄️ Environment Friction")
        s_snow = st.slider("Snow Forecast (cm)", 0, 50, 0)
        s_temp = st.slider("Expected Temp (C)", -30, 40, 15)

    # Simulation Calc
    m_lift = (s_clicks * c['Clicks']) + (s_imp * c['Social_Imp'])
    e_lift = s_attend * (c['Event_Gravity']/100)
    w_loss = (s_snow * c['Snow_cm'])
    
    # 4365 is the hardcoded "Ottawa Baseline" fallback
    predicted_head = max(0, 4365 + ooh_inertia + m_lift + e_lift + w_loss)
    predicted_win = predicted_head * c['Avg_Coin_In'] * (c['Hold_Pct']/100)

    st.divider()
    res1, res2, res3 = st.columns(3)
    res1.metric("Simulated Daily Traffic", f"{int(predicted_head):,} Guests")
    res2.metric("Projected Daily Win", f"${predicted_win:,.2f}")
    res3.metric("OOH Passive Lift", f"+{int(ooh_inertia)} Guests")

    # Interactive What-If
    st.caption("💡 Projections are based on current calibrated weights in the Engine.")

# =================================================================
# 12. PAGE 4: MASTER AUDIT REPORT
# =================================================================
elif page == "📋 Master Audit Report":
    st.header("📋 Comprehensive Forensic Audit")
    
    df_audit = pd.DataFrame(ledger_data)
    df_audit['entry_date'] = pd.to_datetime(df_audit['entry_date'])
    
    audit_range = st.date_input("Audit Selection:", value=(df_audit['entry_date'].min().date(), df_audit['entry_date'].max().date()), key="master_audit_range")
    
    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_a, e_a = audit_range
        df_slice = df_audit[(df_audit['entry_date'].dt.date >= s_a) & (df_audit['entry_date'].dt.date <= e_a)].to_dict(orient='records')
        audit_metrics = get_forensic_metrics(df_slice, st.session_state.coeffs)
        df_rep = audit_metrics['df']
        
        st.write("### 💰 Financial Integrity Analysis")
        a1, a2, a3, a4 = st.columns(4)
        t_traffic = df_rep['actual_traffic'].sum()
        a_spend = float(st.session_state.coeffs['Avg_Coin_In'])
        t_win = t_traffic * a_spend * (float(st.session_state.coeffs['Hold_Pct'])/100)
        
        a1.metric("Aggregated Traffic", f"{t_traffic:,}")
        a2.metric("Audited GGR", f"${t_win:,.2f}")
        a3.metric("Digital ROI Lift", f"{df_rep['residual_lift'].sum():,.0f} Guests")
        a4.metric("Model Confidence", audit_metrics['predictability'])
        
        st.write("### 📊 Attribution Component Breakdown")
        df_rep['OOH'] = audit_metrics['ooh_total_daily']
        chart_data = df_rep.set_index('entry_date')[['baseline_isolated', 'OOH', 'residual_lift', 'gravity_lift']]
        st.area_chart(chart_data)

        # Export Logic
        csv_buffer = df_rep.to_csv(index=False).encode('utf-8')
        st.download_button("📂 Export Audit Data to Excel (CSV)", data=csv_buffer, file_name=f"HR_Ottawa_Audit_{s_a}_{e_a}.csv", mime="text/csv")

# =================================================================
# 13. PAGE 3: ATTRIBUTION ANALYTICS
# =================================================================
elif page == "📊 Attribution Analytics":
    st.header("📊 Multi-Channel Attribution Analytics")
    
    df_an = pd.DataFrame(ledger_data)
    df_an['entry_date'] = pd.to_datetime(df_an['entry_date'])
    
    st.write("### 🧬 Variable Correlation Matrix")
    # Interactive correlation plot
    corr_cols = ['actual_traffic', 'new_members', 'actual_coin_in', 'ad_clicks', 'temp_c', 'snow_cm']
    fig_corr = px.scatter_matrix(df_an, dimensions=corr_cols, color='new_members', title="Daily Variable Scatter")
    st.plotly_chart(fig_corr, use_container_width=True)

    st.divider()
    
    st.write("### 🕒 Day-of-Week Performance (Organic Heartbeat)")
    df_an['day_name'] = df_an['entry_date'].dt.day_name()
    avg_day = df_an.groupby('day_name')['actual_traffic'].mean().reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
    st.bar_chart(avg_day)

# =================================================================
# 14. MOBILE TOGGLE & FINAL FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
