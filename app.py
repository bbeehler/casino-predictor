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
        
        /* Typography Force-Black - Fixing visibility issues */
        h1, h2, h3, h4, h5, h6, p, span, label, div, [data-testid="stMarkdownContainer"] p {
            color: #1A1A1B !important;
            font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        }

        /* Sidebar: Clean Drawer Style (The Sidecar) */
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
        input, textarea, select {
            background-color: #FFFFFF !important;
            border-radius: 8px !important;
            border: 1px solid #CED4DA !important;
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
    MASTER ENGINE: Triangulates Organic Baselines, Adstock, 
    OOH Pressure, Hard Rock LIVE Gravity, and Weather Friction.
    """
    if not df_input:
        return {"predictability": "0.0%", "heartbeats": {}, "ooh_total_daily": 0, "df": pd.DataFrame()}

    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df = df.sort_values('entry_date')
    df['day_name'] = df['entry_date'].dt.day_name()
    
    # 3.1 Extract Calibrated Weights
    c_clicks = float(coeffs.get('Clicks', 0.05))
    c_social = float(coeffs.get('Social_Imp', 0.0002))
    c_eng = float(coeffs.get('Social_Eng', 0.01))
    decay = float(coeffs.get('Ad_Decay', 85.0)) / 100 
    
    # SAFE-FETCH FOR EVENT GRAVITY
    raw_gravity = coeffs.get('event_gravity') or coeffs.get('Event_Gravity') or 25.0
    gravity_capture = float(raw_gravity) / 100
    
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
    df['gravity_lift'] = df.get('attendance', 0) * gravity_capture
    
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
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase = create_client(url, key)
except:
    st.error("🚨 Critical Error: Supabase connection failed. Check your secrets.toml.")

async def fetch_weather():
    try:
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {"current": ec.conditions, "forecast": ec.daily_forecasts, "alerts": ec.alerts}
    except:
        return {"error": "Station Unavailable"}

if 'weather_data' not in st.session_state:
    st.session_state.weather_data = asyncio.run(fetch_weather())

# =================================================================
# 5. HYDRATION & RECOVERY
# =================================================================
try:
    c_res = supabase.table("coefficients").select("*").eq("id", 1).execute()
    if c_res.data:
        st.session_state.coeffs = c_res.data[0]
    
    l_res = supabase.table("ledger").select("*").execute()
    ledger_data = l_res.data if l_res.data else []
except:
    ledger_data = []

# =================================================================
# 6. SIDEBAR NAVIGATION & AUTH (GATEKEEPER OVERHAUL)
# =================================================================
st.sidebar.markdown("<h1 style='color:#0047AB; font-size: 28px; margin-bottom: 0;'>🎰 FloorCast</h1><p style='color:#888;'>Hard Rock Ottawa v4.0</p>", unsafe_allow_html=True)
st.sidebar.divider()

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# --- THE GATEKEEPER ---
if not st.session_state.authenticated:
    # Centered Login UI
    st.markdown("<h1 style='color:#0047AB; text-align:center;'>Executive Access Required</h1>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            e_mail = st.text_input("Email")
            p_word = st.text_input("Password", type="password")
            submit = st.form_submit_button("Unlock Engine", use_container_width=True)
            
            if submit:
                try:
                    res = supabase.auth.sign_in_with_password({"email": e_mail, "password": p_word})
                    if res.user:
                        # Update session state and force a re-run to clear the login screen
                        st.session_state.authenticated = True
                        st.session_state.user_email = res.user.email
                        st.rerun() 
                    else:
                        st.error("Authentication failed. Please check credentials.")
                except Exception as e:
                    st.error("Access Denied: Invalid credentials or connection error.")
    st.stop() # Prevents dashboard from rendering until authenticated

# --- AUTHORIZED SIDEBAR MENU ---
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
    st.session_state.user_email = None
    st.rerun()

# =================================================================
# 7. PAGE 1: EXECUTIVE DASHBOARD (FIXED)
# =================================================================
if page == "📈 Executive Dashboard":
    st.header("📈 Executive Performance Pulse")
    
    if not ledger_data:
        st.info("The Forensic Vault is currently empty. Please populate the Ledger.")
        st.stop()

    df_full = pd.DataFrame(ledger_data)
    df_full['entry_date'] = pd.to_datetime(df_full['entry_date'])
    
    # Date Filter with Smart Defaults to latest available data
    max_db_date = df_full['entry_date'].max().date()
    min_db_date = df_full['entry_date'].min().date()
    default_start = max(min_db_date, max_db_date - datetime.timedelta(days=14))

    col_d, _ = st.columns([1, 3])
    with col_d:
        d_range = st.date_input("Audit Window:", value=(default_start, max_db_date))

    if isinstance(d_range, tuple) and len(d_range) == 2:
        start_d, end_d = d_range
        df_f = df_full[(df_full['entry_date'].dt.date >= start_d) & (df_full['entry_date'].dt.date <= end_d)].to_dict(orient='records')
        
        # RUN THE ENGINE
        m_results = get_forensic_metrics(df_f, st.session_state.coeffs)
        # THE FIX: Engine returns 'df', not 'df_with_awareness'
        df_viz = m_results['df'] 

        # KPI Layer
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Predictability Score", m_results['predictability'])
        k2.metric("Avg OOH Inertia", f"{m_results['ooh_total_daily']:.0f} Guests")
        k3.metric("Total Signups", f"{df_viz['new_members'].sum():,}")
        
        # Spend Logic using calibrated weights
        total_head = df_viz['actual_traffic'].sum()
        spend_head = float(st.session_state.coeffs.get('Avg_Coin_In', 112.50))
        hold_pct = float(st.session_state.coeffs.get('Hold_Pct', 10.0)) / 100
        k4.metric("Est. Floor GGR", f"${(total_head * spend_head * hold_pct):,.0f}")

        # Primary Chart
        st.write("### 🎰 Actual Traffic vs. Engine Prediction")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_viz['entry_date'], y=df_viz['actual_traffic'], name="Actual Headcount", line=dict(color='#0047AB', width=4)))
        fig.add_trace(go.Scatter(x=df_viz['entry_date'], y=df_viz['expected'], name="AI Prediction", line=dict(color='#FFCC00', width=2, dash='dot')))
        fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', margin=dict(l=0, r=0, t=0, b=0), hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
# =================================================================
# 8. PAGE 2: DAILY LEDGER VAULT (FULL HARD ROCK LIVE LOGIC)
# =================================================================
elif page == "📑 Daily Ledger Vault":
    st.header("📑 Forensic Ledger Management")
    
    col_l, col_r = st.columns(2)
    with col_l:
        with st.form("vault_entry_form"):
            st.subheader("✍️ Add Daily Metrics")
            d_entry = st.date_input("Entry Date", datetime.date.today())
            
            # Row 1: Core Financials
            f1, f2, f3 = st.columns(3)
            with f1: traffic = st.number_input("Traffic (Headcount)", min_value=0)
            with f2: coin_in = st.number_input("Coin-In ($)", min_value=0.0, format="%.2f")
            with f3: new_mems = st.number_input("New Members", min_value=0)
            
            st.divider()
            
            # Row 2: Environment
            st.write("**🌦️ Environment & Promotion**")
            w1, w2, w3, w4 = st.columns(4)
            with w1: temp = st.number_input("Temp (°C)", value=15.0)
            with w2: snow = st.number_input("Snow (cm)", min_value=0.0)
            with w3: rain = st.number_input("Rain (mm)", min_value=0.0)
            with w4: promo = st.checkbox("Major Promo?")

            st.divider()

            # Row 3: HARD ROCK LIVE (RECONSTRUCTED)
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
            
            if st.form_submit_button("🔒 Sync to Vault", use_container_width=True):
                payload = {
                    "entry_date": d_entry.isoformat(), 
                    "actual_traffic": int(traffic),
                    "actual_coin_in": float(coin_in), 
                    "new_members": int(new_mems),
                    "temp_c": float(temp),
                    "snow_cm": float(snow),
                    "rain_mm": float(rain),
                    "active_promo": bool(promo),
                    "event_type": event_type,
                    "attendance": int(attendance),
                    "ad_clicks": int(clicks),
                    "ad_impressions": int(imps),
                    "social_engagements": int(social)
                }
                supabase.table("ledger").upsert(payload, on_conflict="entry_date").execute()
                st.success("Vault state updated.")
                st.rerun()

    with col_r:
        st.subheader("📤 Bulk Systems Import")
        csv_file = st.file_uploader("Drop Ledger CSV here", type="csv")
        if csv_file and st.button("🚀 Execute Bulk Sync"):
            df_up = pd.read_csv(csv_file)
            supabase.table("ledger").upsert(df_up.to_dict(orient='records')).execute()
            st.success("Bulk synchronization complete.")
            st.rerun()

    st.divider()
    st.subheader("📜 Universal Ledger Editor")
    if ledger_data:
        df_edit = pd.DataFrame(ledger_data)
        df_edit['entry_date'] = pd.to_datetime(df_edit['entry_date'])
        df_edit = df_edit.sort_values('entry_date', ascending=False)
        
        edited_df = st.data_editor(df_edit, use_container_width=True, hide_index=True)
        if st.button("✅ Confirm Manual Overwrites"):
            final_p = edited_df.to_dict(orient='records')
            # Sanitize dates for DB
            for p in final_p:
                if isinstance(p['entry_date'], (datetime.datetime, pd.Timestamp)):
                    p['entry_date'] = p['entry_date'].strftime('%Y-%m-%d')
            supabase.table("ledger").upsert(final_p).execute()
            st.success("Vault synced.")

# =================================================================
# 13. PAGE 3: ATTRIBUTION ANALYTICS (EXECUTIVE OVERHAUL)
# =================================================================
elif page == "📊 Attribution Analytics":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📊 Attribution Intelligence Suite</h2>
            <p style="color: #444; margin: 0;">Multi-dimensional analysis of marketing equity and environmental friction.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.warning("Forensic Vault is empty. Attribution modeling requires historical data.")
        st.stop()

    df_an = pd.DataFrame(ledger_data)
    df_an['entry_date'] = pd.to_datetime(df_an['entry_date'])
    
    # 1. Run Forensic Engine for the entire history to get lift components
    m_full = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    df_metrics = m_full['df']

    # --- TOP ROW: EQUITY MIX & EFFICIENCY ---
    col_mix, col_eff = st.columns([1, 1])
    
    with col_mix:
        st.write("### 🧬 Total Attribution Mix")
        total_organic = df_metrics['baseline_isolated'].sum()
        total_digital = df_metrics['residual_lift'].sum()
        total_ooh = m_full['ooh_total_daily'] * len(df_metrics)
        total_live = df_metrics['gravity_lift'].sum()
        
        mix_data = pd.DataFrame({
            'Channel': ['Organic Baseline', 'Digital ROI Lift', 'OOH Inertia', 'LIVE Gravity'],
            'Guests': [total_organic, total_digital, total_ooh, total_live]
        })
        
        fig_pie = px.pie(mix_data, values='Guests', names='Channel', 
                         color_discrete_sequence=['#E1E8F0', '#0047AB', '#FFCC00', '#1A1A1B'],
                         hole=0.4)
        fig_pie.update_layout(margin=dict(l=0, r=0, t=30, b=0), legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_eff:
        st.write("### ⚡ Channel Efficiency Indices")
        # Calculate synthetic efficiency (Lift per Unit of Input)
        avg_digital_efficiency = df_metrics['residual_lift'].sum() / (df_metrics['ad_clicks'].sum() + 1)
        avg_event_capture = float(st.session_state.coeffs.get('Event_Gravity', 25.0))
        
        st.info(f"**Digital Equity Score:** Each ad click generates **{avg_digital_efficiency:.2f}** synthetic guests over the ad-decay cycle.")
        st.info(f"**OOH Stability Index:** Your billboard campaign provides a fixed floor of **{m_full['ooh_total_daily']:.0f}** daily guests.")
        st.info(f"**Crossover Gravity:** Concerts are currently migrating **{avg_event_capture}%** of theater attendance to the gaming floor.")

    st.divider()

    # --- MIDDLE ROW: DIGITAL PERSISTENCE MODEL ---
    st.write("### 📈 Digital Awareness Persistence (Adstock Decay)")
    fig_adstock = go.Figure()
    fig_adstock.add_trace(go.Bar(x=df_metrics['entry_date'], y=df_metrics['ad_clicks'], name="Direct Ad Clicks", marker_color='#E1E8F0', opacity=0.5))
    fig_adstock.add_trace(go.Scatter(x=df_metrics['entry_date'], y=df_metrics['residual_lift'], name="Residual Guest Lift", line=dict(color='#0047AB', width=3)))
    
    fig_adstock.update_layout(
        title="Impact of Clicks vs. Latent Traffic Lift (Ad-Decay Modeling)",
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis=dict(title="Volume"),
        hovermode="x unified"
    )
    st.plotly_chart(fig_adstock, use_container_width=True)

    st.divider()

    # --- BOTTOM ROW: ENVIRONMENTAL FRICTION HEATMAP ---
    st.write("### ❄️ Environmental Friction Analysis")
    
    # Prepping Heatmap Data: Day of Week vs. Weather Loss
    df_metrics['day_name'] = df_metrics['entry_date'].dt.day_name()
    df_metrics['weather_loss'] = (df_metrics['snow_cm'] * float(st.session_state.coeffs.get('Snow_cm', -45))) + \
                                 (df_metrics['rain_mm'] * float(st.session_state.coeffs.get('Rain_mm', -12)))
    
    # Sort days properly
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    fig_scatter = px.scatter(df_metrics, x="temp_c", y="actual_traffic", size=np.abs(df_metrics['weather_loss']),
                             color="day_name", category_orders={"day_name": day_order},
                             title="Traffic Density vs. Temperature (Bubble Size = Weather Friction Loss)",
                             labels={"temp_c": "Temperature (°C)", "actual_traffic": "Total Guests"},
                             color_discrete_sequence=px.colors.qualitative.Bold)
    
    fig_scatter.update_layout(plot_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_scatter, use_container_width=True)

    # --- FINAL INSIGHTS GRID ---
    st.write("### 🕒 Day-of-Week Organic Heartbeat")
    avg_day = df_metrics.groupby('day_name')['baseline_isolated'].mean().reindex(day_order)
    st.bar_chart(avg_day, color='#0047AB')
    st.caption("This chart shows the 'Purified' baseline—traffic remaining after removing all marketing and weather variables.")

# =================================================================
# 10. PAGE 4: MASTER AUDIT REPORT
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
        
        # FINANCIAL INTEGRITY PANEL
        st.write("### 💰 Financial Integrity Analysis")
        a1, a2, a3, a4, a5 = st.columns(5)
        t_traffic = df_rep['actual_traffic'].sum()
        a_spend = float(st.session_state.coeffs['Avg_Coin_In'])
        t_win = t_traffic * a_spend * (float(st.session_state.coeffs['Hold_Pct'])/100)
        
        a1.metric("Aggregated Traffic", f"{t_traffic:,}")
        a2.metric("Audited GGR", f"${t_win:,.2f}")
        a3.metric("Digital ROI Lift", f"{df_rep['residual_lift'].sum():,.0f}")
        a4.metric("AI Confidence", audit_metrics['predictability'])
        a5.metric("Guest Density", f"{(t_win / (t_traffic * float(st.session_state.coeffs['Property_Theo']))):.2f}x")

        st.divider()
        st.write("### 📊 Attribution Component Breakdown")
        df_rep['OOH Inertia'] = audit_metrics['ooh_total_daily']
        chart_data = df_rep.set_index('entry_date')[['baseline_isolated', 'OOH Inertia', 'residual_lift', 'gravity_lift']]
        st.area_chart(chart_data)

        csv_buffer = df_rep.to_csv(index=False).encode('utf-8')
        st.download_button("📂 Export Audit Data", data=csv_buffer, file_name=f"HR_Audit_{s_a}_{e_a}.csv", mime="text/csv")

# =================================================================
# 11. PAGE 5: AI ANALYST (MEMORY INTEGRATED)
# =================================================================
elif page == "🧠 FloorCast AI Analyst":
    st.header("🧠 FloorCast Strategic AI")
    
    df_ai = pd.DataFrame(ledger_data)
    dossier = "".join([f"Date: {r.get('entry_date')} | Traffic: {r.get('actual_traffic')} | Signups: {r.get('new_members')} | Promo: {r.get('active_promo')} | Weather: {r.get('temp_c')}C\n" for _, r in df_ai.iterrows()])

    prompt = st.chat_input("Chief, what do you need to know?")
    
    if prompt:
        history_str = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in st.session_state.messages[-8:]])
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            with st.status("🕵️ Auditing Ledger...", expanded=True) as status:
                full_prompt = f"Role: Senior Strategist. Vault:\n{dossier}\nHistory:\n{history_str}\nQuestion: {prompt}"
                response = model.generate_content(full_prompt)
                status.update(label="✅ Analysis Complete!", state="complete")
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]): st.markdown(m["content"])

# =================================================================
# 12. PAGE 6: ENGINE CALIBRATION
# =================================================================
elif page == "⚙️ Engine Calibration":
    st.header("⚙️ Engine Weight Calibration")
    
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
        st.subheader("💰 Financial DNA Anchors")
        f1, f2, f3, f4 = st.columns(4)
        with f1: n_spend = st.number_input("Avg Spend ($)", value=float(st.session_state.coeffs.get('Avg_Coin_In', 112.50)))
        with f2: n_theo = st.number_input("Property Theo ($)", value=float(st.session_state.coeffs.get('Property_Theo', 450.00)))
        with f3: n_hold = st.slider("Hold %", 0.0, 100.0, float(st.session_state.coeffs.get('Hold_Pct', 10.0)))
        with f4: n_grav = st.slider("Event Gravity %", 0.0, 100.0, float(st.session_state.coeffs.get('Event_Gravity', 25.0)))
        
        if st.form_submit_button("🚀 Recalibrate Engine", use_container_width=True):
            st.session_state.coeffs.update({
                "Static_Count": n_sc, "Static_Weight": n_sw,
                "Digital_OOH_Count": n_dc, "Digital_OOH_Weight": n_dw,
                "Avg_Coin_In": n_spend, "Property_Theo": n_theo,
                "Hold_Pct": n_hold, "Event_Gravity": n_grav
            })
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            st.success("Weights saved.")

# =================================================================
# 13. PAGE 7: FORECAST SANDBOX
# =================================================================
elif page == "🧪 Forecast Sandbox":
    st.header("🧪 Strategic Forecast Simulator")
    
    c = st.session_state.coeffs
    ooh_inertia = (float(c['Static_Count']) * float(c['Static_Weight'])) + \
                  (float(c['Digital_OOH_Count']) * float(c['Digital_OOH_Weight']))

    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("🎛️ Market Inputs")
        s_clicks = st.number_input("Planned Ad Clicks", 500)
        s_imp = st.number_input("Planned Impressions", 10000)
        sim_attend = st.number_input("Projected Concert Attendance", 1800)
    
    with col_r:
        st.subheader("❄️ Environment Friction")
        s_snow = st.slider("Snow Forecast (cm)", 0, 50, 0)
        s_rain = st.slider("Rain Forecast (mm)", 0, 50, 0)

    # SIMULATION ENGINE
    m_lift = (s_clicks * c['Clicks']) + (s_imp * c['Social_Imp'])
    e_lift = sim_attend * (c['Event_Gravity']/100)
    w_loss = (s_snow * c['Snow_cm']) + (s_rain * c['Rain_mm'])
    
    pred_head = max(0, 4365 + ooh_inertia + m_lift + e_lift + w_loss)
    pred_win = pred_head * c['Avg_Coin_In'] * (c['Hold_Pct']/100)

    st.divider()
    res1, res2, res3 = st.columns(3)
    res1.metric("Predicted Daily Traffic", f"{int(pred_head):,} Guests")
    res2.metric("Projected Daily Win", f"${pred_win:,.2f}")
    res3.metric("OOH Passive Inertia", f"+{int(ooh_inertia)} Guests")

# =================================================================
# 14. FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
