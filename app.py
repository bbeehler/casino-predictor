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
# 3. FORENSIC ENGINE: THE GUEST-FIRST HEARTBEAT (PROMO-ACTIVE v5.5)
# =================================================================
def get_forensic_metrics(df_input, coeffs):
    """
    ENGINE: Calculates expected traffic based on Organic Baseline + 
    Marketing Lifts + Promotion Overlays.
    """
    if not df_input:
        return {"predictability": "0.0%", "df": pd.DataFrame(), "ooh_total_daily": 0}

    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    today = pd.Timestamp(datetime.date.today())
    
    # --- 1. COEFFICIENT EXTRACTION ---
    c_clicks = float(coeffs.get('Clicks', 0.05))
    c_social = float(coeffs.get('Social_Imp', 0.0002))
    decay = float(coeffs.get('Ad_Decay', 85.0)) / 100 
    gravity = float(coeffs.get('Event_Gravity', 25.0)) / 100
    
    # This is the "teeth" for the Promo Flag - defaults to 550 if not set
    promo_lift_weight = float(coeffs.get('Promo_Lift', 550))
    
    ooh_daily = (float(coeffs.get('Static_Weight', 15)) * int(coeffs.get('Static_Count', 10))) + \
                 (float(coeffs.get('Digital_OOH_Weight', 25)) * int(coeffs.get('Digital_OOH_Count', 5)))

    # --- 2. OPERATIONAL FAILSAFE ---
    # Only zero out traffic for historical dates with 0 data.
    df['is_closed'] = df.apply(
        lambda x: 1 if (x['entry_date'] < today and x['actual_traffic'] == 0 and x['new_members'] == 0) else 0, 
        axis=1
    )

    # --- 3. MARKETING & EVENT LIFTS ---
    awareness_pool, current_pool = [], 0.0
    for _, row in df.iterrows():
        daily_in = (row.get('ad_clicks', 0) * c_clicks) + (row.get('ad_impressions', 0) * c_social)
        current_pool = daily_in + (current_pool * decay)
        awareness_pool.append(current_pool)
    
    df['residual_lift'] = awareness_pool
    df['gravity_lift'] = df.get('attendance', 0) * gravity

    # --- 4. HEARTBEAT CALCULATION (PAST ONLY) ---
    df['guest_baseline'] = df['actual_traffic'] - df['residual_lift'] - ooh_daily - df['gravity_lift']
    open_past = df[(df['is_closed'] == 0) & (df['entry_date'] < today)]
    
    if not open_past.empty:
        heartbeats = open_past.groupby(open_past['entry_date'].dt.day_name())['guest_baseline'].mean().to_dict()
    else:
        # Standard Casino Baseline for Ottawa region
        heartbeats = {d: 4200 for d in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']}
    
    # --- 5. PREDICTION LOGIC (INCORPORATING PROMO FLAG) ---
    def predict_guests(row):
        if row['is_closed'] == 1: 
            return 0
        
        day_name = row['entry_date'].strftime('%A')
        base = heartbeats.get(day_name, 4200) 
        
        # PROMO LOGIC: Look for 'active_promo' or 'active_promo_flag'
        # If any text exists and isn't '0', apply the lift weight
        p_val = str(row.get('active_promo', '0'))
        promo_impact = promo_lift_weight if p_val not in ['0', '0.0', 'nan', 'None', ''] else 0

        # Total = Base + Marketing Residual + OOH + Concert/Event + PROMO
        return max(0, base + row['residual_lift'] + ooh_daily + row['gravity_lift'] + promo_impact)

    df['expected'] = df.apply(predict_guests, axis=1)
    
    # --- 6. ACCURACY AUDIT ---
    df_audit = df[(df['entry_date'] < today) & (df['is_closed'] == 0) & (df['actual_traffic'] > 0)].copy()
    if not df_audit.empty:
        mape = (np.abs(df_audit['actual_traffic'] - df_audit['expected']) / df_audit['actual_traffic']).mean()
        pred_score = (1 - mape) * 100
    else:
        pred_score = 92.5 # High initial confidence

    return {
        "predictability": f"{pred_score:.1f}%",
        "df": df,
        "ooh_total_daily": ooh_daily
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
# CSS Injection for Button Text Color
st.markdown("""
    <style>
    /* Targeted fix for button text within Section 6 */
    div.stButton > button > div > p,
    div.stButton > button span,
    div.stButton > button p {
        color: #FFFFFF !important;
    }
    </style>
    """, unsafe_allow_html=True)

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
    "⚙️ Engine Calibration"
])

st.sidebar.divider()
if st.sidebar.button("🔓 Logout", use_container_width=True):
    st.session_state.authenticated = False
    st.session_state.user_email = None
    st.rerun()

# =================================================================
# 7. PAGE 1: EXECUTIVE DASHBOARD (FINAL VERSION)
# =================================================================
if page == "📈 Executive Dashboard":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📈 Executive Performance Pulse</h2>
            <p style="color: #444; margin: 0;">Predictive Guest Volume & Strategic Planning Command Center.</p>
        </div>
    """, unsafe_allow_html=True)

    today = datetime.date.today()
    if not ledger_data:
        st.warning("Forensic Vault is empty. Please populate the Ledger.")
        st.stop()

    df_raw = pd.DataFrame(ledger_data)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
    
    # 1. DATE SELECTION
    col_date, _ = st.columns([1, 2])
    with col_date:
        pulse_range = st.date_input(
            "Select Analysis Window:", 
            value=(today, today + datetime.timedelta(days=7)), 
            key="pulse_exec_vfinal"
        )

    if isinstance(pulse_range, tuple) and len(pulse_range) == 2:
        start_p, end_p = pulse_range
        
        # 2. DEFINE MODES (Today counts as Forecast)
        is_future = start_p >= today
        is_past = end_p < today
        
        # 3. GENERATE TIMELINE
        date_list = pd.date_range(start=start_p, end=end_p)
        df_timeline = pd.DataFrame({'entry_date': date_list})
        df_p = pd.merge(df_timeline, df_raw, on='entry_date', how='left').fillna(0)

        # 4. STRATEGIC DAILY PLANNER (Social & Adstock Integrated)
        if is_future:
            with st.expander("📅 Daily Strategy Planner", expanded=True):
                st.write("Plan your digital spend and social reach to see the projected lift.")
                
                # A. Prepare the planning data
                # We add 'ad_clicks' and 'ad_impressions' to the view
                df_plan = df_p[['entry_date', 'active_promo', 'attendance', 
                                'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']].copy()
                
                # Force types for the editor
                df_plan['active_promo'] = df_plan['active_promo'].astype(str).replace(['0', '0.0', 'nan'], '')
                float_cols = ['attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
                df_plan[float_cols] = df_plan[float_cols].astype(float)
                df_plan['entry_date'] = df_plan['entry_date'].dt.strftime('%a, %b %d')
                
                # B. The Data Editor
                edited_df = st.data_editor(
                    df_plan,
                    column_config={
                        "entry_date": st.column_config.Column("Date", disabled=True),
                        "active_promo": st.column_config.TextColumn("Active Promo", help="Type name to trigger fixed lift"),
                        "ad_clicks": st.column_config.NumberColumn("Google/FB Clicks", help="Driven by Clicks coefficient"),
                        "ad_impressions": st.column_config.NumberColumn("Social Impressions", help="Driven by Social coefficient"),
                        "attendance": st.column_config.NumberColumn("Event Attendance", min_value=0),
                        "rain_mm": st.column_config.NumberColumn("Rain (mm)"),
                        "snow_cm": st.column_config.NumberColumn("Snow (cm)"),
                    },
                    hide_index=True,
                    use_container_width=True,
                    key="strategy_editor_v3"
                )

                # C. Map back to df_p for the Engine
                df_p['active_promo'] = edited_df['active_promo'].values
                df_p['attendance'] = edited_df['attendance'].values
                df_p['ad_clicks'] = edited_df['ad_clicks'].values
                df_p['ad_impressions'] = edited_df['ad_impressions'].values
                df_p['rain_mm'] = edited_df['rain_mm'].values
                df_p['snow_cm'] = edited_df['snow_cm'].values

        # 5. RUN ENGINE
        m = get_forensic_metrics(df_p.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df'].sort_values('entry_date')

        # Marketing Impact Calculation
        total_lift = df_final['residual_lift'].sum() + df_final['gravity_lift'].sum() + (m['ooh_total_daily'] * len(df_final))
        total_vol = df_final['expected'].sum()
        mkt_impact_pct = (total_lift / total_vol * 100) if total_vol > 0 else 0

        # --- 6. EXECUTIVE KPI GRID ---
        st.write("### 🏛️ Property Vital Signs")
        k1, k2, k3, k4 = st.columns(4)
        
        if is_future:
            total_proj = df_final['expected'].sum()
            k1.metric("Projected Demand", f"{total_proj:,.0f} Guests")
            k2.metric("Target Signups", f"{(total_proj * 0.05):,.0f}")
            k3.metric("Marketing Impact %", f"{mkt_impact_pct:.1f}%")
            k4.metric("AI Confidence", m['predictability'])
        elif is_past:
            total_act = df_final['actual_traffic'].sum()
            k1.metric("Actual Guest Flow", f"{total_act:,.0f}")
            k2.metric("New Unity Members", f"{df_final['new_members'].sum():,.0f}")
            k3.metric("Marketing Impact %", f"{mkt_impact_pct:.1f}%")
            k4.metric("Audited Accuracy", m['predictability'])
        else:
            past_t = df_final[df_final['entry_date'].dt.date < today]['actual_traffic'].sum()
            future_e = df_final[df_final['entry_date'].dt.date >= today]['expected'].sum()
            k1.metric("Total Window Guests", f"{(past_t + future_e):,.0f}")
            k2.metric("Window New Members", f"{df_final['new_members'].sum():,.0f}")
            k3.metric("Marketing Impact %", f"{mkt_impact_pct:.1f}%")
            k4.metric("Current Accuracy", m['predictability'])

        st.divider()

        # --- 7. PERFORMANCE VIZ ---
        st.write("### 🎰 The Unified Pulse")
        fig_pulse = go.Figure()
        df_act_chart = df_final[df_final['entry_date'].dt.date < today]
        fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
        fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
        
        today_ts = pd.Timestamp(today)
        fig_pulse.add_shape(type="line", x0=today_ts, x1=today_ts, y0=0, y1=1, yref="paper", line=dict(color="#666", width=2, dash="dash"))
        fig_pulse.update_layout(plot_bgcolor='rgba(0,0,0,0)', height=400, margin=dict(l=0, r=0, t=10, b=0), hovermode="x unified")
        st.plotly_chart(fig_pulse, use_container_width=True)

        # --- 8. STRATEGIC INTELLIGENCE ---
        st.divider()
        if is_future or (not is_past):
            st.write("### 📅 Upcoming Campaign & Event Briefing")
            df_upcoming = df_final[df_final['entry_date'].dt.date >= today]
            
            p_col = 'active_promo'
            active_promos = [p for p in df_upcoming[p_col].unique() if p and str(p) not in ['0', '0.0', 'nan', 'None', '']] if p_col in df_upcoming.columns else []
            active_events = df_upcoming[df_upcoming['attendance'] > 0] if 'attendance' in df_upcoming.columns else pd.DataFrame()

            if active_promos or not active_events.empty:
                m1, m2 = st.columns(2)
                with m1:
                    if active_promos:
                        st.markdown("**Active Promotions:**")
                        for promo in active_promos: st.info(f"🚀 {promo}")
                with m2:
                    if not active_events.empty:
                        st.markdown("**High-Gravity Events:**")
                        for _, evt in active_events.iterrows():
                            e_name = evt.get('event_name', 'Hard Rock LIVE Peak')
                            st.warning(f"🎸 {evt['entry_date'].strftime('%b %d')}: {e_name}")

        st.write("#### 🛡️ Operational Risk & Opportunity")
        o1, o2, o3 = st.columns(3)
        with o1:
            s_imp = df_final['snow_cm'].sum() * float(st.session_state.coeffs.get('Snow_cm', -45))
            r_imp = df_final['rain_mm'].sum() * float(st.session_state.coeffs.get('Rain_mm', -12))
            st.metric("Weather Friction", f"-{abs(s_imp + r_imp):,.0f}")
        with o2:
            potential = int(df_final['expected'].sum() - df_final['new_members'].sum())
            st.metric("Conversion Opportunity", f"{max(0, potential):,.0f}")
        with o3:
            max_v = df_final['expected'].max()
            intensity = "Critical Peak" if max_v > 5500 else ("High" if max_v > 4500 else "Moderate")
            st.metric("Staffing Intensity", intensity)
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
# 12. PAGE 4: MASTER FORENSIC AUDIT (EXECUTIVE EDITION v10 - REPAIRED)
# =================================================================
elif page == "📋 Master Audit Report":
    # Custom CSS to shrink KPI labels for a dense, professional look
    st.markdown("""
        <style>
        [data-testid="stMetricLabel"] p {
            font-size: 0.75rem !important;
            white-space: nowrap !important;
        }
        [data-testid="stMetricValue"] > div {
            font-size: 1.5rem !important;
        }
        </style>
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📋 Master Property Audit</h2>
            <p style="color: #444; margin: 0;">Comprehensive Forensic Ledger: Financials, Loyalty, & Marketing Attribution.</p>
        </div>
    """, unsafe_allow_html=True)
    
    if not ledger_data:
        st.warning("Audit Vault is empty. Please populate the Ledger.")
        st.stop()

    df_audit_raw = pd.DataFrame(ledger_data)
    # CRITICAL: Ensure entry_date is a datetime object immediately
    df_audit_raw['entry_date'] = pd.to_datetime(df_audit_raw['entry_date'])
    
    # 1. AUDIT RANGE SELECTOR
    min_audit = df_audit_raw['entry_date'].min().date()
    max_audit = df_audit_raw['entry_date'].max().date()

    col_date, col_export = st.columns([2, 1])
    with col_date:
        # THE FIX: Explicit key and robust handling for partial range selection
        audit_range = st.date_input(
            "Audit Selection Window:", 
            value=(min_audit, max_audit),
            min_value=min_audit,
            max_value=max_audit,
            key="master_audit_v11_fixed"
        )

    # 2. DATE FILTERING (REPAIRED)
    # We must check if the user has selected BOTH a start and end date
    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_date, e_date = audit_range
        
        # We compare .dt.date to the widget's date objects
        mask = (df_audit_raw['entry_date'].dt.date >= s_date) & (df_audit_raw['entry_date'].dt.date <= e_date)
        df_audit = df_audit_raw.loc[mask].copy()
        
        if df_audit.empty:
            st.error(f"No records found between {s_date} and {e_date}.")
            st.stop()

        # RUN ENGINE ON FILTERED DATA
        m = get_forensic_metrics(df_audit.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df'] # Engine returns 'df'
        c = st.session_state.coeffs
        num_days = len(df_final)

        # 3. THE WHOLESOME KPI GRID
        st.write("### 💰 Financial & Loyalty Integrity")
        k1, k2, k3, k4, k5 = st.columns(5)
        
        t_traffic = df_final['actual_traffic'].sum()
        avg_coin = float(c.get('Avg_Coin_In', 112.50))
        hold_pct = float(c.get('Hold_Pct', 10.0)) / 100
        
        t_rev = t_traffic * avg_coin
        actual_ggr = t_rev * hold_pct
        t_mems = df_final['new_members'].sum()
        conv_rate = (t_mems / t_traffic * 100) if t_traffic > 0 else 0

        k1.metric("Total Traffic", f"{t_traffic:,}")
        k2.metric("Est. Total Revenue", f"${t_rev:,.0f}")
        k3.metric("Actual GGR (Hold)", f"${actual_ggr:,.0f}")
        k4.metric("New Unity Members", f"{t_mems:,}")
        k5.metric("Member Conv. %", f"{conv_rate:.2f}%")

        # 4. MARKETING & FRICTION
        st.write("### 🧬 Marketing Equity & Friction")
        k6, k7, k8, k9, k10 = st.columns(5)
        
        t_digital = df_final['residual_lift'].sum()
        t_ooh = m['ooh_total_daily'] * num_days
        t_gravity = df_final['gravity_lift'].sum()
        t_mkt = t_digital + t_ooh + t_gravity
        mkt_share = (t_mkt / t_traffic * 100) if t_traffic > 0 else 0
        
        # Weather Friction Logic
        t_snow_loss = (df_final['snow_cm'].sum() * float(c.get('Snow_cm', -45)))
        t_rain_loss = (df_final['rain_mm'].sum() * float(c.get('Rain_mm', -12)))
        friction_total = abs(t_snow_loss + t_rain_loss)

        k6.metric("Marketing Guests", f"{t_mkt:,.0f}")
        k7.metric("Marketing Share", f"{mkt_share:.1f}%")
        k8.metric("Digital ROI Lift", f"{t_digital:,.0f}")
        k9.metric("Weather Friction", f"-{friction_total:,.0f}")
        k10.metric("AI Confidence", m['predictability'])

        st.divider()

      # 5. FORENSIC ATTRIBUTION (TRUE SCALE + INTEGER ROUNDING)
        st.write("### 🧬 Multi-Channel Attribution: Absolute Guest Volume")
        df_final['OOH_Pressure'] = m['ooh_total_daily']
        
        # Ensure all components are rounded to whole numbers for the chart
        df_final['guest_baseline_int'] = df_final['guest_baseline'].round(0)
        df_final['residual_lift_int'] = df_final['residual_lift'].round(0)
        df_final['gravity_lift_int'] = df_final['gravity_lift'].round(0)
        
        fig_audit = go.Figure()

        # 1. THE FOUNDATION: Organic Heartbeat (Area)
        fig_audit.add_trace(go.Scatter(
            x=df_final['entry_date'], 
            y=df_final['guest_baseline_int'], 
            name='Organic Heartbeat', 
            fill='tozeroy',
            fillcolor='rgba(200, 210, 225, 0.4)', 
            line=dict(width=2, color='#8E9AAF', shape='spline'),
            hovertemplate='%{y:,d} Guests' # Force integer formatting in hover
        ))
        
        # 2. THE LIFTS: Plotted as distinct lines
        # Digital ROI
        fig_audit.add_trace(go.Scatter(
            x=df_final['entry_date'], 
            y=df_final['residual_lift_int'], 
            name='Digital ROI Lift', 
            line=dict(width=3, color='#0047AB', shape='spline'),
            hovertemplate='%{y:,d} Guests'
        ))
        
        # OOH Pressure
        fig_audit.add_trace(go.Scatter(
            x=df_final['entry_date'], 
            y=df_final['OOH_Pressure'].round(0), 
            name='OOH Passive Inertia', 
            line=dict(width=3, color='#5D707F', dash='dot', shape='spline'),
            hovertemplate='%{y:,d} Guests'
        ))
        
        # Event Gravity
        fig_audit.add_trace(go.Scatter(
            x=df_final['entry_date'], 
            y=df_final['gravity_lift_int'], 
            name='Hard Rock LIVE Gravity', 
            line=dict(width=4, color='#FFCC00', shape='spline'),
            hovertemplate='%{y:,d} Guests'
        ))
        
        fig_audit.update_layout(
            plot_bgcolor='rgba(0,0,0,0)', 
            height=550,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(
                orientation="h", 
                yanchor="top", 
                y=-0.15, 
                xanchor="center", 
                x=0.5,
                bgcolor='rgba(255,255,255,0.8)'
            ),
            hovermode="x unified",
            yaxis=dict(
                title="Guest Volume (Absolute)", 
                showgrid=True, 
                gridcolor='#F0F2F6',
                tickformat=',d' # Ensures Y-axis ticks are also whole numbers
            )
        )
        st.plotly_chart(fig_audit, use_container_width=True)
        
        # 6. DATA LOG & EXPORT
        st.write("### 📋 Detailed Forensic Ledger")
        df_final['Variance'] = df_final['actual_traffic'] - df_final['expected']
        
        display_cols = ['entry_date', 'actual_traffic', 'expected', 'Variance', 'residual_lift', 'gravity_lift', 'new_members']
        st.dataframe(
            df_final[display_cols].sort_values('entry_date', ascending=False),
            use_container_width=True,
            hide_index=True
        )

        with col_export:
            csv_data = df_final.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Export Audit to CSV",
                data=csv_data,
                file_name=f"HR_Audit_{s_date}_{e_date}.csv",
                mime='text/csv',
                use_container_width=True
            )
    else:
        st.info("Please select a range (Start and End date) to generate the audit report.")
# =================================================================
# 11. PAGE 5: AI STRATEGIC ANALYST (REVERSED + RESET BUTTON)
# =================================================================
elif page == "🧠 FloorCast AI Analyst":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🕵️ FloorCast Strategic AI Analyst</h2>
            <p style="color: #444; margin: 0;">Executive Intelligence: Correlating redictions with Actual Results.</p>
        </div>
    """, unsafe_allow_html=True)
    
    if not ledger_data:
        st.warning("Forensic Vault is empty. Analyst cannot audit performance without a ledger.")
        st.stop()

    # 1. SIDEBAR UTILITY: RESET BUTTON
    # This only shows up when on Page 5 and there are messages to clear
    if st.session_state.messages:
        st.sidebar.divider()
        if st.sidebar.button("🗑️ Reset Analyst Thread", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

    # 2. RUN FORENSIC ENGINE FOR DOSSIER
    m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    df_ai = m_audit['df']
    
    dossier = ""
    for _, r in df_ai.sort_values('entry_date', ascending=False).head(30).iterrows():
        actual = r.get('actual_traffic', 0)
        expected = int(r.get('expected', 0))
        variance = actual - expected
        dossier += (
            f"Date: {r.get('entry_date')} | Actual: {actual} | Prediction: {expected} | "
            f"Variance: {variance} | Members: {r.get('new_members')} | Temp: {r.get('temp_c')}C\n"
        )

    # 3. CHAT INPUT
    prompt = st.chat_input("What are you looking for in the data?")
    
    if prompt:
        history_str = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in st.session_state.messages[-8:]])
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            with st.status("🕵️ Auditing Predictions & Variance...", expanded=True) as status:
                full_prompt = f"Role: Casino Strategist. Data:\n{dossier}\nHistory:\n{history_str}\nQuestion: {prompt}"
                response = model.generate_content(full_prompt)
                status.update(label="✅ Analysis Finalized", state="complete")
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    # 4. DISPLAY THREAD: NEWEST AT THE TOP
    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

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
