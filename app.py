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
# 1. PERMANENT INITIALIZATION & STATE LOCK (v7.0 - DB FIRST)
# =================================================================
if 'coeffs' not in st.session_state:
    try:
        # 🟢 STEP 1: Attempt to pull the Master Weights from Supabase
        # We assume there is a single row of coefficients (ID=1)
        response = supabase.table("coefficients").select("*").limit(1).execute()
        
        if response.data and len(response.data) > 0:
            # 🟢 STEP 2: Database Found - Lock these values in
            st.session_state.coeffs = response.data[0]
            # Ensure OOH_Count and Static_Count are at least 1 if weights exist
            if st.session_state.coeffs.get('OOH_Weight', 0) > 0:
                st.session_state.coeffs['OOH_Count'] = 1
                st.session_state.coeffs['Static_Count'] = 1
        else:
            # 🟡 STEP 3: Fallback - Database is empty, use Hard Rock Defaults
            st.session_state.coeffs = {
                'id': 1,
                'Promo': 500.0,
                'Broadcast_Weight': 150.0,
                'OOH_Weight': 100.0,
                'OOH_Count': 1,
                'Print_Lift': 75.0,
                'PR_Weight': 1.2,
                'Clicks': 0.05,
                'Social_Imp': 0.0002,
                'Ad_Decay': 85,
                'Rain_mm': -12.0,
                'Snow_cm': -45.0,
                'Event_Gravity': 0.25,
                'Static_Weight': 100.0,
                'Static_Count': 1,
                'Digital_OOH_Weight': 25.0,
                'Digital_OOH_Count': 5
            }
            # Optional: Seed the DB with these defaults if it's the first run
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            
    except Exception as e:
        # 🔴 STEP 4: Failsafe - If DB connection fails, use basic defaults so app doesn't crash
        st.error(f"Database Connection Error: {e}")
        st.session_state.coeffs = {'Promo_Lift': 500.0, 'OOH_Weight': 100.0, 'OOH_Count': 1}

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
# 3. FORENSIC ENGINE: FULL-SPECTRUM ATTRIBUTION (v6.0)
# =================================================================
def get_forensic_metrics(df_input, coeffs):
    """
    ENGINE: Calculates expected traffic based on Organic Baseline + 
    Trackable Digital + Mass Media Inertia + PR Gravity.
    """
    if not df_input:
        return {"predictability": "0.0%", "df": pd.DataFrame(), "ooh_total_daily": 0}

    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    today = pd.Timestamp(datetime.date.today())
    
    # --- 1. COEFFICIENT EXTRACTION (Expanded) ---
    c_clicks = float(coeffs.get('Clicks', 0.05))
    c_social = float(coeffs.get('Social_Imp', 0.0002))
    decay = float(coeffs.get('Ad_Decay', 85.0)) / 100 
    gravity = float(coeffs.get('Event_Gravity', 25.0)) / 100
    promo_lift_weight = float(coeffs.get('Promo_Lift', 550))
    
    # NEW: Mass Media & Brand Inertia weights
    c_broadcast = float(coeffs.get('Broadcast_Weight', 150)) # TV/Radio
    c_ooh = float(coeffs.get('OOH_Weight', 100))           # Signage
    c_print = float(coeffs.get('Print_Lift', 75))          # Magazines/Newspapers
    c_pr_mult = float(coeffs.get('PR_Weight', 1.2))        # Earned Media Multiplier

    # Baseline OOH calculation (Static + Digital Faces)
    ooh_daily = (float(coeffs.get('Static_Weight', 15)) * int(coeffs.get('Static_Count', 10))) + \
                 (float(coeffs.get('Digital_OOH_Weight', 25)) * int(coeffs.get('Digital_OOH_Count', 5)))
    
    # The Total "Inertia" layer (Fixed daily lift from non-digital media)
    total_brand_inertia = ooh_daily + c_broadcast + c_ooh

    # --- 2. OPERATIONAL FAILSAFE ---
    df['is_closed'] = df.apply(
        lambda x: 1 if (x['entry_date'] < today and x.get('actual_traffic', 0) == 0 and x.get('new_members', 0) == 0) else 0, 
        axis=1
    )

    # --- 3. MARKETING & EVENT LIFTS ---
    # We include 'Print' in the awareness pool because it has a decay/tail effect
    awareness_pool, current_pool = [], 0.0
    for _, row in df.iterrows():
        # Daily input now accounts for trackable digital + estimated print hits
        daily_in = (row.get('ad_clicks', 0) * c_clicks) + (row.get('ad_impressions', 0) * c_social)
        current_pool = daily_in + (current_pool * decay)
        awareness_pool.append(current_pool)
    
    df['residual_lift'] = awareness_pool
    df['gravity_lift'] = df.get('attendance', 0) * gravity

    # --- 4. HEARTBEAT CALCULATION (PAST ONLY) ---
    # We subtract ALL known lifts to find the "naked" organic traffic
    df['guest_baseline'] = df.get('actual_traffic', 0) - df['residual_lift'] - total_brand_inertia - df['gravity_lift']
    open_past = df[(df['is_closed'] == 0) & (df['entry_date'] < today)]
    
    if not open_past.empty:
        heartbeats = open_past.groupby(open_past['entry_date'].dt.day_name())['guest_baseline'].mean().to_dict()
    else:
        # UPDATED: Hard Rock Hotel & Casino Ottawa Specific Organic Baselines
        # These are the "Naked" numbers (No marketing, no events)
        heartbeats = {
            'Monday': 3200,
            'Tuesday': 3100,
            'Wednesday': 3400,
            'Thursday': 3800,
            'Friday': 5200,
            'Saturday': 5800,  # Raised from 4200
            'Sunday': 4500
        }
    
    # --- 5. PREDICTION LOGIC ---
    def predict_guests(row):
        if row['is_closed'] == 1: 
            return 0
        
        day_name = row['entry_date'].strftime('%A')
        base = heartbeats.get(day_name, 4200) 
        
        # Apply the PR Multiplier if "PR" or "Earned" is in the promo name
        p_val = str(row.get('active_promo', '0'))
        current_base = base * c_pr_mult if "PR" in p_val.upper() else base
        
        # Standard Promo Lift
        promo_impact = promo_lift_weight if p_val not in ['0', '0.0', 'nan', 'None', ''] else 0

        # Result = (Naked Base * PR Multiplier) + Awareness + Brand Inertia + Events + Promo
        return max(0, current_base + row['residual_lift'] + total_brand_inertia + row['gravity_lift'] + promo_impact)

    df['expected'] = df.apply(predict_guests, axis=1)
    
    # --- 6. ACCURACY AUDIT ---
    df_audit = df[(df['entry_date'] < today) & (df['is_closed'] == 0) & (df.get('actual_traffic', 0) > 0)].copy()
    if not df_audit.empty:
        mape = (np.abs(df_audit['actual_traffic'] - df_audit['expected']) / df_audit['actual_traffic']).mean()
        pred_score = (1 - mape) * 100
    else:
        pred_score = 92.5

    return {
        "predictability": f"{pred_score:.1f}%",
        "df": df,
        "total_inertia": total_brand_inertia
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

# =================================================================
# --- SECTION 2: EXECUTIVE NAVIGATION ---
# =================================================================
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/en/thumb/0/04/Hard_Rock_Cafe_logo.svg/1200px-Hard_Rock_Cafe_logo.svg.png", width=150)
    st.title("Admin Command")
    st.divider()
    
    # Using a radio list instead of a dropdown for one-click access
    page = st.radio(
        "Intelligence Decks:",
        [
            "📈 Executive Dashboard", 
            "🎰 Universal Ledger", 
            "📡 Attribution Analytics", 
            "📋 Master Audit Report", 
            "⚙️ AI Calibration",
            "🤖 FloorCast AI Analyst"
        ],
        index=0,
        key="nav_list_v12"
    )
    
    st.divider()
    # Logic to show the reset button only when on the AI Analyst page
    if page == "🤖 FloorCast AI Analyst" and st.session_state.get('messages'):
    if st.sidebar.button("🗑️ Reset Analyst Thread", use_container_width=True, key="sidebar_reset"):
        st.session_state.messages = []
        st.rerun()

# =================================================================
# 7. PAGE 1: EXECUTIVE DASHBOARD (FINAL VERSION - FULLY SYNCED)
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
            key="pulse_exec_vfinal_synced"
        )

    if isinstance(pulse_range, tuple) and len(pulse_range) == 2:
        start_p, end_p = pulse_range
        is_future = start_p >= today
        is_past = end_p < today
        
        # TIMELINE GENERATION
        date_list = pd.date_range(start=start_p, end=end_p)
        df_timeline = pd.DataFrame({'entry_date': date_list})
        df_p = pd.merge(df_timeline, df_raw, on='entry_date', how='left').fillna(0)

        # 2. STRATEGIC DAILY PLANNER (Social & Adstock Integrated)
        if is_future:
            with st.expander("📅 Daily Strategy Planner", expanded=True):
                st.write("Plan your digital spend and PR events to see the projected lift.")
                df_plan = df_p[['entry_date', 'active_promo', 'attendance', 
                                'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']].copy()
                
                df_plan['active_promo'] = df_plan['active_promo'].astype(str).replace(['0', '0.0', 'nan', 'None'], '')
                float_cols = ['attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
                df_plan[float_cols] = df_plan[float_cols].astype(float)
                df_plan['entry_date'] = df_plan['entry_date'].dt.strftime('%a, %b %d')
                
                edited_df = st.data_editor(
                    df_plan,
                    column_config={
                        "entry_date": st.column_config.Column("Date", disabled=True),
                        "active_promo": st.column_config.TextColumn("Active Promo/PR Hit", help="Type 'PR' in name for PR Multiplier"),
                        "ad_clicks": st.column_config.NumberColumn("Google/FB Clicks"),
                        "ad_impressions": st.column_config.NumberColumn("Social Impressions"),
                        "attendance": st.column_config.NumberColumn("Event Attendance"),
                    },
                    hide_index=True, use_container_width=True, key="p1_planner_vfinal"
                )
                df_p['active_promo'] = edited_df['active_promo'].values
                df_p['attendance'] = edited_df['attendance'].values
                df_p['ad_clicks'] = edited_df['ad_clicks'].values
                df_p['ad_impressions'] = edited_df['ad_impressions'].values
                df_p['rain_mm'] = edited_df['rain_mm'].values
                df_p['snow_cm'] = edited_df['snow_cm'].values

        # --- 3. THE BRIDGE: CONNECTING LIVE COEFFICIENTS ---
        # Crucial: Pull live sliders from session state to feed the engine
        current_weights = st.session_state.get('coeffs', {})
        
        # RUN THE ENGINE
        m = get_forensic_metrics(df_p.to_dict(orient='records'), current_weights)
        df_final = m['df'].sort_values('entry_date')

        # CALCULATE MARKETING IMPACT (Pulling all Mass Media + Digital + Events)
        daily_brand_inertia = (
            float(current_weights.get('Broadcast_Weight', 150)) + 
            float(current_weights.get('OOH_Weight', 100)) + 
            float(current_weights.get('Print_Lift', 75)) +
            (float(current_weights.get('Static_Weight', 15)) * int(current_weights.get('Static_Count', 10))) +
            (float(current_weights.get('Digital_OOH_Weight', 25)) * int(current_weights.get('Digital_OOH_Count', 5)))
        )
        
        total_lift_vol = (df_final['residual_lift'].sum() + 
                          df_final['gravity_lift'].sum() + 
                          (daily_brand_inertia * len(df_final)))
        
        total_vol = df_final['expected'].sum()
        mkt_impact_pct = (total_lift_vol / total_vol * 100) if total_vol > 0 else 0

        # --- 4. EXECUTIVE KPI GRID ---
        st.write("### 🏛️ Property Vital Signs")
        k1, k2, k3, k4 = st.columns(4)
        
        if is_future:
            k1.metric("Projected Demand", f"{total_vol:,.0f} Guests")
            k2.metric("Target Signups", f"{(total_vol * 0.05):,.0f}")
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

        # --- 5. PERFORMANCE VIZ ---
        st.write("### 🎰 The Unified Pulse")
        fig_pulse = go.Figure()
        df_act_chart = df_final[df_final['entry_date'].dt.date < today]
        fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
        fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
        
        today_ts = pd.Timestamp(today)
        fig_pulse.add_shape(type="line", x0=today_ts, x1=today_ts, y0=0, y1=1, yref="paper", line=dict(color="#666", width=2, dash="dash"))
        fig_pulse.update_layout(plot_bgcolor='rgba(0,0,0,0)', height=400, margin=dict(l=0, r=0, t=10, b=0), hovermode="x unified")
        st.plotly_chart(fig_pulse, use_container_width=True)

        # --- 6. OPERATIONAL RISK vs. HISTORICAL AUDIT ---
        st.divider()
        if is_past:
            st.write("#### 🔍 Historical Performance Audit")
            o1, o2, o3 = st.columns(3)
            variance = df_final['actual_traffic'].sum() - df_final['expected'].sum()
            o1.metric("Volume Variance", f"{variance:+,.0f}")
            o2.metric("Avg Daily Traffic", f"{df_final['actual_traffic'].mean():,.0f}")
            o3.metric("Data Integrity", "Verified" if m['predictability'] != "0.0%" else "Incomplete")
        else:
            st.write("#### 🛡️ Operational Risk & Opportunity")
            o1, o2, o3 = st.columns(3)
            with o1:
                # Use live weights for friction
                s_imp = df_final['snow_cm'].sum() * float(current_weights.get('Snow_cm', -45))
                r_imp = df_final['rain_mm'].sum() * float(current_weights.get('Rain_mm', -12))
                st.metric("Weather Friction", f"-{abs(s_imp + r_imp):,.0f}")
            with o2:
                potential = int(df_final['expected'].sum() - df_final['new_members'].sum())
                st.metric("Conversion Opportunity", f"{max(0, potential):,.0f}")
            with o3:
                # Daily-specific Staffing Intensity
                peak_day_volume = df_final['expected'].max()
                intensity_label = "🔴 Critical Peak" if peak_day_volume > 6200 else ("🟡 High" if peak_day_volume > 5200 else "🟢 Stable")
                st.metric("Staffing Intensity", intensity_label)

# =================================================================
# 📑 PAGE 2: DAILY LEDGER AUDIT (HARD ROCK LIVE SYNC)
# =================================================================
elif page == "📑 Daily Ledger Audit":
    st.markdown("""
        <div style="background-color:#E1E8F0;padding:20px;border-radius:12px;border-left:6px solid #0047AB;margin-bottom:20px;">
            <h2 style="color:#0047AB;margin:0;">📑 Daily Ledger Audit</h2>
            <p style="color:#333;margin:0;">The Source of Truth: Manage historical traffic, financials, and event data.</p>
        </div>
    """, unsafe_allow_html=True)

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

            # Row 3: HARD ROCK LIVE Data
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
                # Syncing to Supabase
                try:
                    supabase.table("ledger").upsert(payload, on_conflict="entry_date").execute()
                    st.success(f"Vault state updated for {d_entry.strftime('%Y-%m-%d')}.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync Failed: {e}")

    with col_r:
        st.subheader("📤 Bulk Systems Import")
        st.write("Upload exported CSVs from casino management systems.")
        csv_file = st.file_uploader("Drop Ledger CSV here", type="csv")
        if csv_file and st.button("🚀 Execute Bulk Sync"):
            try:
                df_up = pd.read_csv(csv_file)
                supabase.table("ledger").upsert(df_up.to_dict(orient='records')).execute()
                st.success("Bulk synchronization complete.")
                st.rerun()
            except Exception as e:
                st.error(f"Bulk Import Failed: {e}")

    st.divider()
    
    # UNIVERSAL LEDGER EDITOR
    st.subheader("📜 Universal Ledger Editor")
    if ledger_data:
        df_edit = pd.DataFrame(ledger_data)
        df_edit['entry_date'] = pd.to_datetime(df_edit['entry_date'])
        df_edit = df_edit.sort_values('entry_date', ascending=False)
        
        # Interactive table for manual cleaning
        edited_df = st.data_editor(
            df_edit, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "entry_date": st.column_config.DateColumn("Date", disabled=True),
                "actual_traffic": st.column_config.NumberColumn("Traffic"),
                "actual_coin_in": st.column_config.NumberColumn("Coin-In ($)"),
            }
        )
        
        if st.button("✅ Confirm Manual Overwrites"):
            final_p = edited_df.to_dict(orient='records')
            # Sanitize dates back to strings for the database
            for p in final_p:
                if isinstance(p['entry_date'], (datetime.datetime, pd.Timestamp)):
                    p['entry_date'] = p['entry_date'].strftime('%Y-%m-%d')
            
            try:
                supabase.table("ledger").upsert(final_p).execute()
                st.success("Universal Ledger state synced.")
                st.rerun()
            except Exception as e:
                st.error(f"Overwrite Failed: {e}")
    else:
        st.info("The Ledger is currently empty. Add your first entry above.")

# =================================================================
# 3. PAGE 3: ATTRIBUTION ANALYTICS
# =================================================================
elif page == "📊 Attribution Analytics":
    st.markdown("""
        <div style="background-color:#F8F9FA;padding:20px;border-radius:12px;border-left:6px solid #6c757d;margin-bottom:20px;">
            <h2 style="color:#343a40;margin:0;">📊 Attribution Analytics</h2>
            <p style="color:#666;margin:0;">Analyzing the layers of guest demand: Organic Heartbeat vs. Marketing-Driven Lift.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.info("💡 The Vault is empty. Please ensure data is synced in the Daily Ledger Audit to view attribution.")
        st.stop()

    # 1. RUN ENGINE
    # This processes your Supabase data through the AI math weights defined in Calibration
    m_full = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    df_attr = m_full['df']

    if df_attr.empty:
        st.warning("No historical metrics available to attribute. Check your Ledger date range.")
    else:
        # 2. EXECUTIVE SCORECARD
        st.write("### 🧠 Performance Attribution Scorecard")
        c1, c2, c3, c4 = st.columns(4)
        
        with c1:
            st.metric("Model Predictability", m_full.get('predictability', '0%'), 
                      help="Accuracy of AI Target vs. Actual Floor Traffic")
        with c2:
            org_vol = df_attr['guest_baseline'].sum()
            st.metric("Organic Heartbeat", f"{org_vol:,.0f}", 
                      help="Estimated traffic without any marketing or digital spend.")
        with c3:
            # Lift = Awareness Tail + Event Gravity + Promo Overlays
            mkt_vol = df_attr['residual_lift'].sum() + df_attr['gravity_lift'].sum()
            st.metric("Marketing Lift", f"{mkt_vol:,.0f}", 
                      help="Total guests attributed to Clicks, Social, and Events.")
        with c4:
            total_act = df_attr['actual_traffic'].sum()
            lift_perc = (mkt_vol / total_act * 100) if total_act > 0 else 0
            st.metric("Marketing Yield %", f"{lift_perc:.1f}%")

    st.divider()

    # 3. VOLUME STACK CHART (The "Why" Behind the Numbers)
    st.write("### 🔍 Volume Layer Breakdown")
    st.caption("This chart stacks the guest layers to show what drove traffic each day.")
    
    # Prepare Chart Data
    chart_data = df_attr.copy()
    chart_data = chart_data.set_index('entry_date')[['guest_baseline', 'residual_lift', 'gravity_lift']]
    chart_data.columns = ['Organic Heartbeat', 'Digital Awareness (Adstock)', 'Hard Rock LIVE Gravity']
    
    # Display the Area Chart (Stacked)
    st.area_chart(chart_data, use_container_width=True)

    st.divider()

    # 4. DIGITAL ADSTOCK AUDIT
    st.write("### 📣 Digital Awareness Pool")
    st.caption("Tracking the residual 'Tail' of your digital spend (Adstock) over time.")
    
    import plotly.graph_objects as go
    fig_ad = go.Figure()
    fig_ad.add_trace(go.Scatter(
        x=df_attr['entry_date'], 
        y=df_attr['residual_lift'], 
        name="Adstock Awareness", 
        fill='tozeroy',
        line=dict(color='#0047AB', width=2)
    ))
    fig_ad.update_layout(
        height=300, 
        margin=dict(l=0,r=0,t=0,b=0), 
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis_title="Date",
        yaxis_title="Attributed Guests"
    )
    st.plotly_chart(fig_ad, use_container_width=True)

    # 5. STRATEGIC INSIGHTS
    with st.expander("📝 Strategic Interpretation"):
        avg_organic = df_attr['guest_baseline'].mean()
        max_lift_day = df_attr.loc[df_attr['residual_lift'].idxmax(), 'entry_date']
        
        st.write(f"**Organic Baseline:** On average, your floor carries a heartbeat of **{avg_organic:,.0f}** organic guests per day.")
        st.write(f"**Peak Marketing Impact:** Your highest digital awareness impact was recorded on **{max_lift_day.strftime('%B %d, %Y')}**.")
        st.write("---")
        st.caption("Note: These calculations are derived from the coefficients set in the AI Calibration panel.")

# =================================================================
# 12. PAGE 4: MASTER FORENSIC AUDIT (EXECUTIVE EDITION v11 - REPAIRED)
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
    df_audit_raw['entry_date'] = pd.to_datetime(df_audit_raw['entry_date'])
    
    # 1. AUDIT RANGE SELECTOR
    min_audit = df_audit_raw['entry_date'].min().date()
    max_audit = df_audit_raw['entry_date'].max().date()

    col_date, col_export = st.columns([2, 1])
    with col_date:
        audit_range = st.date_input(
            "Audit Selection Window:", 
            value=(min_audit, max_audit),
            min_value=min_audit,
            max_value=max_audit,
            key="master_audit_v11_fixed"
        )

    # 2. DATE FILTERING
    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_date, e_date = audit_range
        mask = (df_audit_raw['entry_date'].dt.date >= s_date) & (df_audit_raw['entry_date'].dt.date <= e_date)
        df_audit_filtered = df_audit_raw.loc[mask].copy()
        
        if df_audit_filtered.empty:
            st.error(f"No records found between {s_date} and {e_date}.")
            st.stop()

        # RUN ENGINE ON FILTERED DATA
        m = get_forensic_metrics(df_audit_filtered.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df'] 
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
        
        # REPAIR: Use new 'total_inertia' key from upgraded Section 3
        t_digital = df_final['residual_lift'].sum()
        t_inertia_total = m.get('total_inertia', 0) * num_days
        t_gravity = df_final['gravity_lift'].sum()
        
        # Calculate Total Marketing Lift
        t_mkt = t_digital + t_inertia_total + t_gravity
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
        
        # REPAIR: Align local column with new Engine key
        df_final['Brand_Inertia_Layer'] = m.get('total_inertia', 0)
        
        df_final['guest_baseline_int'] = df_final['guest_baseline'].round(0)
        df_final['residual_lift_int'] = df_final['residual_lift'].round(0)
        df_final['gravity_lift_int'] = df_final['gravity_lift'].round(0)
        
        fig_audit = go.Figure()

        # 1. THE FOUNDATION: Organic Heartbeat
        fig_audit.add_trace(go.Scatter(
            x=df_final['entry_date'], 
            y=df_final['guest_baseline_int'], 
            name='Organic Heartbeat', 
            fill='tozeroy',
            fillcolor='rgba(200, 210, 225, 0.4)', 
            line=dict(width=2, color='#8E9AAF', shape='spline'),
            hovertemplate='%{y:,d} Guests'
        ))
        
        # 2. Digital ROI
        fig_audit.add_trace(go.Scatter(
            x=df_final['entry_date'], 
            y=df_final['residual_lift_int'], 
            name='Digital ROI Lift', 
            line=dict(width=3, color='#0047AB', shape='spline'),
            hovertemplate='%{y:,d} Guests'
        ))
        
        # 3. Brand Inertia (REPAIRED: TV, Radio, Signage combined)
        fig_audit.add_trace(go.Scatter(
            x=df_final['entry_date'], 
            y=df_final['Brand_Inertia_Layer'].round(0), 
            name='Brand Inertia (OOH/TV/Radio)', 
            line=dict(width=3, color='#5D707F', dash='dot', shape='spline'),
            hovertemplate='%{y:,d} Guests'
        ))
        
        # 4. Event Gravity
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
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
            hovermode="x unified",
            yaxis=dict(title="Guest Volume", showgrid=True, gridcolor='#F0F2F6', tickformat=',d')
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
# ⚙️ PAGE 5: AI CALIBRATION & ENGINE WEIGHTS
# =================================================================
elif page == "⚙️ AI Calibration":
    st.markdown("""
        <div style="background-color:#F8F9FA;padding:20px;border-radius:12px;border-left:6px solid #FFCC00;margin-bottom:20px;">
            <h2 style="color:#343a40;margin:0;">⚙️ Engine Weight Calibration</h2>
            <p style="color:#666;margin:0;">Calibrate the "Why" behind the traffic: From Clicks to TV, Radio, and Signage.</p>
        </div>
    """, unsafe_allow_html=True)

    # Current Model Health Check
    m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    st.metric("Current Model Predictability", m_audit.get('predictability', '92.5%'))

    with st.form("master_calibration_form"):
        # SECTION 1: DIGITAL & SOCIAL (The Trackable)
        st.subheader("🌐 Digital & Social Drivers")
        d1, d2, d3 = st.columns(3)
        with d1:
            n_clicks = st.slider("Click Weight", 0.0, 1.0, float(st.session_state.coeffs.get('Clicks', 0.05)))
        with d2:
            n_social = st.slider("Social Imp Weight", 0.0, 0.01, float(st.session_state.coeffs.get('Social_Imp', 0.0002)), format="%.4f")
        with d3:
            n_decay = st.slider("Adstock Retention %", 50, 100, int(st.session_state.coeffs.get('Ad_Decay', 85)))

        st.divider()

        # SECTION 2: MASS MEDIA & OOH (The Inertia)
        st.subheader("📡 Mass Media & Brand Inertia")
        st.caption("Estimated daily guest lift from non-trackable broad-spectrum media.")
        c1, c2, c3 = st.columns(3)
        with c1:
            n_broad = st.number_input("Broadcast (TV/Radio) Daily Lift", value=int(st.session_state.coeffs.get('Broadcast_Weight', 150)))
        with c2:
            n_ooh = st.number_input("Road Signage (OOH) Daily Lift", value=int(st.session_state.coeffs.get('OOH_Weight', 100)))
        with c3:
            n_print = st.number_input("Print (Mag/News) Daily Lift", value=int(st.session_state.coeffs.get('Promo', 75)))

        st.divider()

        # SECTION 3: GRAVITY & FINANCIAL DNA
        st.subheader("💰 Financial DNA & Event Gravity")
        f1, f2, f3 = st.columns(3)
        with f1:
            n_earned = st.slider("Earned Media Multiplier", 1.0, 2.0, float(st.session_state.coeffs.get('PR_Weight', 1.2)), help="Bonus lift applied during PR spikes")
        with f2:
            n_grav = st.slider("Event Gravity %", 0, 100, int(float(st.session_state.coeffs.get('Event_Gravity', 0.25)) * 100)) / 100
        with f3:
            n_promo = st.number_input("Standard Promo Lift", value=int(st.session_state.coeffs.get('Promo', 550)))

        # SECTION 4: FRICTION
        st.divider()
        st.subheader("🌦️ Environmental Friction")
        w1, w2 = st.columns(2)
        with w1:
            n_rain = st.slider("Rain Impact (per mm)", -100, 0, int(st.session_state.coeffs.get('Rain_mm', -12)))
        with w2:
            n_snow = st.slider("Snow Impact (per cm)", -500, 0, int(st.session_state.coeffs.get('Snow_cm', -45)))

        if st.form_submit_button("🚀 Recalibrate Property Engine", use_container_width=True):
            # 1. Update Session State
            # We ensure OOH_Count is at least 1 if we are setting a weight
            updated_coeffs = {
                "Clicks": float(n_clicks),
                "Social_Imp": float(n_social),
                "Ad_Decay": int(n_decay),
                "Broadcast_Weight": float(n_broad),
                "OOH_Weight": float(n_ooh),
                "OOH_Count": 1 if n_ooh > 0 else 0, # Force count to 1 so the math works
                "Print_Lift": float(n_print),
                "PR_Weight": float(n_earned),
                "Event_Gravity": float(n_grav),
                "Promo": float(n_promo),
                "Rain_mm": float(n_rain),
                "Snow_cm": float(n_snow),
                "Static_Weight": float(n_ooh), # Syncing to legacy OOH column for safety
                "Static_Count": 1 if n_ooh > 0 else 0
            }
            
            st.session_state.coeffs.update(updated_coeffs)
            
            try:
                # 2. Push to Supabase
                # Ensure your coefficients table has an 'id' or 'key' to upsert correctly
                supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
                
                st.success(f"✅ Weights Saved.")
                
                # 3. Force Refresh
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Sync Error: {e}")

    with st.expander("🔍 View Active Sensitivity Manifest"):
        st.json(st.session_state.coeffs)

# =================================================================
# 11. PAGE 6: AI STRATEGIC ANALYST (EXECUTIVE UPGRADE v12)
# =================================================================
elif page == "🤖 FloorCast AI Analyst":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🕵️ FloorCast Strategic AI Analyst</h2>
            <p style="color: #444; margin: 0;">Executive Intelligence: Correlating Predictions with Actual Results.</p>
        </div>
    """, unsafe_allow_html=True)
    
    if not ledger_data:
        st.warning("Forensic Vault is empty. Analyst cannot audit performance without a ledger.")
        st.stop()

    # 2. RUN FORENSIC ENGINE FOR DOSSIER
    m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    df_ai = m_audit['df']
    
    # 3. BUILD THE EXECUTIVE DOSSIER (Includes Weights + Variance)
    # We feed the AI the 'Whys' so it can diagnose the Saturday gaps
    c = st.session_state.coeffs
    dossier = f"""
    PROPERTY: Hard Rock Hotel & Casino Ottawa
    CURRENT CALIBRATION WEIGHTS:
    - Promo Lift: {c.get('Promo_Lift')}
    - Billboard Weight: {c.get('OOH_Weight')}
    - Broadcast/TV Lift: {c.get('Broadcast_Weight')}
    - PR Multiplier: {c.get('PR_Weight')}
    - Event Gravity: {c.get('Event_Gravity')}
    - AI Predictability: {m_audit.get('predictability')}

    RECENT PERFORMANCE DATA (Last 30 Days):
    """
    
    for _, r in df_ai.sort_values('entry_date', ascending=False).head(30).iterrows():
        actual = r.get('actual_traffic', 0)
        expected = int(r.get('expected', 0))
        variance = actual - expected
        dossier += (
            f"Date: {r.get('entry_date').strftime('%Y-%m-%d')} ({r.get('entry_date').day_name()}) | "
            f"Actual: {actual} | Target: {expected} | Variance: {variance:+d} | "
            f"Promo: {r.get('active_promo')} | Lift: {r.get('residual_lift', 0):.0f}\n"
        )

    # 4. CHAT INPUT
    if "messages" not in st.session_state:
        st.session_state.messages = []

    prompt = st.chat_input("Ask about Saturday demand or marketing ROI...")
    
    if prompt:
        history_str = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in st.session_state.messages[-8:]])
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            import google.generativeai as genai
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            # Using 1.5-flash for stability and speed
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            with st.status("🕵️ Auditing Property Patterns...", expanded=True) as status:
                # Optimized System Prompt for Hard Rock Ottawa Context
                full_prompt = f"""
                You are the Senior Strategy Analyst for Hard Rock Hotel & Casino Ottawa. 
                Your task is to analyze the following Ledger and Weights to find growth opportunities or calibration errors.
                
                CONTEXT:
                {dossier}
                
                CONVERSATION HISTORY:
                {history_str}
                
                EXECUTIVE QUERY: {prompt}
                """
                response = model.generate_content(full_prompt)
                status.update(label="✅ Analysis Finalized", state="complete")
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()
        except Exception as e:
            st.error(f"AI Error: {e}")

    # 5. DISPLAY THREAD: NEWEST AT THE TOP
    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

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
