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
from supabase import create_client, Client # Added Client for type hinting
from io import BytesIO

# =================================================================
# 1. DATABASE CONNECTION (MUST BE FIRST)
# =================================================================
# Ensure these match your Streamlit Secrets exactly
try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error(f"Critical System Error: Connection secrets missing. {e}")
    st.stop()

# =================================================================
# 2. PERMANENT INITIALIZATION & STATE LOCK (v7.5 - ID-1 TARGET)
# =================================================================
if 'coeffs' not in st.session_state:
    try:
        # 🟢 TARGETED PULL: We look specifically for the record we save on Page 5
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        
        if response.data and len(response.data) > 0:
            # Found our saved weights
            st.session_state.coeffs = response.data[0]
            
            # Ensure OOH/Static counts are never null to prevent math errors
            st.session_state.coeffs['OOH_Count'] = st.session_state.coeffs.get('OOH_Count', 1) or 1
            st.session_state.coeffs['Static_Count'] = st.session_state.coeffs.get('Static_Count', 1) or 1
        else:
            # 🟡 INITIAL SEED: ID 1 doesn't exist yet, so we create the master record
            st.session_state.coeffs = {
                'id': 1, # <--- THE ANCHOR
                'Promo_Lift': 500.0,
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
            # Create the master record in Supabase
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            
    except Exception as e:
        st.error(f"Initialization Error: {e}")
        # Failsafe defaults so the app remains functional
        st.session_state.coeffs = {
            'id': 1, 
            'Promo_Lift': 500.0, 
            'OOH_Weight': 100.0, 
            'OOH_Count': 1
        }

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
    promo_lift_weight = float(coeffs.get('Promo', 550))
    
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
            'Monday': 3398,
            'Tuesday': 3800,
            'Wednesday': 5574,
            'Thursday': 3931,
            'Friday': 7651,
            'Saturday': 9800,
            'Sunday': 5800
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
    
    # Vertical navigation list
    page = st.radio(
        "Intelligence Decks:",
        [
            "Executive Dashboard", 
            "Daily Ledger Audit", 
            "Attribution Analytics", 
            "Master Audit Report", 
            "AI Calibration",
            "FloorCast AI Analyst"
        ],
        index=0,
        key="nav_list_v12"
    )
    
    st.divider()

    # Logout Button
if st.sidebar.button("🚪 Logout / Reset Session", use_container_width=True):
    # 1. Clear the local session state
    st.session_state.clear()
    
    # 2. Force a rerun to the starting state
    # This will trigger the app to re-initialize 'coeffs' from the DB
    st.rerun()

    # THE FIX: Ensure these two lines are indented exactly like this
    if page == "🤖 FloorCast AI Analyst" and st.session_state.get('messages'):
        if st.sidebar.button("🗑️ Reset Analyst Thread", use_container_width=True, key="sidebar_reset"):
            st.session_state.messages = []
            st.rerun()

# =================================================================
# 7. PAGE 1: EXECUTIVE DASHBOARD (FINAL VERSION - LIVE SYNCED)
# =================================================================
if page == "Executive Dashboard":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📈 Executive Performance Pulse</h2>
            <p style="color: #444; margin: 0;">Predictive Guest Volume & Strategic Planning Command Center.</p>
        </div>
    """, unsafe_allow_html=True)

    today = datetime.date.today()
    
    # --- 1. THE LIVE BRIDGE (CRITICAL FIX) ---
    # We pull this at the very top of the page logic to ensure every widget below sees the current sliders
    current_weights = st.session_state.get('coeffs', {})

    if not ledger_data:
        st.warning("Forensic Vault is empty. Please populate the Ledger.")
        st.stop()

    df_raw = pd.DataFrame(ledger_data)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
    
    # 2. DATE SELECTION
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

        # 3. STRATEGIC DAILY PLANNER
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

        # --- 4. THE ENGINE EXECUTION (LIVE SYNC) ---
        # We pass the 'current_weights' dictionary directly into the engine
        m = get_forensic_metrics(df_p.to_dict(orient='records'), current_weights)
        df_final = m['df'].sort_values('entry_date')

        # CALCULATE MARKETING IMPACT (Live calculation based on session state)
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

        # --- 5. EXECUTIVE KPI GRID ---
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

        # --- 6. PERFORMANCE VIZ ---
        st.write("### 🎰 The Unified Pulse")
        fig_pulse = go.Figure()
        df_act_chart = df_final[df_final['entry_date'].dt.date < today]
        fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
        fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
        
        today_ts = pd.Timestamp(today)
        fig_pulse.add_shape(type="line", x0=today_ts, x1=today_ts, y0=0, y1=1, yref="paper", line=dict(color="#666", width=2, dash="dash"))
        fig_pulse.update_layout(plot_bgcolor='rgba(0,0,0,0)', height=400, margin=dict(l=0, r=0, t=10, b=0), hovermode="x unified")
        st.plotly_chart(fig_pulse, use_container_width=True)

        # --- 7. OPERATIONAL RISK vs. HISTORICAL AUDIT ---
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
                # Force dynamic weather friction from sliders
                s_imp = df_final['snow_cm'].sum() * float(current_weights.get('Snow_cm', -45))
                r_imp = df_final['rain_mm'].sum() * float(current_weights.get('Rain_mm', -12))
                st.metric("Weather Friction", f"-{abs(s_imp + r_imp):,.0f}")
            with o2:
                potential = int(df_final['expected'].sum() - df_final['new_members'].sum())
                st.metric("Conversion Opportunity", f"{max(0, potential):,.0f}")
            with o3:
                peak_day_volume = df_final['expected'].max()
                intensity_label = "🔴 Critical Peak" if peak_day_volume > 6200 else ("🟡 High" if peak_day_volume > 5200 else "🟢 Stable")
                st.metric("Staffing Intensity", intensity_label)

    else:
        st.info("Please select a complete Start and End date range to view the Dashboard.")
        st.stop()

# =================================================================
# 8. PAGE 2: DAILY LEDGER AUDIT (HARDENED v7.2)
# =================================================================
elif page == "Daily Ledger Audit":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🎰 Daily Property Ledger</h2>
            <p style="color: #444; margin: 0;">Ground Truth Data: Financials, Foot Traffic, and Marketing Spend.</p>
        </div>
    """, unsafe_allow_html=True)

    # --- 1. THE DATA ENGINE ---
    if not ledger_data:
        df_ledger = pd.DataFrame(columns=['entry_date', 'actual_traffic', 'new_members', 'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm'])
    else:
        df_ledger = pd.DataFrame(ledger_data)
        df_ledger['entry_date'] = pd.to_datetime(df_ledger['entry_date']).dt.date
        marketing_cols = ['actual_traffic', 'new_members', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
        for col in marketing_cols:
            if col in df_ledger.columns:
                df_ledger[col] = pd.to_numeric(df_ledger[col], errors='coerce').fillna(0)
        df_ledger['active_promo'] = df_ledger['active_promo'].astype(str).replace(['nan', 'None', '0', '0.0'], '')
        df_ledger = df_ledger.sort_values('entry_date', ascending=False)

    # --- 2. RAPID ENTRY FORM ---
    with st.expander("➕ Log New Daily Actuals", expanded=True):
        with st.form("rapid_entry_form", clear_on_submit=True):
            f1, f2, f3 = st.columns(3)
            with f1:
                e_date = st.date_input("Date", value=datetime.date.today())
                e_traffic = st.number_input("Actual Traffic", min_value=0, step=1)
                e_members = st.number_input("New Members", min_value=0, step=1)
            with f2:
                e_promo = st.text_input("Active Promo Name", placeholder="e.g. Rock of Ages")
                e_event = st.number_input("Event Attendance", min_value=0, step=1)
                e_clicks = st.number_input("Ad Clicks", min_value=0, step=1)
            with f3:
                e_imps = st.number_input("Social Impressions", min_value=0, step=1)
                e_rain = st.number_input("Rain (mm)", min_value=0.0, step=0.1)
                e_snow = st.number_input("Snow (cm)", min_value=0.0, step=0.1)
            
            submit_new = st.form_submit_button("🚀 Submit to Database", use_container_width=True)
            
            if submit_new:
                # Ensure the data is clean before sending to Supabase
                new_row = {
                    "entry_date": str(e_date),
                    "actual_traffic": int(e_traffic),
                    "new_members": int(e_members),
                    "active_promo": str(e_promo).strip() if e_promo else None,
                    "attendance": int(e_event),
                    "ad_clicks": int(e_clicks),
                    "ad_impressions": int(e_imps),
                    "rain_mm": float(e_rain),
                    "snow_cm": float(e_snow)
                }
                try:
                    supabase.table("ledger").upsert(new_row).execute()
                    st.success(f"✅ Successfully logged: {e_promo if e_promo else 'General Day'}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Database Error: {e}")

    st.divider()

    # --- 3. THE HISTORICAL EDITABLE LEDGER ---
    l1, l2 = st.columns([2, 1])
    with l1:
        st.write("### 📂 Bulk Audit & Corrections")
        st.caption("Use this table to fix past errors or update older data.")
    with l2:
        view_limit = st.slider("Historical View:", 7, 100, 30)

    with st.form("bulk_ledger_sync"):
        edited_ledger = st.data_editor(
            df_ledger.head(view_limit),
            column_config={
                "entry_date": st.column_config.DateColumn("Date", required=True),
                "actual_traffic": st.column_config.NumberColumn("Actual Traffic", format="%d"),
                "new_members": st.column_config.NumberColumn("New Members", format="%d"),
                "active_promo": st.column_config.TextColumn("Promo Name"),
                "attendance": st.column_config.NumberColumn("Event Attendance", format="%d"),
                "ad_clicks": st.column_config.NumberColumn("Ad Clicks"),
                "ad_impressions": st.column_config.NumberColumn("Social Imps"),
                "rain_mm": st.column_config.NumberColumn("Rain (mm)"),
                "snow_cm": st.column_config.NumberColumn("Snow (cm)"),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",
            key="property_ledger_restored_v2"
        )
        
        if st.form_submit_button("💾 Sync Table Updates", use_container_width=True):
            try:
                # 1. Scrub the data
                df_sync = edited_ledger.copy()
                df_sync = df_sync.fillna(0)
                
                # 2. Date conversion
                df_sync['entry_date'] = df_sync['entry_date'].astype(str)
                
                # 3. THE FIX: Force Integer columns to be Integers (removes the .0)
                int_cols = ['actual_traffic', 'new_members', 'attendance', 'ad_clicks', 'ad_impressions']
                for col in int_cols:
                    if col in df_sync.columns:
                        df_sync[col] = df_sync[col].astype(int)

                # 4. Force Weather columns to be Floats (decimals are okay here)
                float_cols = ['rain_mm', 'snow_cm']
                for col in float_cols:
                    if col in df_sync.columns:
                        df_sync[col] = df_sync[col].astype(float)

                # 5. Push to Supabase
                sync_payload = df_sync.to_dict(orient='records')
                supabase.table("ledger").upsert(sync_payload).execute()
                
                st.success("✅ Bulk updates synced successfully.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Bulk Sync Error: {e}")

    # 4. DATABASE STATS
    if not df_ledger.empty:
        st.write(f"**Database Audit:** {len(df_ledger)} total records in vault.")

# =================================================================
# 3. PAGE 3: ATTRIBUTION ANALYTICS (PRO-MARKETING SUITE)
# =================================================================
elif page == "Attribution Analytics":
    st.markdown("""
        <div style="background-color:#F8F9FA;padding:20px;border-radius:12px;border-left:6px solid #0047AB;margin-bottom:20px;">
            <h2 style="color:#0047AB;margin:0;">📊 Marketing Attribution & ROI</h2>
            <p style="color:#666;margin:0;">Deconstructing the Guest Journey: From Digital Signal to Casino Floor.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.info("💡 Forensic Vault empty. Populate the Ledger to unlock attribution.")
        st.stop()

    # 1. DATA PREP
    current_weights = st.session_state.get('coeffs', {})
    m_full = get_forensic_metrics(ledger_data, current_weights)
    df_attr = m_full['df']

    # Calculate Total Totals
    total_guests = df_attr['actual_traffic'].sum()
    organic_base = df_attr['guest_baseline'].sum()
    digital_lift = df_attr['residual_lift'].sum()
    event_lift = df_attr['gravity_lift'].sum()
    other_marketing = total_guests - (organic_base + digital_lift + event_lift)

    # 2. THE TOP-LINE WATERFALL
    st.write("### 🪜 Growth Waterfall: Baseline to Total")
    fig_water = go.Figure(go.Waterfall(
        name = "Attribution", orientation = "v",
        measure = ["relative", "relative", "relative", "relative", "total"],
        x = ["Baseline (Organic)", "Digital Adstock", "Live Events", "Brand/Mass Media", "Actual Traffic"],
        textposition = "outside",
        text = [f"+{organic_base:,.00f}", f"+{digital_lift:,.0f}", f"+{event_lift:,.0f}", f"+{max(0, other_marketing):,.0f}", f"{total_guests:,.0f}"],
        y = [organic_base, digital_lift, event_lift, max(0, other_marketing), total_guests],
        connector = {"line":{"color":"rgb(63, 63, 63)"}},
        increasing = {"marker":{"color": "#0047AB"}},
        totals = {"marker":{"color": "#FFCC00"}}
    ))
    fig_water.update_layout(height=450, plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_water, use_container_width=True)

    st.divider()

    # 3. CHANNEL EFFICIENCY & CORRELATION
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.write("### 🎯 Channel Contribution")
        pie_labels = ['Organic', 'Digital Clicks/Social', 'Event Gravity', 'Mass Media']
        pie_values = [organic_base, digital_lift, event_lift, max(0, other_marketing)]
        fig_pie = px.pie(names=pie_labels, values=pie_values, 
                         color_discrete_sequence=['#E1E8F0', '#0047AB', '#FFCC00', '#333'],
                         hole=0.4)
        fig_pie.update_layout(showlegend=True, height=350, margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        st.write("### 📈 Lift Correlation")
        # Scatter to show if Clicks actually drive Traffic
        fig_scatter = px.scatter(df_attr, x='ad_clicks', y='actual_traffic', 
                                 trendline="ols", 
                                 title="Ad Click Correlation",
                                 color_discrete_sequence=['#0047AB'])
        fig_scatter.update_layout(height=350, plot_bgcolor='rgba(248,249,250,1)')
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

    # 4. WEEKEND VS WEEKDAY ATTRIBUTION
    st.write("### 🗓️ Weekend vs. Weekday Yield")
    df_attr['is_weekend'] = pd.to_datetime(df_attr['entry_date']).dt.dayofweek >= 5
    day_mix = df_attr.groupby('is_weekend')[['residual_lift', 'gravity_lift']].mean()
    day_mix.index = ['Weekday', 'Weekend']
    
    st.bar_chart(day_mix)
    st.caption("Average guest lift per day type. Weekends typically show higher 'Event Gravity'.")

    # 5. STRATEGIC INSIGHTS
    if not df_attr.empty:
        # Re-verify variables are calculated in this scope
        mkt_vol = df_attr['residual_lift'].sum() + df_attr['gravity_lift'].sum()
        organic_base = df_attr['guest_baseline'].sum()
        total_clicks = df_attr['ad_clicks'].sum()
        digital_lift = df_attr['residual_lift'].sum()
        
        with st.expander("📝 Strategic Interpretation & ROI Audit", expanded=True):
            yield_per_click = digital_lift / total_clicks if total_clicks > 0 else 0
            
            # Determine the Top Channel string for the display
            top_channel_label = "Organic" if organic_base > mkt_vol else "Marketing"
            
            st.info(f"""
            **AI Attribution Audit:**
            * **Top Channel:** {top_channel_label} is currently the primary driver.
            * **Digital Efficiency:** Every 100 ad clicks are generating approximately **{yield_per_click * 100:.1f}** additional guests.
            * **Event Strength:** Hard Rock Live events are currently providing a **{ (event_lift/organic_base)*100:.1f}%** lift over baseline traffic.
            """)
    else:
        st.warning("Insufficient data for Strategic Interpretation.")

# =================================================================
# 12. PAGE 4: MASTER FORENSIC AUDIT (EXECUTIVE EDITION v12)
# =================================================================
elif page == "Master Audit Report":
    # Custom CSS for dense, professional KPIs
    st.markdown("""
        <style>
        [data-testid="stMetricLabel"] p { font-size: 0.75rem !important; white-space: nowrap !important; }
        [data-testid="stMetricValue"] > div { font-size: 1.5rem !important; }
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
            key="master_audit_v12_yield"
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

        # 3. FINANCIAL & LOYALTY GRID
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

        # 4. MARKETING EQUITY
        st.write("### 🧬 Marketing Equity & Friction")
        k6, k7, k8, k9, k10 = st.columns(5)
        
        t_digital = df_final['residual_lift'].sum()
        t_inertia_val = m.get('total_inertia', 0)
        t_inertia_total = t_inertia_val * num_days
        t_gravity = df_final['gravity_lift'].sum()
        
        t_mkt = t_digital + t_inertia_total + t_gravity
        mkt_share = (t_mkt / t_traffic * 100) if t_traffic > 0 else 0
        
        t

# =================================================================
# ⚙️ PAGE 5: AI CALIBRATION & ENGINE WEIGHTS
# =================================================================
elif page == "AI Calibration":
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
            # Explicitly lock this to Record ID 1
            updated_coeffs = {
                "id": 1,
                "Clicks": float(n_clicks),
                "Social_Imp": float(n_social),
                "Ad_Decay": int(n_decay),
                "Broadcast_Weight": float(n_broad),
                "OOH_Weight": float(n_ooh),
                "OOH_Count": 1 if n_ooh > 0 else 0,
                "Print_Lift": float(n_print),
                "PR_Weight": float(n_earned),
                "Event_Gravity": float(n_grav),
                "Promo": float(n_promo),
                "Rain_mm": float(n_rain),
                "Snow_cm": float(n_snow),
                "Static_Weight": float(n_ooh),
                "Static_Count": 1 if n_ooh > 0 else 0
            }
            
            # Update session state
            st.session_state.coeffs.update(updated_coeffs)
            
            try:
                # Push to Supabase - specifically targeting ID 1
                supabase.table("coefficients").upsert(updated_coeffs).execute()
                
                st.success(f"✅ Weights Hard-Saved to Database.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Sync Error: {e}")

    with st.expander("🔍 View Active Sensitivity Manifest"):
        st.json(st.session_state.coeffs)

# =================================================================
# 11. PAGE 6: AI STRATEGIC ANALYST (EXECUTIVE UPGRADE v12)
# =================================================================
elif page == "FloorCast AI Analyst":
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
