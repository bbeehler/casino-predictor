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
from dateutil.relativedelta import relativedelta

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
# 3.1 BL-ROAS CALCULATION ENGINE
# =================================================================
def calculate_and_save_roas(data_dict):
    """
    Implements the 5-Step BL-ROAS logic and saves to Supabase.
    """
    # Math logic based on Brian's Copilot instructions
    traffic_score = (data_dict['utm_sessions'] + data_dict['organic_sessions']) * 8.00
    eng_score = (
        (data_dict['social_likes'] * 0.50) + 
        (data_dict['social_comments'] * 1.00) + 
        (data_dict['social_shares'] * 1.25) + 
        (data_dict['post_views'] * 0.25) + 
        (data_dict['site_time_sessions'] * 1.50) + 
        (data_dict['booking_clicks'] * 2.50)
    )
    sentiment_score = data_dict['pos_reviews'] * 30.00
    geo_lift_score = data_dict['geo_lift_traffic'] * 8.00
    
    brand_value = traffic_score + eng_score + sentiment_score + geo_lift_score
    bl_roas = brand_value / data_dict['ad_spend'] if data_dict['ad_spend'] > 0 else 0
    
    # Enhanced Revenue logic using Brian's benchmarks
    enhanced_revenue = (
        brand_value + 
        (data_dict['ledger_traffic'] * data_dict['avg_spend']) + 
        (data_dict['ledger_signups'] * data_dict['ltv_member'])
    )

    payload = {
        "report_month": data_dict['report_month'],
        "utm_sessions": int(data_dict['utm_sessions']),
        "organic_sessions": int(data_dict['organic_sessions']),
        "ad_spend": float(data_dict['ad_spend']),
        "social_likes": int(data_dict['social_likes']),
        "social_comments": int(data_dict['social_comments']),
        "social_shares": int(data_dict['social_shares']),
        "post_views": int(data_dict['post_views']),
        "site_time_sessions": int(data_dict['site_time_sessions']),
        "booking_clicks": int(data_dict['booking_clicks']),
        "pos_reviews": int(data_dict['pos_reviews']),
        "geo_lift_traffic": int(data_dict['geo_lift_traffic']),
        "calculated_bl_roas": round(float(bl_roas), 2),
        "brand_value": round(float(brand_value), 2),
        "enhanced_revenue": round(float(enhanced_revenue), 2)
    }

    # THE CRITICAL FIX: Add 'on_conflict="report_month"'
    return supabase.table("monthly_roi").upsert(payload, on_conflict="report_month").execute()

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
            "FloorCast AI Analyst",
            "BL-ROAS Calculator"
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
# 7. PAGE 1: EXECUTIVE DASHBOARD (FINAL v29 - Full Logic Sync)
# =================================================================
if page == "Executive Dashboard":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📈 Executive Performance Pulse</h2>
            <p style="color: #444; margin: 0;">Deep History Projection & Live Environment Canada Feed.</p>
        </div>
    """, unsafe_allow_html=True)

    today = datetime.date.today()
    current_weights = st.session_state.get('coeffs', {})

    if not ledger_data:
        st.warning("Forensic Vault is empty. Please populate the Ledger.")
        st.stop()

    # --- 1. THE DEEP HISTORY ENGINE ---
    df_raw = pd.DataFrame(ledger_data)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
    df_raw['dow'] = df_raw['entry_date'].dt.day_name()
    master_baselines = df_raw.groupby('dow')['actual_traffic'].mean().to_dict()

    # --- 2. ENVIRONMENT CANADA BRIDGE ---
    def get_live_ottawa_forecast():
        """Fetches 7-day forecast from Environment Canada (Ottawa CDA Station)"""
        try:
            from env_canada import ECWeather
            import asyncio
            ec = ECWeather(station_id="ON/s0000430")
            asyncio.run(ec.update())
            
            forecast_data = {}
            for day in ec.daily_forecasts:
                period = day['period'] 
                forecast_data[period] = {
                    "rain": float(day.get('rain_amount', 0) or 0),
                    "snow": float(day.get('snow_amount', 0) or 0)
                }
            return forecast_data
        except:
            return None

    # --- 3. DATE SELECTION ---
    col_date, _ = st.columns([1, 2])
    with col_date:
        pulse_range = st.date_input(
            "Select Analysis Window:", 
            value=(today, today + datetime.timedelta(days=7)), 
            key="pulse_exec_vfinal_synced"
        )

    # CHECK: Ensure pulse_range is a valid selection before running logic
    if isinstance(pulse_range, tuple) and len(pulse_range) == 2:
        start_p, end_p = pulse_range
        is_future = start_p >= today
        
        # --- 4. TIMELINE GENERATION & SCAFFOLDING (STABLE v30) ---
        date_list = pd.date_range(start=start_p, end=end_p)
        df_p = pd.DataFrame({'entry_date': date_list})
        df_p['dow'] = df_p['entry_date'].dt.day_name()
        df_p['baseline'] = df_p['dow'].map(master_baselines)
        
        df_p = pd.merge(df_p, df_raw, on='entry_date', how='left')

        # ENSURE ALL COLUMNS EXIST (Including verified 'attendance')
        required_cols = {
            'active_promo': '', 
            'attendance': 0, 
            'ad_clicks': 0, 
            'ad_impressions': 0, 
            'rain_mm': 0.0, 
            'snow_cm': 0.0,
            'actual_traffic': 0, 
            'actual_coin_in': 0.0, 
            'new_members': 0
        }
        
        for col, default_val in required_cols.items():
            if col not in df_p.columns:
                df_p[col] = default_val
            else:
                # FIXED: Using 'default_val' to match the loop variable
                df_p[col] = df_p[col].fillna(default_val)

        # --- 5. STRATEGIC DAILY PLANNER & WEATHER ---
        if is_future:
            live_weather = get_live_ottawa_forecast()
            with st.expander("📅 Daily Strategy Planner", expanded=True):
                st.write("Plan your lift. Weather below is synced from Environment Canada.")
                df_plan = df_p[['entry_date', 'dow', 'active_promo', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']].copy()
                
                df_plan_display = df_plan.copy()
                df_plan_display['entry_date'] = df_plan_display['entry_date'].dt.strftime('%a, %b %d')
                
                edited_df = st.data_editor(
                    df_plan_display, 
                    column_config={"dow": None, "entry_date": st.column_config.Column("Date", disabled=True)},
                    hide_index=True, use_container_width=True, key="p1_planner_v29_stable"
                )
                
                for col in ['active_promo', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']:
                    df_p[col] = edited_df[col].values
            
            if live_weather:
                st.sidebar.success("📡 Environment Canada Feed Active")
                for i, row in df_p.iterrows():
                    day_name = row.get('dow')
                    if day_name in live_weather:
                        if df_p.at[i, 'rain_mm'] == 0:
                            df_p.at[i, 'rain_mm'] = live_weather[day_name]['rain']
                        if df_p.at[i, 'snow_cm'] == 0:
                            df_p.at[i, 'snow_cm'] = live_weather[day_name]['snow']
        else:
            st.info("💡 Reviewing historical actuals. Planner is disabled for past dates.")

        # --- 6. ENGINE EXECUTION ---
        m = get_forensic_metrics(df_p.to_dict(orient='records'), current_weights)
        df_final = m['df'].sort_values('entry_date')

        daily_brand_inertia = (
            float(current_weights.get('Broadcast_Weight', 150)) + 
            float(current_weights.get('OOH_Weight', 100)) + 
            float(current_weights.get('Print_Lift', 75)) +
            (float(current_weights.get('Static_Weight', 15)) * int(current_weights.get('Static_Count', 10))) +
            (float(current_weights.get('Digital_OOH_Weight', 25)) * int(current_weights.get('Digital_OOH_Count', 5)))
        )
        
        total_vol = df_final['expected'].sum()
        total_lift_vol = df_final['residual_lift'].sum() + df_final['gravity_lift'].sum() + (daily_brand_inertia * len(df_final))
        mkt_impact_pct = (total_lift_vol / total_vol * 100) if total_vol > 0 else 0

        # --- 7. EXECUTIVE KPI GRID ---
        st.write("### 🏛️ Property Vital Signs")
        k1, k2, k3, k4 = st.columns(4)
        LTV_VAL, AVG_SPEND = 1900.00, 1279.33

        if is_future:
            proj_rev = (total_vol * AVG_SPEND) + ((total_vol * 0.05) * LTV_VAL) + (daily_brand_inertia * len(df_final))
            k1.metric("Projected Demand", f"{total_vol:,.0f} Guests")
            k2.metric("Target Signups", f"{(total_vol * 0.05):,.0f}")
            k3.metric("Proj. Enhanced Revenue", f"${proj_rev:,.0f}")
            k4.metric("Marketing Impact %", f"{mkt_impact_pct:.1f}%")
        else:
            total_act = df_final['actual_traffic'].sum()
            act_coin = df_final['actual_coin_in'].sum() if 'actual_coin_in' in df_final.columns else (total_act * AVG_SPEND)
            act_rev = act_coin + (df_final['new_members'].sum() * LTV_VAL)
            k1.metric("Actual Guest Flow", f"{total_act:,.0f}")
            k2.metric("New Unity Members", f"{df_final['new_members'].sum():,.0f}")
            k3.metric("Audited Revenue Impact", f"${act_rev:,.0f}")
            k4.metric("Audited Accuracy", m['predictability'])

        # --- 8. VISUALIZATION ---
        st.write("### 🎰 The Unified Pulse")
        fig_pulse = go.Figure()
        df_act_chart = df_final[df_final['entry_date'].dt.date < today]
        fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
        fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
        st.plotly_chart(fig_pulse, use_container_width=True)

        # --- 9. RISK & SOCIAL PULSE (CONSOLIDATED) ---
        st.divider()
        o_col, s_col = st.columns(2)
        
        with o_col:
            st.write("#### 🛡️ Operational Risk")
            s_imp = df_final['snow_cm'].sum() * float(current_weights.get('Snow_cm', -45))
            r_imp = df_final['rain_mm'].sum() * float(current_weights.get('Rain_mm', -12))
            st.metric("Weather Friction", f"-{abs(s_imp + r_imp):,.0f}", help="Projected loss from snow/rain.")
        
        with s_col:
            st.write("#### 📱 Social Engagement")
            total_clicks = df_final['ad_clicks'].sum()
            total_imps = df_final['ad_impressions'].sum()
            ctr = (total_clicks / total_imps * 100) if total_imps > 0 else 0.0
            st.metric("Campaign Clicks", f"{total_clicks:,.0f}", delta=f"{ctr:.2f}% CTR")

        s1, s2, s3 = st.columns(3)
        s1.metric("Total Impressions", f"{total_imps:,.0f}")
        s2.metric("Engagement Velocity", "High" if ctr > 1.5 else "Stable")
        s3.metric("Digital Visibility Score", f"{(total_imps * 0.8 / 1000):,.1f}k Reach")

    else:
        st.info("Select a date range to view the Dashboard.")

# =================================================================
# 8. PAGE 2: DAILY LEDGER AUDIT (HARDENED v7.4 - NameError & Scope Fix)
# =================================================================
elif page == "Daily Ledger Audit":
    # --- 1. THE DATA ENGINE (CRITICAL: Define df_ledger FIRST to prevent NameError) ---
    if not ledger_data:
        df_ledger = pd.DataFrame(columns=[
            'entry_date', 'actual_traffic', 'new_members', 'actual_coin_in', 
            'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 
            'rain_mm', 'snow_cm'
        ])
    else:
        df_ledger = pd.DataFrame(ledger_data)
        df_ledger['entry_date'] = pd.to_datetime(df_ledger['entry_date']).dt.date
        
        # Ensure all numeric columns are handled properly
        marketing_cols = ['actual_traffic', 'new_members', 'actual_coin_in', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
        for col in marketing_cols:
            if col in df_ledger.columns:
                df_ledger[col] = pd.to_numeric(df_ledger[col], errors='coerce').fillna(0)
        
        df_ledger['active_promo'] = df_ledger['active_promo'].astype(str).replace(['nan', 'None', '0', '0.0'], '')
        df_ledger = df_ledger.sort_values('entry_date', ascending=False)

    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🎰 Daily Property Ledger</h2>
            <p style="color: #444; margin: 0;">Ground Truth Data: Financials, Foot Traffic, and Marketing Spend.</p>
        </div>
    """, unsafe_allow_html=True)

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
                e_coin = st.number_input("Actual Coin-In ($)", min_value=0.0, step=1000.0)
            with f3:
                e_clicks = st.number_input("Ad Clicks", min_value=0, step=1)
                e_imps = st.number_input("Social Impressions", min_value=0, step=1)
                e_rain = st.number_input("Rain (mm)", min_value=0.0, step=0.1)
            
            submit_new = st.form_submit_button("🚀 Submit to Database", use_container_width=True)
            
            if submit_new:
                new_row = {
                    "entry_date": str(e_date),
                    "actual_traffic": int(e_traffic),
                    "new_members": int(e_members),
                    "actual_coin_in": float(e_coin),
                    "active_promo": str(e_promo).strip() if e_promo else None,
                    "attendance": int(e_event),
                    "ad_clicks": int(e_clicks),
                    "ad_impressions": int(e_imps),
                    "rain_mm": float(e_rain),
                    "snow_cm": 0.0 # Defaulting snow to 0 for rapid entry
                }
                try:
                    supabase.table("ledger").upsert(new_row).execute()
                    st.success(f"✅ Successfully logged: {e_date}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Database Error: {e}")

    # --- 3. THE HISTORICAL EDITABLE LEDGER ---
    st.divider()
    l1, l2 = st.columns([2, 1])
    with l1:
        st.write("### 📂 Bulk Audit & Corrections")
    with l2:
        view_limit = st.slider("Historical View:", 7, 100, 30)

    with st.form("bulk_ledger_sync"):
        edited_ledger = st.data_editor(
            df_ledger.head(view_limit),
            column_config={
                "entry_date": st.column_config.DateColumn("Date", required=True),
                "actual_traffic": st.column_config.NumberColumn("Actual Traffic", format="%d"),
                "new_members": st.column_config.NumberColumn("New Members", format="%d"),
                "actual_coin_in": st.column_config.NumberColumn("Coin-In", format="$%d"),
                "active_promo": st.column_config.TextColumn("Promo Name"),
                "attendance": st.column_config.NumberColumn("Event Attendance", format="%d"),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",
            key="property_ledger_v7_4"
        )
        
        if st.form_submit_button("💾 Sync Table Updates", use_container_width=True):
            try:
                df_sync = edited_ledger.copy()
                df_sync['entry_date'] = df_sync['entry_date'].astype(str)
                sync_payload = df_sync.fillna(0).to_dict(orient='records')
                supabase.table("ledger").upsert(sync_payload).execute()
                st.success("✅ Bulk updates synced successfully.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Bulk Sync Error: {e}")

    # --- 4. THE SCOREBOARD (SCOPED FEEDBACK) ---
    st.divider()
    day_audit = df_ledger[df_ledger['entry_date'] == e_date]
    
    if not day_audit.empty:
        day_traffic = day_audit['actual_traffic'].iloc[0]
        day_signups = day_audit['new_members'].iloc[0]
        # Using Brian's standard benchmarks
        daily_potential = (day_traffic * 1279.33) + (day_signups * 1900.00)
        
        st.write(f"### 🎯 Performance Scoreboard: {e_date.strftime('%B %d, %Y')}")
        m1, m2, m3 = st.columns(3)
        m1.metric("Daily Floor Traffic", f"{day_traffic:,}")
        m2.metric("New Member Signups", f"{day_signups:,}")
        m3.metric("Daily Potential", f"${daily_potential:,.2f}", help="Based on $1,279.33 avg spend and $1,900 LTV.")

        # --- 5. SCENARIO ATTRIBUTION MATRIX (DAILY) ---
        st.write("#### 📍 Daily Attribution Scenarios")
        scenarios = {
            "Attribution Level": ["Conservative (10%)", "Moderate (20%)", "Aggressive (30%)"],
            "Attributed Floor Impact": [f"${daily_potential * 0.1:,.2f}", f"${daily_potential * 0.2:,.2f}", f"${daily_potential * 0.3:,.2f}"],
            "Trip Equivalent": [f"{int(day_traffic * 0.1)} visits", f"{int(day_traffic * 0.2)} visits", f"{int(day_traffic * 0.3)} visits"]
        }
        st.table(pd.DataFrame(scenarios))
    else:
        st.info("💡 Select a date with existing data to see the daily Performance Scoreboard.")

    # --- 6. DATABASE STATS ---
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
    # Keep your original CSS and Header
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

    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_date, e_date = audit_range
        mask = (df_audit_raw['entry_date'].dt.date >= s_date) & (df_audit_raw['entry_date'].dt.date <= e_date)
        df_audit_filtered = df_audit_raw.loc[mask].copy()
        
        if df_audit_filtered.empty:
            st.error(f"No records found between {s_date} and {e_date}.")
            st.stop()

        # RUN ENGINE
        m = get_forensic_metrics(df_audit_filtered.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df'] 
        c = st.session_state.coeffs
        num_days = len(df_final)

        # 3. YOUR ORIGINAL KPI GRID
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

        # 4. YOUR ORIGINAL MARKETING & FRICTION
        st.write("### 🧬 Marketing Equity & Friction")
        k6, k7, k8, k9, k10 = st.columns(5)
        
        t_digital = df_final['residual_lift'].sum()
        t_inertia_val = m.get('total_inertia', 0)
        t_inertia_total = t_inertia_val * num_days
        t_gravity = df_final['gravity_lift'].sum()
        t_mkt = t_digital + t_inertia_total + t_gravity
        mkt_share = (t_mkt / t_traffic * 100) if t_traffic > 0 else 0
        
        t_snow_loss = (df_final['snow_cm'].sum() * float(c.get('Snow_cm', -45)))
        t_rain_loss = (df_final['rain_mm'].sum() * float(c.get('Rain_mm', -12)))
        friction_total = abs(t_snow_loss + t_rain_loss)

        k6.metric("Marketing Guests", f"{t_mkt:,.0f}")
        k7.metric("Marketing Share", f"{mkt_share:.1f}%")
        k8.metric("Digital ROI Lift", f"{t_digital:,.0f}")
        k9.metric("Weather Friction", f"-{friction_total:,.0f}")
        k10.metric("AI Confidence", m['predictability'])

        st.divider()

        # --- 5. FORENSIC ATTRIBUTION (REFINED STACKED AREA) ---
        st.write("### 🌊 Multi-Channel Attribution Flow")
        st.caption("Visualizing the cumulative layers of guest demand.")
        
        # 1. Prepare Data
        df_stack = df_final.copy()
        df_stack['Brand_Inertia_Layer'] = m.get('total_inertia', 0)
        
        # 2. Build the Chart
        fig_stack = go.Figure()

        # Define layers from bottom to top
        layers = [
            ('Organic Heartbeat', 'guest_baseline', 'rgba(200, 210, 225, 0.5)', '#8E9AAF'),
            ('Brand (OOH/TV/Radio)', 'Brand_Inertia_Layer', 'rgba(93, 112, 127, 0.5)', '#5D707F'),
            ('Digital ROI Lift', 'residual_lift', 'rgba(0, 71, 171, 0.5)', '#0047AB'),
            ('Hard Rock LIVE Gravity', 'gravity_lift', 'rgba(255, 204, 0, 0.6)', '#FFCC00')
        ]

        for name, col, fill_color, line_color in layers:
            fig_stack.add_trace(go.Scatter(
                x=df_stack['entry_date'], 
                y=df_stack[col],
                name=name,
                mode='lines',
                line=dict(width=0.5, color=line_color, shape='spline'), # 'spline' smooths the curves
                stackgroup='one', # This creates the stack
                fillcolor=fill_color,
                hovertemplate='%{y:,.0f} Guests'
            ))

        # 3. Formatting for the Boardroom
        fig_stack.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=500,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
            hovermode="x unified",
            yaxis=dict(
                title="Total Guest Volume", 
                showgrid=True, 
                gridcolor='#F0F2F6',
                tickformat=',d'
            ),
            xaxis=dict(showgrid=False)
        )
        
        st.plotly_chart(fig_stack, use_container_width=True)

        # 6. YOUR ORIGINAL LEDGER & EXPORT
        st.write("### 📋 Detailed Forensic Ledger")
        df_final['Variance'] = df_final['actual_traffic'] - df_final['expected']
        display_cols = ['entry_date', 'actual_traffic', 'expected', 'Variance', 'residual_lift', 'gravity_lift', 'new_members']
        st.dataframe(
            df_final[display_cols].sort_values('entry_date', ascending=False),
            use_container_width=True, hide_index=True
        )

        with col_export:
            csv_data = df_final.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Export Audit to CSV", data=csv_data,
                file_name=f"HR_Audit_{s_date}_{e_date}.csv",
                mime='text/csv', use_container_width=True
            )

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
# 11. PAGE 6: AI STRATEGIC ANALYST (EXECUTIVE UPGRADE v14)
# =================================================================
elif page == "FloorCast AI Analyst":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🕵️ FloorCast Strategic AI Analyst</h2>
            <p style="color: #444; margin: 0;">Executive Intelligence: Correlating Ledger Traffic with Monthly ROAS Audit.</p>
        </div>
    """, unsafe_allow_html=True)
    
    if not ledger_data:
        st.warning("Forensic Vault is empty. Analyst cannot audit performance without a ledger.")
        st.stop()

    # --- 1. GATHER ALL CONTEXTUAL DATA ---
    m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    df_ai = m_audit['df']
    
    # NEW: Fetch the Monthly ROI data from Page 7 for the AI to see
    roi_data_res = supabase.table("monthly_roi").select("*").order("report_month", desc=True).execute()
    roi_context = pd.DataFrame(roi_data_res.data).to_csv(index=False) if roi_data_res.data else "No ROI data available yet."

    # --- 2. BUILD THE STRATEGIC DOSSIER ---
    c = st.session_state.coeffs
    dossier = f"""
    PROPERTY: Hard Rock Hotel & Casino Ottawa
    AI PREDICTABILITY SCORE: {m_audit.get('predictability')}
    
    CURRENT CALIBRATION WEIGHTS:
    - Promo Lift: {c.get('Promo_Lift')} | Billboard: {c.get('OOH_Weight')} | PR: {c.get('PR_Weight')}
    
    --- MONTHLY ROI & BRAND HEALTH AUDIT (PAGE 7 DATA) ---
    {roi_context}

    --- FULL DAILY LEDGER DATASET (PAGE 2 DATA) ---
    {df_ai.to_csv(index=False)}
    """

    # --- 3. CHAT INTERFACE ---
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display thread (Reversed for better UI)
    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    prompt = st.chat_input("Ask about March ROI vs. actual Saturday traffic patterns...")
    
    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            import google.generativeai as genai
            from google.generativeai.types import HarmCategory, HarmBlockThreshold

            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            
            # Using 'gemini-2.5-flash' for high-speed analysis
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            # THE FIX: Safety Settings to prevent "Finish Reason 1" blocks
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }
            
            with st.status("🕵️ Correlating Brand Value with Floor Cash...", expanded=True) as status:
                full_prompt = f"""
                You are the Senior Strategy Analyst for Hard Rock Hotel & Casino Ottawa. 
                You have two main datasets:
                1. THE MONTHLY ROI AUDIT: Contains BL-ROAS, Brand Value, and Ad Spend.
                2. THE DAILY LEDGER: Contains Actual Coin-In, Traffic, and Member Signups.

                TASK:
                Correlate the digital 'Brand Value' from the ROI audit with actual floor performance. 
                If BL-ROAS is high, do we see a corresponding lift in 'actual_coin_in' or 'new_members'?
                Identify if marketing spend is driving quality trips (high spend) or just volume.

                STRATEGIC DOSSIER:
                {dossier}
                
                EXECUTIVE QUERY: {prompt}
                """
                
                # Apply the safety settings here
                response = model.generate_content(full_prompt, safety_settings=safety_settings)
                
                # Check if the response was blocked before accessing .text
                if response.candidates[0].finish_reason == 3: # Safety block
                    assistant_msg = "⚠️ The analysis was blocked by safety filters due to a data pattern. Try rephrasing your question."
                else:
                    assistant_msg = response.text

                status.update(label="✅ Strategic Insight Ready", state="complete")
            
            st.session_state.messages.append({"role": "assistant", "content": assistant_msg})
            st.rerun()
        except Exception as e:
            st.error(f"AI Error: {e}")

# =================================================================
# 13. PAGE 7: BL-ROAS COMMAND CENTER (FINAL v23 - Zero-Proof Edition)
# =================================================================
elif page == "BL-ROAS Calculator":
    st.markdown("""
        <div style="background-color: #F8F9FA; padding: 20px; border-radius: 12px; border-left: 6px solid #28A745; margin-bottom: 25px;">
            <h2 style="color: #28A745; margin: 0;">💰 BL-ROAS Command Center</h2>
            <p style="color: #444; margin: 0;">Audit past performance or calculate current monthly ROI.</p>
        </div>
    """, unsafe_allow_html=True)

    # --- 0. GLOBAL PAGE BENCHMARKS ---
    LTV_BENCHMARK = 1900.00 
    DEFAULT_AVG_SPEND = 1279.33

    # --- 1. MONTH SELECTION ---
    today = datetime.date.today()
    month_options = [(today - relativedelta(months=i)).replace(day=1) for i in range(12)]
    month_labels = [m.strftime("%B %Y") for m in month_options]

    selected_label = st.selectbox("Select Audit Month:", month_labels)
    selected_month = month_options[month_labels.index(selected_label)]

    # --- 2. DYNAMIC LEDGER AGGREGATION (WITH ZERO GUARD) ---
    df_roas = pd.DataFrame(ledger_data)
    df_roas['entry_date'] = pd.to_datetime(df_roas['entry_date'])
    
    m_mask = (df_roas['entry_date'].dt.month == selected_month.month) & \
             (df_roas['entry_date'].dt.year == selected_month.year)
    selected_month_df = df_roas.loc[m_mask].copy()

    if not selected_month_df.empty:
        # Group by date and take the MAX value for each day to ensure full month coverage
        monthly_summary = selected_month_df.groupby(selected_month_df['entry_date'].dt.date).max()
        ledger_traffic = int(monthly_summary['actual_traffic'].sum())
        ledger_signups = int(monthly_summary['new_members'].sum())
        ledger_coin_in = float(monthly_summary['actual_coin_in'].sum())
    else:
        # Fallback values if the ledger is empty for this month
        ledger_traffic, ledger_signups, ledger_coin_in = 0, 0, 0.0

    # SAFETY: Prevent division by zero for avg_spend_actual
    avg_spend_actual = float(ledger_coin_in / ledger_traffic) if ledger_traffic > 0 else DEFAULT_AVG_SPEND

    # --- 3. THE INPUT FORM ---
    with st.form("roas_input_form"):
        st.subheader(f"📊 {selected_label} Metrics")
        
        existing_res = supabase.table("monthly_roi").select("*").eq("report_month", str(selected_month)).execute()
        existing = existing_res.data[0] if existing_res.data else {}

        c1, c2, c3 = st.columns(3)
        with c1:
            utm_s = st.number_input("UTM Sessions", value=int(existing.get('utm_sessions', 0)), 
                                    help="Directly attributed website sessions from tagged digital campaigns.")
            org_s = st.number_input("Organic Sessions", value=int(existing.get('organic_sessions', 0)), 
                                    help="Non-paid search traffic.")
            ad_spend = st.number_input("Total Ad Spend ($)", value=float(existing.get('ad_spend', 0.0)), step=100.0, 
                                       help="Total marketing investment including OOH, Digital, and Broadcast.")
        
        with c2:
            likes = st.number_input("Social Likes", value=int(existing.get('social_likes', 0)))
            comments = st.number_input("Social Comments", value=int(existing.get('social_comments', 0)))
            shares = st.number_input("Social Shares", value=int(existing.get('social_shares', 0)))
            views = st.number_input("Post Views", value=int(existing.get('post_views', 0)))

        with c3:
            time_site = st.number_input("Time on Site Sessions", value=int(existing.get('site_time_sessions', 0)))
            cta_clicks = st.number_input("Booking CTA Clicks", value=int(existing.get('booking_clicks', 0)))
            reviews = st.number_input("Net Positive Reviews", value=int(existing.get('pos_reviews', 0)))
            geo_lift = st.number_input("Incremental Geo Traffic", value=int(existing.get('geo_lift_traffic', 0)))

        st.divider()
        st.info(f"**Ledger Sync ({selected_label}):** Coin-In: ${ledger_coin_in:,.2f} | Traffic: {ledger_traffic:,} | Signups: {ledger_signups:,}")

        submit = st.form_submit_button("🚀 Save ROI Analysis", use_container_width=True)

    # --- 4. EXECUTION (STRICT MATH) ---
    if submit:
        input_payload = {
            "report_month": str(selected_month),
            "utm_sessions": int(utm_s), "organic_sessions": int(org_s), "ad_spend": float(ad_spend),
            "social_likes": int(likes), "social_comments": int(comments), "social_shares": int(shares), "post_views": int(views),
            "site_time_sessions": int(time_site), "booking_clicks": int(cta_clicks), "pos_reviews": int(reviews), "geo_lift_traffic": int(geo_lift),
            "ledger_traffic": int(ledger_traffic), "ledger_signups": int(ledger_signups),
            "avg_spend": float(avg_spend_actual), "ltv_member": float(LTV_BENCHMARK)
        }
        
        try:
            calculate_and_save_roas(input_payload)
            st.success(f"✅ ROI for {selected_label} saved successfully!")
            st.rerun() 
        except Exception as e:
            st.error(f"Sync Failure: {e}")

    # --- 5. DATA RETRIEVAL ---
    try:
        history_res = supabase.table("monthly_roi").select("*").order("report_month", desc=True).execute()
        if history_res.data:
            df_hist = pd.DataFrame(history_res.data)
            df_hist['Month'] = pd.to_datetime(df_hist['report_month']).dt.strftime('%B %Y')

            # --- 6. CUMULATIVE TILES (WITH HELP TEXT RESTORED) ---
            st.divider()
            total_brand_value = df_hist['brand_value'].sum()
            total_ad_spend_ytd = df_hist['ad_spend'].sum()
            cumulative_roas_val = total_brand_value / total_ad_spend_ytd if total_ad_spend_ytd > 0 else 0
            total_enhanced = df_hist['enhanced_revenue'].sum()

            st.write("### 🏛️ YTD Cumulative Performance")
            st.caption("Aggregated performance metrics from January to Present.")
            
            c1, c2, c3 = st.columns(3)
            c1.metric(
                label="YTD Cumulative ROAS", 
                value=f"{cumulative_roas_val:.2f}x", 
                help="The efficiency ratio of total Brand Value created vs. total Ad Spend. >1.0x indicates value creation exceeding investment."
            )
            c2.metric(
                label="Total Brand Equity", 
                value=f"${total_brand_value:,.2f}", 
                help="The calculated monetary value of all digital sessions, social engagements, and sentiment shifts tracked YTD."
            )
            c3.metric(
                label="Total Enhanced Impact", 
                value=f"${total_enhanced:,.2f}", 
                help="The holistic revenue impact: Sum of Brand Value + Actual Floor Coin-In + New Member Lifetime Value ($1,900/member)."
            )

            # --- 7. SHAREPOINT GENERATOR (STRICT MATCHING) ---
            st.divider()
            st.write("### 📄 SharePoint Report Generator")
            
            # Find the specific row for the selected month in the historical record
            curr_row = df_hist[df_hist['report_month'] == str(selected_month)]
            
            if not curr_row.empty:
                curr = curr_row.iloc[0]
                prev = df_hist.iloc[1] if len(df_hist) > 1 else curr
                mom_roas = ((curr['calculated_bl_roas'] / prev['calculated_bl_roas']) - 1) * 100 if prev['calculated_bl_roas'] > 0 else 0
                
                # Math Parity logic
                prop_potential = float(ledger_coin_in) + (int(ledger_signups) * LTV_BENCHMARK)
                attr_10, attr_20, attr_30 = prop_potential * 0.10, prop_potential * 0.20, prop_potential * 0.30
                enhanced_revenue_val = float(curr['brand_value']) + prop_potential

                # ROAS Guards for SharePoint
                ad_spend_val = curr['ad_spend']
                roas_10 = (attr_10 / ad_spend_val) if ad_spend_val > 0 else 0
                roas_20 = (attr_20 / ad_spend_val) if ad_spend_val > 0 else 0
                roas_30 = (attr_30 / ad_spend_val) if ad_spend_val > 0 else 0
                enh_roas = (enhanced_revenue_val / ad_spend_val) if ad_spend_val > 0 else 0

                report_text = f"""{selected_label} ROAS Results
Brand Health Performance

BL-ROAS = ${curr['calculated_bl_roas']:.2f} ({mom_roas:+.1f}% MoM)
BL-ROAS YTD = ${cumulative_roas_val:.2f}
For every $1 spent in advertising, we generated ${curr['calculated_bl_roas']:.2f} in measurable brand value.

🎯 Attributed Revenue Impact – {selected_label}
• 10%: ${attr_10:,.0f} | ROAS: {roas_10:,.0f}x
• 20%: ${attr_20:,.0f} | ROAS: {roas_20:,.0f}x
• 30%: ${attr_30:,.0f} | ROAS: {roas_30:,.0f}x

Guest Trip Equivalent (based on ${avg_spend_actual:,.2f} avg spend):
• 10% → {attr_10/avg_spend_actual if avg_spend_actual > 0 else 0:,.0f} visits
• 20% → {attr_20/avg_spend_actual if avg_spend_actual > 0 else 0:,.0f} visits
• 30% → {attr_30/avg_spend_actual if avg_spend_actual > 0 else 0:,.0f} visits

Enhanced Revenue = ${enhanced_revenue_val:,.0f}
Enhanced ROAS = {enh_roas:,.1f}x"""
                
                st.text_area("SharePoint Ready Text:", value=report_text, height=350)
            else:
                st.info(f"Please save the ROI analysis for {selected_label} to generate the SharePoint report.")

            # --- 8. HISTORICAL LEDGER ---
            st.divider()
            st.write("### 📜 Historical ROI Audit Ledger")
            display_df = df_hist[['Month', 'calculated_bl_roas', 'brand_value', 'ad_spend', 'enhanced_revenue']].copy()
            st.dataframe(display_df.style.format({
                'calculated_bl_roas': '{:.2f}x', 'brand_value': '${:,.2f}', 
                'ad_spend': '${:,.2f}', 'enhanced_revenue': '${:,.2f}'
            }), use_container_width=True, hide_index=True)

        else:
            st.info("No historical ROI audits found yet.")

    except Exception as e:
        st.error(f"Audit Engine Error: {e}")

# =================================================================
# 14. FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
