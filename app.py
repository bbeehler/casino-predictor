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
import os
import uuid
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# =================================================================
# 1. DATABASE CONNECTION & GLOBAL SAAS CONTEXT
# =================================================================
try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error(f"Critical System Error: Connection secrets missing. {e}")
    st.stop()

# --- NEW: SAAS IDENTITY LAYER ---
if 'current_property_name' not in st.session_state:
    st.session_state.current_property_name = "Hard Rock Ottawa"

if 'current_property_id' not in st.session_state:
    try:
        # Fetch the UUID for the active property
        prop_res = supabase.table("properties").select("id").eq("property_name", st.session_state.current_property_name).single().execute()
        st.session_state.current_property_id = prop_res.data['id']
    except:
        st.session_state.current_property_id = None

# =================================================================
# 2. PERMANENT INITIALIZATION (SaaS Aware)
# =================================================================
if 'coeffs' not in st.session_state:
    try:
        # UPDATED: Filter by property_id instead of hardcoded ID 1
        response = supabase.table("coefficients")\
            .select("*")\
            .eq("property_id", st.session_state.current_property_id)\
            .execute()
        
        if response.data and len(response.data) > 0:
            st.session_state.coeffs = response.data[0]
        else:
            # SaaS Default Coefficients for new properties
            st.session_state.coeffs = {
                'property_id': st.session_state.current_property_id,
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
            # Upsert ensures the new property gets its own specific coefficients row
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            
    except Exception as e:
        st.error(f"Initialization Error: {e}")
        st.session_state.coeffs = {'Promo': 500.0, 'OOH_Weight': 100.0}

# =================================================================
# 3. GLOBAL PAGE CONFIG & EXECUTIVE THEME
# =================================================================
st.set_page_config(
    page_title="FloorCast Pro", 
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
# 4. FORENSIC ENGINE: OTTAWA REALITY (v6.14 - HEARTBEAT SYNC)
# =================================================================
def get_forensic_metrics(df_input, coeffs):
    if not df_input:
        return {"predictability": "0.0%", "df": pd.DataFrame(), "total_inertia": 0}

    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    today = pd.Timestamp(datetime.date.today())
    
    # --- 1. DYNAMIC COEFFICIENTS ---
    c_clicks = float(coeffs.get('Clicks', 0.05))
    c_social = float(coeffs.get('Social_Imp', 0.0002))
    decay = float(coeffs.get('Ad_Decay', 85)) / 100 
    gravity = float(coeffs.get('Event_Gravity', 0.25))
    
    # Check for both naming conventions to be safe
    promo_lift_weight = float(coeffs.get('Promo', coeffs.get('Promo', 500.0)))
    c_pr_mult = float(coeffs.get('PR_Weight', 1.2)) 

    # Brand Inertia Layer
    ooh_daily = (float(coeffs.get('Static_Weight', 100.0)) * int(coeffs.get('Static_Count', 1))) + \
                (float(coeffs.get('Digital_OOH_Weight', 25.0)) * int(coeffs.get('Digital_OOH_Count', 5)))
    total_brand_inertia = ooh_daily + float(coeffs.get('Broadcast_Weight', 150.0)) + float(coeffs.get('OOH_Weight', 100.0))

    # --- 2. DATA PREPARATION ---
    df['is_closed'] = df.apply(lambda x: 1 if (x['entry_date'] < today and x.get('actual_traffic', 0) == 0) else 0, axis=1)
    df['clean_attendance'] = pd.to_numeric(df['attendance'], errors='coerce').fillna(0).astype(float)
    df['gravity_lift'] = df['clean_attendance'] * gravity
    
    # Calculate Residual Lift (Ad Spend Decay Modeling)
    awareness_pool, current_pool = [], 0.0
    for _, row in df.iterrows():
        daily_in = (float(row.get('ad_clicks', 0)) * c_clicks) + (float(row.get('ad_impressions', 0)) * c_social)
        current_pool = daily_in + (current_pool * decay)
        awareness_pool.append(current_pool)
    df['residual_lift'] = awareness_pool

    # --- 3. THE ACTUAL OTTAWA FLOOR (UPDATED 2026) ---
    heartbeats = {
        'Monday': 3398, 'Tuesday': 3525, 'Wednesday': 6312,
        'Thursday': 4924, 'Friday': 7523, 'Saturday': 9863, 'Sunday': 5894
    }

    # --- 4. PREDICTION LOGIC ---
    def predict_guests(row):
        if row.get('is_closed', 0) == 1: 
            return 0
            
        day_name = row['entry_date'].strftime('%A')
        # Ensure we fallback to 4000 if a day is missing
        base = float(heartbeats.get(day_name, 4000))
        
        # PR Multiplier logic
        p_val = str(row.get('active_promo', '0'))
        current_base = base * c_pr_mult if "PR" in p_val.upper() else base
        
        # Calculate Lifts
        promo_impact = float(promo_lift_weight) if p_val not in ['0', '0.0', 'nan', 'None', ''] else 0
        event_lift = float(row.get('gravity_lift', 0))
        digital_lift = float(row.get('residual_lift', 0))

        return max(0, current_base + digital_lift + total_brand_inertia + event_lift + promo_impact)

    # --- 5. EXECUTION ---
    df['expected'] = df.apply(predict_guests, axis=1)
    df['baseline'] = df['entry_date'].dt.day_name().map(heartbeats).astype(float)

    return {
        "df": df,
        "total_inertia": total_brand_inertia,
        "heartbeats": heartbeats
    }

# --- 4.5 CLOUD SENTIMENT ENGINE (v17.6 SaaS Update) ---
def archive_sentiment_entry(raw_text, asset_name, manual_score=0.0):
    """
    Evaluates sentiment via Gemini if no manual score is provided, 
    then archives to Supabase.
    """
    nlp_score = manual_score

    if nlp_score == 0.0:
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            response = model.generate_content(
                f"Analyze the sentiment of this casino/hotel review. "
                f"Return ONLY a single number between -1.0 (very negative) and 1.0 (very positive): {raw_text}"
            )
            
            clean_val = response.text.strip().replace(" ", "")
            nlp_score = float(clean_val)
        except Exception as e:
            st.error(f"AI Scoring Error: {e}")
            nlp_score = 0.0 

    if nlp_score > 0.3:
        category, icon = "Positive", "🟢"
    elif nlp_score < -0.3:
        category, icon = "Negative", "🔴"
    else:
        category, icon = "Neutral", "🟡"

    abs_score = abs(nlp_score)
    intensity = "High" if abs_score > 0.7 else "Moderate" if abs_score > 0.3 else "Low"

    new_entry = {
        "message_id": f"MSG-{uuid.uuid4().hex[:6].upper()}",
        "raw_text": raw_text,
        "asset": asset_name,
        "sentiment_score": round(float(nlp_score), 2),
        "sentiment_category": category,
        "intensity_level": intensity,
        "property_id": st.session_state.current_property_id  # <--- CRITICAL ADDITION
    }

    try:
        supabase.table("sentiment_history").insert(new_entry).execute()
        return category, icon, intensity
    except Exception as e:
        st.error(f"Cloud Database Error: {e}")
        return "Error", "⚠️", "Unknown"

# =================================================================
# 5. DATA INFRASTRUCTURE (SUPABASE & WEATHER)
# =================================================================
try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase = create_client(url, key)
except Exception as e:
    st.error("🚨 Critical Error: Supabase connection failed. Check your secrets.toml.")
    st.stop()

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
# 6. HYDRATION & RECOVERY (SaaS Filtered)
# =================================================================
try:
    # UPDATED: Only pull data belonging to THIS property
    c_res = supabase.table("coefficients")\
        .select("*")\
        .eq("property_id", st.session_state.current_property_id)\
        .execute()
    
    if c_res.data:
        st.session_state.coeffs = c_res.data[0]
    
    l_res = supabase.table("ledger")\
        .select("*")\
        .eq("property_id", st.session_state.current_property_id)\
        .execute()
    ledger_data = l_res.data if l_res.data else []
    
except Exception as e:
    ledger_data = []

# =================================================================
# 7. SIDEBAR NAVIGATION, AUTH & SAAS GATEKEEPER (v18.5)
# =================================================================
st.markdown("""
    <style>
    div.stButton > button > div > p,
    div.stButton > button span,
    div.stButton > button p {
        color: #FFFFFF !important;
    }
    </style>
    """, unsafe_allow_html=True)

st.sidebar.markdown("<h1 style='color:#0047AB; font-size: 28px; margin-bottom: 0;'>🎰 FloorCast AI</h1><p style='color:#888;'>Global Casino Intelligence</p>", unsafe_allow_html=True)
st.sidebar.divider()

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# --- THE GATEKEEPER ---
if not st.session_state.authenticated:
    st.markdown("<h1 style='color:#0047AB; text-align:center;'>Executive Access Required</h1>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            e_mail = st.text_input("Email")
            p_word = st.text_input("Password", type="password")
            submit = st.form_submit_button("Unlock Engine", use_container_width=True)
            
            if submit:
                try:
                    # 1. Authenticate with Supabase Auth
                    res = supabase.auth.sign_in_with_password({"email": e_mail, "password": p_word})
                    if res.user:
                        # 2. SAAS MULTI-TENANT MAPPING: Link user to their property
                        access_res = supabase.table("user_property_access")\
                            .select("property_id, properties(property_name), user_role")\
                            .eq("user_email", e_mail).single().execute()
                        
                        if access_res.data:
                            st.session_state.authenticated = True
                            st.session_state.user_email = e_mail
                            st.session_state.user_role = access_res.data['user_role']
                            st.session_state.current_property_id = access_res.data['property_id']
                            st.session_state.current_property_name = access_res.data['properties']['property_name']
                            st.rerun()
                        else:
                            st.error("Authentication successful, but no property assigned. Contact Global Admin.")
                    else:
                        st.error("Authentication failed.")
                except Exception as e:
                    st.error("Access Denied: Invalid credentials or database link error.")
    st.stop() 

# --- PORTFOLIO SWITCHER (For Global Admin Demos) ---
# This allows you to switch properties on the fly for sales pitches
if st.session_state.get('user_role') == "Admin":
    with st.sidebar.expander("🏢 Global Portfolio Switcher", expanded=False):
        try:
            # Fetch all properties in the system
            all_props = supabase.table("properties").select("*").execute()
            if all_props.data:
                prop_map = {p['property_name']: p['id'] for p in all_props.data}
                selected_name = st.selectbox(
                    "Active View:", 
                    options=list(prop_map.keys()),
                    index=list(prop_map.keys()).index(st.session_state.current_property_name)
                )
                
                if selected_name != st.session_state.current_property_name:
                    st.session_state.current_property_name = selected_name
                    st.session_state.current_property_id = prop_map[selected_name]
                    # Clear cache to force data reload for the new property
                    st.cache_data.clear()
                    st.rerun()
        except:
            st.sidebar.warning("Could not load portfolio list.") 

# =================================================================
# 8. EXECUTIVE NAVIGATION (v18.6 SaaS Enabled)
# =================================================================
with st.sidebar:
    # SaaS Branding: In the future, replace this URL with st.session_state.get('logo_url')
    st.image("https://casino.hardrock.com/ottawa/-/media/project/shrss/hri/casinos/hard-rock/ottawa/logos-and-icons/logo.png?h=171&iar=0&w=224&rev=914ac0eae6734be995b93d76ad2b1e8f", width=150)
    
    st.title(f"{st.session_state.current_property_name}")
    st.caption(f"User: {st.session_state.get('user_email', 'Internal')}")
    st.divider()
    
    # 1. DYNAMIC NAV LIST
    nav_options = [
        "Executive Dashboard", 
        "Daily Ledger Audit", 
        "Attribution Analytics", 
        "Master Audit Report", 
        "AI Calibration",
        "FloorCast AI Analyst",
        "BL-ROAS Calculator"
    ]

    # 2. PROVISIONING GATE: Only show Admin Console to "Admin" role
    if st.session_state.get('user_role') == "Admin":
        nav_options.append("Global Admin Console")

    page = st.radio(
        "Intelligence Decks:",
        nav_options,
        index=0,
        key="nav_list_v18_6"
    )
    
    st.divider()
    
    # 3. LOGOUT & SESSION MANAGEMENT
    if st.button("🚪 Logout / Reset Session", use_container_width=True):
        # We clear session state to ensure no property data leaks to the next user
        st.session_state.clear()
        st.rerun()

    # Analyst thread reset (Only visible on Analyst page)
    if page == "FloorCast AI Analyst" and st.session_state.get('messages'):
        if st.button("🗑️ Reset Analyst Thread", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

# =================================================================
# 9. PAGE 1: EXECUTIVE DASHBOARD (v48.0 - SaaS Dynamic Assets)
# =================================================================
if page == "Executive Dashboard":
    # 1. HEADER
    st.markdown(f"""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📈 Executive Performance Pulse: {st.session_state.current_property_name}</h2>
            <p style="color: #444; margin: 0;">Strategic Demand Projection & Marketing Impact for this property.</p>
        </div>
    """, unsafe_allow_html=True)

    today = datetime.date.today()
    current_weights = st.session_state.get('coeffs', {})

    if not ledger_data:
        st.warning(f"Forensic Vault for {st.session_state.current_property_name} is empty. Please populate the Ledger.")
        st.stop()

    # --- 2. PREPARE RAW DATA (Filtered via Section 6 Hydration) ---
    df_raw = pd.DataFrame(ledger_data)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
    df_raw['dow'] = df_raw['entry_date'].dt.day_name()
    master_baselines = df_raw.groupby('dow')['actual_traffic'].mean().to_dict()

    # --- 3. DATE SELECTION ---
    col_date, _ = st.columns([1, 2])
    with col_date:
        pulse_range = st.date_input(
            "Select Analysis Window:", 
            value=(today, today + datetime.timedelta(days=7)), 
            key="pulse_exec_unique" 
        )

    if isinstance(pulse_range, tuple) and len(pulse_range) == 2:
        start_p, end_p = pulse_range
        date_list = pd.date_range(start=start_p, end=end_p)
        df_p = pd.DataFrame({'entry_date': date_list})
        df_p['entry_date'] = pd.to_datetime(df_p['entry_date'])
        df_p['dow'] = df_p['entry_date'].dt.day_name()
        
        ledger_lookup = df_raw.set_index(df_raw['entry_date'].dt.strftime('%Y-%m-%d')).to_dict('index')
        
        def map_data(row, col_name):
            d_str = row['entry_date'].strftime('%Y-%m-%d')
            if d_str in ledger_lookup:
                val = ledger_lookup[d_str].get(col_name, 0)
                return val if val is not None else 0
            return "" if col_name == 'active_promo' else 0.0

        map_cols = ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm', 'actual_traffic', 'new_members', 'actual_coin_in']
        for c in map_cols:
            df_p[c] = df_p.apply(lambda r: map_data(r, c), axis=1)

        df_p['baseline'] = df_p['dow'].map(master_baselines).fillna(0)

        # --- 4. STRATEGIC DAILY PLANNER ---
        with st.expander("📅 Strategic Daily Planner & Simulator", expanded=True):
            st.write("Plan your lift. Inputs here directly scale the Vital Signs below.")
            planner_cols = ['entry_date', 'dow', 'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
            df_plan_display = df_p[planner_cols].copy()
            df_plan_display['entry_date'] = df_plan_display['entry_date'].dt.strftime('%a, %b %d')
            
            edited_df = st.data_editor(
                df_plan_display, 
                column_config={
                    "dow": None, 
                    "entry_date": st.column_config.Column("Date", disabled=True),
                    "attendance": st.column_config.NumberColumn("Event Attendance", format="%d"),
                },
                hide_index=True, use_container_width=True, key="p1_planner_v46_editor"
            )
            
            for field in ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']:
                df_p[field] = edited_df[field].values

        # --- 5. ENGINE EXECUTION ---
        m = get_forensic_metrics(df_p.to_dict(orient='records'), current_weights)
        df_final = m['df'].sort_values('entry_date')
        
        df_final['new_members'] = pd.to_numeric(df_final['new_members'], errors='coerce').fillna(0)
        df_final['actual_traffic'] = pd.to_numeric(df_final['actual_traffic'], errors='coerce').fillna(0)
        df_final['actual_coin_in'] = pd.to_numeric(df_final['actual_coin_in'], errors='coerce').fillna(0)
        
        total_vol = df_final['expected'].sum()
        organic_vol = sum(df_final['baseline']) if 'baseline' in df_final.columns else 0
        mkt_impact_pct = ((total_vol - organic_vol) / total_vol * 100) if total_vol > 0 else 0

        # --- 6. THE UNIFIED PULSE CHART ---
        st.write("### 🎰 The Unified Pulse")
        fig_pulse = go.Figure()
        df_act_chart = df_final[df_final['entry_date'].dt.date < today]
        fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
        fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
        st.plotly_chart(fig_pulse, use_container_width=True)

        # --- 7. EXECUTIVE KPI GRID ---
        st.write("### 🏛️ Property Vital Signs")
        k1, k2, k3, k4, k5 = st.columns(5)
        LTV_VAL, AVG_SPEND = 1900.00, 1100.31

        if start_p >= today:
            proj_rev = (total_vol * AVG_SPEND) + ((total_vol * 0.05) * LTV_VAL)
            k1.metric("Projected Demand", f"{total_vol:,.0f} Guests")
            k2.metric("Target Signups", f"{(total_vol * 0.0170):,.0f}")
            k3.metric("Proj. Enhanced Revenue", f"${proj_rev:,.0f}")
            k4.metric("Marketing Impact %", f"{mkt_impact_pct:.1f}%")
            k5.metric("Ledger Revenue", "$0.00")
        else:
            total_act = df_final['actual_traffic'].sum()
            actual_signups = df_final['new_members'].sum()
            ledger_rev = df_final['actual_coin_in'].sum()
            
            base_est_rev = (total_act * AVG_SPEND)
            act_impact_rev = base_est_rev + (actual_signups * LTV_VAL)

            if act_impact_rev > 0:
                rev_diff_pct = ((ledger_rev - act_impact_rev) / act_impact_rev) * 100
            else:
                rev_diff_pct = 0

            if total_act > 0:
                expected_sum = df_final['expected'].sum()
                acc_val = (1 - abs(total_act - expected_sum) / total_act) * 100
                accuracy_display = f"{max(0, acc_val):.1f}%"
            else:
                accuracy_display = "N/A"

            k1.metric("Actual Guest Flow", f"{total_act:,.0f}")
            k2.metric("New Unity Members", f"{actual_signups:,.0f}")
            k3.metric("Audited Revenue Impact", f"${act_impact_rev:,.0f}")
            k4.metric("Ledger Revenue", f"${ledger_rev:,.0f}", delta=f"{rev_diff_pct:.1f}% Diff")
            k5.metric("Audited Accuracy", accuracy_display)

        # --- 8. BRAND SENTIMENT PULSE (SaaS Dynamic Assets) ---
        st.divider()
        st.write("### 🏛️ Executive Brand Sentiment Pulse")

        col_h1, col_h2 = st.columns([2, 1])
        with col_h2:
            g_months = [(today - relativedelta(months=i)).replace(day=1) for i in range(2)]
            g_labels = ["Current (Live)"] + [m.strftime("%B %Y") for m in g_months[1:]]
            sel_period = st.selectbox("Audit Period:", g_labels, key="gauge_historical_select")

        overall_score = 0.0
        try:
            # SaaS: Filter Global Score by Property
            global_query = supabase.table("sentiment_history").select("sentiment_score").eq("property_id", st.session_state.current_property_id)
            if sel_period == "Current (Live)":
                g_res = global_query.order("timestamp", desc=True).limit(50).execute()
            else:
                sel_date = g_months[g_labels.index(sel_period)]
                s_d = sel_date.strftime("%Y-%m-%d")
                e_d = (sel_date + relativedelta(months=1)).strftime("%Y-%m-%d")
                g_res = global_query.filter("timestamp", "gte", s_d).filter("timestamp", "lt", e_d).execute()
            
            if g_res.data:
                overall_score = np.mean([d['sentiment_score'] for d in g_res.data])
        except:
            pass

        st.metric(
            label=f"Consolidated Property Pulse ({sel_period})", 
            value=f"{overall_score:+.2f}",
            delta="Positive Impact" if overall_score > 0.3 else "High Friction" if overall_score < -0.3 else "Neutral",
            delta_color="normal" if abs(overall_score) > 0.3 else "off"
        )

        # 8c. DYNAMIC ASSET GRID (SaaS Step 5 Implementation)
        try:
            asset_res = supabase.table("property_assets").select("asset_name").eq("property_id", st.session_state.current_property_id).execute()
            tags = [item['asset_name'] for item in asset_res.data] if asset_res.data else ["Overall Property"]
        except:
            tags = ["Overall Property"]

        gauge_cols = st.columns(len(tags))

        for i, tag in enumerate(tags):
            with gauge_cols[i]:
                tag_score = 0.0
                try:
                    # SaaS: Filter each asset's score by Property and Tag
                    tag_query = supabase.table("sentiment_history").select("sentiment_score")\
                        .eq("property_id", st.session_state.current_property_id)\
                        .eq("asset", tag)
                    
                    if sel_period == "Current (Live)":
                        t_res = tag_query.order("timestamp", desc=True).limit(50).execute()
                    else:
                        sel_date = g_months[g_labels.index(sel_period)]
                        s_d = sel_date.strftime("%Y-%m-%d")
                        e_d = (sel_date + relativedelta(months=1)).strftime("%Y-%m-%d")
                        t_res = tag_query.filter("timestamp", "gte", s_d).filter("timestamp", "lt", e_d).execute()
                    
                    if t_res.data:
                        tag_score = np.mean([d['sentiment_score'] for d in t_res.data])
                except:
                    pass

                fig = go.Figure(go.Indicator(
                    mode = "gauge+number", 
                    value = tag_score,
                    number = {'font': {'size': 20}, 'valueformat': ".2f"},
                    gauge = {
                        'axis': {'range': [-1, 1], 'tickwidth': 1},
                        'bar': {'color': "#0047AB"},
                        'steps': [
                            {'range': [-1, -0.3], 'color': "#FF4B4B"},
                            {'range': [-0.3, 0.3], 'color': "#F0F2F6"},
                            {'range': [0.3, 1], 'color': "#28A745"}
                        ],
                        'threshold': {'line': {'color': "black", 'width': 3}, 'value': tag_score}
                    }
                ))
                fig.update_layout(height=150, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
                st.markdown(f"<p style='text-align: center; font-weight: bold; font-size: 14px;'>{tag}</p>", unsafe_allow_html=True)
        
# =================================================================
# 10. PAGE 2: DAILY LEDGER AUDIT (SAAS MULTI-TENANT v9.0)
# =================================================================
elif page == "Daily Ledger Audit":
    # 1. HEADER
    st.markdown(f"""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📈 Daily Ledger Audit: {st.session_state.current_property_name}</h2>
            <p style="color: #444; margin: 0;">Enter daily results or see past performance for this property.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # --- 2. THE DATA ENGINE (Filtered by Property) ---
    if not ledger_data:
        df_ledger = pd.DataFrame(columns=[
            'entry_date', 'actual_traffic', 'new_members', 'actual_coin_in', 
            'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 
            'rain_mm', 'snow_cm', 'property_id'
        ])
    else:
        # DATA IS ALREADY FILTERED IN SECTION 6 (HYDRATION)
        df_ledger = pd.DataFrame(ledger_data)
        df_ledger['entry_date'] = pd.to_datetime(df_ledger['entry_date']).dt.date
        
        # Ensure all numeric columns are handled properly
        marketing_cols = ['actual_traffic', 'new_members', 'actual_coin_in', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
        for col in marketing_cols:
            if col in df_ledger.columns:
                df_ledger[col] = pd.to_numeric(df_ledger[col], errors='coerce').fillna(0)
        
        df_ledger['active_promo'] = df_ledger['active_promo'].astype(str).replace(['nan', 'None', '0', '0.0'], '')
        df_ledger = df_ledger.sort_values('entry_date', ascending=False)

    # --- 3. RAPID ENTRY FORM (Tagged with Property ID) ---
    with st.expander("➕ Log New Daily Actuals", expanded=False):
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
                    "snow_cm": 0.0,
                    "property_id": st.session_state.current_property_id  # <--- SAAS TAG
                }
                try:
                    supabase.table("ledger").upsert(new_row).execute()
                    st.success(f"✅ Successfully logged: {e_date} for {st.session_state.current_property_name}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Database Error: {e}")

    # --- 4. HISTORICAL VIEW SLIDER ---
    st.write("### 📂 Performance Audit Range")
    view_limit = st.slider("Select Audit Depth (Days):", 7, 100, 30, key="audit_slider_top")
    df_audit_period = df_ledger.head(view_limit).copy()
    
    # --- 5. DYNAMIC PERFORMANCE SCOREBOARD ---
    st.write(f"### 🎯 Performance Scoreboard: Last {view_limit} Days")
    
    if not df_audit_period.empty:
        total_period_traffic = df_audit_period['actual_traffic'].sum()
        total_period_signups = df_audit_period['new_members'].sum()
        total_potential = (total_period_traffic * 1100.31) + (total_period_signups * 1900.00)
        avg_traffic = total_period_traffic / len(df_audit_period)
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Period Traffic", f"{total_period_traffic:,.0f}", delta=f"{avg_traffic:,.0f} avg/day")
        m2.metric("Total New Members", f"{total_period_signups:,.0f}", delta=f"{total_period_signups / len(df_audit_period):,.1f} avg/day")
        m3.metric("Audited Potential", f"${total_potential:,.2f}")
    else:
        st.info("No data available for the selected range.")

    st.divider()

    # --- 6. THE HISTORICAL EDITABLE LEDGER (Bulk Sync Fix) ---
    st.write("### 📂 Bulk Audit & Corrections")
    with st.form("bulk_ledger_sync"):
        # We don't want the user editing property_id manually, so we hide it from view
        display_df = df_audit_period.drop(columns=['property_id']) if 'property_id' in df_audit_period.columns else df_audit_period
        
        edited_ledger = st.data_editor(
            display_df, 
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
            key="property_ledger_v9_0"
        )
        
        if st.form_submit_button("💾 Sync Table Updates", use_container_width=True):
            try:
                df_sync = edited_ledger.copy()
                df_sync['entry_date'] = df_sync['entry_date'].astype(str)
                # CRITICAL: Re-attach the property_id to every row before syncing back
                df_sync['property_id'] = st.session_state.current_property_id
                
                sync_payload = df_sync.fillna(0).to_dict(orient='records')
                supabase.table("ledger").upsert(sync_payload).execute()
                
                st.success(f"✅ Bulk updates synced for {st.session_state.current_property_name}.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Bulk Sync Error: {e}")

# =================================================================
# 11. PAGE 3: ATTRIBUTION ANALYTICS (PRO-MARKETING SUITE v17.0)
# =================================================================
elif page == "Attribution Analytics":
    st.markdown("""
        <div style="background-color:#F8F9FA;padding:20px;border-radius:12px;border-left:6px solid #0047AB;margin-bottom:20px;">
            <h2 style="color:#0047AB;margin:0;">📊 Marketing Attribution & ROI</h2>
            <p style="color:#666;margin:0;">Multi-Touch Analysis: Correlating Digital Signal with Physical Property Yield.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.info("💡 Forensic Vault empty. Populate the Ledger to unlock attribution.")
        st.stop()

    # 1. DATA PREP & MTA LOGIC
    current_weights = st.session_state.get('coeffs', {})
    m_full = get_forensic_metrics(ledger_data, current_weights)
    df_attr = m_full['df']
    
    # Calculate Component Parts
    total_guests = df_attr['actual_traffic'].sum()
    organic_base = df_attr['baseline'].sum()
    digital_lift = df_attr['residual_lift'].sum()
    gravity_lift = df_attr['gravity_lift'].sum()
    brand_inertia = (current_weights.get('Broadcast_Weight', 150) + current_weights.get('OOH_Weight', 100)) * len(df_attr)

    # --- 2. MULTI-TOUCH ATTRIBUTION (TIME DECAY VIEW) ---
    st.write("### 🕰️ Multi-Touch Attribution (Time Decay Model)")
    st.caption("Weighting the guest journey based on proximity to visit date (Adstock Decay).")
    
    # Simulating MTA split based on your Adstock Decay coefficient
    decay_val = current_weights.get('Ad_Decay', 85) / 100
    mta_digital = digital_lift * decay_val
    mta_brand = brand_inertia * (1 - decay_val)
    mta_gravity = gravity_lift
    
    mta_cols = st.columns(3)
    mta_cols[0].metric("Last-Touch (Digital)", f"{digital_lift:,.0f}", help="Immediate click-to-floor conversion.")
    mta_cols[1].metric("Assisted (Brand)", f"{brand_inertia:,.0f}", help="OOH/Broadcast awareness priming.")
    mta_cols[2].metric("Conversion (Gravity)", f"{gravity_lift:,.0f}", help="Event-driven floor closure.")

    st.divider()

    # --- 3. OFFLINE-TO-ONLINE CONTRIBUTION ---
    st.write("### 📡 Offline-to-Online Attribution Channel Contribution")
    col_pie, col_water = st.columns([1, 1.5])

    with col_pie:
        pie_labels = ['Organic (Baseline)', 'Online (Digital)', 'Offline (Brand/Media)', 'Event Gravity']
        pie_values = [organic_base, digital_lift, brand_inertia, gravity_lift]
        fig_pie = px.pie(names=pie_labels, values=pie_values, 
                         color_discrete_sequence=['#E1E8F0', '#0047AB', '#5D707F', '#FFCC00'],
                         hole=0.5)
        fig_pie.update_layout(showlegend=True, height=350, margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_water:
        # Waterfall showing how different layers build to the final traffic
        fig_water = go.Figure(go.Waterfall(
            orientation = "v",
            measure = ["relative", "relative", "relative", "relative", "total"],
            x = ["Organic", "Offline Media", "Online Signal", "Event Gravity", "Total Floor"],
            y = [organic_base, brand_inertia, digital_lift, gravity_lift, total_guests],
            decreasing = {"marker":{"color":"#FF4B4B"}},
            increasing = {"marker":{"color":"#0047AB"}},
            totals = {"marker":{"color":"#FFCC00"}}
        ))
        fig_water.update_layout(height=350, margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig_water, use_container_width=True)

    st.divider()

    # --- 4. LIFT CORRELATION ---
    st.write("### 📈 Lift Correlation")
    # Scatter plot correlating Marketing Spend/Signals with Actual Traffic
    fig_corr = px.scatter(df_attr, x='ad_clicks', y='actual_traffic', 
                          trendline="ols", 
                          labels={'ad_clicks': 'Digital Signal (Clicks)', 'actual_traffic': 'Property Traffic'},
                          color_discrete_sequence=['#0047AB'])
    fig_corr.update_layout(height=400, plot_bgcolor='rgba(248,249,250,1)')
    st.plotly_chart(fig_corr, use_container_width=True)

    st.divider()

    # --- 5. STRATEGIC INTERPRETATION & ROI AUDIT ---
    st.write("### 💎 Strategic Interpretation & ROI Audit")
    
    if not df_attr.empty:
        # Fetching average coin-in for ROI audit
        avg_coin = float(current_weights.get('Avg_Coin_In', 112.50))
        mkt_guests = digital_lift + brand_inertia + gravity_lift
        mkt_revenue = mkt_guests * avg_coin
        
        # Calculate Efficiency Metrics
        yield_per_click = digital_lift / df_attr['ad_clicks'].sum() if df_attr['ad_clicks'].sum() > 0 else 0
        brand_leverage = (brand_inertia / organic_base) if organic_base > 0 else 0
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Marketing Yield (Est. $)", f"${mkt_revenue:,.0f}", help="Total revenue attributed to marketing layers.")
        c2.metric("Guest Pull Efficiency", f"{(mkt_guests/total_guests)*100:.1f}%", help="Percentage of total traffic driven by marketing.")
        c3.metric("Digital Conversion Rate", f"{yield_per_click:.2f}x", help="Guests gained per digital click signal.")

        st.info(f"""
        **FloorCast Strategic Audit Summary:**
        * **MTA Insight:** The {current_weights.get('Ad_Decay', 85)}% Adstock retention indicates a strong **Time Decay** effect, meaning marketing influence remains active on the floor for multiple days post-exposure.
        * **Channel Mix:** **{'Digital' if digital_lift > brand_inertia else 'Offline Media'}** is currently providing the highest marginal lift per dollar.
        * **ROI Validation:** Based on a ${avg_coin:.2f} Avg Coin-In, marketing activities have contributed an estimated **{mkt_guests:,.0f}** guests to the audit window, effectively supporting property revenue goals.
        """)
    else:
        st.warning("Insufficient data for full ROI Audit.")

# =================================================================
# 12. PAGE 4: MASTER FORENSIC AUDIT (v16.4 - Unified Fix)
# =================================================================
elif page == "Master Audit Report":
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
        st.warning("Audit Vault is empty.")
        st.stop()

    df_audit_raw = pd.DataFrame(ledger_data)
    df_audit_raw['entry_date'] = pd.to_datetime(df_audit_raw['entry_date'])
    
    min_audit = df_audit_raw['entry_date'].min().date()
    max_audit = df_audit_raw['entry_date'].max().date()

    col_date, col_export = st.columns([2, 1])
    with col_date:
        audit_range = st.date_input("Audit Window:", value=(min_audit, max_audit), key="master_audit_v16_final")

    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_date, e_date = audit_range
        mask = (df_audit_raw['entry_date'].dt.date >= s_date) & (df_audit_raw['entry_date'].dt.date <= e_date)
        df_audit_filtered = df_audit_raw.loc[mask].copy()
        
        if df_audit_filtered.empty:
            st.error(f"No records found between {s_date} and {e_date}.")
            st.stop()

        # --- 1. ENGINE & GLOBAL VARIABLE INITIALIZATION ---
        m = get_forensic_metrics(df_audit_filtered.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df'] 
        c = st.session_state.coeffs
        num_days = len(df_final)
        
        # Benchmarks & Config
        LTV_VAL = 1900.00
        avg_coin = float(c.get('Avg_Coin_In', 112.50))
        hold_pct = float(c.get('Hold_Pct', 10.2)) / 100

        # Global Totals Calculation
        t_traffic = df_final['actual_traffic'].sum()
        t_actual_rev = df_final['actual_coin_in'].sum()
        actual_ggr = t_actual_rev * hold_pct
        t_digital = df_final['residual_lift'].sum()
        t_gravity = df_final['gravity_lift'].sum()
        t_inertia_total = m.get('total_inertia', 0) * num_days
        t_mkt = t_digital + t_inertia_total + t_gravity
        t_mems = df_final['new_members'].sum()
        friction_total = abs((df_final['snow_cm'].sum() * float(c.get('Snow_cm', -45))) + (df_final['rain_mm'].sum() * float(c.get('Rain_mm', -12))))
        digital_dollar = t_digital * avg_coin

        # --- 2. DATE-AWARE ROI FETCH ---
        try:
            roi_res = supabase.table("monthly_roi").select("brand_value, calculated_bl_roas, ad_spend") \
                .filter("report_month", "gte", s_date.strftime('%Y-%m-%d')) \
                .filter("report_month", "lte", e_date.strftime('%Y-%m-%d')).execute()
            if roi_res.data:
                roi_df = pd.DataFrame(roi_res.data)
                avg_bl_roas = roi_df['calculated_bl_roas'].mean()
                total_brand_val = roi_df['brand_value'].sum()
                total_ad_spend = roi_df['ad_spend'].sum()
            else:
                avg_bl_roas, total_brand_val, total_ad_spend = 0.0, 0.0, 0.0
        except:
            avg_bl_roas, total_brand_val, total_ad_spend = 0.0, 0.0, 0.0

        rev_multiplier = (actual_ggr + total_brand_val) / total_ad_spend if total_ad_spend > 0 else 0

        # --- 3. EXECUTIVE SUMMARY & MoM PERFORMANCE TABLE ---
        st.write("### 📊 Executive Summary & Monthly Performance")
        df_final['month_year'] = df_final['entry_date'].dt.to_period('M')
        months = sorted(df_final['month_year'].unique())
        
        summary_list = []
        raw_mom_values = {"traffic": [], "revenue": [], "digital": []}
        
        for i, month in enumerate(months):
            df_m = df_final[df_final['month_year'] == month]
            m_traffic = df_m['actual_traffic'].sum()
            m_rev = df_m['actual_coin_in'].sum()
            m_digital = df_m['residual_lift'].sum()
            m_fric = abs((df_m['snow_cm'].sum() * float(c.get('Snow_cm', -45))) + (df_m['rain_mm'].sum() * float(c.get('Rain_mm', -12))))
            
            mom_t, mom_r, mom_d = "---", "---", "---"
            if i > 0:
                p_m = months[i-1]
                df_p = df_final[df_final['month_year'] == p_m]
                p_t, p_r, p_d = df_p['actual_traffic'].sum(), df_p['actual_coin_in'].sum(), df_p['residual_lift'].sum()
                if p_t > 0: 
                    chg = ((m_traffic - p_t)/p_t)*100
                    raw_mom_values["traffic"].append(chg)
                    mom_t = f"{chg:+.1f}%"
                if p_r > 0:
                    chg = ((m_rev - p_r)/p_r)*100
                    raw_mom_values["revenue"].append(chg)
                    mom_r = f"{chg:+.1f}%"
                if p_d > 0:
                    chg = ((m_digital - p_d)/p_d)*100
                    raw_mom_values["digital"].append(chg)
                    mom_d = f"{chg:+.1f}%"

            summary_list.append({
                "Month": month.strftime('%B %Y'), "Traffic": m_traffic, "Traffic MoM": mom_t,
                "Actual Revenue": m_rev, "Revenue MoM": mom_r, "Digital Lift": m_digital,
                "Digital MoM": mom_d, "Digital $ Impact": m_digital * avg_coin, "Weather Penalty": -m_fric
            })

        df_summary_table = pd.DataFrame(summary_list)
        
        # Add Total Row
        def get_avg_str(v_list): return f"{np.mean(v_list):+.1f}% Avg" if v_list else "---"
        total_row = pd.Series({
            "Month": "**TOTAL AUDIT WINDOW**", "Traffic": df_summary_table["Traffic"].sum(),
            "Traffic MoM": get_avg_str(raw_mom_values["traffic"]), "Actual Revenue": df_summary_table["Actual Revenue"].sum(),
            "Revenue MoM": get_avg_str(raw_mom_values["revenue"]), "Digital Lift": df_summary_table["Digital Lift"].sum(),
            "Digital MoM": get_avg_str(raw_mom_values["digital"]), "Digital $ Impact": df_summary_table["Digital $ Impact"].sum(),
            "Weather Penalty": df_summary_table["Weather Penalty"].sum()
        })
        df_summary_table = pd.concat([df_summary_table, total_row.to_frame().T], ignore_index=True)

        # Formatting Table
        fmt_map = {"Traffic": "{:,.0f}", "Actual Revenue": "${:,.0f}", "Digital Lift": "{:,.0f}", "Digital $ Impact": "${:,.0f}", "Weather Penalty": "{:,.0f}"}
        for col, f_string in fmt_map.items():
            df_summary_table[col] = df_summary_table[col].apply(lambda x: f_string.format(x) if isinstance(x, (int, float)) else x)
        
        st.table(df_summary_table)

        # --- 4. YTD CAPTION ---
        current_year = 2026
        ytd_df_raw = df_audit_raw[df_audit_raw['entry_date'].dt.year == current_year].copy()
        if not ytd_df_raw.empty:
            m_ytd = get_forensic_metrics(ytd_df_raw.to_dict(orient='records'), c)
            df_y = m_ytd['df']
            y_traf, y_dig = df_y['actual_traffic'].sum(), df_y['residual_lift'].sum()
            st.caption(f"**{current_year} YTD:** {y_traf:,.0f} Guests | ${df_y['actual_coin_in'].sum():,.0f} Revenue | {df_y['new_members'].sum():,.0f} Members.  \n**YTD Digital Impact:** {y_dig:,.0f} Guests ({(y_dig/y_traf*100 if y_traf > 0 else 0):.1f}% contribution).")

        # --- 5. METRIC CARDS SECTIONS ---
        st.write("### 💰 Financial & Loyalty Integrity")
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Traffic", f"{t_traffic:,}")
        k2.metric("Actual Revenue", f"${t_actual_rev:,.0f}")
        k3.metric("Actual GGR (Hold)", f"${actual_ggr:,.0f}")
        k4.metric("New Unity Members", f"{t_mems:,}")
        k5.metric("Member Conv. %", f"{(t_mems/t_traffic*100 if t_traffic > 0 else 0):.2f}%")

        st.write("### 🧬 Marketing Equity & Friction")
        k6, k7, k8, k9, k10 = st.columns(5)
        k6.metric("Marketing Guests", f"{t_mkt:,.0f}")
        k7.metric("Marketing Share", f"{(t_mkt/t_traffic*100 if t_traffic > 0 else 0):.1f}%")
        k8.metric("Digital ROI Lift", f"{t_digital:,.0f}")
        k9.metric("Weather Friction", f"-{friction_total:,.0f}")
        k10.metric("AI Confidence", m.get('predictability', '92.5%'))

        st.write("### 💎 BL-ROAS & Equity Efficiency")
        
        # Strength UI Logic
        def get_stat_ui(val, mode="m"):
            if mode=="m": # Multiplier thresholds
                if val >= 5.0: return "💎 ELITE", "#008000"
                if val >= 3.0: return "✅ STRONG", "#2E8B57"
                return "⚠️ MONITOR", "#B8860B"
            else: # Efficiency thresholds
                if val >= 20.0: return "🚀 OPTIMIZED", "#008000"
                if val >= 10.0: return "📈 STABLE", "#2E8B57"
                return "🔍 UNDER-LEVERAGED", "#B8860B"
        
        m_status, m_color = get_stat_ui(rev_multiplier, "m")
        e_pct = (t_mkt/t_traffic*100 if t_traffic > 0 else 0)
        e_status, e_color = get_stat_ui(e_pct, "e")

        kb1, kb2, kb3, kb4, kb5 = st.columns(5)
        kb1.metric("Avg. BL-ROAS", f"{avg_bl_roas:.2f}x")
        kb2.metric("Total Brand Value", f"${total_brand_val:,.0f}")
        kb3.metric("Rev Multiplier", f"{rev_multiplier:.1f}x")
        kb4.metric("Equity Efficiency", f"{e_pct:.1f}%")
        kb5.metric("LTV Equity Growth", f"${(t_mems*LTV_VAL):,.0f}")

        # Integrated Status Badges
        sb1, sb2, sb3, sb4, sb5 = st.columns(5)
        with sb3: st.markdown(f"<div style='text-align:center;padding:5px;border-radius:5px;background-color:{m_color};color:white;font-size:0.7rem;font-weight:bold;margin-top:-10px;'>{m_status}</div>", unsafe_allow_html=True)
        with sb4: st.markdown(f"<div style='text-align:center;padding:5px;border-radius:5px;background-color:{e_color};color:white;font-size:0.7rem;font-weight:bold;margin-top:-10px;'>{e_status}</div>", unsafe_allow_html=True)

        # --- 6. SOCIAL PERFORMANCE & AWARENESS ---
        st.write("### 📱 Social Performance & Awareness")
        ks1, ks2, ks3, ks4, ks5 = st.columns(5)
        t_imps, t_clicks = df_final['ad_impressions'].sum(), df_final['ad_clicks'].sum()
        ctr = (t_clicks / t_imps * 100) if t_imps > 0 else 0
        reach_efficiency = (t_traffic / t_imps * 1000) if t_imps > 0 else 0

        ks1.metric("Ad Impressions", f"{t_imps:,.0f}", help="Total times content was displayed.")
        ks2.metric("Ad Clicks", f"{t_clicks:,.0f}", help="Total clicks on digital content.")
        ks3.metric("Total Engagement", f"{(t_imps + t_clicks):,.0f}", help="Combined impressions and clicks.")
        ks4.metric("Click-Thru Rate", f"{ctr:.2f}%", help="Percentage of impressions resulting in clicks.")
        ks5.metric("Traffic per 1k Imps", f"{reach_efficiency:.1f}", help="Guests per 1,000 ad impressions.")

        st.divider()

        # --- 7. FORENSIC ATTRIBUTION FLOW ---
        st.write("### 🌊 Multi-Channel Attribution Flow")
        df_stack = df_final.copy()
        df_stack['Brand_Inertia_Layer'] = m.get('total_inertia', 0)
        fig_stack = go.Figure()
        layers = [
            ('Organic Heartbeat', 'baseline', 'rgba(200, 210, 225, 0.5)', '#8E9AAF'),
            ('Brand (OOH/Broadcast)', 'Brand_Inertia_Layer', 'rgba(93, 112, 127, 0.5)', '#5D707F'),
            ('Digital ROI Lift', 'residual_lift', 'rgba(0, 71, 171, 0.5)', '#0047AB'),
            ('Hard Rock LIVE Gravity', 'gravity_lift', 'rgba(255, 204, 0, 0.6)', '#FFCC00')
        ]
        for name, col, fill_color, line_color in layers:
            if col in df_stack.columns:
                fig_stack.add_trace(go.Scatter(x=df_stack['entry_date'], y=df_stack[col], name=name, mode='lines', 
                                              stackgroup='one', fillcolor=fill_color, line=dict(width=0.5, color=line_color)))
        fig_stack.update_layout(height=500, margin=dict(l=10, r=10, t=10, b=10), hovermode="x unified", template="plotly_white")
        st.plotly_chart(fig_stack, use_container_width=True)

        # --- 8. DETAILED FORENSIC LEDGER ---
        st.write("### 📋 Detailed Forensic Ledger")
        df_final['Variance'] = df_final['actual_traffic'] - df_final['expected'].round(0)
        st.dataframe(df_final[['entry_date', 'actual_traffic', 'expected', 'Variance', 'residual_lift', 'gravity_lift', 'new_members']].sort_values('entry_date', ascending=False), use_container_width=True, hide_index=True)

        with col_export:
            st.download_button("📥 Export Audit to CSV", data=df_final.to_csv(index=False).encode('utf-8'), file_name=f"HR_Audit_{s_date}_{e_date}.csv", use_container_width=True)
            
# =================================================================
# 13. PAGE 5: AI CALIBRATION & ENGINE WEIGHTS (v18.0 SaaS Multi-Tenant)
# =================================================================
elif page == "AI Calibration":
    st.markdown(f"""
        <div style="background-color:#F8F9FA;padding:20px;border-radius:12px;border-left:6px solid #FFCC00;margin-bottom:20px;">
            <h2 style="color:#343a40;margin:0;">⚙️ Engine Weight Calibration: {st.session_state.current_property_name}</h2>
            <p style="color:#666;margin:0;">Calibrate the unique 'DNA' for this specific property location.</p>
        </div>
    """, unsafe_allow_html=True)

    # --- LIVE LEDGER FINANCIAL CALCULATION ---
    # ledger_data is already filtered by property_id in the Section 6 hydration
    df_ledger = pd.DataFrame(ledger_data)
    if not df_ledger.empty and 'actual_coin_in' in df_ledger.columns:
        total_rev = pd.to_numeric(df_ledger['actual_coin_in']).sum()
        total_traf = pd.to_numeric(df_ledger['actual_traffic']).sum()
        live_avg_coin_in = (total_rev / total_traf) if total_traf > 0 else 112.50
    else:
        live_avg_coin_in = 112.50

    # Current Model Health Check
    m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    st.metric(f"Model Predictability ({st.session_state.current_property_name})", m_audit.get('predictability', '92.5%'))

    with st.form("master_calibration_form"):
        # SECTION 1: FINANCIAL DNA & BENCHMARKS
        st.subheader("💰 Financial DNA & Benchmarks")
        st.write(f"**Current Ledger Performance:** Average Coin-In is `${live_avg_coin_in:.2f}` per guest.")
        
        b1, b2 = st.columns(2)
        with b1:
            n_avg_coin = st.number_input(
                "Target Avg Coin-In ($)", 
                value=float(st.session_state.coeffs.get('Avg_Coin_In', live_avg_coin_in)),
                step=0.01
            )
        with b2:
            n_hold = st.number_input(
                "Property Hold %", 
                value=float(st.session_state.coeffs.get('Hold_Pct', 10.0)),
                step=0.1,
                format="%.1f"
            )

        st.divider()

        # SECTION 2: DIGITAL & SOCIAL DRIVERS
        st.subheader("🌐 Digital & Social Drivers")
        d1, d2, d3 = st.columns(3)
        with d1:
            n_clicks = st.number_input(
                "Click Weight (Traffic per Click)", 
                value=float(st.session_state.coeffs.get('Clicks', 0.05)),
                step=0.01,
                format="%.2f"
            )
        with d2:
            n_social = st.number_input(
                "Social Impression Weight", 
                value=float(st.session_state.coeffs.get('Social_Imp', 0.0002)),
                step=0.0001,
                format="%.4f"
            )
        with d3:
            n_decay = st.number_input(
                "Adstock Retention %", 
                value=int(st.session_state.coeffs.get('Ad_Decay', 85)),
                step=1
            )

        st.divider()

        # SECTION 3: MASS MEDIA & OOH
        st.subheader("📡 Mass Media & Brand Inertia")
        c1, c2, c3 = st.columns(3)
        with c1:
            n_broad = st.number_input("Broadcast (TV/Radio) Daily Lift", value=int(st.session_state.coeffs.get('Broadcast_Weight', 150)))
        with c2:
            n_ooh = st.number_input("Road Signage (OOH) Daily Lift", value=int(st.session_state.coeffs.get('OOH_Weight', 100)))
        with c3:
            n_print = st.number_input("Print (Mag/News) Daily Lift", value=int(st.session_state.coeffs.get('Print_Lift', 75)))

        st.divider()

        # SECTION 4: GRAVITY & PROMOTIONS
        st.subheader("🚀 Gravity & Event Impact")
        g1, g2 = st.columns(2)
        with g1:
            n_grav = st.number_input(
                "Event Gravity (Multiplier)", 
                value=float(st.session_state.coeffs.get('Event_Gravity', 0.25)),
                step=0.01,
                format="%.2f"
            )
        with g2:
            n_promo = st.number_input("Standard Promo Lift", value=int(st.session_state.coeffs.get('Promo', 550)))

        st.divider()

        # SECTION 5: ENVIRONMENTAL FRICTION
        st.subheader("🌦️ Environmental Friction")
        w1, w2 = st.columns(2)
        with w1:
            n_rain = st.number_input("Rain Impact (Loss per mm)", value=int(st.session_state.coeffs.get('Rain_mm', -12)))
        with w2:
            n_snow = st.number_input("Snow Impact (Loss per cm)", value=int(st.session_state.coeffs.get('Snow_cm', -45)))

        if st.form_submit_button("🚀 Recalibrate Property Engine", use_container_width=True):
            updated_coeffs = {
                "property_id": st.session_state.current_property_id, # <--- SAAS TAG
                "Avg_Coin_In": float(n_avg_coin),
                "Hold_Pct": float(n_hold),
                "Clicks": float(n_clicks),
                "Social_Imp": float(n_social),
                "Ad_Decay": int(n_decay),
                "Broadcast_Weight": float(n_broad),
                "OOH_Weight": float(n_ooh),
                "OOH_Count": 1 if n_ooh > 0 else 0,
                "Print_Lift": float(n_print),
                "Event_Gravity": float(n_grav),
                "Promo": float(n_promo),
                "Rain_mm": float(n_rain),
                "Snow_cm": float(n_snow),
                "Static_Weight": float(n_ooh),
                "Static_Count": 1 if n_ooh > 0 else 0
            }
            
            try:
                # Use upsert with property_id as the unique constraint to keep settings isolated
                supabase.table("coefficients").upsert(updated_coeffs, on_conflict="property_id").execute()
                st.session_state.coeffs.update(updated_coeffs)
                st.success(f"✅ Intelligence Weights hard-saved for {st.session_state.current_property_name}.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Sync Error: {e}")

    with st.expander("🔍 View Active Sensitivity Manifest"):
        st.json(st.session_state.coeffs)

# =================================================================
# 14. PAGE 6: AI STRATEGIC ANALYST (v18.0 - SaaS Multi-Tenant)
# =================================================================
elif page == "FloorCast AI Analyst":
    st.markdown(f"""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🕵️ FloorCast Strategic AI Analyst: {st.session_state.current_property_name}</h2>
            <p style="color: #444; margin: 0;">Unified Intelligence: Correlating Ledger, Sentiment, ROI Audits, & Events.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # --- INITIALIZE VARIABLES ---
    ledger = st.session_state.get('ledger', [])
    ledger_csv = "No ledger data available."
    sent_csv = "No sentiment data available."
    roi_csv = "No ROI records available."
    promo_csv = "No promotion data available."

    # --- DYNAMIC ASSET FETCH (For Selection Boxes) ---
    try:
        asset_res = supabase.table("property_assets").select("asset_name").eq("property_id", st.session_state.current_property_id).execute()
        tags = [item['asset_name'] for item in asset_res.data] if asset_res.data else ["Overall Property"]
    except:
        tags = ["Overall Property"]

    # --- 14.1 ENTRY MODULES ---
    col_input1, col_input2 = st.columns(2)

    with col_input1:
        with st.expander("📝 Manual Sentiment Entry", expanded=True):
            st.write("Log a specific review or high-value comment.")
            with st.form("manual_sentiment_form", clear_on_submit=True):
                manual_tag = st.selectbox("Assign to Asset (Tag):", tags, key="manual_tag_select")
                f_text = st.text_area("Review Text", placeholder="Paste Google review here...")
                
                if st.form_submit_button("🛡️ Archive & AI Score"):
                    if f_text:
                        # archive_sentiment_entry was updated in Step 3 to be SaaS aware
                        cat, icon, intens = archive_sentiment_entry(f_text, manual_tag, 0.0)
                        st.success(f"**Archived to {manual_tag}!**")
                        st.cache_data.clear()

    with col_input2:
        from docx import Document
        with st.expander("📄 Intelligent Word Doc Upload", expanded=False):
            st.write("Extracts reviews from text AND pasted tables.")
            uploaded_doc = st.file_uploader("Select .docx file", type="docx", key="word_sent_upload")
            bulk_tag = st.selectbox("Assign ALL to Asset (Tag):", tags, key="bulk_tag_select")
            
            if uploaded_doc and st.button("🚀 Parse & AI Score Bulk"):
                doc = Document(uploaded_doc)
                entries = []
                for table in doc.tables:
                    for row in table.rows:
                        row_text = " ".join([cell.text.strip() for cell in row.cells if cell.text.strip()])
                        if len(row_text) > 10:
                            entries.append({"user": "Table Entry", "text": row_text})
                
                current_user = "Unknown User"
                for para in doc.paragraphs:
                    text = para.text.strip()
                    if not text: continue
                    if len(text) < 45 and not text.endswith(('.', '!', '?')):
                        current_user = text
                    else:
                        entries.append({"user": current_user, "text": text})
                
                if entries:
                    for entry in entries:
                        archive_sentiment_entry(f"Source: {entry['user']} | {entry['text']}", bulk_tag, 0.0)
                    st.success(f"✅ Successfully archived {len(entries)} reviews!")
                    st.cache_data.clear()

    st.divider()

    # --- 14.2 SILENT DATA AGGREGATION (Filtered by property_id) ---
    with st.status(f"🔗 Synchronizing {st.session_state.current_property_name} Intelligence...", expanded=False) as status:
        # 1. LEDGER (Already filtered by Section 6 Hydration)
        if ledger:
            try:
                m_audit = get_forensic_metrics(ledger, st.session_state.coeffs)
                ledger_csv = m_audit['df'].to_csv(index=False)
            except: pass

        # 2. SENTIMENT (Filtered by Property)
        try:
            sent_res = supabase.table("sentiment_history").select("*")\
                .eq("property_id", st.session_state.current_property_id)\
                .order("timestamp", desc=True).limit(100).execute()
            if sent_res.data: sent_csv = pd.DataFrame(sent_res.data).to_csv(index=False)
        except: pass

        # 3. ROI & PROMOS (Filtered by Property)
        try:
            roi_res = supabase.table("monthly_roi").select("*")\
                .eq("property_id", st.session_state.current_property_id).execute()
            if roi_res.data: roi_csv = pd.DataFrame(roi_res.data).to_csv(index=False)
            
            promo_res = supabase.table("promotions").select("*")\
                .eq("property_id", st.session_state.current_property_id).execute()
            if promo_res.data: promo_csv = pd.DataFrame(promo_res.data).to_csv(index=False)
        except: pass

        status.update(label="✅ All Property Nodes Synchronized", state="complete")

    # --- 14.3 THE CHAT INTERFACE ---
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    prompt = st.chat_input("Ask about property performance or guest sentiment...")
    
    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash') 
            
            with st.chat_message("assistant"):
                with st.spinner("🕵️ AI Analyst is correlating data points..."):
                    # The Dossier now contains ONLY this property's information
                    dossier = f"PROPERTY: {st.session_state.current_property_name}\n\nLEDGER:\n{ledger_csv}\n\nSENTIMENT:\n{sent_csv}\n\nROI:\n{roi_csv}\n\nPROMOS:\n{promo_csv}"
                    full_query = f"Context: You are the lead consultant for {st.session_state.current_property_name}. Use this dossier to answer: {prompt}\n\nDossier:\n{dossier}"
                    res = model.generate_content(full_query)
                    st.markdown(res.text)
            
            st.session_state.messages.append({"role": "assistant", "content": res.text})
        except Exception as e:
            st.error(f"AI Error: {e}")

# =================================================================
# 15. PAGE 7: BL-ROAS COMMAND CENTER (FINAL v23 - Zero-Proof Edition)
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
    DEFAULT_AVG_SPEND = 1100.31

    # --- 1. MONTH SELECTION ---
    today = datetime.date.today()
    month_options = [(today - relativedelta(months=i)).replace(day=1) for i in range(12)]
    month_labels = [m.strftime("%B %Y") for m in month_options]

    selected_label = st.selectbox("Select Audit Month:", month_labels)
    selected_month = month_options[month_labels.index(selected_label)]

    # --- 2. DYNAMIC LEDGER AGGREGATION[cite: 1] ---
    df_roas = pd.DataFrame(ledger_data)
    if not df_roas.empty:
        df_roas['entry_date'] = pd.to_datetime(df_roas['entry_date'])
        
        m_mask = (df_roas['entry_date'].dt.month == selected_month.month) & \
                 (df_roas['entry_date'].dt.year == selected_month.year)
        selected_month_df = df_roas.loc[m_mask].copy()

        if not selected_month_df.empty:
            # Group by date and take the MAX value for each day to ensure full month coverage[cite: 1]
            monthly_summary = selected_month_df.groupby(selected_month_df['entry_date'].dt.date).max()
            ledger_traffic = int(monthly_summary['actual_traffic'].sum())
            ledger_signups = int(monthly_summary['new_members'].sum())
            ledger_coin_in = float(monthly_summary['actual_coin_in'].sum())
        else:
            ledger_traffic, ledger_signups, ledger_coin_in = 0, 0, 0.0
    else:
        ledger_traffic, ledger_signups, ledger_coin_in = 0, 0, 0.0

    # SAFETY: Prevent division by zero[cite: 1]
    avg_spend_actual = float(ledger_coin_in / ledger_traffic) if ledger_traffic > 0 else DEFAULT_AVG_SPEND

    # --- 3. THE INPUT FORM ---
    with st.form("roas_input_form"):
        st.subheader(f"📊 {selected_label} Metrics")
        
        # Check for existing data in Supabase[cite: 1]
        existing_res = supabase.table("monthly_roi").select("*").eq("report_month", str(selected_month)).execute()
        existing = existing_res.data[0] if existing_res.data else {}

        c1, c2, c3 = st.columns(3)
        with c1:
            utm_s = st.number_input("UTM Sessions", value=int(existing.get('utm_sessions', 0)))
            org_s = st.number_input("Organic Sessions", value=int(existing.get('organic_sessions', 0)))
            ad_spend = st.number_input("Total Ad Spend ($)", value=float(existing.get('ad_spend', 0.0)), step=100.0)
        
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

        submit = st.form_submit_button("🚀 Save & Calculate ROI")

    # --- 4. CALCULATION LOGIC (Streamlined - No Unused Columns) ---
    if submit:
        # Business logic for Brand Value calculation
        brand_value = (utm_s * 1.5) + (org_s * 0.5) + (likes * 0.1) + (shares * 0.5) + (geo_lift * 2.0)
        bl_roas = brand_value / ad_spend if ad_spend > 0 else 0
        
        # We still use ledger_signups for the local calculation, but we don't save it to the DB
        enhanced_rev = brand_value + ledger_coin_in + (ledger_signups * LTV_BENCHMARK)

        # Removed 'ledger_signups' from this dictionary to stop the Sync Failure
        roi_payload = {
            "report_month": str(selected_month),
            "utm_sessions": utm_s, 
            "organic_sessions": org_s, 
            "ad_spend": ad_spend,
            "social_likes": likes, 
            "social_comments": comments, 
            "social_shares": shares, 
            "post_views": views,
            "site_time_sessions": time_site, 
            "booking_clicks": cta_clicks, 
            "pos_reviews": reviews, 
            "geo_lift_traffic": geo_lift, 
            "brand_value": brand_value, 
            "calculated_bl_roas": bl_roas, 
            "enhanced_revenue": enhanced_rev
        }
        
        try:
            # This will now succeed because all keys match existing Supabase columns
            supabase.table("monthly_roi").upsert(roi_payload).execute()
            st.success(f"✅ ROI for {selected_label} saved successfully!")
            st.rerun() 
        except Exception as e:
            st.error(f"Sync Failure: {e}")

    # --- 5. REPORT GENERATOR[cite: 1] ---
    st.divider()
    history_res = supabase.table("monthly_roi").select("*").order("report_month", desc=True).execute()
    if history_res.data:
        df_hist = pd.DataFrame(history_res.data)
        curr_row = df_hist[df_hist['report_month'] == str(selected_month)]
        
        if not curr_row.empty:
            curr = curr_row.iloc[0]
            prop_potential = ledger_coin_in + (ledger_signups * LTV_BENCHMARK)
            
            report_text = f"""{selected_label} ROAS Results
Brand Health Performance

BL-ROAS = {curr['calculated_bl_roas']:.2f}x
For every $1 spent in advertising, we generated ${curr['brand_value']:,.2f} in measurable brand value.

🎯 Attributed Revenue Impact (Floor)
• 10% Attribution: ${(prop_potential * 0.1):,.0f}
• 20% Attribution: ${(prop_potential * 0.2):,.0f}
• 30% Attribution: ${(prop_potential * 0.3):,.0f}

Enhanced Total Impact = ${curr['enhanced_revenue']:,.0f}"""
            
            st.subheader("📄 SharePoint Ready Text")
            st.text_area("Copy/Paste this into the monthly report:", value=report_text, height=250)

            st.write("### 📜 Audit History")
            st.dataframe(df_hist[['report_month', 'calculated_bl_roas', 'brand_value', 'enhanced_revenue']], use_container_width=True, hide_index=True)

# =================================================================
# 15. PAGE: GLOBAL ADMIN CONSOLE (SaaS Provisioning Factory)
# =================================================================
elif page == "Global Admin Console":
    st.markdown("""
        <div style="background-color: #0047AB; padding: 20px; border-radius: 12px; margin-bottom: 25px;">
            <h2 style="color: white; margin: 0;">🌐 Global SaaS Provisioning</h2>
            <p style="color: #E1E8F0; margin: 0;">Onboard new properties and manage executive access credentials.</p>
        </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["🏢 Property Manager", "👤 User Provisioning", "🛠️ System Health"])

    # --- TAB 1: PROPERTY MANAGER ---
    with tab1:
        st.subheader("Register a New Property Tenant")
        with st.form("new_property_form"):
            new_p_name = st.text_input("Property Name", placeholder="e.g., Caesars Palace Las Vegas")
            new_p_region = st.selectbox("Region", ["North America", "EMEA", "APAC", "LATAM"])
            
            if st.form_submit_button("🏗️ Build Tenant Space", use_container_width=True):
                if new_p_name:
                    try:
                        # 1. Create the Property Entry
                        prop_data = {"property_name": new_p_name, "region": new_p_region}
                        res = supabase.table("properties").insert(prop_data).execute()
                        new_id = res.data[0]['id']
                        
                        # 2. SEED: Initialize default assets for the new property
                        # This ensures the dashboard gauges work immediately
                        default_assets = [{"property_id": new_id, "asset_name": a} for a in ["Overall Property", "Hotel", "Gaming Floor"]]
                        supabase.table("property_assets").insert(default_assets).execute()
                        
                        # 3. SEED: Initialize default AI weights (Coefficients)
                        # Prevents the "empty vault" error on first login
                        seed_coeffs = {"property_id": new_id, "Promo": 500, "Ad_Decay": 85, "Clicks": 0.05}
                        supabase.table("coefficients").insert(seed_coeffs).execute()

                        st.success(f"🎉 Tenant '{new_p_name}' successfully provisioned with ID: {new_id}")
                    except Exception as e:
                        st.error(f"Provisioning Failure: {e}")

    # --- TAB 2: USER PROVISIONING ---
    with tab2:
        st.subheader("Executive Account Creation")
        # Fetch current properties to map the user
        props = supabase.table("properties").select("id, property_name").execute()
        prop_options = {p['property_name']: p['id'] for p in props.data} if props.data else {}

        with st.form("new_user_provision_form"):
            u_email = st.text_input("Corporate Email Address")
            u_pass = st.text_input("Initial Temporary Password", type="password")
            u_prop = st.selectbox("Assign to Property", options=list(prop_options.keys()))
            u_role = st.selectbox("Authorization Level", ["Executive", "Manager", "Analyst", "Admin"])
            
            if st.form_submit_button("🔐 Create & Map Account", use_container_width=True):
                try:
                    # 1. Create the User in Supabase Auth (Handles the encryption/login)
                    auth_res = supabase.auth.sign_up({"email": u_email, "password": u_pass})
                    
                    if auth_res.user:
                        # 2. Map the User to the Property in our access table
                        mapping = {
                            "user_email": u_email,
                            "property_id": prop_options[u_prop],
                            "user_role": u_role
                        }
                        supabase.table("user_property_access").insert(mapping).execute()
                        st.success(f"✅ Credentials active. {u_email} now has access to {u_prop}.")
                except Exception as e:
                    st.error(f"Error provisioning user: {e}")

    # --- TAB 3: SYSTEM HEALTH ---
    with tab3:
        st.subheader("Global Tenant Statistics")
        try:
            p_count = len(supabase.table("properties").select("id").execute().data)
            u_count = len(supabase.table("user_property_access").select("id").execute().data)
            
            col_a, col_b = st.columns(2)
            col_a.metric("Total Properties", p_count)
            col_b.metric("Active Global Users", u_count)
        except:
            st.info("Statistics unavailable during sync.")

# =================================================================
# 16. FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
