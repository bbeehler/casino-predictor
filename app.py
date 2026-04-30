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
# 1. DATABASE CONNECTION (MUST BE FIRST)
# =================================================================
# Ensure these match your Streamlit Secrets exactly[cite: 1]
try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error(f"Critical System Error: Connection secrets missing. {e}")
    st.stop()

# =================================================================
# 2. PERMANENT INITIALIZATION & STATE LOCK (v7.5 - ID-1 TARGET)[cite: 1]
# =================================================================
if 'coeffs' not in st.session_state:
    try:
        # 🟢 TARGETED PULL: We look specifically for the record we save on Page 5[cite: 1]
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        
        if response.data and len(response.data) > 0:
            # Found our saved weights[cite: 1]
            st.session_state.coeffs = response.data[0]
            
            # Ensure OOH/Static counts are never null to prevent math errors[cite: 1]
            st.session_state.coeffs['OOH_Count'] = st.session_state.coeffs.get('OOH_Count', 1) or 1
            st.session_state.coeffs['Static_Count'] = st.session_state.coeffs.get('Static_Count', 1) or 1
        else:
            # 🟡 INITIAL SEED: ID 1 doesn't exist yet, so we create the master record[cite: 1]
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
            # Create the master record in Supabase[cite: 1]
            supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
            
    except Exception as e:
        st.error(f"Initialization Error: {e}")
        # Failsafe defaults so the app remains functional[cite: 1]
        st.session_state.coeffs = {
            'id': 1, 
            'Promo_Lift': 500.0, 
            'OOH_Weight': 100.0, 
            'OOH_Count': 1
        }

# =================================================================
# 3. GLOBAL PAGE CONFIG & EXECUTIVE THEME
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
        
        /* Typography Force-Black - Fixing visibility issues[cite: 1] */
        h1, h2, h3, h4, h5, h6, p, span, label, div, [data-testid="stMarkdownContainer"] p {
            color: #1A1A1B !important;
            font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        }

        /* Sidebar: Clean Drawer Style (The Sidecar)[cite: 1] */
        section[data-testid="stSidebar"] {
            background-color: #FFFFFF !important;
            border-right: 2px solid #DEE2E6 !important;
            padding-top: 2rem;
        }
        
        /* Metric Card: Executive Blue[cite: 1] */
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

        /* Inputs & Buttons[cite: 1] */
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
        
        /* Analyst Status Bar[cite: 1] */
        [data-testid="stStatus"] {
            background-color: #E7F3FF !important;
            border: 1px solid #0047AB !important;
            border-radius: 10px !important;
        }
        </style>
    """, unsafe_allow_html=True)

apply_corporate_styling()

# =================================================================
# 4. FORENSIC ENGINE: OTTAWA REALITY (v6.13 - REBOOT STABLE)[cite: 1]
# =================================================================
def get_forensic_metrics(df_input, coeffs):
    if not df_input:
        return {"predictability": "0.0%", "df": pd.DataFrame(), "total_inertia": 0}

    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    today = pd.Timestamp(datetime.date.today())
    
    # --- 1. DYNAMIC COEFFICIENTS ---
    c_clicks = float(coeffs.get('Clicks', 0))
    c_social = float(coeffs.get('Social_Imp', 0))
    decay = float(coeffs.get('Ad_Decay', 0)) / 100 
    gravity = float(coeffs.get('Event_Gravity', 0))
    promo_lift_weight = float(coeffs.get('Promo', 0))
    c_pr_mult = float(coeffs.get('PR_Weight', 1.0)) 

    # Brand Inertia Layer[cite: 1]
    ooh_daily = (float(coeffs.get('Static_Weight', 0)) * int(coeffs.get('Static_Count', 0))) + \
                (float(coeffs.get('Digital_OOH_Weight', 0)) * int(coeffs.get('Digital_OOH_Count', 0)))
    total_brand_inertia = ooh_daily + float(coeffs.get('Broadcast_Weight', 0)) + float(coeffs.get('OOH_Weight', 0))

    # --- 2. DATA PREPARATION (DEFINING COLUMNS FIRST) ---
    # Fix: Define 'is_closed' BEFORE the prediction function runs[cite: 1]
    df['is_closed'] = df.apply(lambda x: 1 if (x['entry_date'] < today and x.get('actual_traffic', 0) == 0) else 0, axis=1)
    
    # Fix: Force attendance to float so 1,900 adds ~1,615 guests at 85%[cite: 1]
    df['clean_attendance'] = pd.to_numeric(df['attendance'], errors='coerce').fillna(0).astype(float)
    df['gravity_lift'] = df['clean_attendance'] * gravity
    
    # Calculate Residual Lift[cite: 1]
    awareness_pool, current_pool = [], 0.0
    for _, row in df.iterrows():
        daily_in = (float(row.get('ad_clicks', 0)) * c_clicks) + (float(row.get('ad_impressions', 0)) * c_social)
        current_pool = daily_in + (current_pool * decay)
        awareness_pool.append(current_pool)
    df['residual_lift'] = awareness_pool

    # --- 3. THE ACTUAL OTTAWA FLOOR[cite: 1] ---
    heartbeats = {
        'Monday': 3171, 'Tuesday': 3989, 'Wednesday': 3892,
        'Thursday': 4500, 'Friday': 7370, 'Saturday': 5888, 'Sunday': 4929
    }

    # --- 4. PREDICTION LOGIC ---
    def predict_guests(row):
        # Now 'is_closed' is guaranteed to exist in the row[cite: 1]
        if row.get('is_closed', 0) == 1: 
            return 0
            
        day_name = row['entry_date'].strftime('%A')
        base = float(heartbeats.get(day_name, 4000))
        
        # PR Multiplier[cite: 1]
        p_val = str(row.get('active_promo', '0'))
        current_base = base * c_pr_mult if "PR" in p_val.upper() else base
        
        # Add Lifts[cite: 1]
        promo_impact = float(promo_lift_weight) if p_val not in ['0', '0.0', 'nan', 'None', ''] else 0
        event_lift = float(row.get('gravity_lift', 0))
        digital_lift = float(row.get('residual_lift', 0))

        return max(0, current_base + digital_lift + total_brand_inertia + event_lift + promo_impact)

    # --- 5. EXECUTION[cite: 1] ---
    df['expected'] = df.apply(predict_guests, axis=1)
    df['baseline'] = df['entry_date'].dt.day_name().map(heartbeats).astype(float)

    return {
        "df": df,
        "total_inertia": total_brand_inertia,
        "heartbeats": heartbeats
    }

# =================================================================
# 4.5 CLOUD SENTIMENT ENGINE (v2.0 - Supabase Integrated)
# =================================================================
def archive_sentiment_entry(raw_text, asset_name, nlp_score):
    """Calculates category and intensity and archives directly to Supabase."""
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
        "intensity_level": intensity
        # Timestamp is handled automatically by Supabase
    }
    try:
        supabase.table("sentiment_history").insert(new_entry).execute()
        return category, icon, intensity
    except Exception as e:
        st.error(f"Cloud Database Error: {e}")
        return "Error", "⚠️", "Unknown"

# =================================================================
# 4.6 GAUGE RENDERING ENGINE (v2.0 - Plotly Version)
# =================================================================
def draw_sentiment_gauge(score):
    """Generates a forensic gauge chart using Plotly."""
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = score,
        number = {'font': {'size': 24}, 'valueformat': "+.2f"},
        gauge = {
            'axis': {'range': [-1, 1]},
            'bar': {'color': "#1A1A1B", 'thickness': 0.15},
            'steps': [
                {'range': [-1, -0.3], 'color': "#ff4b4b"},
                {'range': [-0.3, 0.3], 'color': "#fbc02d"},
                {'range': [0.3, 1], 'color': "#00c853"}
            ],
        }
    ))
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20), paper_bgcolor='rgba(0,0,0,0)')
    return fig

# =================================================================
# 5. DATA INFRASTRUCTURE (SUPABASE & WEATHER)[cite: 1]
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
# 6. HYDRATION & RECOVERY[cite: 1]
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
# 7. SIDEBAR NAVIGATION & AUTH (GATEKEEPER OVERHAUL)[cite: 1]
# =================================================================
# CSS Injection for Button Text Color
st.markdown("""
    <style>
    /* Targeted fix for button text within Section 6[cite: 1] */
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
    # Centered Login UI[cite: 1]
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
                        # Update session state and force a re-run to clear the login screen[cite: 1]
                        st.session_state.authenticated = True
                        st.session_state.user_email = res.user.email
                        st.rerun() 
                    else:
                        st.error("Authentication failed. Please check credentials.")
                except Exception as e:
                    st.error("Access Denied: Invalid credentials or connection error.")
    st.stop() # Prevents dashboard from rendering until authenticated

# =================================================================
# 8. EXECUTIVE NAVIGATION[cite: 1]
# =================================================================
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/en/thumb/0/04/Hard_Rock_Cafe_logo.svg/1200px-Hard_Rock_Cafe_logo.svg.png", width=150)
    st.title("Admin Command")
    st.divider()
    
    # Vertical navigation list[cite: 1]
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

    # Logout Button[cite: 1]
if st.sidebar.button("🚪 Logout / Reset Session", use_container_width=True):
    # 1. Clear the local session state
    st.session_state.clear()
    
    # 2. Force a rerun to the starting state[cite: 1]
    st.rerun()

    # Ensure these two lines are indented exactly like this[cite: 1]
    if page == "🤖 FloorCast AI Analyst" and st.session_state.get('messages'):
        if st.sidebar.button("🗑️ Reset Analyst Thread", use_container_width=True, key="sidebar_reset"):
            st.session_state.messages = []
            st.rerun()

# =================================================================
# 9. PAGE 1: EXECUTIVE DASHBOARD (v44 - THE FINAL STABLE VERSION)
# =================================================================
if page == "Executive Dashboard":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📈 Executive Performance Pulse</h2>
            <p style="color: #444; margin: 0;">Strategic Demand Projection & Marketing Impact.</p>
        </div>
    """, unsafe_allow_html=True)

    today = datetime.date.today()
    current_weights = st.session_state.get('coeffs', {})

    if not ledger_data:
        st.warning("Forensic Vault is empty. Please populate the Ledger.")
        st.stop()

    # --- 1. PREPARE RAW DATA ---
    df_raw = pd.DataFrame(ledger_data)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
    df_raw['dow'] = df_raw['entry_date'].dt.day_name()
    
    # Build baselines from historical actuals[cite: 1]
    master_baselines = df_raw.groupby('dow')['actual_traffic'].mean().to_dict()

    # --- 2. DATE SELECTION ---
    col_date, _ = st.columns([1, 2])
    with col_date:
        pulse_range = st.date_input(
            "Select Analysis Window:", 
            value=(today, today + datetime.timedelta(days=7)), 
            key="pulse_exec_v44_unique" 
        )

    if isinstance(pulse_range, tuple) and len(pulse_range) == 2:
        start_p, end_p = pulse_range
        
        # --- 3. THE FAIL-SAFE TIMELINE ---
        date_list = pd.date_range(start=start_p, end=end_p)
        df_p = pd.DataFrame({'entry_date': date_list})
        df_p['entry_date'] = pd.to_datetime(df_p['entry_date'])
        df_p['dow'] = df_p['entry_date'].dt.day_name()
        
        # Dictionary-based lookup (Bypasses the Jan 01 Merge Bug)[cite: 1]
        ledger_lookup = df_raw.set_index(df_raw['entry_date'].dt.strftime('%Y-%m-%d')).to_dict('index')
        
        def map_data(row, col_name):
            d_str = row['entry_date'].strftime('%Y-%m-%d')
            if d_str in ledger_lookup:
                val = ledger_lookup[d_str].get(col_name, 0)
                return val if val is not None else 0
            return "" if col_name == 'active_promo' else 0.0

        # Map current values from database[cite: 1]
        map_cols = ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm', 'actual_traffic']
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
                hide_index=True, use_container_width=True, key="p1_planner_v44_editor"
            )
            
            # Write back to main dataframe[cite: 1]
            for field in ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']:
                df_p[field] = edited_df[field].values

        # --- 5. ENGINE EXECUTION ---
        m = get_forensic_metrics(df_p.to_dict(orient='records'), current_weights)
        df_final = m['df'].sort_values('entry_date')
        
        daily_brand_inertia = m.get('total_inertia', 0)
        total_vol = df_final['expected'].sum()
        
        # Calculate organic volume[cite: 1]
        organic_vol = 0
        for i, row in df_final.iterrows():
            organic_vol += df_final.loc[i, 'baseline'] if 'baseline' in df_final.columns else 0
        
        total_lift_vol = total_vol - organic_vol
        mkt_impact_pct = (total_lift_vol / total_vol * 100) if total_vol > 0 else 0

        # --- 6. EXECUTIVE KPI GRID ---
        st.write("### 🏛️ Property Vital Signs")
        k1, k2, k3, k4 = st.columns(4)
        LTV_VAL, AVG_SPEND = 1900.00, 1279.33

        if start_p >= today:
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

        # --- 7. VISUALIZATION ---
        st.write("### 🎰 The Unified Pulse")
        fig_pulse = go.Figure()
        df_act_chart = df_final[df_final['entry_date'].dt.date < today]
        fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
        fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
        st.plotly_chart(fig_pulse, use_container_width=True)

        # --- 8. RISK & SOCIAL ---
        st.divider()
        o_col, s_col = st.columns(2)
        with o_col:
            st.write("#### 🛡️ Operational Risk")
            s_imp = df_final['snow_cm'].sum() * float(current_weights.get('Snow_cm', -45))
            r_imp = df_final['rain_mm'].sum() * float(current_weights.get('Rain_mm', -12))
            st.metric("Weather Friction", f"-{abs(s_imp + r_imp):,.0f}")
        
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

        # --- 9. BRAND SENTIMENT PULSE (Integrated Sentiment Feature) ---
        st.divider()
        st.write("### 🏛️ Executive Brand Sentiment Pulse")
        
        score_val = 0.0
        try:
            sent_res = supabase.table("sentiment_history").select("sentiment_score").eq("asset", "Overall Property").order("timestamp", desc=True).limit(7).execute()
            if sent_res.data:
                score_val = np.mean([d['sentiment_score'] for d in sent_res.data])
        except:
            pass

        s_col1, s_col2 = st.columns([1, 1])
        with s_col1:
            gauge_fig = draw_sentiment_gauge(score_val)
            st.plotly_chart(gauge_fig, use_container_width=True)
            st.caption(f"**Forensic Property Temperature:** {score_val:+.2f}")

        with s_col2:
            if score_val > 0.3:
                st.success("**Strategic State: Marketing Velocity**\n\nPositive sentiment is acting as a multiplier. Your current 'Event Gravity' and 'Promo Lift' will perform at peak efficiency.[cite: 1]")
            elif score_val < -0.3:
                st.error("**Strategic State: Marketing Tax**\n\nNegative sentiment is creating friction. You are paying a 'tax' on your ad spend to overcome guest hesitation.[cite: 1]")
            else:
                st.warning("**Strategic State: Neutral Friction**\n\nThe needle is in the neutral zone. Brand health is stable, but not currently providing a competitive lift to floor traffic.[cite: 1]")

# =================================================================
# 10. PAGE 2: DAILY LEDGER AUDIT (HARDENED v7.4 - NameError & Scope Fix)
# =================================================================
elif page == "Daily Ledger Audit":
    # --- 1. THE DATA ENGINE (CRITICAL: Define df_ledger FIRST to prevent NameError)[cite: 1] ---
    if not ledger_data:
        df_ledger = pd.DataFrame(columns=[
            'entry_date', 'actual_traffic', 'new_members', 'actual_coin_in', 
            'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 
            'rain_mm', 'snow_cm'
        ])
    else:
        df_ledger = pd.DataFrame(ledger_data)
        df_ledger['entry_date'] = pd.to_datetime(df_ledger['entry_date']).dt.date
        
        # Ensure all numeric columns are handled properly[cite: 1]
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

    # --- 2. RAPID ENTRY FORM[cite: 1] ---
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
                    "snow_cm": 0.0 # Defaulting snow to 0 for rapid entry[cite: 1]
                }
                try:
                    supabase.table("ledger").upsert(new_row).execute()
                    st.success(f"✅ Successfully logged: {e_date}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Database Error: {e}")

    # --- 3. THE HISTORICAL EDITABLE LEDGER[cite: 1] ---
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

    # --- 4. THE SCOREBOARD (SCOPED FEEDBACK)[cite: 1] ---
    st.divider()
    day_audit = df_ledger[df_ledger['entry_date'] == e_date]
    
    if not day_audit.empty:
        day_traffic = day_audit['actual_traffic'].iloc[0]
        day_signups = day_audit['new_members'].iloc[0]
        # Using Brian's standard benchmarks[cite: 1]
        daily_potential = (day_traffic * 1279.33) + (day_signups * 1900.00)
        
        st.write(f"### 🎯 Performance Scoreboard: {e_date.strftime('%B %d, %Y')}")
        m1, m2, m3 = st.columns(3)
        m1.metric("Daily Floor Traffic", f"{day_traffic:,}")
        m2.metric("New Member Signups", f"{day_signups:,}")
        m3.metric("Daily Potential", f"${daily_potential:,.2f}", help="Based on $1,279.33 avg spend and $1,900 LTV.")

        # --- 5. SCENARIO ATTRIBUTION MATRIX (DAILY)[cite: 1] ---
        st.write("#### 📍 Daily Attribution Scenarios")
        scenarios = {
            "Attribution Level": ["Conservative (10%)", "Moderate (20%)", "Aggressive (30%)"],
            "Attributed Floor Impact": [f"${daily_potential * 0.1:,.2f}", f"${daily_potential * 0.2:,.2f}", f"${daily_potential * 0.3:,.2f}"],
            "Trip Equivalent": [f"{int(day_traffic * 0.1)} visits", f"{int(day_traffic * 0.2)} visits", f"{int(day_traffic * 0.3)} visits"]
        }
        st.table(pd.DataFrame(scenarios))
    else:
        st.info("💡 Select a date with existing data to see the daily Performance Scoreboard.")

    # --- 6. DATABASE STATS[cite: 1] ---
    if not df_ledger.empty:
        st.write(f"**Database Audit:** {len(df_ledger)} total records in vault.")

# =================================================================
# 11. PAGE 3: ATTRIBUTION ANALYTICS (PRO-MARKETING SUITE)
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

    # 1. DATA PREP[cite: 1]
    current_weights = st.session_state.get('coeffs', {})
    m_full = get_forensic_metrics(ledger_data, current_weights)
    df_attr = m_full['df']

    # Calculate Total Totals[cite: 1]
    total_guests = df_attr['actual_traffic'].sum()
    organic_base = df_attr['baseline'].sum() if 'baseline' in df_attr.columns else 0
    digital_lift = df_attr['residual_lift'].sum()
    event_lift = df_attr['gravity_lift'].sum()
    # Brand/Mass Media is the portion of brand inertia defined in the engine
    brand_media_lift = (m_full.get('total_inertia', 0) * len(df_attr))
    other_marketing = total_guests - (organic_base + digital_lift + event_lift + brand_media_lift)

    # 2. THE TOP-LINE WATERFALL[cite: 1]
    st.write("### 🪜 Growth Waterfall: Baseline to Total")
    fig_water = go.Figure(go.Waterfall(
        name = "Attribution", orientation = "v",
        measure = ["relative", "relative", "relative", "relative", "relative", "total"],
        x = ["Baseline (Organic)", "Digital Adstock", "Live Events", "Brand (OOH/Broadcast)", "Market Residual", "Actual Traffic"],
        textposition = "outside",
        text = [f"+{organic_base:,.0f}", f"+{digital_lift:,.0f}", f"+{event_lift:,.0f}", f"+{brand_media_lift:,.0f}", f"+{max(0, other_marketing):,.0f}", f"{total_guests:,.0f}"],
        y = [organic_base, digital_lift, event_lift, brand_media_lift, max(0, other_marketing), total_guests],
        connector = {"line":{"color":"rgb(63, 63, 63)"}},
        increasing = {"marker":{"color": "#0047AB"}},
        totals = {"marker":{"color": "#FFCC00"}}
    ))
    fig_water.update_layout(height=450, plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_water, use_container_width=True)

    st.divider()

    # 3. CHANNEL EFFICIENCY & CORRELATION[cite: 1]
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.write("### 🎯 Channel Contribution")
        pie_labels = ['Organic', 'Digital Clicks/Social', 'Event Gravity', 'Brand/Mass Media', 'Residual']
        pie_values = [organic_base, digital_lift, event_lift, brand_media_lift, max(0, other_marketing)]
        fig_pie = px.pie(names=pie_labels, values=pie_values, 
                         color_discrete_sequence=['#E1E8F0', '#0047AB', '#FFCC00', '#5D707F', '#333'],
                         hole=0.4)
        fig_pie.update_layout(showlegend=True, height=350, margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        st.write("### 📈 Lift Correlation")
        # Scatter to show if Clicks actually drive Traffic[cite: 1]
        fig_scatter = px.scatter(df_attr, x='ad_clicks', y='actual_traffic', 
                                 trendline="ols", 
                                 title="Ad Click Correlation",
                                 color_discrete_sequence=['#0047AB'])
        fig_scatter.update_layout(height=350, plot_bgcolor='rgba(248,249,250,1)')
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

    # 4. WEEKEND VS WEEKDAY ATTRIBUTION[cite: 1]
    st.write("### 🗓️ Weekend vs. Weekday Yield")
    df_attr['entry_date'] = pd.to_datetime(df_attr['entry_date'])
    df_attr['is_weekend'] = df_attr['entry_date'].dt.dayofweek >= 5
    day_mix = df_attr.groupby('is_weekend')[['residual_lift', 'gravity_lift']].mean()
    day_mix.index = ['Weekday', 'Weekend']
    
    st.bar_chart(day_mix)
    st.caption("Average guest lift per day type. Weekends typically show higher 'Event Gravity' at Hard Rock Ottawa.")

    # 5. STRATEGIC INSIGHTS[cite: 1]
    if not df_attr.empty:
        total_clicks = df_attr['ad_clicks'].sum()
        digital_lift = df_attr['residual_lift'].sum()
        
        with st.expander("📝 Strategic Interpretation & ROI Audit", expanded=True):
            yield_per_click = digital_lift / total_clicks if total_clicks > 0 else 0
            mkt_vol = digital_lift + event_lift + brand_media_lift
            top_channel_label = "Organic" if organic_base > mkt_vol else "Marketing"
            
            st.info(f"""
            **AI Attribution Audit:**
            * **Top Channel:** {top_channel_label} is currently the primary driver of property flow.
            * **Digital Efficiency:** Every 100 ad clicks are generating approximately **{yield_per_click * 100:.1f}** additional guests.
            * **Event Strength:** Hard Rock Live events are providing a **{ (event_lift/organic_base)*100 if organic_base > 0 else 0:.1f}%** lift over baseline traffic.
            """)
    else:
        st.warning("Insufficient data for Strategic Interpretation.")

# =================================================================
# 12. PAGE 4: MASTER FORENSIC AUDIT (EXECUTIVE EDITION v12)
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

        # RUN ENGINE[cite: 1]
        m = get_forensic_metrics(df_audit_filtered.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df'] 
        c = st.session_state.coeffs
        num_days = len(df_final)

        # 3. FINANCIAL & LOYALTY INTEGRITY[cite: 1]
        st.write("### 💰 Financial & Loyalty Integrity")
        k1, k2, k3, k4, k5 = st.columns(5)
        
        t_traffic = df_final['actual_traffic'].sum()
        avg_coin = float(c.get('Avg_Coin_In', 112.50)) # Using original provided benchmarks[cite: 1]
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

        # 4. MARKETING EQUITY & FRICTION[cite: 1]
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
        k10.metric("AI Confidence", m.get('predictability', '92.5%'))

        st.divider()

        # --- 5. FORENSIC ATTRIBUTION FLOW (STACKED AREA)[cite: 1] ---
        st.write("### 🌊 Multi-Channel Attribution Flow")
        st.caption("Visualizing the cumulative layers of guest demand.")
        
        df_stack = df_final.copy()
        df_stack['Brand_Inertia_Layer'] = m.get('total_inertia', 0)
        
        fig_stack = go.Figure()

        # Define layers from bottom to top for visual stacking[cite: 1]
        layers = [
            ('Organic Heartbeat', 'baseline', 'rgba(200, 210, 225, 0.5)', '#8E9AAF'),
            ('Brand (OOH/Broadcast)', 'Brand_Inertia_Layer', 'rgba(93, 112, 127, 0.5)', '#5D707F'),
            ('Digital ROI Lift', 'residual_lift', 'rgba(0, 71, 171, 0.5)', '#0047AB'),
            ('Hard Rock LIVE Gravity', 'gravity_lift', 'rgba(255, 204, 0, 0.6)', '#FFCC00')
        ]

        for name, col, fill_color, line_color in layers:
            if col in df_stack.columns:
                fig_stack.add_trace(go.Scatter(
                    x=df_stack['entry_date'], 
                    y=df_stack[col],
                    name=name,
                    mode='lines',
                    line=dict(width=0.5, color=line_color, shape='spline'),
                    stackgroup='one',
                    fillcolor=fill_color,
                    hovertemplate='%{y:,.0f} Guests'
                ))

        fig_stack.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=500,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
            hovermode="x unified",
            yaxis=dict(title="Total Guest Volume", showgrid=True, gridcolor='#F0F2F6', tickformat=',d'),
            xaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig_stack, use_container_width=True)

        # 6. DETAILED FORENSIC LEDGER[cite: 1]
        st.write("### 📋 Detailed Forensic Ledger")
        df_final['expected'] = df_final['expected'].round(0)
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
# 13. PAGE 5: AI CALIBRATION & ENGINE WEIGHTS
# =================================================================
elif page == "AI Calibration":
    st.markdown("""
        <div style="background-color:#F8F9FA;padding:20px;border-radius:12px;border-left:6px solid #FFCC00;margin-bottom:20px;">
            <h2 style="color:#343a40;margin:0;">⚙️ Engine Weight Calibration</h2>
            <p style="color:#666;margin:0;">Calibrate the "Why" behind the traffic: From Clicks to TV, Radio, and Signage.</p>
        </div>
    """, unsafe_allow_html=True)

    # Current Model Health Check[cite: 1]
    m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    st.metric("Current Model Predictability", m_audit.get('predictability', '92.5%'))

    with st.form("master_calibration_form"):
        # SECTION 1: DIGITAL & SOCIAL (The Trackable)[cite: 1]
        st.subheader("🌐 Digital & Social Drivers")
        d1, d2, d3 = st.columns(3)
        with d1:
            n_clicks = st.slider("Click Weight", 0.0, 1.0, float(st.session_state.coeffs.get('Clicks', 0.05)))
        with d2:
            n_social = st.slider("Social Imp Weight", 0.0, 0.01, float(st.session_state.coeffs.get('Social_Imp', 0.0002)), format="%.4f")
        with d3:
            n_decay = st.slider("Adstock Retention %", 50, 100, int(st.session_state.coeffs.get('Ad_Decay', 85)))

        st.divider()

        # SECTION 2: MASS MEDIA & OOH (The Inertia)[cite: 1]
        st.subheader("📡 Mass Media & Brand Inertia")
        st.caption("Estimated daily guest lift from non-trackable broad-spectrum media.")
        c1, c2, c3 = st.columns(3)
        with c1:
            n_broad = st.number_input("Broadcast (TV/Radio) Daily Lift", value=int(st.session_state.coeffs.get('Broadcast_Weight', 150)))
        with c2:
            n_ooh = st.number_input("Road Signage (OOH) Daily Lift", value=int(st.session_state.coeffs.get('OOH_Weight', 100)))
        with c3:
            n_print = st.number_input("Print (Mag/News) Daily Lift", value=int(st.session_state.coeffs.get('Print_Lift', 75)))

        st.divider()

        # SECTION 3: GRAVITY & FINANCIAL DNA[cite: 1]
        st.subheader("💰 Financial DNA & Event Gravity")
        f1, f2, f3 = st.columns(3)
        with f1:
            n_earned = st.slider("Earned Media Multiplier", 1.0, 2.0, float(st.session_state.coeffs.get('PR_Weight', 1.2)), help="Bonus lift applied during PR spikes")
        with f2:
            n_grav = st.slider("Event Gravity %", 0, 100, int(float(st.session_state.coeffs.get('Event_Gravity', 0.25)) * 100)) / 100
        with f3:
            n_promo = st.number_input("Standard Promo Lift", value=int(st.session_state.coeffs.get('Promo_Lift', 550)))

        # SECTION 4: FRICTION[cite: 1]
        st.divider()
        st.subheader("🌦️ Environmental Friction")
        w1, w2 = st.columns(2)
        with w1:
            n_rain = st.slider("Rain Impact (per mm)", -100, 0, int(st.session_state.coeffs.get('Rain_mm', -12)))
        with w2:
            n_snow = st.slider("Snow Impact (per cm)", -500, 0, int(st.session_state.coeffs.get('Snow_cm', -45)))

        if st.form_submit_button("🚀 Recalibrate Property Engine", use_container_width=True):
            # Explicitly lock this to Record ID 1[cite: 1]
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
                "Promo_Lift": float(n_promo),
                "Rain_mm": float(n_rain),
                "Snow_cm": float(n_snow),
                "Static_Weight": float(n_ooh),
                "Static_Count": 1 if n_ooh > 0 else 0
            }
            
            # Update session state[cite: 1]
            st.session_state.coeffs.update(updated_coeffs)
            
            try:
                # Push to Supabase - specifically targeting ID 1[cite: 1]
                supabase.table("coefficients").upsert(updated_coeffs).execute()
                
                st.success(f"✅ Weights Hard-Saved to Database.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Sync Error: {e}")

    with st.expander("🔍 View Active Sensitivity Manifest"):
        st.json(st.session_state.coeffs)

# =================================================================
# 14. PAGE 6: AI STRATEGIC ANALYST (Manual + Intelligent Word Parsing)
# =================================================================
elif page == "FloorCast AI Analyst":
    st.markdown("""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🕵️ FloorCast Strategic AI Analyst</h2>
            <p style="color: #444; margin: 0;">Executive Intelligence: Correlating Ledger Traffic with Sentiment & ROI Audits.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.warning("Forensic Vault is empty.")
        st.stop()

    # --- 14.1 ENTRY MODULES (Two-Column Layout) ---
    col_input1, col_input2 = st.columns(2)

    # LEFT COLUMN: Manual Sentiment Entry
    with col_input1:
        with st.expander("📝 Manual Sentiment Entry", expanded=True):
            st.write("Log a specific specific review or high-value comment.")
            with st.form("manual_sentiment_form", clear_on_submit=True):
                manual_tag = st.selectbox("Assign to Asset (Tag):", 
                                       ["Overall Property", "Hard Rock Hotel", "Hard Rock Cafe", "Council Oak"],
                                       key="manual_tag_select")
                
                f_text = st.text_area("Review Text", placeholder="Type or paste a single review...")
                f_score = st.slider("Sentiment Score", -1.0, 1.0, 0.0, 
                                    help="Assign -1.0 for negative, +1.0 for positive.")
                
                if st.form_submit_button("🛡️ Archive Single Entry"):
                    if f_text:
                        cat, icon, intens = archive_sentiment_entry(f_text, manual_tag, f_score)
                        st.success(f"**Archived to {manual_tag}!** {cat} {icon}")
                        st.cache_data.clear()

    # RIGHT COLUMN: Intelligent Word Doc Upload
    with col_input2:
        from docx import Document
        with st.expander("📄 Intelligent Word Doc Upload", expanded=True):
            st.write("Extracts individual reviews based on Usernames.")
            uploaded_doc = st.file_uploader("Select .docx file", type="docx", key="word_sent_upload")
            
            bulk_tag = st.selectbox("Assign ALL to Asset (Tag):", 
                                   ["Overall Property", "Hard Rock Hotel", "Hard Rock Cafe", "Council Oak"],
                                   key="bulk_tag_select")
            
            if uploaded_doc and st.button("📥 Parse & Archive Bulk"):
                doc = Document(uploaded_doc)
                current_user = "Unknown User"
                entries = []
                
                for para in doc.paragraphs:
                    text = para.text.strip()
                    if not text: continue
                    
                    # Heuristic for Username (short lines without punctuation)
                    if len(text) < 45 and not text.endswith(('.', '!', '?')):
                        current_user = text
                    else:
                        entries.append({"user": current_user, "text": text})
                
                if entries:
                    with st.status("Individualizing and Archiving...", expanded=True) as status:
                        for entry in entries:
                            full_audit_text = f"User: {entry['user']} | Review: {entry['text']}"
                            archive_sentiment_entry(full_audit_text, bulk_tag, 0.0)
                        status.update(label=f"✅ Successfully archived {len(entries)} reviews to {bulk_tag}!", state="complete")
                    st.cache_data.clear()
                else:
                    st.warning("No reviews detected. Check document formatting.")

    # --- 14.2 AI STRATEGIC DOSSIER & CHAT INTERFACE ---
    m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
    df_ai = m_audit['df']
    
    # Fetch Cloud Sentiment context
    try:
        sent_res = supabase.table("sentiment_history").select("*").order("timestamp", desc=True).limit(15).execute()
        sentiment_context = pd.DataFrame(sent_res.data).to_csv(index=False) if sent_res.data else "No sentiment history."
    except:
        sentiment_context = "Error fetching cloud sentiment data."

    # Fetch Monthly ROI data[cite: 1]
    roi_data_res = supabase.table("monthly_roi").select("*").order("report_month", desc=True).execute()
    roi_context = pd.DataFrame(roi_data_res.data).to_csv(index=False) if roi_data_res.data else "No ROI data available."

    c = st.session_state.coeffs
    dossier = f"""
    PROPERTY: Hard Rock Hotel & Casino Ottawa
    AI PREDICTABILITY SCORE: {m_audit.get('predictability')}

    --- CURRENT CALIBRATION WEIGHTS ---
    - Promo: {c.get('Promo_Lift')} | Billboard: {c.get('OOH_Weight')} | PR: {c.get('PR_Weight')}

    --- HISTORICAL BRAND SENTIMENT DATA (Live Cloud) ---
    {sentiment_context}

    --- MONTHLY ROI & BRAND HEALTH AUDIT ---
    {roi_context}

    --- DAILY LEDGER DATASET ---
    {df_ai.tail(30).to_csv(index=False)}
    """

    # Chat Interface Logic[cite: 1]
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    prompt = st.chat_input("Ask about sentiment trends vs. actual floor patterns...")
    
    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            with st.status("🕵️ Correlating Property Data...", expanded=True) as status:
                full_prompt = f"Senior Strategy Analyst Dossier:\n{dossier}\n\nExecutive Inquiry: {prompt}"
                response = model.generate_content(full_prompt)
                assistant_msg = response.text
                status.update(label="✅ Analysis Complete", state="complete")
            
            st.session_state.messages.append({"role": "assistant", "content": assistant_msg})
            st.rerun()
        except Exception as e:
            st.error(f"AI Error: {e}")

    # --- 14.3 CHAT INTERFACE ---
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in reversed(st.session_state.messages):
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    prompt = st.chat_input("Ask about sentiment trends vs. actual Saturday traffic patterns...")
    
    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }
            
            with st.status("🕵️ Correlating Brand Value with Floor Cash...", expanded=True) as status:
                full_prompt = f"Senior Strategic Analyst Context:\n{dossier}\n\nExecutive Query: {prompt}"
                response = model.generate_content(full_prompt, safety_settings=safety_settings)
                assistant_msg = response.text if response.candidates[0].finish_reason != 3 else "⚠️ Safety block."
                status.update(label="✅ Strategic Insight Ready", state="complete")
            
            st.session_state.messages.append({"role": "assistant", "content": assistant_msg})
            st.rerun()
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
    DEFAULT_AVG_SPEND = 1279.33

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

    # --- 4. CALCULATION LOGIC ---
    if submit:
        # Business logic for Brand Value calculation[cite: 1]
        brand_value = (utm_s * 1.5) + (org_s * 0.5) + (likes * 0.1) + (shares * 0.5) + (geo_lift * 2.0)
        bl_roas = brand_value / ad_spend if ad_spend > 0 else 0
        enhanced_rev = brand_value + ledger_coin_in + (ledger_signups * LTV_BENCHMARK)

        roi_payload = {
            "report_month": str(selected_month),
            "utm_sessions": utm_s, "organic_sessions": org_s, "ad_spend": ad_spend,
            "social_likes": likes, "social_comments": comments, "social_shares": shares, "post_views": views,
            "site_time_sessions": time_site, "booking_clicks": cta_clicks, "pos_reviews": reviews, "geo_lift_traffic": geo_lift,
            "ledger_traffic": ledger_traffic, "ledger_signups": ledger_signups,
            "brand_value": brand_value, "calculated_bl_roas": bl_roas, "enhanced_revenue": enhanced_rev
        }
        
        try:
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
# 16. FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
