# =================================================================
# BLOCK 1: IMPORTS & DATABASE CONNECTION
# =================================================================
import re
import traceback
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
from supabase import create_client, Client
from io import BytesIO
from dateutil.relativedelta import relativedelta
import os
import uuid
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# Global Constants
today = datetime.date.today()

try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error(f"Critical System Error: Connection secrets missing. {e}")
    st.stop()

# =================================================================
# BLOCK 2: UTILITIES (Permissions, Sentiment, Prediction)
# =================================================================

def check_permission(capability):
    """Checks if the user's current role allows for a specific action."""
    perms = st.session_state.get('user_permissions', {})
    return perms.get(capability, False)

def archive_sentiment_entry(text, asset_tag):
    try:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-2.5-flash') 
        score_prompt = f"Analyze sentiment. Return ONLY a single float between -1.0 and 1.0: {text}"
        ai_res = model.generate_content(score_prompt)
        try:
            sentiment_score = float(ai_res.text.strip())
        except:
            sentiment_score = 0.0

        if sentiment_score > 0.3: sentiment_category = "Positive"
        elif sentiment_score < -0.3: sentiment_category = "Negative"
        else: sentiment_category = "Neutral"

        abs_score = abs(sentiment_score)
        intensity_level = "Extreme" if abs_score >= 0.8 else "Moderate" if abs_score >= 0.4 else "Low"

        payload = {
            "message_id": str(uuid.uuid4()),
            "property_id": st.session_state.current_property_id,
            "asset": asset_tag,
            "sentiment_score": sentiment_score,
            "sentiment_category": sentiment_category,
            "intensity_level": intensity_level,
            "raw_text": text,
            "timestamp": datetime.datetime.now().isoformat()
        }
        supabase.table("sentiment_history").insert(payload).execute()
        return True
    except Exception as e:
        st.error(f"Archival Sync Error: {e}")
        return False

def generate_ai_prediction(target_date, property_name):
    """Forensic DNA Forecasting logic for Ledger Entries."""
    try:
        coeffs = st.session_state.get('coeffs', {})
        base_load = 5000  
        rain_impact = coeffs.get('Rain_mm', -12.0)
        snow_impact = coeffs.get('Snow_cm', -45.0)
        prediction = base_load + (rain_impact * 2) + (snow_impact * 5)
        return int(prediction)
    except:
        return 4500

# =================================================================
# BLOCK 3: GLOBAL AI ENGINES (v86.0 - PR & Total Recall)
# =================================================================

def get_forensic_omniscience():
    try:
        pid = st.session_state.get('current_property_id')
        full_ledger = st.session_state.get('ledger_data', [])
        df_full = pd.DataFrame(full_ledger)
        
        if not df_full.empty:
            grand_traffic = df_full['actual_traffic'].sum()
            grand_revenue = df_full['actual_coin_in'].sum()
            vault_summary = f"--- ABSOLUTE VAULT TOTALS ---\n- TOTAL ACTUAL TRAFFIC: {grand_traffic:,.0f}\n- TOTAL GAMING REVENUE: ${grand_revenue:,.2f}"
        else:
            vault_summary = "--- VAULT STATUS: EMPTY ---"

        l_res = supabase.table("ledger").select("*").eq("property_id", pid).order("entry_date", desc=True).limit(120).execute()
        p_res = supabase.table("pr_scorecard").select("*").eq("property_id", pid).order("report_month", desc=True).execute()
        
        context = f"""
        YOU ARE THE OMNISCIENT ANALYST FOR HARD ROCK OTTAWA.
        {vault_summary}
        --- DATA: PR ---
        {pd.DataFrame(p_res.data).to_string(index=False) if p_res.data else "Empty"}
        --- DATA: LEDGER ---
        {pd.DataFrame(l_res.data).to_string(index=False) if l_res.data else "Empty"}
        """
        return context
    except Exception as e:
        return f"Database Connectivity Error: {e}"

def ask_omniscient_ai(user_query):
    try:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-2.5-flash')
        vault_context = get_forensic_omniscience()
        prompt = f"{vault_context}\n\nEXECUTIVE INQUIRY: {user_query}\n\nFORENSIC ANALYSIS:"
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Analyst unavailable: {e}"

# =================================================================
# BLOCK 4: PREMIUM STYLING & PAGE CONFIG
# =================================================================
st.set_page_config(
    page_title="FloorCast Pro", 
    layout="wide", 
    page_icon="🎰", 
    initial_sidebar_state="expanded"
)

def apply_high_end_styling():
    st.markdown("""
        <style>
        /* IMPORT HIGH-END FONTS */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');

        /* GLOBAL RESET & TYPOGRAPHY */
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
            color: #1A1C1E;
        }

        /* --- THE BLACK SIDEBAR REFINEMENT --- */
        [data-testid="stSidebar"] {
            background-color: #000000 !important;
            border-right: 1px solid #222222;
        }

        /* Global Sidebar Text Default (White) */
        [data-testid="stSidebar"] *, 
        [data-testid="stSidebar"] .stMarkdown p,
        [data-testid="stSidebar"] .st-at {
            color: #FFFFFF !important;
        }

        /* 1. SELECTBOX FIX - Forcing Black Text inside the White Selection Box */
        [data-testid="stSidebar"] div[data-baseweb="select"] span,
        [data-testid="stSidebar"] div[data-baseweb="select"] div {
            color: #000000 !important;
            -webkit-text-fill-color: #000000 !important;
        }

        /* Ensure the input background stays white for contrast */
        [data-testid="stSidebar"] div[data-baseweb="input"] > div, 
        [data-testid="stSidebar"] div[data-baseweb="select"] > div {
            background-color: #FFFFFF !important;
        }

        /* 2. Force the dropdown arrow and clear button to be DARK */
        [data-testid="stSidebar"] div[data-baseweb="select"] svg {
            fill: #000000 !important;
        }

        /* 3. WIDGET LABEL (Switch Environment / Select Property) - HARD ROCK GOLD */
        [data-testid="stSidebar"] label[data-testid="stWidgetLabel"] p {
            color: #FFCC00 !important; 
            font-weight: 700 !important;
            font-size: 0.9rem !important;
        }

        /* 4. Sidebar Captions (PROPERTIES / NAVIGATION) */
        [data-testid="stSidebar"] .stCaption {
            color: #A1A1A1 !important;
            font-weight: 600;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }

        /* Sidebar Divider & Buttons */
        [data-testid="stSidebar"] hr { border-color: #333333 !important; }
        [data-testid="stSidebar"] .stButton>button {
            background-color: #1A1A1A !important;
            color: #FFFFFF !important;
            border: 1px solid #333333 !important;
        }

        /* RESPONSIVE PADDING */
        .main .block-container {
            padding-top: 2rem;
            padding-bottom: 5rem;
            padding-left: 4rem;
            padding-right: 4rem;
            max-width: 1400px;
        }

        /* HIGH-END EXECUTIVE HEADER */
        .glass-header {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            border: 1px solid rgba(255, 255, 255, 0.1);
            padding: 28px;
            border-radius: 18px;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.2);
            margin-bottom: 35px;
            color: white !important;
        }

        /* --- METRIC CARDS STYLE SUITE --- */
        [data-testid="stMetric"] {
            background: #FFFFFF !important;
            border: 1px solid #E1E8F0 !important;
            padding: 20px !important;
            border-radius: 12px !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05) !important;
        }

        /* Metric Label (Title) */
        [data-testid="stMetricLabel"] p {
            font-size: 0.9rem !important;
            font-weight: 400 !important;
            color: #1A1C1E !important;
        }

        /* Metric Value (Big Number) */
        [data-testid="stMetricValue"] div {
            font-size: 1.0rem !important;
            font-weight: 600 !important;
            color: #1A1C1E !important;
        }

        /* Metric Delta (+/-) */
        [data-testid="stMetricDelta"] div {
            font-size: 0.8rem !important;
        }

        /* --- MAIN ACTION BUTTONS --- */
        .stButton>button {
            border-radius: 8px !important;
            font-weight: 600 !important;
            background-color: #0047AB !important;
            color: white !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
            transition: all 0.2s;
        }

        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header[data-testid="stHeader"] { background-color: transparent !important; }
        </style>
    """, unsafe_allow_html=True)

def render_styled_header(title, subtitle, badge_text="Live"):
    st.markdown(f"""
        <div class="glass-header">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h1 style="margin: 0; font-size: 2rem; font-weight: 800;">{title}</h1>
                    <p style="margin: 8px 0 0 0; color: #94a3b8; font-size: 1.1rem;">{subtitle}</p>
                </div>
                <div style="background: rgba(255, 204, 0, 0.15); color: #FFCC00; padding: 6px 16px; border-radius: 12px; font-weight: 700;">● {badge_text}</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

apply_high_end_styling()

# =================================================================
# BLOCK 5: IDENTITY & COEFFICIENT INITIALIZATION
# =================================================================
if 'current_property_name' not in st.session_state:
    st.session_state.current_property_name = "Hard Rock Ottawa"

if 'current_property_id' not in st.session_state or st.session_state.current_property_id is None:
    try:
        if st.session_state.current_property_name == "All Properties":
            st.session_state.current_property_id = "GLOBAL"
        else:
            p_res = supabase.table("properties").select("id").eq("property_name", st.session_state.current_property_name).execute()
            st.session_state.current_property_id = p_res.data[0]['id'] if p_res.data else None
    except:
        st.session_state.current_property_id = None

if 'coeffs' not in st.session_state:
    cur_id = st.session_state.get('current_property_id')
    if cur_id and cur_id != "GLOBAL":
        try:
            c_res = supabase.table("coefficients").select("*").eq("property_id", cur_id).execute()
            st.session_state.coeffs = c_res.data[0] if c_res.data else {'Promo': 500.0}
        except: st.session_state.coeffs = {'Promo': 500.0}
    else:
        st.session_state.coeffs = {'Promo': 500.0}

# =================================================================
# BLOCK 6: AUTHENTICATION GATE
# =================================================================
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    _, col_login, _ = st.columns([1, 1.5, 1])
    with col_login:
        st.image("https://casino.hardrock.com/ottawa/-/media/project/shrss/hri/casinos/hard-rock/ottawa/logos-and-icons/logo.png", width=200)
        with st.form("login_form", border=True):
            e_mail = st.text_input("Email").strip().lower()
            p_word = st.text_input("Password", type="password")
            if st.form_submit_button("Login", use_container_width=True):
                try:
                    auth_res = supabase.auth.sign_in_with_password({"email": e_mail, "password": p_word})
                    if auth_res.user:
                        acc = supabase.table("user_property_access").select("*, properties(property_name)").eq("user_email", e_mail).execute()
                        if acc.data:
                            u = acc.data[0]
                            st.session_state.authenticated, st.session_state.user_email = True, e_mail
                            st.session_state.user_role, st.session_state.current_property_id = u['user_role'], u['property_id']
                            st.session_state.current_property_name = u['properties']['property_name']
                            perm = supabase.table("role_permissions").select("perms").eq("role_name", u['user_role']).execute()
                            st.session_state.user_permissions = perm.data[0]['perms'] if perm.data else {"view_analytics": True}
                            st.rerun()
                except Exception as e: st.error(f"Login Error: {e}")
    st.stop()

# =================================================================
# BLOCK 7: NAVIGATION & AI HUB (Instructional Switcher & Full Menu)
# =================================================================
if 'show_ai_hub' not in st.session_state: 
    st.session_state.show_ai_hub = False

def reset_hub_on_nav():
    st.session_state.show_ai_hub = False
    if "last_ai_response" in st.session_state: 
        del st.session_state.last_ai_response

# Role Scan for Switcher Visibility
user_links = supabase.table("user_property_access").select("user_role").eq("user_email", st.session_state.user_email).execute()
is_global = any(r['user_role'] in ["Super Admin", "Manager", "Admin"] for r in user_links.data)

with st.sidebar:
    st.markdown('<div style="padding-bottom:30px;"><img src="https://casino.hardrock.com/ottawa/-/media/project/shrss/hri/casinos/hard-rock/ottawa/logos-and-icons/logo.png" width="160"></div>', unsafe_allow_html=True)
    
    # 1. SCOPE SWITCHER WITH INSTRUCTIONAL LABEL
    if is_global:
        st.caption("SYSTEM SCOPE")
        try:
            all_props = supabase.table("properties").select("id, property_name").execute()
            p_map = {p['property_name']: p['id'] for p in all_props.data}
            p_options = ["📊 CONSOLIDATED VIEW"] + list(p_map.keys())
            
            curr_v = "📊 CONSOLIDATED VIEW" if st.session_state.current_property_id == "GLOBAL" else st.session_state.current_property_name
            
            # Replaced "label_visibility=collapsed" with visible instruction
            sel_view = st.selectbox(
                "Select Property or View:", # This is your indicator
                p_options, 
                index=p_options.index(curr_v) if curr_v in p_options else 0, 
                on_change=reset_hub_on_nav, 
                key="sidebar_switcher_v88"
            )
            
            if sel_view == "📊 CONSOLIDATED VIEW" and st.session_state.current_property_id != "GLOBAL":
                st.session_state.current_property_id, st.session_state.current_property_name = "GLOBAL", "All Properties"
                st.rerun()
            elif sel_view != "📊 CONSOLIDATED VIEW" and st.session_state.current_property_id != p_map.get(sel_view):
                st.session_state.current_property_id, st.session_state.current_property_name = p_map[sel_view], sel_view
                st.rerun()
        except Exception as e:
            st.error("Switcher Registry Error")

    st.markdown("<br>", unsafe_allow_html=True)
    st.caption("STRATEGIC NAVIGATION")
    nav = ["Executive Dashboard"]
    
    # 2. FULL FEATURE MENU (Including Scenario, A/B, and Alerts)
    if st.session_state.current_property_id != "GLOBAL":
        if check_permission("view_ledger"): nav.append("Daily Ledger Audit")
        if check_permission("view_pr_scorecard"): nav.append("PR Scorecard")
        if check_permission("view_analytics"): nav.extend(["Attribution Analytics", "Sentiment Scoring"])
        if check_permission("view_reports"): nav.append("Master Audit Report")
        
        # RESTORED MISSING MODULES
        if check_permission("run_simulations"): nav.append("Scenario Simulator")
        if check_permission("run_experiments"): nav.append("Experiment Vault") # This is your A/B page
        if check_permission("manage_alerts"): nav.append("Strategic Alerts")
        
        if check_permission("calibrate_ai"): nav.extend(["AI Calibration", "BL-ROAS Calculator"])
        if st.session_state.user_role == "Super Admin": nav.append("Global Admin Console")

    page = st.radio("Navigation", nav, label_visibility="collapsed", on_change=reset_hub_on_nav, key="main_nav_radio_v88")

    # 3. AI HUB & LOGOUT
    st.divider()
    if st.button("🕵️ Open Strategic AI Hub", use_container_width=True, type="primary", key="btn_ai_trigger"):
        st.session_state.show_ai_hub = True
        st.rerun()
    
    if st.button("LOGOUT", use_container_width=True, key="btn_logout"):
        st.session_state.clear()
        st.rerun()

# 4. GLOBAL MODAL HANDLER
if st.session_state.show_ai_hub:
    @st.dialog("Strategic AI Analyst Hub", width="large")
    def ai_hub_modal():
        st.markdown("### 🤖 FloorCast AI Analyst")
        st.caption(f"Context: {st.session_state.current_property_name}")
        q = st.text_input("Query forensic intelligence:", key="ai_q_input")
        if st.button("Execute Analysis", use_container_width=True, key="ai_proc_btn"):
            if q: 
                with st.spinner("Analyzing data nodes..."):
                    st.session_state.last_ai_response = ask_omniscient_ai(q)
        if "last_ai_response" in st.session_state:
            st.markdown("---")
            st.markdown(st.session_state.last_ai_response)
    ai_hub_modal()

# =================================================================
# BLOCK 8: DATA HYDRATION & VAULT GUARDRAIL
# =================================================================

def get_forensic_metrics(df_input, coeffs):
    if not df_input: return {"df": pd.DataFrame()}
    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    hb = {d: float(coeffs.get(f'{d[:3]}_Base', 5000)) for d in ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']}
    df['baseline'] = df['entry_date'].dt.day_name().map(hb).astype(float)
    dec, c1, c2 = float(coeffs.get('Ad_Decay', 85))/100, float(coeffs.get('Clicks', 0.05)), float(coeffs.get('Social_Imp', 0.0002))
    pool, lift = 0.0, []
    for _, r in df.iterrows():
        pool = ((float(r.get('ad_clicks', 0) or 0)*c1) + (float(r.get('ad_impressions', 0) or 0)*c2)) + (pool * dec)
        lift.append(pool)
    df['residual_lift'] = lift
    df['expected'] = df['baseline'] + df['residual_lift'] + float(coeffs.get('Promo', 500.0))
    return {"df": df}

@st.cache_data(ttl=60)
def get_hydrated_data(property_id, _client):
    try:
        p_res = _client.table("properties").select("id, property_name").execute()
        p_map = {p['id']: p['property_name'] for p in p_res.data}
        q = _client.table("ledger").select("*")
        if property_id != "GLOBAL": q = q.eq("property_id", str(property_id))
        l_res = q.order("entry_date", desc=True).execute()
        if not l_res.data: return pd.DataFrame(), []
        raw, frames = l_res.data, []
        for pid in list(set([r['property_id'] for r in raw])):
            c_res = _client.table("coefficients").select("*").eq("property_id", pid).execute()
            processed = get_forensic_metrics([r for r in raw if r['property_id'] == pid], c_res.data[0] if c_res.data else {'Promo': 500.0})
            p_df = processed['df']
            p_df['Property'] = p_map.get(pid, "Unknown")
            frames.append(p_df)
        return pd.concat(frames, ignore_index=True), raw
    except: return pd.DataFrame(), []

df, ledger_data = get_hydrated_data(st.session_state.current_property_id, supabase)
st.session_state.ledger_data = ledger_data

if df.empty and page not in ["Global Admin Console", "Master Audit Report", "Daily Ledger Audit", "PR Scorecard"]:
    st.warning("🎰 Forensic Vault is currently empty.")
    st.stop()

# =================================================================
# 9. PAGE 1: EXECUTIVE DASHBOARD (v72.0 - Hard Rock Vital Signs Restored)
# =================================================================
if page == "Executive Dashboard":
    
    # --- 0. DYNAMIC MONTH BOUNDARIES ---
    today = datetime.date.today()
    first_of_month = today.replace(day=1)
    next_month = (today.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    last_of_month = next_month - datetime.timedelta(days=1)

    # --- THE ABSOLUTE STATE FORCER (v9) ---
    if "global_range_v9" not in st.session_state:
        st.session_state.global_range_v9 = (first_of_month, last_of_month)
    if "pulse_range_v9" not in st.session_state:
        st.session_state.pulse_range_v9 = (first_of_month, last_of_month)

    # --- A. CONSOLIDATED GLOBAL VIEW ---
    if st.session_state.get('current_property_id') == "GLOBAL":
        render_styled_header("Global Network Intelligence", "Aggregate Portfolio Performance", "Global")
        if df.empty:
            st.warning("No network data found.")
            st.stop()

        df['entry_date'] = pd.to_datetime(df['entry_date'])
        min_date, max_date = df['entry_date'].min().date(), df['entry_date'].max().date()
        col_date, _ = st.columns([1.5, 2.5])
        with col_date:
            global_range = st.date_input("Network Audit Window:", value=st.session_state.global_range_v9, key="global_range_v9")

        if isinstance(global_range, tuple) and len(global_range) == 2:
            start_g, end_g = global_range
            mask = (df['entry_date'].dt.date >= start_g) & (df['entry_date'].dt.date <= end_g)
            df_filtered = df.loc[mask].copy()
        else:
            df_filtered = df.copy()
            start_g, end_g = first_of_month, last_of_month

        total_rev = df_filtered['actual_coin_in'].sum()
        total_traffic = df_filtered['actual_traffic'].sum()
        total_mems = df_filtered['new_members'].sum()

        k1, k2, k3 = st.columns(3)
        k1.metric("Network Revenue", f"${total_rev:,.0f}")
        k2.metric("Network Traffic", f"{total_traffic:,.0f}")
        k3.metric("Network New Members", f"{total_mems:,.0f}")

        st.divider()
        st.write(f"### 🏆 Property Performance Leaderboard ({start_g} to {end_g})")
        leaderboard = df_filtered.groupby('Property').agg({'actual_coin_in': 'sum', 'actual_traffic': 'sum', 'new_members': 'sum'}).reset_index()
        leaderboard['Rank'] = leaderboard['actual_coin_in'].rank(ascending=False, method='min').astype(int)
        st.table(leaderboard.sort_values('Rank'))

    # --- B. INDIVIDUAL PROPERTY VIEW (The Pulse) ---
    else:
        render_styled_header(f"{st.session_state.current_property_name} Pulse", "Strategic Demand Projection & Marketing Impact", "Operational")
        current_weights = st.session_state.get('coeffs', {})
        
        df_raw = df.copy()
        df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
        df_raw['dow'] = df_raw['entry_date'].dt.day_name()
        master_baselines = df_raw.groupby('dow')['actual_traffic'].mean().to_dict()

        col_date, _ = st.columns([1.5, 2.5])
        with col_date:
            pulse_range = st.date_input("Analysis Window:", value=st.session_state.pulse_range_v9, key="pulse_range_v9")

        if isinstance(pulse_range, tuple) and len(pulse_range) == 2:
            start_p, end_p = pulse_range
            date_list = pd.date_range(start=start_p, end=end_p)
            df_p = pd.DataFrame({'entry_date': date_list})
            df_p['entry_date'] = pd.to_datetime(df_p['entry_date'])
            df_p['dow'] = df_p['entry_date'].dt.day_name()
            
            ledger_lookup = df_raw.set_index(df_raw['entry_date'].dt.strftime('%Y-%m-%d')).to_dict('index')
            def map_data(row, col_name):
                d_str = row['entry_date'].strftime('%Y-%m-%d')
                return ledger_lookup[d_str].get(col_name, 0) if d_str in ledger_lookup else (0 if col_name != 'active_promo' else "")

            map_cols = ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm', 'actual_traffic', 'new_members', 'actual_coin_in', 'predicted_traffic']
            for c in map_cols:
                df_p[c] = df_p.apply(lambda r: map_data(r, c), axis=1)

            df_p['baseline'] = df_p['dow'].map(master_baselines).fillna(0)

            with st.expander("📅 Strategic Daily Planner", expanded=True):
                planner_cols = ['entry_date', 'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
                df_plan_display = df_p[planner_cols].copy()
                df_plan_display['entry_date'] = df_plan_display['entry_date'].dt.strftime('%a, %b %d')
                edited_df = st.data_editor(df_plan_display, hide_index=True, use_container_width=True, key="p1_planner_v72")
                for field in ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']:
                    df_p[field] = edited_df[field].values

            m = get_forensic_metrics(df_p.to_dict(orient='records'), current_weights)
            df_final = m['df'].sort_values('entry_date')
            
            total_vol = df_final['expected'].sum()
            st.write("### 🎰 The Unified Pulse")
            fig_pulse = go.Figure()
            df_act_chart = df_final[df_final['entry_date'].dt.date < today]
            fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
            fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
            fig_pulse.update_layout(height=400, margin=dict(l=10, r=10, t=10, b=10), template="plotly_white")
            st.plotly_chart(fig_pulse, use_container_width=True, key="pulse_chart_v72")

            # 7. EXECUTIVE KPI GRID (Hard Rock Ottawa Vital Signs Restored)
            st.write(f"### 🏛️ {st.session_state.current_property_name} Vital Signs vs. Network Avg")
            k1, k2, k3, k4, k5 = st.columns(5)
            LTV_VAL, AVG_SPEND = 1900.00, 1100.31

            total_act = df_final['actual_traffic'].sum()
            ledger_rev = df_final['actual_coin_in'].sum()
            actual_signups = df_final['new_members'].sum()
            local_yield = (ledger_rev / total_act) if total_act > 0 else 0
            local_conv = (actual_signups / total_act * 100) if total_act > 0 else 0
            
            y_delta = local_yield - st.session_state.get('net_avg_yield', 0)
            c_delta = local_conv - st.session_state.get('net_avg_conv', 0)

            # --- ACCURACY SYNC ---
            df_audit = df_final[df_final['actual_traffic'] > 0].copy()
            if not df_audit.empty:
                s_act, s_pred = df_audit['actual_traffic'].sum(), df_audit['predicted_traffic'].sum()
                accuracy_display = f"{(1 - (abs(s_act - s_pred) / s_act)) * 100:.1f}%"
            else: accuracy_display = "---"

            if start_p >= today:
                proj_rev = (total_vol * AVG_SPEND) + ((total_vol * 0.05) * LTV_VAL)
                k1.metric("Projected Demand", f"{total_vol:,.0f} Guests")
                k2.metric("Target Signups", f"{(total_vol * 0.0170):,.0f}")
                k3.metric("Proj. Revenue", f"${proj_rev:,.0f}")
                k4.metric("Marketing Impact", f"{((total_vol - df_p['baseline'].sum()) / total_vol * 100):.1f}%")
                k5.metric("Model Reliability", accuracy_display)
            else:
                k1.metric("Actual Guest Flow", f"{total_act:,.0f}")
                k2.metric("Yield / Guest", f"${local_yield:,.2f}", delta=f"${y_delta:+.2f} vs Net")
                k3.metric("Enrollment %", f"{local_conv:.2f}%", delta=f"{c_delta:+.2f}% vs Net")
                k4.metric("Ledger Revenue", f"${ledger_rev:,.0f}")
                k5.metric("AI Accuracy", accuracy_display)

            # 8. EXECUTIVE BRAND SENTIMENT PULSE
            st.divider()
            st.write("### 🏛️ Executive Brand Sentiment Pulse")
            col_h1, col_h2 = st.columns([2, 1])
            with col_h2:
                from dateutil.relativedelta import relativedelta
                g_months = [(today - relativedelta(months=i)).replace(day=1) for i in range(3)]
                g_labels = ["Current (Live)"] + [m.strftime("%B %Y") for m in g_months[1:]]
                sel_period = st.selectbox("Audit Period:", g_labels, key="gauge_historical_select_v72")

            overall_score = 0.0
            try:
                base_query = supabase.table("sentiment_history").select("sentiment_score").eq("property_id", st.session_state.current_property_id)
                if sel_period == "Current (Live)":
                    g_res = base_query.order("timestamp", desc=True).limit(50).execute()
                else:
                    sel_date = g_months[g_labels.index(sel_period)]
                    g_res = base_query.gte("timestamp", sel_date.strftime("%Y-%m-%d")).lte("timestamp", (sel_date + relativedelta(months=1)).strftime("%Y-%m-%d")).execute()
                if g_res.data:
                    mapped = [(s['sentiment_score'] * 2 - 1) if 0 <= s['sentiment_score'] <= 1 else s['sentiment_score'] for s in g_res.data]
                    overall_score = np.mean(mapped)
            except: pass
            
            st.metric(label=f"Property Pulse ({sel_period})", value=f"{overall_score:+.2f}")

            # --- DYNAMIC ASSET GAUGES (Full Restore) ---
            try:
                asset_res = supabase.table("property_assets").select("asset_name").eq("property_id", st.session_state.current_property_id).execute()
                tags = [item['asset_name'] for item in asset_res.data] if asset_res.data else ["Overall"]
                gauge_cols = st.columns(len(tags))
                for i, tag in enumerate(tags):
                    with gauge_cols[i]:
                        tag_score = 0.0
                        try:
                            t_query = supabase.table("sentiment_history").select("sentiment_score").eq("property_id", st.session_state.current_property_id).eq("asset", tag)
                            if sel_period == "Current (Live)":
                                t_res = t_query.order("timestamp", desc=True).limit(15).execute()
                            else:
                                t_res = t_query.gte("timestamp", sel_date.strftime("%Y-%m-%d")).lte("timestamp", (sel_date + relativedelta(months=1)).strftime("%Y-%m-%d")).execute()
                            if t_res.data:
                                tag_score = np.mean([(s['sentiment_score'] * 2 - 1) if 0 <= s['sentiment_score'] <= 1 else s['sentiment_score'] for s in t_res.data])
                        except: pass
                        fig = go.Figure(go.Indicator(mode="gauge+number", value=tag_score, number={'font': {'size': 18}, 'valueformat': ".2f"},
                                                     gauge={'axis': {'range': [-1, 1]}, 'bar': {'color': "#0047AB"},
                                                            'steps': [{'range': [-1, -0.3], 'color': "#FF4B4B"}, {'range': [-0.3, 0.3], 'color': "#F0F2F6"}, {'range': [0.3, 1], 'color': "#28A745"}]}))
                        fig.update_layout(height=140, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)')
                        st.plotly_chart(fig, use_container_width=True, key=f"gauge_{tag}_{i}")
                        st.markdown(f"<p style='text-align: center; font-size: 12px;'>{tag}</p>", unsafe_allow_html=True)
            except: pass

# =================================================================
# 10. PAGE 2: DAILY LEDGER AUDIT (v60.9 - Forensic Backfill Ready)
# =================================================================
elif page == "Daily Ledger Audit":
    render_styled_header(
        f"Ledger Audit: {st.session_state.current_property_name}", 
        "Operational Actuals Management & Real-Time AI Inference Audit", 
        "Data Active"
    )

    # --- 1. THE DATA ENGINE ---
    if not ledger_data:
        df_ledger = pd.DataFrame(columns=[
            'entry_date', 'actual_traffic', 'predicted_traffic', 'new_members', 
            'actual_coin_in', 'active_promo', 'attendance', 'ad_clicks', 
            'ad_impressions', 'rain_mm', 'snow_cm', 'experiment_tag', 'property_id'
        ])
    else:
        df_ledger = pd.DataFrame(ledger_data)
        df_ledger['entry_date'] = pd.to_datetime(df_ledger['entry_date']).dt.date
        
        num_cols = ['actual_traffic', 'predicted_traffic', 'new_members', 'actual_coin_in', 'attendance', 'ad_clicks', 'ad_impressions']
        for col in num_cols:
            if col in df_ledger.columns:
                df_ledger[col] = pd.to_numeric(df_ledger[col], errors='coerce').fillna(0)
        
        df_ledger = df_ledger.sort_values('entry_date', ascending=False)

    # --- 3. RAPID ENTRY ACTION CARD ---
    with st.expander("➕ Register Daily Performance Nodes", expanded=False):
        with st.form("rapid_entry_form_v60", clear_on_submit=True, border=False):
            f1, f2, f3 = st.columns(3)
            with f1:
                e_date = st.date_input("Audit Date", value=datetime.date.today())
                e_traffic = st.number_input("Actual Traffic", min_value=0, step=1)
                e_members = st.number_input("New Members", min_value=0, step=1)
            with f2:
                e_promo = st.text_input("Active Promotion", placeholder="e.g. Unity Bonus")
                e_event = st.number_input("Event Attendance", min_value=0, step=1)
                e_coin = st.number_input("Actual Coin-In ($)", min_value=0.0, step=1000.0)
            with f3:
                e_tag = st.text_input("Experiment Tag", placeholder="e.g. Control")
                e_clicks = st.number_input("Ad Clicks", min_value=0, step=1)
                e_imps = st.number_input("Social Impressions", min_value=0, step=1)
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button("🚀 Commit to Forensic Vault", use_container_width=True):
                with st.spinner("🤖 AI Analyst is generating context-aware prediction..."):
                    # Call the Global Prediction Engine
                    ai_pred = generate_ai_prediction(e_date, st.session_state.current_property_name)
                    
                    payload = {
                        "property_id": st.session_state.current_property_id,
                        "entry_date": str(e_date),
                        "actual_traffic": int(e_traffic),
                        "predicted_traffic": ai_pred,
                        "new_members": int(e_members),
                        "actual_coin_in": float(e_coin),
                        "active_promo": str(e_promo).strip() if e_promo else None,
                        "experiment_tag": str(e_tag).strip() if e_tag else None,
                        "attendance": int(e_event),
                        "ad_clicks": int(e_clicks),
                        "ad_impressions": int(e_imps)
                    }
                    try:
                        supabase.table("ledger").upsert(payload).execute()
                        st.success(f"✅ Success. AI Predicted {ai_pred:,} vs Actual {e_traffic:,}")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Vault Error: {e}")

    # --- 4. PERFORMANCE SCOREBOARD (WITH VARIANCE) ---
    st.markdown("<br>", unsafe_allow_html=True)
    c_lim, _ = st.columns([1, 2])
    with c_lim:
        view_limit = st.select_slider("Audit Depth (Days):", options=[7, 14, 30, 60], value=14)
    
    df_audit_period = df_ledger.head(view_limit).copy()
    
    if not df_audit_period.empty:
        total_actual = df_audit_period['actual_traffic'].sum()
        total_pred = df_audit_period['predicted_traffic'].sum()
        variance = total_actual - total_pred
        var_pct = (variance / total_pred * 100) if total_pred > 0 else 0
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Actual Traffic", f"{total_actual:,.0f}")
        m2.metric("AI Predicted", f"{total_pred:,.0f}")
        m3.metric("AI Variance", f"{variance:+,}", delta=f"{var_pct:.1f}%")
        
    st.divider()

    # --- 5. THE HISTORICAL EDITABLE LEDGER ---
    st.markdown("### 📂 Bulk Audit & Corrections")
    with st.form("bulk_ledger_sync", border=False):
        display_df = df_audit_period.drop(columns=['property_id'], errors='ignore').copy()
        with st.container(border=True):
            edited_ledger = st.data_editor(
                display_df, 
                column_config={
                    "entry_date": st.column_config.DateColumn("Date", required=True),
                    "actual_traffic": st.column_config.NumberColumn("Actual Guests", format="%d"),
                    "predicted_traffic": st.column_config.NumberColumn("AI Forecast", format="%d", disabled=True),
                    "actual_coin_in": st.column_config.NumberColumn("Revenue", format="$%d"),
                },
                hide_index=True,
                use_container_width=True,
                key="ledger_editor_v60"
            )
        
        if st.form_submit_button("💾 Sync Table Updates to Cloud", use_container_width=True):
            try:
                df_sync = pd.DataFrame(edited_ledger)
                df_sync['entry_date'] = df_sync['entry_date'].astype(str)
                df_sync['property_id'] = st.session_state.current_property_id
                sync_payload = df_sync.fillna(0).to_dict(orient='records')
                supabase.table("ledger").upsert(sync_payload).execute()
                st.success("✅ Cloud Sync Complete.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Sync Error: {e}")

# =================================================================
# 11. PAGE 3: ATTRIBUTION ANALYTICS (v52.0 - High-End Suite)
# =================================================================
elif page == "Attribution Analytics":
    # 1. PREMIUM HEADER
    render_styled_header(
        "Marketing Attribution & ROI", 
        "Multi-Touch Analysis: Correlating Digital Signal with Physical Property Yield", 
        "Analytics"
    )

    if not ledger_data:
        st.info("💡 Forensic Vault empty. Populate the Ledger to unlock attribution modeling.")
        st.stop()

    # --- 1. DATA PREP & MTA ENGINE ---
    current_weights = st.session_state.get('coeffs', {})
    m_full = get_forensic_metrics(ledger_data, current_weights)
    df_attr = m_full['df']
    
    # Calculate Component Parts
    total_guests = df_attr['actual_traffic'].sum()
    organic_base = df_attr['baseline'].sum()
    digital_lift = df_attr['residual_lift'].sum()
    gravity_lift = df_attr['gravity_lift'].sum()
    
    # Brand Inertia calculation based on established weights
    num_days = len(df_attr)
    brand_inertia = (current_weights.get('Broadcast_Weight', 150) + current_weights.get('OOH_Weight', 100)) * num_days

    # --- 2. EXECUTIVE ATTRIBUTION SUMMARY (Responsive Grid) ---
    st.markdown("### 🕰️ Multi-Touch Attribution (Time Decay Model)")
    st.caption("Weighting the guest journey based on proximity to visit date (Adstock Decay).")
    
    mta_cols = st.columns(3)
    mta_cols[0].metric(
        "Last-Touch (Digital)", 
        f"{digital_lift:,.0f}", 
        help="Immediate click-to-floor conversion."
    )
    mta_cols[1].metric(
        "Assisted (Brand)", 
        f"{brand_inertia:,.0f}", 
        help="OOH/Broadcast awareness priming."
    )
    mta_cols[2].metric(
        "Conversion (Gravity)", 
        f"{gravity_lift:,.0f}", 
        help="Event-driven floor closure."
    )

    st.divider()

    # --- 3. CHANNEL CONTRIBUTION VISUALS (Split Layout) ---
    st.markdown("### 📡 Offline-to-Online Attribution Contribution")
    col_pie, col_water = st.columns([1, 1.5])

    with col_pie:
        pie_labels = ['Organic (Baseline)', 'Online (Digital)', 'Offline (Brand)', 'Event Gravity']
        pie_values = [organic_base, digital_lift, brand_inertia, gravity_lift]
        fig_pie = px.pie(
            names=pie_labels, 
            values=pie_values, 
            color_discrete_sequence=['#E1E8F0', '#0047AB', '#5D707F', '#FFCC00'],
            hole=0.6
        )
        fig_pie.update_layout(showlegend=True, height=350, margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fig_pie, use_container_width=True, key="attr_pie_v52")

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
        fig_water.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10), template="plotly_white")
        st.plotly_chart(fig_water, use_container_width=True, key="attr_waterfall_v52")

    st.divider()

    # --- 4. LIFT CORRELATION ---
    st.markdown("### 📈 Signal Correlation Analysis")
    fig_corr = px.scatter(
        df_attr, x='ad_clicks', y='actual_traffic', 
        trendline="ols", 
        labels={'ad_clicks': 'Digital Signal (Clicks)', 'actual_traffic': 'Property Traffic'},
        color_discrete_sequence=['#0047AB']
    )
    fig_corr.update_layout(height=400, plot_bgcolor='rgba(248,249,250,1)', template="plotly_white")
    st.plotly_chart(fig_corr, use_container_width=True, key="attr_corr_v52")

    st.divider()

    # --- 5. STRATEGIC INTERPRETATION & ROI AUDIT ---
    st.markdown("### 💎 Strategic Interpretation & ROI Audit")
    
    if not df_attr.empty:
        avg_coin = float(current_weights.get('Avg_Coin_In', 112.50))
        mkt_guests = digital_lift + brand_inertia + gravity_lift
        mkt_revenue = mkt_guests * avg_coin
        
        # Calculate Efficiency Metrics
        yield_per_click = digital_lift / df_attr['ad_clicks'].sum() if df_attr['ad_clicks'].sum() > 0 else 0
        
        # Action Cards for ROI
        c1, c2, c3 = st.columns(3)
        c1.metric("Marketing Yield (Est. $)", f"${mkt_revenue:,.0f}")
        c2.metric("Guest Pull Efficiency", f"{(mkt_guests/total_guests)*100:.1f}%")
        c3.metric("Digital Conversion", f"{yield_per_click:.2f}x")

        # Premium Themed Summary Box
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%); 
                        padding: 25px; border-radius: 12px; border: 1px solid #e2e8f0; margin-top: 20px;">
                <h4 style="margin-top:0; color: #0f172a;">FloorCast Strategic Audit Summary</h4>
                <ul style="color: #475569; font-size: 0.95rem; line-height: 1.6;">
                    <li><b>MTA Insight:</b> The {current_weights.get('Ad_Decay', 85)}% Adstock retention indicates a strong <b>Time Decay</b> effect, ensuring marketing remains active for multiple days post-exposure.</li>
                    <li><b>Channel Mix:</b> <b>{'Digital' if digital_lift > brand_inertia else 'Offline Media'}</b> is currently delivering the highest marginal lift per dollar.</li>
                    <li><b>ROI Validation:</b> Based on a ${avg_coin:.2f} Avg Coin-In, marketing contributed an estimated <b>{mkt_guests:,.0f}</b> guests to the audit window.</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)
    else:
        st.warning("Insufficient data for full ROI Audit.")

# =================================================================
# 12. PAGE 4: MASTER FORENSIC AUDIT (v85.0 - PR & Earned Media Integrated)
# =================================================================
elif page == "Master Audit Report":
    # 1. PREMIUM HEADER
    render_styled_header(
        f"Master Property Audit: {st.session_state.current_property_name}",
        "Forensic Ledger: Financials, Multi-Channel Attribution, & Earned Media",
        "Audit Ready"
    )
    
    # --- 1. SAAS INGESTION FACTORY ---
    with st.expander("📥 Bulk Ingest Forensic Ledger (CSV)", expanded=not ledger_data):
        st.markdown('<div style="padding: 10px;">', unsafe_allow_html=True)
        uploaded_file = st.file_uploader("Choose CSV File", type="csv", key="vault_uploader")
        
        if uploaded_file:
            try:
                up_df = pd.read_csv(uploaded_file)
                up_df['property_id'] = st.session_state.current_property_id
                
                if st.button("🚀 Commit Bulk Upload to Vault", use_container_width=True):
                    payload = up_df.to_dict(orient='records')
                    supabase.table("ledger").upsert(payload).execute()
                    st.success(f"Successfully ingested {len(up_df)} records!")
                    st.cache_data.clear()
                    st.rerun()
            except Exception as e:
                st.error(f"Ingestion Error: {e}")
        st.markdown('</div>', unsafe_allow_html=True)

    if not ledger_data:
        st.warning(f"⚠️ Audit Vault for {st.session_state.current_property_name} is empty.")
        st.stop()

    # --- 2. AUDIT WINDOW & DATA PREP ---
    df_audit_raw = pd.DataFrame(ledger_data)
    df_audit_raw['entry_date'] = pd.to_datetime(df_audit_raw['entry_date'])
    min_audit, max_audit = df_audit_raw['entry_date'].min().date(), df_audit_raw['entry_date'].max().date()

    col_date, col_export = st.columns([2, 1])
    with col_date:
        audit_range = st.date_input("Audit Window:", value=(min_audit, max_audit), key="master_audit_v85")

    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_date, e_date = audit_range
        mask = (df_audit_raw['entry_date'].dt.date >= s_date) & (df_audit_raw['entry_date'].dt.date <= e_date)
        df_audit_filtered = df_audit_raw.loc[mask].copy()
        
        if df_audit_filtered.empty:
            st.error("No records found for selected range.")
            st.stop()

        # Engine Sync
        m = get_forensic_metrics(df_audit_filtered.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df']
        
        # Calculations
        t_rev = df_final['actual_coin_in'].sum()
        t_traf = df_final['actual_traffic'].sum()
        t_mems = df_final['new_members'].sum()
        t_clicks = df_final['ad_clicks'].sum() if 'ad_clicks' in df_final.columns else 0
        t_imps = df_final['ad_impressions'].sum() if 'ad_impressions' in df_final.columns else 0
        t_pred = df_final['predicted_traffic'].sum() if 'predicted_traffic' in df_final.columns else 0
        accuracy = (1 - (abs(t_traf - t_pred) / t_traf)) * 100 if t_traf > 0 else 0

        # --- 3. EXECUTIVE SCOREBOARD ---
        st.markdown("### 📊 Executive Summary")
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Total Traffic", f"{t_traf:,}")
        k2.metric("Actual Revenue", f"${t_rev:,.0f}")
        k3.metric("Ad Clicks", f"{t_clicks:,.0f}")
        k4.metric("New Members", f"{t_mems:,}")
        k5.metric("Social Reach", f"{t_imps:,.0f}")
        k6.metric("AI Accuracy", f"{accuracy:.1f}%")

        # --- 4. PR & EARNED MEDIA IMPACT (NEW SECTION) ---
        st.divider()
        st.markdown("### 📢 Earned Media & PR Audit")
        
        # Fetch PR data for the selected audit range
        try:
            pr_res = supabase.table("pr_scorecard")\
                .select("*")\
                .eq("property_id", st.session_state.current_property_id)\
                .gte("report_month", s_date.strftime("%Y-%m-01"))\
                .lte("report_month", e_date.strftime("%Y-%m-%d"))\
                .execute()
            
            if pr_res.data:
                df_pr_audit = pd.DataFrame(pr_res.data)
                total_pr_imps = df_pr_audit['earned_impressions'].sum()
                total_pr_mentions = df_pr_audit['earned_mentions'].sum()
                
                p1, p2, p3 = st.columns([1, 1, 2])
                p1.metric("Earned Reach", f"{total_pr_imps:,}")
                p2.metric("Media Placements", f"{total_pr_mentions}")
                
                # Halo Effect Calculation: PR Impressions per Guest
                halo = (total_pr_imps / t_traf) if t_traf > 0 else 0
                p3.metric("PR Halo Index", f"{halo:.2f} Imps/Guest", help="Volume of earned media reach relative to physical footfall.")
                
                with st.expander("🔍 View Narrative PR Wins for this Period"):
                    for _, pr_row in df_pr_audit.iterrows():
                        st.markdown(f"**{pd.to_datetime(pr_row['report_month']).strftime('%B %Y')}:** {pr_row['mediums']}")
                        st.caption(pr_row['executive_summary'])
            else:
                st.info("No PR Scorecard data found for this audit window.")
        except Exception as e:
            st.caption(f"PR Data unavailable for this range: {e}")

        # --- 5. ATTRIBUTION FLOW CHART ---
        st.divider()
        st.markdown("### 🌊 Multi-Channel Attribution Flow")
        fig_stack = go.Figure()
        layers = [
            ('Organic Heartbeat', 'baseline', '#8E9AAF'),
            ('Digital ROI Lift', 'residual_lift', '#0047AB'),
            ('Event Gravity', 'gravity_lift', '#FFCC00')
        ]
        for name, col, color in layers:
            if col in df_final.columns:
                fig_stack.add_trace(go.Scatter(
                    x=df_final['entry_date'], y=df_final[col], 
                    name=name, stackgroup='one', 
                    line=dict(width=0.5, color=color),
                    fill='tonexty'
                ))
        fig_stack.update_layout(height=400, margin=dict(l=10, r=10, t=10, b=10), template="plotly_white")
        st.plotly_chart(fig_stack, use_container_width=True)

        # --- 6. AI VARIANCE AUDIT ---
        st.markdown("### 🎯 Prediction vs. Reality")
        v_col, i_col = st.columns([2, 1])
        with v_col:
            fig_var = go.Figure()
            fig_var.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=3)))
            fig_var.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['predicted_traffic'], name="AI Forecast", line=dict(color='#FFCC00', width=2, dash='dot')))
            fig_var.update_layout(height=350, template="plotly_white", margin=dict(l=10, r=10, t=10, b=10), hovermode="x unified", legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig_var, use_container_width=True)
            
        with i_col:
            with st.container(border=True):
                st.markdown("#### 🏁 Model Reliability")
                avg_error = abs(t_traf - t_pred) / len(df_final) if len(df_final) > 0 else 0
                st.metric("Avg Daily Variance", f"{avg_error:,.0f} guests")
                if accuracy > 90: st.success("Elite Precision Tracking.")
                elif accuracy > 75: st.warning("Moderate Drift: Calibration Suggested.")
                else: st.error("High Variance: Manual Audit Required.")

        # --- 7. EXPORT ---
        st.divider()
        st.download_button("📥 Export Integrated Audit", 
                           data=df_final.to_csv(index=False).encode('utf-8'), 
                           file_name=f"Master_Audit_{s_date}.csv", use_container_width=True)

# =================================================================
# 13. PAGE 5: AI CALIBRATION & ENGINE WEIGHTS (v73.0 SaaS Sync)
# =================================================================
elif page == "AI Calibration":
    render_styled_header(
        f"Engine Calibration: {st.session_state.current_property_name}",
        "Fine-tune the Forensic Attribution Weights and Financial Benchmarks",
        "Tuning"
    )

    # 1. DATA DISCOVERY
    df_ledger = pd.DataFrame(ledger_data)
    
    # Calculate Live Averages
    if not df_ledger.empty:
        total_coin = df_ledger['actual_coin_in'].sum()
        total_traffic = df_ledger['actual_traffic'].sum()
        live_avg = (total_coin / total_traffic) if total_traffic > 0 else 112.50
        
        # --- SYNCED MODEL CONFIDENCE CALCULATION ---
        # Filter for completed nodes only (Actuals > 0)
        df_audit = df_ledger[df_ledger['actual_traffic'] > 0].copy()
        
        if not df_audit.empty:
            s_act = df_audit['actual_traffic'].sum()
            s_pred = df_audit['predicted_traffic'].sum() if 'predicted_traffic' in df_audit.columns else 0
            
            # Use Aggregated Variance Logic to match Executive Dashboard
            accuracy_val = (1 - (abs(s_act - s_pred) / s_act)) * 100 if s_act > 0 else 0
            conf_score = f"{accuracy_val:.1f}%"
            conf_delta = "Optimized" if accuracy_val >= 95 else "Stable" if accuracy_val >= 85 else "Recalibration Suggested"
        else:
            conf_score, conf_delta = "---", "Awaiting Data"
    else:
        live_avg = 112.50
        conf_score, conf_delta = "---", "No Vault Data"

    # 2. RENDER PERFORMANCE METRIC
    c_health, _ = st.columns([1.5, 2])
    with c_health:
        st.metric("Model Confidence", conf_score, delta=conf_delta)

    # 3. CALIBRATION FORM
    with st.form("master_calibration_form", border=True):
        st.markdown("#### 💰 Financial DNA & Benchmarks")
        st.caption(f"Current Ledger Average: ${live_avg:.2f} per guest")
        
        b1, b2 = st.columns(2)
        with b1:
            n_avg_coin = st.number_input("Target Avg Coin-In ($)", value=float(st.session_state.coeffs.get('Avg_Coin_In', live_avg)))
        with b2:
            n_hold = st.number_input("Property Hold %", value=float(st.session_state.coeffs.get('Hold_Pct', 10.0)), format="%.1f")

        st.divider()
        st.markdown("#### 🌐 Digital & Mass Media Drivers")
        d1, d2, d3 = st.columns(3)
        with d1:
            n_clicks = st.number_input("Click Weight", value=float(st.session_state.coeffs.get('Clicks', 0.05)), format="%.2f")
        with d2:
            n_social = st.number_input("Social Impression Weight", value=float(st.session_state.coeffs.get('Social_Imp', 0.0002)), format="%.4f")
        with d3:
            n_decay = st.number_input("Adstock Retention %", value=int(st.session_state.coeffs.get('Ad_Decay', 85)))

        st.markdown("<br>#### 📡 Branding & Friction", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            n_broad = st.number_input("Broadcast Weight", value=int(st.session_state.coeffs.get('Broadcast_Weight', 150)))
        with c2:
            n_rain = st.number_input("Rain Loss (mm)", value=int(st.session_state.coeffs.get('Rain_mm', -12)))
        with c3:
            n_snow = st.number_input("Snow Loss (cm)", value=int(st.session_state.coeffs.get('Snow_cm', -45)))

        st.markdown("<br>", unsafe_allow_html=True)
        if st.form_submit_button("🚀 Hard-Save Property DNA", use_container_width=True):
            updated = {
                "property_id": st.session_state.current_property_id, 
                "Avg_Coin_In": n_avg_coin,
                "Hold_Pct": n_hold, 
                "Clicks": n_clicks, 
                "Social_Imp": n_social,
                "Ad_Decay": n_decay, 
                "Broadcast_Weight": n_broad, 
                "Rain_mm": n_rain, 
                "Snow_cm": n_snow
            }
            try:
                supabase.table("coefficients").upsert(updated, on_conflict="property_id").execute()
                st.session_state.coeffs.update(updated)
                st.success("✅ Calibration Table Synchronized.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Sync Failure: {e}")

    with st.expander("🔍 View Sensitivity Manifest"):
        st.json(st.session_state.coeffs)

# =================================================================
# 14. PAGE 6: SENTIMENT SCORING (v64.5 - Smart Scale Fix)
# =================================================================
elif page == "Sentiment Scoring":
    render_styled_header(
        f"Sentiment Scoring: {st.session_state.current_property_name}",
        "Vault Research: Categorical Analysis of Guest Sentiment",
        "Vault Active"
    )
    
    # --- 1. DATA HYDRATION ---
    try:
        asset_res = supabase.table("property_assets").select("asset_name").eq("property_id", st.session_state.current_property_id).execute()
        tags = [item['asset_name'] for item in asset_res.data] if asset_res.data else ["Overall Property"]
    except:
        tags = ["Overall Property"]

    # --- 2. ENTRY MODULES (Role Restricted) ---
    authorized_roles = ["Super Admin", "Admin", "Manager"]
    user_role = st.session_state.get('user_role', 'Viewer')

    if user_role in authorized_roles:
        col_input1, col_input2 = st.columns(2)
        with col_input1:
            with st.expander("📝 Manual Sentiment Archival", expanded=True):
                with st.form("manual_sentiment_form", clear_on_submit=True, border=False):
                    manual_tag = st.selectbox("Assign to Asset:", tags)
                    f_text = st.text_area("Review Content", placeholder="Paste review text...", height=150)
                    if st.form_submit_button("🛡️ Archive & AI Score", use_container_width=True):
                        if f_text and archive_sentiment_entry(f_text, manual_tag):
                            st.success("Entry Scored & Vaulted.")
                            st.cache_data.clear()
                            st.rerun()

        with col_input2:
            from docx import Document
            with st.expander("📄 Intelligence Bulk Loader", expanded=True):
                uploaded_doc = st.file_uploader("Upload .docx Source", type="docx")
                bulk_tag = st.selectbox("Bulk Assign:", tags)
                if uploaded_doc and st.button("🚀 Execute Bulk Parse", use_container_width=True):
                    doc = Document(uploaded_doc)
                    for para in doc.paragraphs:
                        if len(para.text) > 20:
                            archive_sentiment_entry(para.text, bulk_tag)
                    st.success("Bulk Ingestion Complete.")
                    st.cache_data.clear()
                    st.rerun()
        st.divider()
    else:
        st.caption("🔒 Data Ingestion tools restricted to Management roles.")

    # --- 3. SENTIMENT VAULT RESEARCH (Categorical Filter) ---
    st.markdown("### 🔍 Sentiment Vault Research")

    f1, f2, f3, f4 = st.columns([1.2, 1, 1.2, 1.2])

    with f1:
        search_query = st.text_input("Search Content", placeholder="Keyword search...")
    with f2:
        filter_asset = st.selectbox("Asset Filter", ["All Assets"] + tags)
    with f3:
        today = datetime.date.today()
        last_30 = today - datetime.timedelta(days=30)
        date_range = st.date_input("Audit Window", value=(last_30, today))
    with f4:
        categories = ["All Categories", "Positive", "Neutral", "Negative"]
        filter_cat = st.selectbox("Sentiment Category", categories, index=0)

    # Fetch Data
    try:
        query = supabase.table("sentiment_history").select("*").eq("property_id", st.session_state.current_property_id)
        if filter_asset != "All Assets":
            query = query.eq("asset", filter_asset)
        
        if filter_cat != "All Categories":
            query = query.eq("sentiment_category", filter_cat)
        
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
            query = query.gte("timestamp", start_date.isoformat()).lte("timestamp", f"{end_date.isoformat()}T23:59:59")
        
        vault_res = query.order("timestamp", desc=True).limit(100).execute()

        if vault_res.data:
            df_vault = pd.DataFrame(vault_res.data)
            
            if search_query and not df_vault.empty:
                df_vault = df_vault[df_vault['raw_text'].str.contains(search_query, case=False)]

            if not df_vault.empty:
                # --- THE CONDITIONAL SCALER FIX ---
                # 1. Force numeric conversion
                df_vault['sentiment_score'] = pd.to_numeric(df_vault['sentiment_score'], errors='coerce').fillna(0.5)
                
                # 2. Apply Smart Mapping
                # Only maps if value is between 0 and 1. Preserves existing negatives like -0.95.
                df_vault['display_score'] = df_vault['sentiment_score'].apply(
                    lambda x: (x * 2) - 1 if 0 <= x <= 1 else x
                )

                for _, row in df_vault.iterrows():
                    with st.container(border=True):
                        v_col1, v_col2 = st.columns([4, 1])
                        with v_col1:
                            st.markdown(f"**Asset:** `{row.get('asset', 'General')}`")
                            st.write(row.get('raw_text', 'No content available.'))
                            
                            cat_label = row.get('sentiment_category', 'N/A')
                            ts_display = str(row.get('timestamp'))[:10]
                            st.caption(f"Category: **{cat_label}** | Date: {ts_display}")
                        
                        with v_col2:
                            d_score = row.get('display_score', 0.0)
                            # Forensic color coding
                            score_color = "#E63946" if cat_label in ["Critical", "Negative"] or d_score < -0.3 else "#F4A261" if cat_label == "Neutral" else "#2A9D8F"
                            st.metric("AI Score", f"{d_score:.2f}")
                            st.markdown(f"<div style='height:8px; width:100%; background:{score_color}; border-radius:4px;'></div>", unsafe_allow_html=True)
            else:
                st.info("No records found in this category.")
        else:
            st.info("No sentiment data found.")
            
    except Exception as e:
        st.error(f"Vault Retrieval Error: {e}")

# =================================================================
# 15. PAGE 7: BL-ROAS COMMAND CENTER (v52.0 - High-End ROI Audit)
# =================================================================
elif page == "BL-ROAS Calculator":
    # 1. PREMIUM HEADER
    render_styled_header(
        "BL-ROAS Command Center", 
        "Monthly Brand Lift & Marketing ROI Validation Engine", 
        "Financials"
    )

    # --- 0. GLOBAL PAGE BENCHMARKS ---
    LTV_BENCHMARK = 1900.00 
    DEFAULT_AVG_SPEND = 1100.31

    # --- 1. MONTH SELECTION ---
    today = datetime.date.today()
    month_options = [(today - relativedelta(months=i)).replace(day=1) for i in range(12)]
    month_labels = [m.strftime("%B %Y") for m in month_options]

    c_sel, _ = st.columns([1.5, 2])
    with c_sel:
        selected_label = st.selectbox("Audit Fiscal Period:", month_labels)
    
    selected_month = month_options[month_labels.index(selected_label)]

    # --- 2. DYNAMIC LEDGER AGGREGATION ---
    df_roas = pd.DataFrame(ledger_data)
    if not df_roas.empty:
        df_roas['entry_date'] = pd.to_datetime(df_roas['entry_date'])
        m_mask = (df_roas['entry_date'].dt.month == selected_month.month) & \
                 (df_roas['entry_date'].dt.year == selected_month.year)
        selected_month_df = df_roas.loc[m_mask].copy()

        if not selected_month_df.empty:
            monthly_summary = selected_month_df.groupby(selected_month_df['entry_date'].dt.date).max()
            ledger_traffic = int(monthly_summary['actual_traffic'].sum())
            ledger_signups = int(monthly_summary['new_members'].sum())
            ledger_coin_in = float(monthly_summary['actual_coin_in'].sum())
        else:
            ledger_traffic, ledger_signups, ledger_coin_in = 0, 0, 0.0
    else:
        ledger_traffic, ledger_signups, ledger_coin_in = 0, 0, 0.0

    # --- 3. INPUT ACTION CARD ---
    with st.form("roas_v52_form", border=True):
        st.markdown(f"#### 📊 {selected_label} Performance Metrics")
        
        # Fetch Existing Data
        existing_res = supabase.table("monthly_roi").select("*")\
            .eq("property_id", st.session_state.current_property_id)\
            .eq("report_month", str(selected_month)).execute()
        existing = existing_res.data[0] if existing_res.data else {}

        c1, c2, c3 = st.columns(3)
        with c1:
            utm_s = st.number_input("UTM Sessions", value=int(existing.get('utm_sessions', 0)))
            org_s = st.number_input("Organic Sessions", value=int(existing.get('organic_sessions', 0)))
            ad_spend = st.number_input("Total Ad Spend ($)", value=float(existing.get('ad_spend', 0.0)), step=500.0)
        
        with c2:
            likes = st.number_input("Social Engagement", value=int(existing.get('social_likes', 0)))
            shares = st.number_input("Social Shares", value=int(existing.get('social_shares', 0)))
            views = st.number_input("Reach / Impressions", value=int(existing.get('post_views', 0)))

        with c3:
            time_site = st.number_input("Time-on-Site Sessions", value=int(existing.get('site_time_sessions', 0)))
            cta_clicks = st.number_input("Booking CTA Clicks", value=int(existing.get('booking_clicks', 0)))
            geo_lift = st.number_input("Incremental Geo Traffic", value=int(existing.get('geo_lift_traffic', 0)))

        st.divider()
        
        # Live Ledger Preview Card inside Form
        st.markdown(f"""
            <div style="background: rgba(0, 71, 171, 0.05); padding: 15px; border-radius: 8px; border: 1px solid rgba(0, 71, 171, 0.2); margin-bottom: 20px;">
                <p style="margin:0; font-size: 0.8rem; color: #0047AB; font-weight: 700; text-transform: uppercase;">Linked Ledger Sync</p>
                <p style="margin:0; font-size: 0.95rem; color: #1e293b;">
                    Coin-In: <b>${ledger_coin_in:,.2f}</b> | Traffic: <b>{ledger_traffic:,}</b> | Signups: <b>{ledger_signups:,}</b>
                </p>
            </div>
        """, unsafe_allow_html=True)

        if st.form_submit_button("🚀 Save & Generate ROI Audit", use_container_width=True):
            brand_value = (utm_s * 1.5) + (org_s * 0.5) + (likes * 0.1) + (shares * 0.5) + (geo_lift * 2.0)
            bl_roas = brand_value / ad_spend if ad_spend > 0 else 0
            enhanced_rev = brand_value + ledger_coin_in + (ledger_signups * LTV_BENCHMARK)

            roi_payload = {
                "property_id": st.session_state.current_property_id,
                "report_month": str(selected_month),
                "utm_sessions": utm_s, "organic_sessions": org_s, "ad_spend": ad_spend,
                "social_likes": likes, "social_shares": shares, "post_views": views,
                "site_time_sessions": time_site, "booking_clicks": cta_clicks, 
                "geo_lift_traffic": geo_lift, "brand_value": brand_value, 
                "calculated_bl_roas": bl_roas, "enhanced_revenue": enhanced_rev
            }
            
            try:
                supabase.table("monthly_roi").upsert(roi_payload, on_conflict="property_id, report_month").execute()
                st.success(f"Audit for {selected_label} Synchronized.")
                st.rerun() 
            except Exception as e:
                st.error(f"Sync Failure: {e}")

    # --- 4. EXECUTIVE REPORT GENERATOR ---
    st.divider()
    history_res = supabase.table("monthly_roi").select("*")\
        .eq("property_id", st.session_state.current_property_id)\
        .order("report_month", desc=True).execute()
        
    if history_res.data:
        df_hist = pd.DataFrame(history_res.data)
        curr_row = df_hist[df_hist['report_month'] == str(selected_month)]
        
        if not curr_row.empty:
            curr = curr_row.iloc[0]
            prop_potential = ledger_coin_in + (ledger_signups * LTV_BENCHMARK)
            
            report_text = f"""{selected_label} ROAS Results | {st.session_state.current_property_name}
--------------------------------------------------
BRAND HEALTH PERFORMANCE
BL-ROAS = {curr['calculated_bl_roas']:.2f}x
Measured Brand Value Generated: ${curr['brand_value']:,.2f}

ATTRIBUTED REVENUE IMPACT (ESTIMATED)
• 10% Attribution: ${(prop_potential * 0.1):,.0f}
• 20% Attribution: ${(prop_potential * 0.2):,.0f}
• 30% Attribution: ${(prop_potential * 0.3):,.0f}

ENHANCED TOTAL IMPACT: ${curr['enhanced_revenue']:,.0f}"""
            
            st.markdown("### 📄 Executive Summary (SharePoint Ready)")
            st.text_area("Audit Output Clip:", value=report_text, height=220)

            st.markdown("### 📜 Audit History")
            with st.container(border=True):
                st.dataframe(
                    df_hist[['report_month', 'calculated_bl_roas', 'brand_value', 'enhanced_revenue']], 
                    use_container_width=True, hide_index=True
                )

# =================================================================
# 16. PAGE 8: GLOBAL ADMIN CONSOLE (v25.0 RBAC & PR Integrated)
# =================================================================
elif page == "Global Admin Console":
    st.markdown(f"""
        <div style="background-color: #1A1A1B; padding: 20px; border-radius: 12px; border-left: 6px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🛠️ Global Admin Console</h2>
            <p style="color: #DDD; margin: 0;">System Provisioning, Role Management, and Property Orchestration.</p>
        </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs(["🏗️ Property Provisioning", "👥 User Access & Roles", "📊 System Health & Security"])

    # --- TAB 1: PROPERTY PROVISIONING ---
    with tabs[0]:
        st.subheader("Provision New SaaS Tenant")
        with st.form("provision_property_form"):
            new_p_name = st.text_input("Property Name", placeholder="e.g. Hard Rock Las Vegas")
            new_p_region = st.selectbox("Region", ["North America", "EMEA", "APAC", "LATAM"])
            
            if st.form_submit_button("🚀 Build Property Tenant"):
                if new_p_name:
                    try:
                        # 1. Create Property
                        p_res = supabase.table("properties").insert({
                            "property_name": new_p_name, 
                            "region": new_p_region
                        }).execute()
                        
                        if p_res.data:
                            new_id = p_res.data[0]['id']
                            # 2. Seed Coefficients (Copy Ottawa DNA)
                            seed_coeffs = st.session_state.coeffs.copy()
                            seed_coeffs['property_id'] = new_id
                            if 'id' in seed_coeffs: del seed_coeffs['id']
                            
                            supabase.table("coefficients").insert(seed_coeffs).execute()
                            st.success(f"Tenant {new_p_name} provisioned with ID: {new_id}")
                    except Exception as e:
                        st.error(f"Provisioning Error: {e}")

    # --- TAB 2: USER ACCESS & ROLES ---
    with tabs[1]:
        st.subheader("👥 System User Directory")
    
        # 1. SEARCH & FILTER
        search_q = st.text_input("🔍 Search by Email:", placeholder="Enter email to find user access records...")
        
        # 2. FETCH & DISPLAY
        access_res = supabase.table("user_property_access").select("*, properties(property_name)").execute()
        
        if access_res.data:
            df_access = pd.DataFrame(access_res.data)
            df_access['Property Name'] = df_access['properties'].apply(lambda x: x['property_name'] if x else "N/A")
            
            if search_q:
                df_access = df_access[df_access['user_email'].str.contains(search_q, case=False)]

            st.write(f"Showing **{len(df_access)}** access records:")
            
            for i, row in df_access.iterrows():
                label = f"👤 {row['user_email']} | {row['Property Name']} ({row['user_role']})"
                
                with st.expander(label):
                    c1, c2, c3 = st.columns([2, 2, 1])
                    with c1:
                        role_list = ["Viewer", "Manager", "Admin", "Super Admin"]
                        current_role = row['user_role'] if row['user_role'] in role_list else "Viewer"
                        new_role = st.selectbox("Role:", role_list, index=role_list.index(current_role), key=f"role_{row['id']}")
                    
                    with c2:
                        st.write(f"**Linked Property:** {row['Property Name']}")
                        st.caption(f"Access ID: {row['id']}")
                    
                    with c3:
                        if st.button("Update", key=f"upd_{row['id']}", use_container_width=True):
                            supabase.table("user_property_access").update({"user_role": new_role}).eq("id", row['id']).execute()
                            st.success("Synced.")
                            st.rerun()
                        
                        if st.button("🗑️ Revoke", key=f"rev_{row['id']}", type="secondary", use_container_width=True):
                            try:
                                supabase.table("user_property_access").delete().eq("id", row['id']).execute()
                                st.warning(f"Access Revoked for {row['user_email']}")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Deletion Error: {e}")
        else:
            st.info("No user access records found.")

        st.divider()
        st.subheader("➕ Assign User to Additional Property")
        with st.form("assign_multi_prop"):
            target_email = st.text_input("User Email (Primary Key)", placeholder="user@company.com")
            all_p_res = supabase.table("properties").select("id, property_name").execute()
            p_opts = {p['property_name']: p['id'] for p in all_p_res.data} if all_p_res.data else {}
            target_prop_name = st.selectbox("Select Property to Link", list(p_opts.keys()))
            target_role = st.selectbox("Assign Role", ["Viewer", "Manager", "Admin", "Super Admin"])
            
            if st.form_submit_button("🚀 Link User to Property", use_container_width=True):
                if target_email and target_prop_name:
                    clean_email = target_email.lower().strip()
                    target_uuid = p_opts.get(target_prop_name)
                    check = supabase.table("user_property_access").select("*").eq("user_email", clean_email).eq("property_id", target_uuid).execute()
                    
                    if check.data:
                        st.error(f"User {clean_email} already linked to {target_prop_name}.")
                    else:
                        try:
                            supabase.table("user_property_access").insert({"user_email": clean_email, "property_id": target_uuid, "user_role": target_role}).execute()
                            st.success(f"✅ Linked {clean_email}")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Database Error: {e}")

    # --- TAB 3: SYSTEM HEALTH & SECURITY MATRIX ---
    with tabs[2]:
        st.write("### 📊 Database Orchestration Stats")
        try:
            prop_res = supabase.table("properties").select("*", count="exact").execute()
            user_res = supabase.table("user_property_access").select("*", count="exact").execute()
            h1, h2 = st.columns(2)
            h1.metric("Active Tenants", prop_res.count or 0)
            h2.metric("Managed Users", user_res.count or 0)
        except Exception as e: 
            st.error(f"Stats Error: {e}")
            
        st.divider()
        st.subheader("🛡️ Global Role Authorization Matrix")
        
        target_role_config = st.selectbox("Select Role to Configure:", ["Viewer", "Manager", "Admin", "Super Admin"], key="role_selector_admin")
        
        existing_perms = {}
        try:
            perm_fetch = supabase.table("role_permissions").select("perms").eq("role_name", target_role_config).execute()
            if perm_fetch.data:
                existing_perms = perm_fetch.data[0].get('perms', {})
        except Exception as e:
            st.caption(f"Role '{target_role_config}' not yet initialized.")

        # Updated Capabilities List with PR Scorecard
        capabilities = {
            "view_analytics": "Access Attribution & Executive Dashboards",
            "view_ledger": "Access Daily Ledger Audit",
            "view_pr_scorecard": "Access PR Scorecard (Earned Media Tracking)",
            "view_reports": "Access Master Audit Reports",
            "run_simulations": "Access Predictive Scenario Simulator",
            "manage_alerts": "Create/Delete Strategic Watchdogs",
            "calibrate_ai": "Change AI Coefficients & ROAS",
            "run_experiments": "Access A/B Experimentation Vault"
        }
        
        with st.form(f"perm_matrix_form_{target_role_config}"):
            st.write(f"Adjusting capabilities for: **{target_role_config}**")
            updated_perms = {}
            col1, col2 = st.columns(2)
            for i, (cap_id, cap_desc) in enumerate(capabilities.items()):
                target_col = col1 if i % 2 == 0 else col2
                is_checked = existing_perms.get(cap_id, False)
                updated_perms[cap_id] = target_col.checkbox(cap_desc, value=is_checked, key=f"check_{target_role_config}_{cap_id}")
                
            if st.form_submit_button("💾 Save Role Configuration", use_container_width=True):
                try:
                    supabase.table("role_permissions").upsert({"role_name": target_role_config, "perms": updated_perms}, on_conflict="role_name").execute()
                    st.success(f"✅ '{target_role_config}' permissions are now live.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Security Matrix Sync Error: {e}")

# =================================================================
# 17. PAGE 9: STRATEGIC ALERTS (v60.2 - Multi-Role Response Engine)
# =================================================================
elif page == "Strategic Alerts":
    # 1. PREMIUM HEADER
    render_styled_header(
        "Strategic Watchdogs", 
        "Autonomous Performance Monitoring & Multi-Role Response Engine", 
        "Monitoring Active"
    )
    
    col_a, col_b = st.columns([1, 1.5])
    
    with col_a:
        st.markdown("### 🛠️ Create New Trigger")
        if st.session_state.current_property_id == "GLOBAL":
            st.info("💡 Please select a specific property environment to deploy a watchdog.")
        else:
            # Action Card for Trigger Creation
            with st.form("new_alert_form_v60", border=True):
                st.markdown("#### Sentinel Configuration")
                a_name = st.text_input("Watchdog Alias", placeholder="e.g. Critical Revenue Floor")
                
                # MULTI-ROLE SELECTION: Users can now tag multiple groups
                available_roles = ["Super Admin", "Admin", "Manager", "Viewer", "Executive"]
                target_roles = st.multiselect(
                    "Recipient Roles", 
                    available_roles, 
                    default=["Admin"],
                    help="All users assigned to these roles will be notified."
                )
                
                a_metric = st.selectbox("Intelligence Metric", ["Revenue", "Guest Traffic", "Sentiment Score"])
                
                c1, c2 = st.columns(2)
                with c1:
                    a_op = st.selectbox("Condition", ["Drops Below", "Exceeds"])
                with c2:
                    a_val = st.number_input("Threshold", value=0.0, step=100.0)
                
                st.markdown("<br>", unsafe_allow_html=True)
                if st.form_submit_button("🛰️ Deploy Watchdog", use_container_width=True):
                    if not target_roles:
                        st.error("Please select at least one recipient role group.")
                    else:
                        # PAYLOAD UPDATED FOR ARRAY COLUMN (text[])
                        payload = {
                            "property_id": st.session_state.current_property_id,
                            "alert_name": a_name,
                            "metric_target": a_metric,
                            "threshold_val": float(a_val),
                            "comparison_operator": "<" if a_op == "Drops Below" else ">",
                            "user_email": st.session_state.user_email,
                            "target_role": target_roles, # Sends as Python list
                            "is_active": True
                        }
                        try:
                            supabase.table("strategic_alerts").insert(payload).execute()
                            st.success(f"Watchdog deployed to {len(target_roles)} groups.")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Deployment Error: {e}")

    with col_b:
        st.markdown("### 📋 Active Network Watchdogs")
        target_id = st.session_state.get('current_property_id')
        
        try:
            # Flexible Query logic
            query = supabase.table("strategic_alerts").select("*")
            if target_id != "GLOBAL":
                query = query.eq("property_id", str(target_id))
            
            alerts_res = query.execute()

            if alerts_res and alerts_res.data:
                for alert in alerts_res.data:
                    with st.container(border=True):
                        c1, c2 = st.columns([3, 1])
                        with c1:
                            st.markdown(f"**🔔 {alert.get('alert_name')}**")
                            
                            # RENDER ROLES AS BADGES
                            roles = alert.get('target_role', [])
                            if isinstance(roles, list):
                                role_badges = " ".join([f"`{r}`" for r in roles])
                            else:
                                # Fallback for old single-string data
                                role_badges = f"`{roles}`"
                                
                            st.markdown(f"**Routing to:** {role_badges}")
                            
                            op_display = "Below" if alert.get('comparison_operator') == "<" else "Above"
                            st.caption(f"Target: {alert.get('metric_target')} | Condition: {op_display} {alert.get('threshold_val')}")
                            st.caption(f"Creator: {alert.get('user_email')}")
                        with c2:
                            if st.button("Disable", key=f"dis_{alert['id']}", use_container_width=True):
                                supabase.table("strategic_alerts").delete().eq("id", alert['id']).execute()
                                st.cache_data.clear()
                                st.rerun()
            else:
                st.markdown("""
                    <div style="text-align:center; padding: 40px; color: #64748b; border: 2px dashed #e2e8f0; border-radius: 12px;">
                        No active watchdogs found in this sector.
                    </div>
                """, unsafe_allow_html=True)
                
        except Exception as e: 
            st.error(f"Monitoring Sync Error: {e}")

# =================================================================
# 18. PAGE 10: SCENARIO SIMULATION (v60.0 - Experiment Integrated)
# =================================================================
elif page == "Scenario Simulator":
    render_styled_header(
        "Predictive Scenario Simulator",
        "Market-Aware Demand Projection & A/B Test Integration",
        "Predictive"
    )

    with st.container(border=True):
        st.markdown("#### 🛠️ Configure Simulation Parameters")
        c1, c2, c3, c4 = st.columns(4)
        
        with c1:
            sim_date = st.date_input("Target Date", value=datetime.date.today() + datetime.timedelta(days=14))
            sim_season = st.selectbox("Business Season", ["Winter (Jan-Feb)", "Spring (Mar-Jun)", "Summer (Jul-Aug)", "Autumn (Sep-Nov)", "Peak (December)"])
        
        with c2:
            sim_event = st.number_input("Event Attendance", value=0, step=500)
            sim_clicks = st.number_input("Planned Ad Clicks", value=1000, step=100)
        
        with c3:
            sim_imps = st.number_input("Planned Impressions", value=50000, step=5000)
            sim_rain = st.slider("Rain (mm)", 0, 50, 0)
            
        with c4:
            sim_snow = st.slider("Snow (cm)", 0, 30, 0)

        # --- STEP 4 TIE-IN: THE EXPERIMENT OVERLAY ---
        st.divider()
        exp_col1, exp_col2 = st.columns([2, 1])
        with exp_col1:
            # This allows you to apply a "Winning" lift found in your Experiment Vault
            applied_test = st.text_input("Apply Experiment Lift (Tag Name)", placeholder="e.g. Test_V1")
        with exp_col2:
            test_lift_pct = st.number_input("Proven Lift %", value=0.0, step=0.5, help="Enter the % lift found in your Experiment Vault for this tag.")

        run_sim = st.button("🚀 Run Seasonal Scenario Projection", use_container_width=True)

    if run_sim:
        weights = st.session_state.coeffs
        try:
            dow = sim_date.strftime('%A')
            
            # 1. ESTABLISH LIFETIME BASELINE
            if 'df' in locals() and not df.empty:
                dow_history = df[(df['entry_date'].dt.day_name() == dow) & (df['actual_traffic'] > 0)].copy()
                lifetime_baseline = dow_history['actual_traffic'].mean() if not dow_history.empty else 1500
            else:
                lifetime_baseline = 1500

            # 2. APPLY SEASONAL MULTIPLIERS
            seasonal_map = {
                "Winter (Jan-Feb)": 0.85, "Spring (Mar-Jun)": 1.05,
                "Summer (Jul-Aug)": 1.15, "Autumn (Sep-Nov)": 1.20, "Peak (December)": 1.35
            }
            season_mult = seasonal_map.get(sim_season, 1.0)
            seasonal_baseline = lifetime_baseline * season_mult

            # 3. APPLY DNA MULTIPLIERS
            digital_lift = (sim_clicks * weights.get('Clicks', 0.05)) + (sim_imps * weights.get('Social_Imp', 0.0002))
            gravity_lift = sim_event * weights.get('Event_Gravity', 0.25)
            friction = (sim_rain * weights.get('Rain_mm', -12)) + (sim_snow * weights.get('Snow_cm', -45))
            
            # --- STEP 4 CALCULATION: APPLY EXPERIMENT LIFT ---
            test_impact = 0
            if test_lift_pct != 0:
                # Apply the proven lift to the seasonal baseline
                test_impact = seasonal_baseline * (test_lift_pct / 100)

            # 4. FINAL CALCULATION
            projected_guests = max(0, seasonal_baseline + digital_lift + gravity_lift + friction + test_impact)
            projected_rev = projected_guests * weights.get('Avg_Coin_In', 112.50)
            
            # --- OUTPUT ---
            st.divider()
            res1, res2, res3, res4 = st.columns(4)
            res1.metric(f"Lifetime {dow}", f"{lifetime_baseline:,.0f}")
            res2.metric("Seasonal Base", f"{seasonal_baseline:,.0f}", delta=f"{(season_mult-1)*100:+.0f}%")
            res3.metric("AI Projection", f"{projected_guests:,.0f}", delta=f"{test_impact:+.0f} from Test" if test_impact != 0 else None)
            res4.metric("Proj. Revenue", f"${projected_rev:,.0f}")

            # --- AI SPECIALIST MEMO ---
            st.markdown("<br>", unsafe_allow_html=True)
            with st.status("🕵️ AI Specialist generating strategic recommendations...", expanded=True) as status:
                genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                model = genai.GenerativeModel('gemini-2.5-flash')
                
                specialist_prompt = f"""
                As a Casino Marketing Specialist for {st.session_state.current_property_name}, analyze this scenario:
                Date: {sim_date} ({dow}) | Season: {sim_season}
                
                FORENSIC DATA:
                - Projected guests: {projected_guests:,.0f}
                - Weather friction: {friction:,.0f}
                - Applied Experiment: {applied_test if applied_test else 'None'} ({test_lift_pct}% lift)
                
                Provide a professional memo focusing on how to maximize this {test_lift_pct}% incremental lift 
                and protect the baseline from the weather impact.
                """
                
                ai_response = model.generate_content(specialist_prompt)
                status.update(label="✅ Strategic Memo Generated", state="complete")

            st.markdown(f"""
                <div style="background: #FFFFFF; padding: 25px; border-radius: 12px; border: 1px solid #E1E8F0; border-top: 5px solid #0047AB;">
                    <span style="font-weight: 800; color: #0047AB;">INTERNAL STRATEGIC MEMO</span>
                </div>
            """, unsafe_allow_html=True)
            st.markdown(ai_response.text)

        except Exception as e:
            st.error(f"Simulation Error: {e}")

# =================================================================
# 19. PAGE 11: EXPERIMENT VAULT (v60.6 - Unified Command Edition)
# =================================================================
elif page == "Experiment Vault":
    render_styled_header(
        "A/B Experimentation Vault",
        "Market Science & ROI Attribution",
        "Scientific"
    )

    tab_results, tab_manage = st.tabs(["📊 Performance Results", "⚙️ Manage Registry"])

    # --- TAB 1: PERFORMANCE RESULTS ---
    with tab_results:
        try:
            reg_res = supabase.table("experiment_registry").select("*").eq("property_id", st.session_state.current_property_id).execute()
            
            if reg_res.data:
                # 1. Selection Logic
                exp_options = {e['test_name']: e for e in reg_res.data}
                sel_exp_name = st.selectbox("Select Active Experiment:", list(exp_options.keys()))
                active_exp = exp_options[sel_exp_name]
                
                test_name = active_exp['test_name']
                tag_a = str(active_exp['version_a_tag']).strip()
                tag_b = str(active_exp['version_b_tag']).strip()
                
                st.markdown(f"#### 🔎 Unified Audit: {tag_a} vs {tag_b}")

                # --- 2. THE UNIFIED ANALYTICS ENGINE ---
                if 'df' in locals() and not df.empty:
                    # Clean and align tags
                    df['experiment_tag'] = df['experiment_tag'].astype(str).str.strip()
                    df_a = df[df['experiment_tag'] == tag_a]
                    df_b = df[df['experiment_tag'] == tag_b]
                    
                    if not df_a.empty and not df_b.empty:
                        st.divider()
                        rev_col = 'actual_coin_in'
                        
                        # PILLAR 1: VOLUME (Traffic)
                        vol_a, vol_b = df_a['actual_traffic'].mean(), df_b['actual_traffic'].mean()
                        vol_lift = ((vol_b - vol_a) / vol_a) * 100 if vol_a > 0 else 0
                        incremental_guests = (vol_b - vol_a) * len(df_b)
                        
                        # PILLAR 2: VALUE (Yield per Guest)
                        yield_a = df_a[rev_col].sum() / df_a['actual_traffic'].sum() if df_a['actual_traffic'].sum() > 0 else 0
                        yield_b = df_b[rev_col].sum() / df_b['actual_traffic'].sum() if df_b['actual_traffic'].sum() > 0 else 0
                        yield_delta = ((yield_b - yield_a) / yield_a) * 100 if yield_a > 0 else 0
                        
                        # PILLAR 3: REVENUE (Financial Impact)
                        total_revenue_lift = incremental_guests * yield_b

                        # --- EXECUTIVE SCOREBOARD ---
                        m1, m2, m3 = st.columns(3)
                        
                        with m1:
                            st.metric("Total Volume Lift", f"+{incremental_guests:,.0f} Guests", 
                                      delta=f"{vol_lift:.1f}% vs Baseline")
                        
                        with m2:
                            st.metric("Guest Value Shift", f"${yield_b:,.2f}", 
                                      delta=f"{yield_delta:.1f}% Yield")
                        
                        with m3:
                            st.metric("Estimated Rev Impact", f"${total_revenue_lift:,.0f}", 
                                      delta="Incremental")

                        st.divider()

                        # ROW 2: DECISION SUPPORT
                        c1, c2, c3 = st.columns(3)
                        
                        # Winner logic based on Total Performance (Volume * Yield)
                        is_winner = tag_b if (vol_b * yield_b) > (vol_a * yield_a) else tag_a
                        is_reliable = "✅ Highly Reliable" if len(df_b) >= 7 else "⚠️ Trending (Needs 7+ days)"
                        
                        c1.markdown(f"**Top Performing Version**<br><span style='color:#FFCC00; font-size:1.2rem; font-weight:bold;'>🏆 {is_winner}</span>", unsafe_allow_html=True)
                        c2.markdown(f"**Data Reliability**<br>{is_reliable}", unsafe_allow_html=True)
                        c3.markdown(f"**Test Sample Size**<br>{len(df_b)} Active Days", unsafe_allow_html=True)

                        # --- 3. AUTO-FOCUS AI SPECIALIST ---
                        st.markdown("<br>", unsafe_allow_html=True)
                        with st.expander("🕵️ Specialist's Integrated Verdict", expanded=True):
                            with st.spinner("Analyzing cross-metric correlations..."):
                                genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                                model = genai.GenerativeModel('gemini-2.5-flash')
                                
                                # Feed full context to the AI for holistic analysis
                                t_context = {
                                    "Volume_Change": f"{vol_lift:.1f}%",
                                    "Yield_Change": f"{yield_delta:.1f}%",
                                    "Revenue_Impact": f"${total_revenue_lift:,.0f}",
                                    "Incremental_Guests": f"{incremental_guests:,.0f}"
                                }
                                
                                prompt = f"""
                                You are a Senior Casino Marketing Director. Audit this A/B test for {st.session_state.current_property_name}.
                                Data Summary: {t_context}
                                
                                Provide a concise, energetic verdict:
                                1. Evaluate the trade-off: Did we sacrifice Guest Quality (Yield) to get Volume (Traffic)?
                                2. Business Case: Is the incremental revenue high enough to make this the new baseline?
                                3. Strategic Action: One clear next step for the marketing team.
                                """
                                
                                ai_report = model.generate_content(prompt)
                                st.markdown(ai_report.text)
                    else:
                        st.warning(f"🎰 Data Missing: Ensure ledger entries are tagged '{tag_a}' and '{tag_b}'.")
                else:
                    st.error("No ledger data available.")
            else:
                st.info("No experiments registered. Switch to 'Manage Registry' to create your first test.")
        except Exception as e:
            st.error(f"Registry Error: {e}")

    # --- TAB 2: MANAGE REGISTRY ---
    with tab_manage:
        st.subheader("🏗️ Provision New Experiment")
        with st.form("new_experiment_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                n_name = st.text_input("Experiment Name", placeholder="e.g. March Cars Promo")
                n_start = st.date_input("Launch Date")
            with c2:
                n_a = st.text_input("Control Tag (Version A)", value="Control")
                n_b = st.text_input("Test Tag (Version B)", value="Test_V1")
            
            n_obj = st.text_area("Strategic Objective")
            
            if st.form_submit_button("🚀 Deploy to Registry"):
                if n_name and n_a and n_b:
                    payload = {
                        "property_id": st.session_state.current_property_id, "test_name": n_name,
                        "version_a_tag": n_a.strip(), "version_b_tag": n_b.strip(),
                        "start_date": str(n_start), "objective": n_obj
                    }
                    supabase.table("experiment_registry").insert(payload).execute()
                    st.success(f"Experiment '{n_name}' live in registry.")
                    st.rerun()
                else:
                    st.error("Fill in all fields to deploy.")

        st.divider()
        st.subheader("📂 Active Registry")
        if reg_res.data:
            for exp in reg_res.data:
                with st.expander(f"🔬 {exp['test_name']} ({exp['version_a_tag']} vs {exp['version_b_tag']})"):
                    st.write(f"**Objective:** {exp['objective']}")
                    if st.button("🗑️ Delete Experiment", key=f"del_{exp['id']}", use_container_width=True):
                        supabase.table("experiment_registry").delete().eq("id", exp['id']).execute()
                        st.rerun()

# =================================================================
# 14. PAGE 6: PR SCORECARD & EARNED MEDIA (v1.3 - CRUD Enabled)
# =================================================================
elif page == "PR Scorecard":
    import datetime
    from dateutil.relativedelta import relativedelta
    
    today_pr = datetime.date.today()

    render_styled_header(
        f"PR Scorecard: {st.session_state.current_property_name}",
        "Tracking Earned Media Impact and Brand Authority",
        "Public Relations"
    )

    # --- 1. DATA RETRIEVAL ---
    pr_res = supabase.table("pr_scorecard").select("*").eq("property_id", st.session_state.current_property_id).order("report_month", desc=True).execute()
    df_pr = pd.DataFrame(pr_res.data) if pr_res.data else pd.DataFrame()

    # --- 2. DATA ENTRY MODAL ---
    with st.expander("📝 Log Monthly PR Metrics", expanded=False):
        with st.form("pr_entry_form", clear_on_submit=True):
            f1, f2, f3 = st.columns(3)
            with f1: m_date = st.date_input("Report Month", value=today_pr.replace(day=1))
            with f2: m_imp = st.number_input("Earned Impressions", min_value=0, step=1000)
            with f3: m_ment = st.number_input("Earned Mentions", min_value=0, step=1)
            
            m_mediums = st.text_input("Primary Mediums (e.g., CTV News, Ottawa Citizen)")
            m_comment = st.text_area("Executive Commentary (Key Wins/Narrative)")
            
            if st.form_submit_button("Vault PR Entry", use_container_width=True):
                entry = {
                    "property_id": st.session_state.current_property_id,
                    "report_month": m_date.strftime("%Y-%m-%d"),
                    "earned_impressions": m_imp,
                    "earned_mentions": m_ment,
                    "mediums": m_mediums,
                    "executive_summary": m_comment
                }
                supabase.table("pr_scorecard").upsert(entry, on_conflict="property_id, report_month").execute()
                st.success(f"PR Metrics for {m_date.strftime('%B %Y')} Vaulted.")
                st.rerun()

    if df_pr.empty:
        st.info("The PR Scorecard vault is currently empty. Log your first month to see analytics.")
        st.stop()

    # --- 3. DATA PREP & CALCULATIONS ---
    df_pr['report_month'] = pd.to_datetime(df_pr['report_month'])
    curr = df_pr.iloc[0]
    prev = df_pr.iloc[1] if len(df_pr) > 1 else curr
    avg_3m_df = df_pr.head(3)
    avg_3m_imp = avg_3m_df['earned_impressions'].mean()
    avg_3m_ment = avg_3m_df['earned_mentions'].mean()

    # --- 4. METRIC CARDS: MOM PERFORMANCE ---
    st.markdown("### 📊 Performance against MoM Baseline")
    k1, k2 = st.columns(2)
    imp_mom_pct = ((curr['earned_impressions'] - prev['earned_impressions']) / prev['earned_impressions'] * 100) if prev['earned_impressions'] > 0 else 0
    k1.metric("Earned Media Impressions", f"{curr['earned_impressions']:,}", delta=f"{imp_mom_pct:+.1f}% MoM")

    ment_mom_pct = ((curr['earned_mentions'] - prev['earned_mentions']) / prev['earned_mentions'] * 100) if prev['earned_mentions'] > 0 else 0
    k2.metric("Earned Media Mentions", f"{curr['earned_mentions']}", delta=f"{ment_mom_pct:+.1f}% MoM")

    # --- 5. METRIC CARDS: 3-MONTH AVERAGE PERFORMANCE ---
    st.markdown("### 🏛️ Performance against 3-Month Average")
    k3, k4 = st.columns(2)
    imp_3m_pct = ((curr['earned_impressions'] - avg_3m_imp) / avg_3m_imp * 100) if avg_3m_imp > 0 else 0
    k3.metric("Earned Media Impressions", f"{curr['earned_impressions']:,}", delta=f"{imp_3m_pct:+.1f}% vs 3M Avg")

    ment_3m_pct = ((curr['earned_mentions'] - avg_3m_ment) / avg_3m_ment * 100) if avg_3m_ment > 0 else 0
    k4.metric("Earned Media Mentions", f"{curr['earned_mentions']}", delta=f"{ment_3m_pct:+.1f}% vs 3M Avg")

    # --- 6. VISUAL PERFORMANCE TREND ---
    st.write("### 📈 Earned Media Traction Trend")
    fig_pr = go.Figure()
    df_chart = df_pr.sort_values('report_month')
    fig_pr.add_trace(go.Scatter(x=df_chart['report_month'], y=df_chart['earned_impressions'], name="Impressions", line=dict(color='#FFCC00', width=4), yaxis="y"))
    fig_pr.add_trace(go.Bar(x=df_chart['report_month'], y=df_chart['earned_mentions'], name="Mentions", marker_color='rgba(255, 255, 255, 0.2)', yaxis="y2"))
    fig_pr.update_layout(template="plotly_dark", yaxis=dict(title="Impressions", showgrid=False), yaxis2=dict(title="Mentions", overlaying="y", side="right", showgrid=False),
                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), margin=dict(l=10, r=10, t=30, b=10), height=400, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_pr, use_container_width=True)

    # --- 7. EXECUTIVE SUMMARY & CONTROLS ---
    st.write("### 📜 Monthly PR Narrative Archive")
    for index, row in df_pr.iterrows():
        date_label = row['report_month'].strftime('%B %Y')
        row_id = row['id']
        
        with st.expander(f"Audit: {date_label} — {row['mediums']}", expanded=(index==0)):
            st.markdown(f"**Earned Reach:** {row['earned_impressions']:,} impressions across {row['earned_mentions']} placements.")
            st.info(row['executive_summary'] if row['executive_summary'] else "No summary vaulted for this period.")
            
            # Action Buttons
            c1, c2, _ = st.columns([1, 1, 4])
            with c1:
                if st.button("✏️ Edit", key=f"edit_{row_id}"):
                    st.session_state.edit_pr_id = row_id
                    st.rerun()
            with c2:
                if st.button("🗑️ Delete", key=f"del_{row_id}"):
                    supabase.table("pr_scorecard").delete().eq("id", row_id).execute()
                    st.toast(f"Deleted {date_label} entry.")
                    st.rerun()

    # --- 8. EDIT DIALOG ---
    if st.session_state.get('edit_pr_id'):
        @st.dialog("Edit PR Record")
        def edit_pr_dialog(entry_id):
            target = df_pr[df_pr['id'] == entry_id].iloc[0]
            with st.form("edit_pr_form"):
                new_imp = st.number_input("Impressions", value=int(target['earned_impressions']))
                new_ment = st.number_input("Mentions", value=int(target['earned_mentions']))
                new_med = st.text_input("Mediums", value=target['mediums'])
                new_sum = st.text_area("Summary", value=target['executive_summary'])
                if st.form_submit_button("Save Changes"):
                    supabase.table("pr_scorecard").update({"earned_impressions": new_imp, "earned_mentions": new_ment, "mediums": new_med, "executive_summary": new_sum}).eq("id", entry_id).execute()
                    st.session_state.edit_pr_id = None
                    st.rerun()
            if st.button("Cancel"):
                st.session_state.edit_pr_id = None
                st.rerun()
        
        edit_pr_dialog(st.session_state.edit_pr_id)

# =================================================================
# 18. FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
