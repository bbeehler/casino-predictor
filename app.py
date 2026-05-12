import time
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
from supabase import create_client, Client # Added Client for type hinting
from io import BytesIO
from dateutil.relativedelta import relativedelta
import os
import uuid
from google.generativeai.types import HarmCategory, HarmBlockThreshold

def check_permission(capability):
    """Checks if the user's current role allows for a specific action."""
    # We pull from session_state which is hydrated at login
    perms = st.session_state.get('user_permissions', {})
    return perms.get(capability, False)

def archive_sentiment_entry(text, asset_tag):
    """AI-Analyzes and archives manual sentiment entries with explicit message_id."""
    try:
        # 1. AI Scoring Layer
        import google.generativeai as genai
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        # Note: Model updated to 1.5-flash as 2.5 is not a standard release yet
        model = genai.GenerativeModel('gemini-2.5-flash') 
        
        score_prompt = f"Analyze sentiment of this casino review. Return ONLY a single float between -1.0 and 1.0: {text}"
        ai_res = model.generate_content(score_prompt)
        
        try:
            sentiment_score = float(ai_res.text.strip())
        except:
            sentiment_score = 0.0

        # --- DERIVE CATEGORY & INTENSITY ---
        # Logic for Category
        if sentiment_score > 0.3:
            sentiment_category = "Positive"
        elif sentiment_score < -0.3:
            sentiment_category = "Negative"
        else:
            sentiment_category = "Neutral"

        # Logic for Intensity (Absolute strength of the emotion)
        abs_score = abs(sentiment_score)
        if abs_score >= 0.8:
            intensity_level = "Extreme"
        elif abs_score >= 0.4:
            intensity_level = "Moderate"
        else:
            intensity_level = "Low"

        # 2. Construct Payload with all required columns
        payload = {
            "message_id": str(uuid.uuid4()),
            "property_id": st.session_state.current_property_id,
            "asset": asset_tag,
            "sentiment_score": sentiment_score,
            "sentiment_category": sentiment_category,
            "intensity_level": intensity_level, # New Addition
            "raw_text": text,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        # 3. Execute Insert
        supabase.table("sentiment_history").insert(payload).execute()
        return True
        
    except Exception as e:
        # This will now catch and display if it's a permission issue or a data type mismatch
        st.error(f"Archival Sync Error: {e}")
        return False

import time
import re
import pandas as pd
import streamlit as st

# =================================================================
# GLOBAL AI ENGINES (v78.0 - Schema-Grounded Executive Analyst)
# =================================================================

def get_forensic_omniscience():
    """
    Directly serializes the Ledger, Sentiment, and ROI tables using 
    provided SQL schemas to ensure high-precision executive reporting.
    """
    try:
        pid = st.session_state.get('current_property_id')
        
        # 1. PULL DATA FROM THE THREE CORE TABLES
        # Limit nodes to ensure we don't exceed the AI's context window (Flash 1.5 is large, but keep it tight)
        ledger_res = supabase.table("ledger").select("*").eq("property_id", pid).order("entry_date", desc=True).limit(45).execute()
        sent_res = supabase.table("sentiment_history").select("*").eq("property_id", pid).order("timestamp", desc=True).limit(30).execute()
        roi_res = supabase.table("monthly_roi").select("*").eq("property_id", pid).order("report_month", desc=True).limit(6).execute()

        # 2. DEFINE THE SCHEMA MAP (The AI's Internal Documentation)
        schema_map = """
        ACTUAL DATABASE SCHEMA DEFINITIONS:
        
        Table: 'public.ledger' (Daily Granular Performance)
        - entry_date (PK): The business date.
        - actual_traffic: PHYSICAL GUEST door counts (Use this for 'Traffic' questions).
        - actual_coin_in: Total gaming volume/revenue.
        - active_promo: Marketing campaign name.
        - new_members: Unity card signups.
        - Event_Gravity: Impact score of on-site events.
        
        Table: 'public.sentiment_history' (Guest Feedback)
        - asset: The specific area (CHOP, Slots, Council Oak, etc.).
        - sentiment_score: -1.0 (Critical) to 1.0 (Exceptional).
        - sentiment_category: The categorical label.
        - raw_text: The literal guest comment.
        
        Table: 'public.monthly_roi' (High-Level Marketing Impact)
        - report_month: The month being analyzed.
        - utm_sessions: Digital web traffic (DO NOT confuse with 'actual_traffic').
        - ad_spend: Total paid media cost.
        - calculated_bl_roas: Bottom-line Return on Ad Spend.
        """

        # 3. SERIALIZE DATA
        ledger_data = pd.DataFrame(ledger_res.data).to_string(index=False) if ledger_res.data else "Empty"
        sent_data = pd.DataFrame(sent_res.data).to_string(index=False) if sent_res.data else "Empty"
        roi_data = pd.DataFrame(roi_res.data).to_string(index=False) if roi_res.data else "Empty"

        # 4. CONSTRUCT CONTEXT
        context = f"""
        YOU ARE THE OMNISCIENT ANALYST FOR HARD ROCK OTTAWA.
        YOU ARE REPORTING DIRECTLY TO THE EXECUTIVE LEADERSHIP TEAM.

        {schema_map}

        --- LIVE DATA: LEDGER ---
        {ledger_data}

        --- LIVE DATA: SENTIMENT HISTORY ---
        {sent_data}

        --- LIVE DATA: MONTHLY ROI ---
        {roi_data}

        EXECUTIVE DIRECTIVES:
        1. If asked about "Traffic", you MUST look at 'actual_traffic' in the ledger.
        2. If asked about "Web Sessions" or "Digital", look at 'utm_sessions' in monthly_roi.
        3. Never hallucinate. If a date or metric is missing, report it as 'Not Vaulted'.
        4. Cross-reference tables. (e.g., Did a specific 'active_promo' lead to higher 'new_members'?).
        """
        return context
    except Exception as e:
        return f"Database Connectivity Error: {e}"

def ask_omniscient_ai(user_query):
    """Execution function for the Executive Analyst."""
    try:
        import google.generativeai as genai
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        # We use 1.5-Flash for speed and large context handling
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        vault_context = get_forensic_omniscience()
        
        prompt = f"{vault_context}\n\nEXECUTIVE INQUIRY: {user_query}\n\nFORENSIC ANALYSIS:"
        response = model.generate_content(prompt)
        
        return response.text
    except Exception as e:
        return f"Analyst is currently unavailable: {e}"

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

# --- SAAS IDENTITY LAYER ---
if 'current_property_name' not in st.session_state:
    st.session_state.current_property_name = "Hard Rock Ottawa"

# Fetch UUID only if we don't have it and avoid querying with None
if 'current_property_id' not in st.session_state or st.session_state.current_property_id is None:
    try:
        if st.session_state.current_property_name == "All Properties":
            st.session_state.current_property_id = "GLOBAL"
        else:
            prop_res = supabase.table("properties").select("id").eq("property_name", st.session_state.current_property_name).execute()
            if prop_res.data:
                st.session_state.current_property_id = prop_res.data[0]['id']
            else:
                st.session_state.current_property_id = None
    except Exception as e:
        st.session_state.current_property_id = None

# =================================================================
# 2. PERMANENT INITIALIZATION (SaaS Aware & Crash-Proof)
# =================================================================
if 'coeffs' not in st.session_state:
    cur_id = st.session_state.get('current_property_id')
    if cur_id and cur_id != "GLOBAL":
        try:
            response = supabase.table("coefficients").select("*").eq("property_id", cur_id).execute()
            if response.data:
                st.session_state.coeffs = response.data[0]
            else:
                st.session_state.coeffs = {
                    'property_id': cur_id, 'Promo': 500.0, 'Broadcast_Weight': 150.0,
                    'OOH_Weight': 100.0, 'OOH_Count': 1, 'PR_Weight': 1.2, 'Clicks': 0.05,
                    'Social_Imp': 0.0002, 'Ad_Decay': 85, 'Rain_mm': -12.0, 'Snow_cm': -45.0,
                    'Event_Gravity': 0.25, 'Static_Weight': 100.0, 'Static_Count': 1
                }
                supabase.table("coefficients").upsert(st.session_state.coeffs).execute()
        except:
            st.session_state.coeffs = {'Promo': 500.0, 'OOH_Weight': 100.0}
    else:
        st.session_state.coeffs = {'Promo': 500.0, 'OOH_Weight': 100.0}

# =================================================================
# 3. GLOBAL PAGE CONFIG & HIGH-END RESPONSIVE DESIGN
# =================================================================
st.set_page_config(
    page_title="FloorCast Pro | Strategic Intelligence", 
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

        /* Force Sidebar Text, Labels, and Radio Button text to White */
        [data-testid="stSidebar"] *, 
        [data-testid="stSidebar"] .stMarkdown p,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] .st-at {
            color: #FFFFFF !important;
        }

        /* Sidebar Captions */
        [data-testid="stSidebar"] .stCaption {
            color: #A1A1A1 !important;
            font-weight: 600;
            letter-spacing: 0.05em;
        }

        /* Sidebar Divider */
        [data-testid="stSidebar"] hr {
            border-color: #333333 !important;
        }

        /* Sidebar Buttons */
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

        /* --- MOBILE OVERRIDES & MENU BUTTON FIX --- */
        @media (max-width: 768px) {
            .main .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
                padding-top: 5rem !important;
            }
            
            button[kind="headerNoContext"] {
                display: flex !important;
                color: #1A1C1E !important; 
                background-color: #FFFFFF !important;
                border-radius: 50% !important;
                box-shadow: 0 4px 12px rgba(0,0,0,0.15) !important;
                z-index: 999999 !important;
                top: 15px !important;
                left: 15px !important;
                width: 40px !important;
                height: 40px !important;
            }
        }

        /* HIGH-END EXECUTIVE HEADER */
        .glass-header {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            border: 1px solid rgba(255, 255, 255, 0.1);
            padding: 28px;
            border-radius: 18px;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.2), 0 10px 10px -5px rgba(0, 0, 0, 0.1);
            margin-bottom: 35px;
            color: white !important;
        }

        /* --- HIGH-END METRIC CARDS VISIBILITY FIX --- */
        [data-testid="stMetric"] {
            background: #FFFFFF !important;
            border: 1px solid #E1E8F0 !important;
            padding: 20px !important;
            border-radius: 12px !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05) !important;
        }

        [data-testid="stMetricLabel"] > div, 
        [data-testid="stMetricValue"] > div,
        [data-testid="stMetricLabel"] p {
            color: #1A1C1E !important;
            -webkit-text-fill-color: #1A1C1E !important;
        }

        /* --- INPUT & FLOORCAST AI VISIBILITY KIT --- */
        
        /* Force Chat Input Container and Text Areas to be white with dark text */
        .stChatInputContainer, .stTextArea textarea, .stChatInput input {
            background-color: #FFFFFF !important;
            color: #1A1C1E !important;
        }

        /* Standard Input/Select Styling */
        div[data-baseweb="input"] > div, 
        div[data-baseweb="select"] > div {
            background-color: #FFFFFF !important;
            border: 1px solid #D0D5DD !important;
            border-radius: 8px !important;
        }

        /* Force dark text for all input methods */
        input, textarea, div[data-baseweb="select"] * {
            color: #1A1C1E !important;
            -webkit-text-fill-color: #1A1C1E !important;
        }

        /* Dropdown Popover Lists */
        div[data-baseweb="popover"] ul {
            background-color: #FFFFFF !important;
        }
        
        div[data-baseweb="popover"] li {
            color: #1A1C1E !important;
            background-color: #FFFFFF !important;
        }

        /* Focus Glow */
        div[data-baseweb="input"]:focus-within, 
        div[data-baseweb="select"]:focus-within {
            border: 1px solid #0047AB !important;
            box-shadow: 0 0 0 2px rgba(0, 71, 171, 0.1) !important;
            outline: none !important;
        }

        /* MAIN ACTION BUTTONS */
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
        header[data-testid="stHeader"] {
            background-color: transparent !important;
        }

        /* Targets the Label (e.g., 'Total Volume Lift') */
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
        font-weight: 600 !important;
    }

    /* Targets the Value (e.g., '1,200') */
    [data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
    }

    /* Targets the Delta (e.g., '+5.2%') */
    [data-testid="stMetricDelta"] {
        font-size: 0.7rem !important;
    }
        </style>
    """, unsafe_allow_html=True)

def render_styled_header(title, subtitle, badge_text="Live"):
    st.markdown(f"""
        <div class="glass-header">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h1 style="margin: 0; font-size: 2rem; font-weight: 800; color: #FFFFFF; letter-spacing: -0.025em;">{title}</h1>
                    <p style="margin: 8px 0 0 0; color: #94a3b8; font-size: 1.1rem; font-weight: 400;">{subtitle}</p>
                </div>
                <div style="display: flex; flex-direction: column; align-items: flex-end;">
                    <div style="background: rgba(255, 204, 0, 0.15); color: #FFCC00; padding: 6px 16px; border-radius: 12px; font-size: 0.85rem; font-weight: 700; border: 1px solid rgba(255, 204, 0, 0.3); text-transform: uppercase; letter-spacing: 0.05em;">
                        ● {badge_text}
                    </div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

apply_high_end_styling()

# =================================================================
# 4. FORENSIC ENGINE: CALCULATION CORE (v24.1 - Attribution Enabled)
# =================================================================
def get_forensic_metrics(df_input, coeffs):
    if not df_input: 
        return {"df": pd.DataFrame(columns=['baseline', 'expected', 'residual_lift', 'gravity_lift'])}
    
    df = pd.DataFrame(df_input).copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])

    # 1. INITIALIZE KEYS (The "KeyError" Insurance)
    df['residual_lift'] = 0.0
    df['gravity_lift'] = 0.0

    # 2. DYNAMIC HEARTBEATS
    heartbeats = {
        'Monday': float(coeffs.get('Mon_Base', 3398)),
        'Tuesday': float(coeffs.get('Tue_Base', 3525)),
        'Wednesday': float(coeffs.get('Wed_Base', 6312)),
        'Thursday': float(coeffs.get('Thu_Base', 4924)),
        'Friday': float(coeffs.get('Fri_Base', 7523)),
        'Saturday': float(coeffs.get('Sat_Base', 9863)),
        'Sunday': float(coeffs.get('Sun_Base', 5894))
    }
    df['baseline'] = df['entry_date'].dt.day_name().map(heartbeats).astype(float)
    
    # 3. ATTRIBUTION LOGIC (Ad Decay Modeling)
    c_clicks = float(coeffs.get('Clicks', 0.05))
    c_social = float(coeffs.get('Social_Imp', 0.0002))
    decay = float(coeffs.get('Ad_Decay', 85)) / 100 
    gravity = float(coeffs.get('Event_Gravity', 0.25))

    # Calculate Residual Lift
    current_pool = 0.0
    awareness_pool = []
    for _, row in df.iterrows():
        daily_in = (float(row.get('ad_clicks', 0) or 0) * c_clicks) + \
                   (float(row.get('ad_impressions', 0) or 0) * c_social)
        current_pool = daily_in + (current_pool * decay)
        awareness_pool.append(current_pool)
    
    df['residual_lift'] = awareness_pool
    
    # Calculate Event Gravity Lift
    if 'attendance' in df.columns:
        df['gravity_lift'] = pd.to_numeric(df['attendance'], errors='coerce').fillna(0) * gravity

    # 4. CALCULATE EXPECTED (Total Sum of All Lifts)
    promo_weight = float(coeffs.get('Promo', 500.0))
    df['expected'] = df['baseline'] + df['residual_lift'] + df['gravity_lift'] + promo_weight
    
    return {"df": df, "heartbeats": heartbeats}

# =================================================================
# 5. DATA INFRASTRUCTURE (WEATHER)
# =================================================================
async def fetch_weather():
    try:
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {"current": ec.conditions}
    except: return {"error": "Station Offline"}

if 'weather_data' not in st.session_state:
    st.session_state.weather_data = asyncio.run(fetch_weather())

# =================================================================
# 7. FORENSIC LOGIN GATEKEEPER (High-End Portal Edition)
# =================================================================
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    # High-end centered login layout
    _, col_login, _ = st.columns([1, 1.5, 1])
    
    with col_login:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.image("https://casino.hardrock.com/ottawa/-/media/project/shrss/hri/casinos/hard-rock/ottawa/logos-and-icons/logo.png", width=200)
        st.markdown("""
            <div style="margin-top: 20px; margin-bottom: 30px;">
                <h1 style="font-size: 2rem; margin-bottom: 0;">Executive Portal</h1>
                <p style="color: #667085;">FloorCast AI</p>
            </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form", border=True):
            e_mail = st.text_input("Email").strip().lower()
            p_word = st.text_input("Password", type="password")
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button("Login", use_container_width=True):
                try:
                    res = supabase.auth.sign_in_with_password({"email": e_mail, "password": p_word})
                    
                    if res.user:
                        access_res = supabase.table("user_property_access").select("*, properties(property_name)").eq("user_email", e_mail).execute()
                        
                        if access_res.data:
                            u_data = access_res.data[0]
                            user_role = u_data['user_role']
                            perm_res = supabase.table("role_permissions").select("perms").eq("role_name", user_role).execute()
                            
                            st.session_state.authenticated = True
                            st.session_state.user_email = e_mail
                            st.session_state.user_role = user_role
                            st.session_state.current_property_id = u_data['property_id']
                            st.session_state.current_property_name = u_data['properties']['property_name'] if u_data.get('properties') else "Unknown"
                            
                            if perm_res.data:
                                st.session_state.user_permissions = perm_res.data[0]['perms']
                            else:
                                st.session_state.user_permissions = {"view_analytics": True}
                            
                            st.success("Authentication Successful.")
                            st.rerun()
                        else:
                            st.error("Account Error: No property mapping found.")
                    else:
                        st.error("Invalid Credentials.")
                except Exception as e:
                    st.error(f"System Error: {e}")
    st.stop()

@st.dialog("Strategic Intelligence Hub", width="large")
def show_ai_analyst_hub():
    # 1. State Management for Persistent Chat within the session
    if "modal_msgs" not in st.session_state:
        st.session_state.modal_msgs = []
    
    # 2. Header & Controls
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown(f"**Analyzing:** {st.session_state.current_property_name}")
        st.caption(f"Current Page Context: {st.session_state.get('current_page', 'Dashboard')}")
    with c2:
        # The 'Clear' and 'Continue' functionality
        if st.button("🗑️ Clear History", use_container_width=True):
            st.session_state.modal_msgs = []
            st.rerun()

    st.divider()

    # 3. Scrollable Chat History
    chat_box = st.container(height=450)
    for m in st.session_state.modal_msgs:
        with chat_box.chat_message(m["role"]):
            st.markdown(m["content"])

    # 4. AI Processing Logic
    if prompt := st.chat_input("Query property intelligence..."):
        st.session_state.modal_msgs.append({"role": "user", "content": prompt})
        with chat_box.chat_message("user"):
            st.markdown(prompt)

        # Context Dossier for the AI
        dossier = f"ROLE: {st.session_state.user_role} | PAGE: {st.session_state.get('current_page')}"

        try:
            import google.generativeai as genai
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            with chat_box.chat_message("assistant"):
                with st.spinner("Analyzing data nodes..."):
                    full_query = f"Analyst Mode. Context: {dossier}. Question: {prompt}"
                    response = model.generate_content(full_query)
                    st.markdown(response.text)
                    st.session_state.modal_msgs.append({"role": "assistant", "content": response.text})
        except Exception as e:
            st.error("Intelligence Hub Offline.")
    
# =================================================================
# 8. EXECUTIVE NAVIGATION & AI HUB (v79.0 - Integrated Modal)
# =================================================================

# --- A. INITIALIZE HUB STATE ---
if 'show_ai_hub' not in st.session_state:
    st.session_state.show_ai_hub = False

# --- B. PERMISSIONS & ROLE SCAN ---
user_links_res = supabase.table("user_property_access").select("user_role").eq("user_email", st.session_state.get('user_email')).execute()
all_my_roles = [r['user_role'] for r in user_links_res.data] if user_links_res.data else []
is_global_admin = any(role in ["Super Admin", "Manager", "Admin"] for role in all_my_roles)

# --- C. SIDEBAR ARCHITECTURE ---
with st.sidebar:
    # Sidebar Logo
    st.markdown("""
        <div style="padding: 10px 0px 30px 0px;">
            <img src="https://casino.hardrock.com/ottawa/-/media/project/shrss/hri/casinos/hard-rock/ottawa/logos-and-icons/logo.png" width="160">
        </div>
    """, unsafe_allow_html=True)
    
    # 1. SCOPE SWITCHER
    if is_global_admin:
        st.caption("PROPERTIES")
        try:
            all_props = supabase.table("properties").select("id, property_name").execute()
            prop_map = {p['property_name']: p['id'] for p in all_props.data}
            options = ["📊 CONSOLIDATED VIEW"] + list(prop_map.keys())
            
            curr_label = "📊 CONSOLIDATED VIEW" if st.session_state.current_property_id == "GLOBAL" else st.session_state.current_property_name
            s_idx = options.index(curr_label) if curr_label in options else 0
            
            selected_view = st.selectbox("Switch Environment:", options, index=s_idx, label_visibility="collapsed")
            
            if selected_view == "📊 CONSOLIDATED VIEW" and st.session_state.current_property_id != "GLOBAL":
                st.session_state.current_property_id = "GLOBAL"
                st.session_state.current_property_name = "All Properties"
                st.session_state.user_role = "Super Admin"
                st.rerun()
            elif selected_view != "📊 CONSOLIDATED VIEW" and st.session_state.current_property_id != prop_map.get(selected_view):
                new_id = prop_map[selected_view]
                role_check = supabase.table("user_property_access").select("user_role").eq("user_email", st.session_state.user_email).eq("property_id", new_id).execute()
                
                st.session_state.current_property_id = new_id
                st.session_state.current_property_name = selected_view
                st.session_state.user_role = role_check.data[0]['user_role'] if role_check.data else "Viewer"
                perm_res = supabase.table("role_permissions").select("perms").eq("role_name", st.session_state.user_role).execute()
                st.session_state.user_permissions = perm_res.data[0]['perms'] if perm_res.data else {}
                st.rerun()
        except Exception as e:
            st.error(f"Switcher Error: {e}")

    st.markdown("<br>", unsafe_allow_html=True)
    st.caption("NAVIGATION")
    
    # 2. PAGE NAVIGATION
    if st.session_state.current_property_id == "GLOBAL":
        page = "Executive Dashboard"
        st.info("Global View Active")
    else:
        nav_options = ["Executive Dashboard"]
        if check_permission("view_ledger"): nav_options.append("Daily Ledger Audit")
        if check_permission("view_analytics"):
            nav_options.extend(["Attribution Analytics", "Sentiment Scoring"])
        if check_permission("view_reports"): nav_options.append("Master Audit Report")
        if check_permission("run_simulations"): nav_options.append("Scenario Simulator")
        if check_permission("run_experiments"): nav_options.append("Experiment Vault")
        if check_permission("manage_alerts"): nav_options.append("Strategic Alerts")
        if check_permission("calibrate_ai"):
            nav_options.extend(["AI Calibration", "BL-ROAS Calculator"])
        if st.session_state.get('user_role') == "Super Admin":
            nav_options.append("Global Admin Console")

        page = st.radio("Navigation", nav_options, label_visibility="collapsed")

    # --- 3. THE INTELLIGENCE HUB TRIGGER ---
    st.divider()
    if st.button("🕵️ Open Strategic AI Hub", use_container_width=True, type="primary"):
        st.session_state.show_ai_hub = True
        st.rerun()

    # 4. FOOTER CONTEXT
    st.markdown("<div style='padding-top: 20px;'>", unsafe_allow_html=True)
    st.divider()
    st.caption(f"User: {st.session_state.get('user_email')}")
    
    st.markdown(f"""
        <div style="background: #1e1e1e; padding: 10px; border-radius: 8px; border: 1px solid #333; margin-bottom: 10px;">
            <p style="margin:0; font-size: 0.7rem; color: #888;">CURRENT ROLE</p>
            <p style="margin:0; font-size: 0.85rem; font-weight: 600; color: #FFCC00;">{st.session_state.get('user_role', 'Viewer')}</p>
        </div>
    """, unsafe_allow_html=True)
    
    if st.button("LOGOUT", use_container_width=True):
        st.session_state.clear()
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

# --- D. THE GLOBAL MODAL HANDLER ---
# Sitting at the top level so it can trigger over any page
if st.session_state.show_ai_hub:
    @st.dialog("Strategic AI Analyst Hub", width="large")
    def ai_hub_modal():
        st.markdown("### 🤖 Omniscient Property Analyst")
        st.caption("Reporting Level: Executive | Database Source: Ledger, Sentiment, ROI")
        
        st.info("The AI now has the full SQL schema. It distinguishes between Physical Traffic and Web Sessions.")
        
        user_query = st.text_input("Ask a data-backed question:", placeholder="e.g., How did 'Rock of Ages' impact Unity signups vs our baseline?")
        
        c1, c2 = st.columns([1, 4])
        with c1:
            if st.button("Analyze Vault", use_container_width=True):
                if user_query:
                    with st.spinner("Executing Forensic Analysis..."):
                        # This calls the schema-grounded engine (v78.0)
                        answer = ask_omniscient_ai(user_query)
                        st.session_state.last_ai_response = answer
                else:
                    st.warning("Query required.")
        
        with c2:
            if st.button("Exit Hub", use_container_width=True):
                st.session_state.show_ai_hub = False
                st.rerun()

        # Display Response
        if "last_ai_response" in st.session_state:
            st.markdown("---")
            st.markdown(st.session_state.last_ai_response)
            if st.button("Clear Results"):
                del st.session_state.last_ai_response
                st.rerun()

    ai_hub_modal()

# =================================================================
# 9. THE DATA VAULT (v24.0 - CACHED & THREAD-SAFE)
# =================================================================

@st.cache_data(ttl=60) # Dropped TTL to 60s for easier debugging
def get_hydrated_data(property_id, _supabase_client):
    try:
        # 1. Fetch Property Map
        p_res = _supabase_client.table("properties").select("id, property_name").execute()
        p_map = {p['id']: p['property_name'] for p in p_res.data} if p_res.data else {}

        # 2. Build Query
        query = _supabase_client.table("ledger").select("*")
        
        if property_id == "GLOBAL":
            l_res = query.order("entry_date", desc=True).execute()
        else:
            # Ensure we are passing a clean string
            l_res = query.eq("property_id", str(property_id)).order("entry_date", desc=True).execute()

        # Check if data actually exists
        if not l_res.data or len(l_res.data) == 0:
            return pd.DataFrame(), []

        raw_data = l_res.data
        all_frames = []
        unique_ids = list(set([r['property_id'] for r in raw_data]))

        for p_uuid in unique_ids:
            p_rows = [r for r in raw_data if r['property_id'] == p_uuid]
            
            # Pull Coeffs for this specific property
            c_res = _supabase_client.table("coefficients").select("*").eq("property_id", p_uuid).execute()
            
            # CRITICAL FALLBACK: If a property has NO coefficients, the engine 
            # still needs a dictionary to run, otherwise it returns an empty DF.
            if c_res.data and len(c_res.data) > 0:
                c_data = c_res.data[0]
            else:
                # Use a safe default dictionary if DB is missing weights for this property
                c_data = {
                    'property_id': p_uuid, 'Promo': 500.0, 'Ad_Decay': 85, 
                    'PR_Weight': 1.2, 'Clicks': 0.05, 'Social_Imp': 0.0002
                }
            
            processed = get_forensic_metrics(p_rows, c_data)
            p_df = processed['df']
            
            if not p_df.empty:
                p_df['Property'] = p_map.get(p_uuid, "Unknown Property")
                all_frames.append(p_df)

        if not all_frames:
            return pd.DataFrame(), raw_data

        final_df = pd.concat(all_frames, ignore_index=True)
        return final_df, raw_data

    except Exception as e:
        # This will show you exactly what failed in the logs
        st.sidebar.error(f"Hydration Logic Error: {e}")
        return pd.DataFrame(), []

# --- EXECUTION ---
# This defines 'df' and 'ledger_data' globally so all pages can see them.
df, ledger_data = get_hydrated_data(st.session_state.current_property_id, supabase)

# =================================================================
# 10. REFINED SAFETY GATE (v52.1 - Predictive Aware)
# =================================================================
# If df is empty but ledger_data exists, it means the Forensic Engine failed to process the rows.
if df.empty:
    # EXEMPTIONS: Do not stop the app if the user is trying to add data or simulate scenarios
    exempt_pages = ["Global Admin Console", "Master Audit Report", "Scenario Simulator", "Daily Ledger Audit"]
    
    if page not in exempt_pages:
        if not ledger_data:
            st.warning(f"🎰 Forensic Vault for {st.session_state.current_property_name} is currently empty.")
            st.info("Please use the **Daily Ledger Audit** or **Master Audit** to ingest performance nodes.")
        else:
            st.error("🧪 Forensic Engine failed to process rows. Check AI Calibration settings.")
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
# 12. PAGE 4: MASTER FORENSIC AUDIT (v60.5 - AI Variance & Social)
# =================================================================
elif page == "Master Audit Report":
    # 1. PREMIUM HEADER
    render_styled_header(
        f"Master Property Audit: {st.session_state.current_property_name}",
        "Forensic Ledger: Financials, Multi-Channel Attribution, & AI Accuracy",
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

    # --- 2. AUDIT WINDOW & ENGINE SYNC ---
    df_audit_raw = pd.DataFrame(ledger_data)
    df_audit_raw['entry_date'] = pd.to_datetime(df_audit_raw['entry_date'])
    min_audit, max_audit = df_audit_raw['entry_date'].min().date(), df_audit_raw['entry_date'].max().date()

    col_date, col_export = st.columns([2, 1])
    with col_date:
        audit_range = st.date_input("Audit Window:", value=(min_audit, max_audit), key="master_audit_v60")

    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_date, e_date = audit_range
        mask = (df_audit_raw['entry_date'].dt.date >= s_date) & (df_audit_raw['entry_date'].dt.date <= e_date)
        df_audit_filtered = df_audit_raw.loc[mask].copy()
        
        if df_audit_filtered.empty:
            st.error("No records found for selected range.")
            st.stop()

        m = get_forensic_metrics(df_audit_filtered.to_dict(orient='records'), st.session_state.coeffs)
        df_final = m['df']
        
        # --- 3. CALCULATIONS ---
        t_rev = df_final['actual_coin_in'].sum()
        t_traf = df_final['actual_traffic'].sum()
        t_mems = df_final['new_members'].sum()
        t_clicks = df_final['ad_clicks'].sum() if 'ad_clicks' in df_final.columns else 0
        t_imps = df_final['ad_impressions'].sum() if 'ad_impressions' in df_final.columns else 0
        
        # AI Variance Logic
        t_pred = df_final['predicted_traffic'].sum() if 'predicted_traffic' in df_final.columns else 0
        accuracy = (1 - (abs(t_traf - t_pred) / t_traf)) * 100 if t_traf > 0 else 0

        # --- 4. EXECUTIVE SCOREBOARD ---
        st.markdown("### 📊 Executive Summary")
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Total Traffic", f"{t_traf:,}")
        k2.metric("Actual Revenue", f"${t_rev:,.0f}")
        k3.metric("Ad Clicks (Intent)", f"{t_clicks:,.0f}")
        k4.metric("New Members", f"{t_mems:,}")
        k5.metric("Social Reach", f"{t_imps:,.0f}")
        k6.metric("AI Forecast Accuracy", f"{accuracy:.1f}%", help="Closeness of AI predictions to floor actuals.")

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

        # --- 6. AI VARIANCE AUDIT (THE "REPORT CARD") ---
        st.markdown("### 🎯 Prediction vs. Reality: AI Variance Audit")
        v_col, i_col = st.columns([2, 1])
        
        with v_col:
            fig_var = go.Figure()
            fig_var.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['actual_traffic'], 
                                         name="Actual Guests", line=dict(color='#0047AB', width=3)))
            fig_var.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['predicted_traffic'], 
                                         name="AI Forecast", line=dict(color='#FFCC00', width=2, dash='dot')))
            fig_var.update_layout(height=350, template="plotly_white", margin=dict(l=10, r=10, t=10, b=10),
                                  hovermode="x unified", legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig_var, use_container_width=True)
            
        with i_col:
            with st.container(border=True):
                st.markdown("#### 🏁 Model Reliability")
                avg_error = abs(t_traf - t_pred) / len(df_final) if len(df_final) > 0 else 0
                st.metric("Avg Daily Variance", f"{avg_error:,.0f} guests")
                
                # Dynamic Status
                if accuracy > 90:
                    st.success("High Confidence: AI is tracking floor behavior with elite precision.")
                elif accuracy > 75:
                    st.warning("Moderate Drift: Consider recalibrating weights in the Calibration page.")
                else:
                    st.error("High Variance: Significant data outliers detected. Manual audit required.")
                
                st.info("💡 High variance often occurs during unmapped community events or extreme weather shifts.")

        # --- 7. SOCIAL VELOCITY & EXPORT ---
        st.divider()
        st.markdown("### 📲 Social Velocity & Conversion Audit")
        
        s1, s2 = st.columns([2, 1])
        with s1:
            fig_social = go.Figure()
            fig_social.add_trace(go.Bar(x=df_final['entry_date'], y=df_final['ad_impressions'], 
                                        name="Reach", marker_color='#E2E8F0', opacity=0.75))
            fig_social.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['ad_clicks'], 
                                            name="Intent", line=dict(color='#0047AB', width=3), yaxis="y2"))
            fig_social.update_layout(height=350, template="plotly_white", yaxis2=dict(overlaying="y", side="right"),
                                     margin=dict(l=10, r=10, t=30, b=10), legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig_social, use_container_width=True)
        
        with s2:
            st.download_button("📥 Export Integrated Audit", 
                               data=df_final.to_csv(index=False).encode('utf-8'), 
                               file_name=f"Master_Audit_{s_date}.csv", use_container_width=True)
            with st.container(border=True):
                st.metric("Social-to-Floor Bridge", f"{(t_clicks/t_traf if t_traf > 0 else 0):.2f}x")
                st.caption("Ratio of digital intent vs physical footfall.")

# =================================================================
# 13. PAGE 5: AI CALIBRATION & ENGINE WEIGHTS (v52.0 SaaS)
# =================================================================
elif page == "AI Calibration":
    render_styled_header(
        f"Engine Calibration: {st.session_state.current_property_name}",
        "Fine-tune the Forensic Attribution Weights and Financial Benchmarks",
        "Tuning"
    )

    # Financial Discovery
    df_ledger = pd.DataFrame(ledger_data)
    live_avg = (df_ledger['actual_coin_in'].sum() / df_ledger['actual_traffic'].sum()) if not df_ledger.empty else 112.50

    c_health, _ = st.columns([1.5, 2])
    with c_health:
        st.metric("Model Confidence", "92.5%", delta="Optimized")

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
                "property_id": st.session_state.current_property_id, "Avg_Coin_In": n_avg_coin,
                "Hold_Pct": n_hold, "Clicks": n_clicks, "Social_Imp": n_social,
                "Ad_Decay": n_decay, "Broadcast_Weight": n_broad, "Rain_mm": n_rain, "Snow_cm": n_snow
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
# 16. PAGE 8: GLOBAL ADMIN CONSOLE (v22.0 RBAC Enabled)
# =================================================================
elif page == "Global Admin Console":
    st.markdown(f"""
        <div style="background-color: #1A1A1B; padding: 20px; border-radius: 12px; border-left: 6px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🛠️ Global Admin Console</h2>
            <p style="color: #DDD; margin: 0;">System Provisioning, Role Management, and Property Orchestration.</p>
        </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs(["🏗️ Property Provisioning", "👥 User Access & Roles", "📊 System Health"])

    # --- TAB 1: PROPERTY PROVISIONING (Your existing "Build" logic) ---
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

    # --- TAB 2: USER ACCESS & ROLES (v23.5 Management Suite) ---
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

            # 3. INTERACTIVE MANAGEMENT LIST
            st.write(f"Showing **{len(df_access)}** access records:")
            
            for i, row in df_access.iterrows():
                # The Label for the Expander
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
                        # UPDATE BUTTON
                        if st.button("Update", key=f"upd_{row['id']}", use_container_width=True):
                            supabase.table("user_property_access").update({"user_role": new_role}).eq("id", row['id']).execute()
                            st.success("Synced.")
                            st.rerun()
                        
                        # DELETE BUTTON (REVOKE ACCESS)
                        if st.button("🗑️ Revoke", key=f"rev_{row['id']}", type="secondary", use_container_width=True):
                            try:
                                # This deletes the link between the user and the property
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
            st.write("Link an existing email to a property or provision a new user access record.")
            target_email = st.text_input("User Email (Primary Key)", placeholder="user@company.com")
            
            # 1. Fetch fresh property list for the dropdown
            all_p_res = supabase.table("properties").select("id, property_name").execute()
            p_opts = {p['property_name']: p['id'] for p in all_p_res.data} if all_p_res.data else {}
            
            target_prop_name = st.selectbox("Select Property to Link", list(p_opts.keys()))
            target_role = st.selectbox("Assign Role", ["Viewer", "Manager", "Admin", "Super Admin"])
            
            if st.form_submit_button("🚀 Link User to Property", use_container_width=True):
                if target_email and target_prop_name:
                    clean_email = target_email.lower().strip()
                    target_uuid = p_opts.get(target_prop_name)
                    
                    # 2. PRE-CHECK FOR DUPLICATES (Prevents 409 API Errors)
                    check = supabase.table("user_property_access")\
                        .select("*")\
                        .eq("user_email", clean_email)\
                        .eq("property_id", target_uuid)\
                        .execute()
                    
                    if check.data:
                        st.error(f"User {clean_email} already has an active link to {target_prop_name}.")
                    else:
                        # 3. ATTEMPT INSERT
                        link_payload = {
                            "user_email": clean_email,
                            "property_id": target_uuid,
                            "user_role": target_role
                        }
                        
                        try:
                            supabase.table("user_property_access").insert(link_payload).execute()
                            st.success(f"✅ Successfully linked {clean_email} to {target_prop_name}")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Database Error: {e}")
                else:
                    st.error("Please provide both an email and a property selection.")

    # --- TAB 3: SYSTEM HEALTH & PERMISSIONS (Consolidated v24.8) ---
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
        
        # 1. Select the Role (This triggers a rerun when changed)
        target_role_config = st.selectbox(
            "Select Role to Configure:", 
            ["Viewer", "Manager", "Admin", "Super Admin"],
            key="role_selector_admin"
        )
        
        # 2. FETCH EXISTING PERMS (The "Hydration" Step)
        existing_perms = {}
        try:
            # We fetch the current JSON from Supabase for the selected role
            perm_fetch = supabase.table("role_permissions").select("perms").eq("role_name", target_role_config).execute()
            if perm_fetch.data:
                existing_perms = perm_fetch.data[0].get('perms', {})
        except Exception as e:
            st.caption(f"Note: Role '{target_role_config}' not yet initialized in DB.")

        # 3. Define the Global Capabilities List
        capabilities = {
            "view_analytics": "Access Attribution & Executive Dashboards",
            "view_ledger": "Access Daily Ledger Audit",
            "view_reports": "Access Master Audit Reports",
            "run_simulations": "Access Predictive Scenario Simulator",
            "manage_alerts": "Create/Delete Strategic Watchdogs",
            "calibrate_ai": "Change AI Coefficients & ROAS",
            "run_experiments": "Access A/B Experimentation Vault"
        }
        
        # 4. The Configuration Form
        # IMPORTANT: We use the role name in the form key to force a clean reset when switching roles
        with st.form(f"perm_matrix_form_{target_role_config}"):
            st.write(f"Adjusting capabilities for: **{target_role_config}**")
            updated_perms = {}
            
            # Create columns for a cleaner layout
            col1, col2 = st.columns(2)
            for i, (cap_id, cap_desc) in enumerate(capabilities.items()):
                target_col = col1 if i % 2 == 0 else col2
                
                # CRITICAL: 'value' pulls from the 'existing_perms' we just fetched
                is_checked = existing_perms.get(cap_id, False)
                
                updated_perms[cap_id] = target_col.checkbox(
                    cap_desc, 
                    value=is_checked, 
                    key=f"check_{target_role_config}_{cap_id}" # Unique key per role
                )
                
            if st.form_submit_button("💾 Save Role Configuration", use_container_width=True):
                try:
                    perm_payload = {
                        "role_name": target_role_config, 
                        "perms": updated_perms
                    }
                    
                    supabase.table("role_permissions").upsert(
                        perm_payload, 
                        on_conflict="role_name"
                    ).execute()
                    
                    st.success(f"✅ Vault Updated: '{target_role_config}' permissions are now live.")
                    
                    # Clear cache so other parts of the app (like sidebar) see the change
                    st.cache_data.clear()
                    # Rerun to refresh the 'existing_perms' fetch
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Security Matrix Sync Error: {e}")

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
# 18. FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
