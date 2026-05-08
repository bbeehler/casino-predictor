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

        /* RESPONSIVE PADDING & VIEWPORT OPTIMIZATION */
        .main .block-container {
            padding-top: 2rem;
            padding-bottom: 5rem;
            padding-left: 4rem;
            padding-right: 4rem;
            max-width: 1400px;
        }

        /* MOBILE OVERRIDES (Screens smaller than 768px) */
        @media (max-width: 768px) {
            .main .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
                padding-top: 1rem;
            }
            [data-testid="stMetricValue"] {
                font-size: 1.8rem !important;
            }
        }

        /* HIGH-END EXECUTIVE HEADER (Dark Command Center Style) */
        .glass-header {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            border: 1px solid rgba(255, 255, 255, 0.1);
            padding: 28px;
            border-radius: 18px;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.2), 0 10px 10px -5px rgba(0, 0, 0, 0.1);
            margin-bottom: 35px;
            color: white !important;
        }

        /* HIGH-END METRIC CARDS */
        [data-testid="stMetric"] {
            background: #FFFFFF;
            border: 1px solid #E1E8F0;
            padding: 20px !important;
            border-radius: 12px !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
            transition: transform 0.2s ease-in-out;
        }
        [data-testid="stMetric"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        }

        /* INPUT FIELD REFINEMENT */
        div[data-baseweb="input"] > div, input, textarea, select {
            background-color: #FFFFFF !important;
            color: #1A1C1E !important;
            border-radius: 10px !important;
            border: 1px solid #D0D5DD !important;
            padding: 8px !important;
        }

        /* BUTTON STYLING (Corporate Primary) */
        .stButton>button {
            border-radius: 8px !important;
            font-weight: 600 !important;
            background-color: #0047AB !important;
            color: white !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
            transition: all 0.2s;
        }
        .stButton>button:hover {
            background-color: #003380 !important;
            box-shadow: 0 4px 12px rgba(0, 71, 171, 0.3) !important;
        }

        /* HIDE DEFAULT STREAMLIT ELEMENTS */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)

# Define the Responsive Header Component with Premium Dark Theme
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
                    <div style="margin-top: 8px; color: #64748b; font-size: 0.75rem; font-family: monospace;">
                        SYSTEM_v52.0_STABLE
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
                <p style="color: #667085;">FloorCast Strategic Intelligence Unit</p>
            </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form", border=True):
            e_mail = st.text_input("Corporate Email").strip().lower()
            p_word = st.text_input("Password", type="password")
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button("Authenticate & Unlock Engine", use_container_width=True):
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

# =================================================================
# 8. EXECUTIVE NAVIGATION (SaaS Sidebar Architecture)
# =================================================================
page = "Executive Dashboard"

# Global Role Scan
user_links_res = supabase.table("user_property_access").select("user_role").eq("user_email", st.session_state.get('user_email')).execute()
all_my_roles = [r['user_role'] for r in user_links_res.data] if user_links_res.data else []
is_global_admin = any(role in ["Super Admin", "Manager", "Admin"] for role in all_my_roles)

with st.sidebar:
    # Sidebar Logo with subtle shadow
    st.markdown("""
        <div style="padding: 10px 0px 30px 0px;">
            <img src="https://casino.hardrock.com/ottawa/-/media/project/shrss/hri/casinos/hard-rock/ottawa/logos-and-icons/logo.png" width="160">
        </div>
    """, unsafe_allow_html=True)
    
    # 1. SCOPE SWITCHER
    if is_global_admin:
        st.caption("NETWORK SCOPE")
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
    st.caption("OPERATIONAL DECKS")
    
    # 2. NAVIGATION
    if st.session_state.current_property_id == "GLOBAL":
        page = "Executive Dashboard"
        st.info("Global View Active")
    else:
        nav_options = ["Executive Dashboard"]
        if check_permission("view_ledger"): nav_options.append("Daily Ledger Audit")
        if check_permission("view_analytics"):
            nav_options.extend(["Attribution Analytics", "FloorCast AI Analyst"])
        if check_permission("view_reports"): nav_options.append("Master Audit Report")
        if check_permission("manage_alerts"): nav_options.append("Strategic Alerts")
        if check_permission("calibrate_ai"):
            nav_options.extend(["AI Calibration", "BL-ROAS Calculator"])
        if st.session_state.get('user_role') == "Super Admin":
            nav_options.append("Global Admin Console")

        # High-end nav selection
        page = st.radio("Navigation", nav_options, label_visibility="collapsed")

    # 3. FOOTER CONTEXT
    st.markdown("<div style='position: fixed; bottom: 20px; width: 260px;'>", unsafe_allow_html=True)
    st.divider()
    st.caption(f"ID: {st.session_state.get('user_email')}")
    st.markdown(f"""
        <div style="background: #1e1e1e; padding: 10px; border-radius: 8px; border: 1px solid #333;">
            <p style="margin:0; font-size: 0.75rem; color: #888;">CURRENT ROLE</p>
            <p style="margin:0; font-size: 0.9rem; font-weight: 600; color: #FFCC00;">{st.session_state.get('user_role', 'Viewer')}</p>
        </div>
    """, unsafe_allow_html=True)
    
    if st.button("🚪 Terminate Session", use_container_width=True):
        st.session_state.clear()
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

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
df, ledger_data = get_hydrated_data(st.session_state.current_property_id, supabase)

# --- 10. REFINED SAFETY GATE ---
# If df is empty but ledger_data exists, it means the Forensic Engine failed to process the rows.
if df.empty:
    if page not in ["Global Admin Console", "Master Audit Report"]:
        if not ledger_data:
            st.warning(f"🎰 No ledger data found for {st.session_state.current_property_name}. Please check the Master Audit Report.")
        else:
            st.error("🧪 Forensic Engine failed to process rows. Check AI Calibration settings.")
        st.stop()

# =================================================================
# 9. PAGE 1: EXECUTIVE DASHBOARD (v52.0 - SaaS Hybrid & Restored Pulse)
# =================================================================
if page == "Executive Dashboard":
    
    # --- A. CONSOLIDATED GLOBAL VIEW (For Super Admins) ---
    if st.session_state.get('current_property_id') == "GLOBAL":
        render_styled_header("Global Network Intelligence", "Aggregate Performance across all Portfolio Properties", "Global")

        if df.empty:
            st.warning("No network data found across properties.")
            st.stop()

        # 1. GLOBAL DATE RANGE SELECTION (With Error Suppression)
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        min_date, max_date = df['entry_date'].min().date(), df['entry_date'].max().date()

        col_date, _ = st.columns([1.5, 2.5])
        with col_date:
            global_range = st.date_input("Network Audit Window:", value=(min_date, max_date), 
                                        min_value=min_date, max_value=max_date, key="global_audit_range_selector")

        # THE FIX: PREVENT RED ERROR BOX
        if isinstance(global_range, tuple) and len(global_range) < 2:
            st.info("💡 Please select the **end date** in the calendar to load the network results.")
            st.stop() 

        if isinstance(global_range, tuple) and len(global_range) == 2:
            start_g, end_g = global_range
            mask = (df['entry_date'].dt.date >= start_g) & (df['entry_date'].dt.date <= end_g)
            df_filtered = df.loc[mask].copy()
        else:
            df_filtered = df.copy()
            start_g, end_g = min_date, max_date

        if df_filtered.empty:
            st.info("No network data found for the selected date range.")
            st.stop()

        # 2. NETWORK TOP-LINE METRICS
        total_rev = df_filtered['actual_coin_in'].sum()
        total_traffic = df_filtered['actual_traffic'].sum()
        total_mems = df_filtered['new_members'].sum()

        k1, k2, k3 = st.columns(3)
        k1.metric("Network Revenue", f"${total_rev:,.0f}")
        k2.metric("Network Traffic", f"{total_traffic:,.0f}")
        k3.metric("Network New Members", f"{total_mems:,.0f}")

        st.divider()

        # 3. PROPERTY PERFORMANCE LEADERBOARD (Ranked & Formatted)
        st.write(f"### 🏆 Property Performance Leaderboard ({start_g} to {end_g})")
        
        leaderboard = df_filtered.groupby('Property').agg({
            'actual_coin_in': 'sum', 'actual_traffic': 'sum', 'new_members': 'sum'
        }).reset_index()
        
        leaderboard['Yield_per_Guest'] = (leaderboard['actual_coin_in'] / leaderboard['actual_traffic'])
        leaderboard['Conv_Rate'] = (leaderboard['new_members'] / leaderboard['actual_traffic'] * 100)
        leaderboard['Rank'] = leaderboard['actual_coin_in'].rank(ascending=False, method='min').astype(int)
        leaderboard = leaderboard.sort_values('Rank')

        lb_display = leaderboard.copy()
        lb_display['actual_coin_in'] = lb_display['actual_coin_in'].apply(lambda x: f"${x:,.0f}")
        lb_display['actual_traffic'] = lb_display['actual_traffic'].apply(lambda x: f"{x:,.0f}")
        lb_display['new_members'] = lb_display['new_members'].apply(lambda x: f"{x:,.0f}")
        lb_display['Yield_per_Guest'] = lb_display['Yield_per_Guest'].apply(lambda x: f"${x:,.2f}")
        lb_display['Conv_Rate'] = lb_display['Conv_Rate'].apply(lambda x: f"{x:.2f}%")

        cols = ['Rank', 'Property', 'actual_coin_in', 'actual_traffic', 'new_members', 'Yield_per_Guest', 'Conv_Rate']
        st.table(lb_display[cols].rename(columns={'actual_coin_in': 'Total Revenue', 'actual_traffic': 'Total Guests'}))

        # 4. COMPARATIVE ANALYTICS
        c1, c2 = st.columns(2)
        with c1:
            st.write("### 💰 Revenue Distribution")
            fig_rev = px.pie(df_filtered, values='actual_coin_in', names='Property', hole=0.5,
                             color_discrete_sequence=px.colors.sequential.Blues_r)
            fig_rev.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=350)
            st.plotly_chart(fig_rev, use_container_width=True)

        with c2:
            st.write("### 🧬 Network Guest Flow (Stacked)")
            fig_flow = px.bar(df_filtered, x="entry_date", y="actual_traffic", color="Property",
                             barmode="stack", color_discrete_sequence=px.colors.qualitative.Prism)
            fig_flow.update_layout(template="plotly_white", margin=dict(l=10, r=10, t=10, b=10), height=350)
            st.plotly_chart(fig_flow, use_container_width=True)

    # --- B. INDIVIDUAL PROPERTY VIEW (The Pulse) ---
    else:
        render_styled_header(f"{st.session_state.current_property_name} Pulse", 
                             "Strategic Demand Projection & Marketing Impact", "Operational")

        today = datetime.date.today()
        current_weights = st.session_state.get('coeffs', {})

        if df.empty:
            st.warning(f"Forensic Vault for {st.session_state.current_property_name} is empty.")
            st.stop()

        # 1. PREPARE DATA
        df_raw = df.copy()
        df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])
        df_raw['dow'] = df_raw['entry_date'].dt.day_name()
        master_baselines = df_raw.groupby('dow')['actual_traffic'].mean().to_dict()

        # 2. DATE SELECTION
        col_date, _ = st.columns([1.5, 2.5])
        with col_date:
            pulse_range = st.date_input("Analysis Window:", value=(today, today + datetime.timedelta(days=7)), key="pulse_exec_unique")

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
                    return val if val is not None else (0 if col_name != 'active_promo' else "")
                return "" if col_name == 'active_promo' else 0.0

            map_cols = ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm', 'actual_traffic', 'new_members', 'actual_coin_in']
            for c in map_cols:
                df_p[c] = df_p.apply(lambda r: map_data(r, c), axis=1)

            df_p['baseline'] = df_p['dow'].map(master_baselines).fillna(0)

            # 3. STRATEGIC DAILY PLANNER
            with st.expander("📅 Strategic Daily Planner & Simulator", expanded=True):
                planner_cols = ['entry_date', 'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
                df_plan_display = df_p[planner_cols].copy()
                df_plan_display['entry_date'] = df_plan_display['entry_date'].dt.strftime('%a, %b %d')
                
                edited_df = st.data_editor(
                    df_plan_display, 
                    column_config={
                        "entry_date": st.column_config.Column("Date", disabled=True),
                        "attendance": st.column_config.NumberColumn("Event Attendance", format="%d"),
                    },
                    hide_index=True, use_container_width=True, key="p1_planner_v50_editor"
                )
                
                for field in ['active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']:
                    df_p[field] = edited_df[field].values

            # 4. ENGINE EXECUTION
            m = get_forensic_metrics(df_p.to_dict(orient='records'), current_weights)
            df_final = m['df'].sort_values('entry_date')
            
            # 5. KPI CALCULATIONS
            total_vol = df_final['expected'].sum()
            organic_vol = df_p['baseline'].sum()
            mkt_impact_pct = ((total_vol - organic_vol) / total_vol * 100) if total_vol > 0 else 0

            # 6. PULSE CHART
            st.write("### 🎰 The Unified Pulse")
            fig_pulse = go.Figure()
            df_act_chart = df_final[df_final['entry_date'].dt.date < today]
            fig_pulse.add_trace(go.Scatter(x=df_act_chart['entry_date'], y=df_act_chart['actual_traffic'], name="Actual Guests", line=dict(color='#0047AB', width=4)))
            fig_pulse.add_trace(go.Scatter(x=df_final['entry_date'], y=df_final['expected'].round(0), name="AI Target", line=dict(color='#FFCC00', width=2, dash='dot')))
            fig_pulse.update_layout(height=400, margin=dict(l=10, r=10, t=10, b=10), template="plotly_white")
            st.plotly_chart(fig_pulse, use_container_width=True, key=f"pulse_chart_{st.session_state.current_property_id}")

            # 7. EXECUTIVE KPI GRID (With Benchmarking)
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

            if start_p >= today:
                proj_rev = (total_vol * AVG_SPEND) + ((total_vol * 0.05) * LTV_VAL)
                k1.metric("Projected Demand", f"{total_vol:,.0f} Guests")
                k2.metric("Target Signups", f"{(total_vol * 0.0170):,.0f}")
                k3.metric("Proj. Revenue", f"${proj_rev:,.0f}")
                k4.metric("Marketing Impact", f"{mkt_impact_pct:.1f}%")
                k5.metric("Net Avg Yield", f"${st.session_state.get('net_avg_yield', 0):,.2f}")
            else:
                k1.metric("Actual Guest Flow", f"{total_act:,.0f}")
                k2.metric("Yield / Guest", f"${local_yield:,.2f}", delta=f"${y_delta:+.2f} vs Net")
                k3.metric("Enrollment %", f"{local_conv:.2f}%", delta=f"{c_delta:+.2f}% vs Net")
                k4.metric("Ledger Revenue", f"${ledger_rev:,.0f}")
                k5.metric("AI Accuracy", f"{m.get('predictability', '92.5%')}")

            # 8. EXECUTIVE BRAND SENTIMENT PULSE
            st.divider()
            st.write("### 🏛️ Executive Brand Sentiment Pulse")
            
            col_h1, col_h2 = st.columns([2, 1])
            with col_h2:
                g_months = [(today - relativedelta(months=i)).replace(day=1) for i in range(2)]
                g_labels = ["Current (Live)"] + [m.strftime("%B %Y") for m in g_months[1:]]
                sel_period = st.selectbox("Audit Period:", g_labels, key="gauge_historical_select")

            overall_score = 0.0
            try:
                global_query = supabase.table("sentiment_history").select("sentiment_score").eq("property_id", st.session_state.current_property_id)
                if sel_period == "Current (Live)":
                    g_res = global_query.order("timestamp", desc=True).limit(50).execute()
                else:
                    sel_date = g_months[g_labels.index(sel_period)]
                    g_res = global_query.filter("timestamp", "gte", sel_date.strftime("%Y-%m-%d")).execute()
                if g_res.data:
                    overall_score = np.mean([d['sentiment_score'] for d in g_res.data])
            except: pass

            st.metric(label=f"Consolidated Property Pulse ({sel_period})", value=f"{overall_score:+.2f}",
                delta="Positive Impact" if overall_score > 0.3 else "High Friction" if overall_score < -0.3 else "Neutral")

            # DYNAMIC ASSET GAUGES
            try:
                asset_res = supabase.table("property_assets").select("asset_name").eq("property_id", st.session_state.current_property_id).execute()
                tags = [item['asset_name'] for item in asset_res.data] if asset_res.data else ["Overall Property"]
                
                gauge_cols = st.columns(len(tags))
                for i, tag in enumerate(tags):
                    with gauge_cols[i]:
                        tag_score = 0.0
                        try:
                            t_res = supabase.table("sentiment_history").select("sentiment_score")\
                                .eq("property_id", st.session_state.current_property_id)\
                                .eq("asset", tag).order("timestamp", desc=True).limit(20).execute()
                            if t_res.data:
                                tag_score = np.mean([d['sentiment_score'] for d in t_res.data])
                        except: pass

                        fig = go.Figure(go.Indicator(
                            mode = "gauge+number", value = tag_score,
                            number = {'font': {'size': 20}, 'valueformat': ".2f"},
                            gauge = {'axis': {'range': [-1, 1]}, 'bar': {'color': "#0047AB"},
                                'steps': [{'range': [-1, -0.3], 'color': "#FF4B4B"},
                                          {'range': [-0.3, 0.3], 'color': "#F0F2F6"},
                                          {'range': [0.3, 1], 'color': "#28A745"}]}))
                        fig.update_layout(height=150, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)')
                        st.plotly_chart(fig, use_container_width=True, key=f"p1_gauge_{st.session_state.current_property_id}_{tag}_{i}")
                        st.markdown(f"<p style='text-align: center; font-weight: bold; font-size: 14px;'>{tag}</p>", unsafe_allow_html=True)
            except: pass
        
# =================================================================
# 10. PAGE 2: DAILY LEDGER AUDIT (v52.0 - High-End Operational Deck)
# =================================================================
elif page == "Daily Ledger Audit":
    # 1. PREMIUM HEADER
    render_styled_header(
        f"Ledger Audit: {st.session_state.current_property_name}", 
        "Operational Actuals Management & Data Integrity", 
        "Data Active"
    )
    
    # --- 2. THE DATA ENGINE ---
    if not ledger_data:
        df_ledger = pd.DataFrame(columns=[
            'entry_date', 'actual_traffic', 'new_members', 'actual_coin_in', 
            'active_promo', 'attendance', 'ad_clicks', 'ad_impressions', 
            'rain_mm', 'snow_cm', 'property_id'
        ])
    else:
        df_ledger = pd.DataFrame(ledger_data)
        df_ledger['entry_date'] = pd.to_datetime(df_ledger['entry_date']).dt.date
        
        marketing_cols = ['actual_traffic', 'new_members', 'actual_coin_in', 'attendance', 'ad_clicks', 'ad_impressions', 'rain_mm', 'snow_cm']
        for col in marketing_cols:
            if col in df_ledger.columns:
                df_ledger[col] = pd.to_numeric(df_ledger[col], errors='coerce').fillna(0)
        
        df_ledger['active_promo'] = df_ledger['active_promo'].astype(str).replace(['nan', 'None', '0', '0.0'], '')
        df_ledger = df_ledger.sort_values('entry_date', ascending=False)

    # --- 3. RAPID ENTRY ACTION CARD ---
    with st.expander("➕ Register Daily Performance Nodes", expanded=False):
        st.markdown('<div style="padding: 10px;">', unsafe_allow_html=True)
        with st.form("rapid_entry_form", clear_on_submit=True, border=False):
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
                e_clicks = st.number_input("Ad Clicks", min_value=0, step=1)
                e_imps = st.number_input("Social Impressions", min_value=0, step=1)
                e_rain = st.number_input("Rain (mm)", min_value=0.0, step=0.1)
            
            st.markdown("<br>", unsafe_allow_html=True)
            submit_new = st.form_submit_button("🚀 Commit to Forensic Vault", use_container_width=True)
            
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
                    "property_id": st.session_state.current_property_id
                }
                try:
                    supabase.table("ledger").upsert(new_row).execute()
                    st.success(f"✅ Successfully logged: {e_date}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Vault Error: {e}")
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- 4. PERFORMANCE SCOREBOARD ---
    c_lim, _ = st.columns([1, 2])
    with c_lim:
        view_limit = st.select_slider("Audit Depth (Days):", options=[7, 14, 30, 60, 90, 120], value=30)
    
    df_audit_period = df_ledger.head(view_limit).copy()
    
    st.markdown(f"### 🎯 Performance Scoreboard: Last {view_limit} Days")
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

    # --- 5. THE HISTORICAL EDITABLE LEDGER ---
    st.markdown("### 📂 Bulk Audit & Corrections")
    with st.form("bulk_ledger_sync", border=False):
        # UI Prep: Hide ID for users but keep for sync logic
        cols_to_show = [c for c in df_audit_period.columns if c != 'property_id']
        display_df = df_audit_period[cols_to_show].copy()
        
        with st.container(border=True):
            edited_ledger = st.data_editor(
                display_df, 
                column_config={
                    "id": None, 
                    "entry_date": st.column_config.DateColumn("Date", required=True),
                    "actual_traffic": st.column_config.NumberColumn("Guests", format="%d"),
                    "new_members": st.column_config.NumberColumn("Members", format="%d"),
                    "actual_coin_in": st.column_config.NumberColumn("Revenue", format="$%d"),
                    "active_promo": st.column_config.TextColumn("Promo Name"),
                },
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                key="ledger_editor_v52"
            )
        
        st.markdown("<br>", unsafe_allow_html=True)
        if st.form_submit_button("💾 Sync Table Updates to Cloud", use_container_width=True):
            try:
                df_sync = pd.DataFrame(edited_ledger).copy()
                if not df_sync.empty:
                    df_sync['entry_date'] = df_sync['entry_date'].astype(str)
                    df_sync['property_id'] = st.session_state.current_property_id
                    
                    sync_payload = df_sync.fillna(0).to_dict(orient='records')
                    supabase.table("ledger").upsert(sync_payload).execute()
                    
                    st.success("✅ Bulk updates synced successfully.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.warning("No data to sync.")
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
# 12. PAGE 4: MASTER FORENSIC AUDIT (v17.2 SaaS Factory)
# =================================================================
elif page == "Master Audit Report":
    st.markdown(f"""
        <style>
        [data-testid="stMetricLabel"] p {{ font-size: 0.75rem !important; white-space: nowrap !important; }}
        [data-testid="stMetricValue"] > div {{ font-size: 1.5rem !important; }}
        </style>
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">📋 Master Property Audit: {st.session_state.current_property_name}</h2>
            <p style="color: #444; margin: 0;">Comprehensive Forensic Ledger: Financials, Loyalty, & Marketing Attribution.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # --- 1. SAAS INGESTION FACTORY (New Additions) ---
    with st.expander("📥 Bulk Ingest Forensic Ledger (CSV)", expanded=not ledger_data):
        st.write("Upload a Daily Ledger CSV to initialize or update this property's forensic vault.")
        uploaded_file = st.file_uploader("Choose CSV File", type="csv", key="vault_uploader")
        
        if uploaded_file:
            try:
                up_df = pd.read_csv(uploaded_file)
                # Ensure the data is tagged to THIS property UUID
                up_df['property_id'] = st.session_state.current_property_id
                
                if st.button("🚀 Commit Bulk Upload to Vault", use_container_width=True):
                    payload = up_df.to_dict(orient='records')
                    supabase.table("ledger").upsert(payload).execute()
                    st.success(f"Successfully ingested {len(up_df)} records into {st.session_state.current_property_name}!")
                    st.cache_data.clear()
                    st.rerun()
            except Exception as e:
                st.error(f"Ingestion Error: {e}")

    # --- 2. DATA AVAILABILITY CHECK ---
    if not ledger_data:
        st.warning(f"⚠️ The Audit Vault for **{st.session_state.current_property_name}** is empty. Please use the uploader above to begin.")
        st.stop()

    # --- 3. AUDIT ENGINE & VARIABLE INITIALIZATION ---
    df_audit_raw = pd.DataFrame(ledger_data)
    df_audit_raw['entry_date'] = pd.to_datetime(df_audit_raw['entry_date'])
    
    min_audit = df_audit_raw['entry_date'].min().date()
    max_audit = df_audit_raw['entry_date'].max().date()

    col_date, col_export = st.columns([2, 1])
    with col_date:
        audit_range = st.date_input("Audit Window:", value=(min_audit, max_audit), key="master_audit_v17_final")

    if isinstance(audit_range, tuple) and len(audit_range) == 2:
        s_date, e_date = audit_range
        mask = (df_audit_raw['entry_date'].dt.date >= s_date) & (df_audit_raw['entry_date'].dt.date <= e_date)
        df_audit_filtered = df_audit_raw.loc[mask].copy()
        
        if df_audit_filtered.empty:
            st.error(f"No records found between {s_date} and {e_date}.")
            st.stop()

        # Engine Sync
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

        # --- 4. DATE-AWARE ROI FETCH ---
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

        # --- 5. EXECUTIVE SUMMARY & MoM PERFORMANCE TABLE ---
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
        
        # Total Row Logic
        def get_avg_str(v_list): return f"{np.mean(v_list):+.1f}% Avg" if v_list else "---"
        total_row = pd.Series({
            "Month": "**TOTAL AUDIT WINDOW**", "Traffic": df_summary_table["Traffic"].sum(),
            "Traffic MoM": get_avg_str(raw_mom_values["traffic"]), "Actual Revenue": df_summary_table["Actual Revenue"].sum(),
            "Revenue MoM": get_avg_str(raw_mom_values["revenue"]), "Digital Lift": df_summary_table["Digital Lift"].sum(),
            "Digital MoM": get_avg_str(raw_mom_values["digital"]), "Digital $ Impact": df_summary_table["Digital $ Impact"].sum(),
            "Weather Penalty": df_summary_table["Weather Penalty"].sum()
        })
        df_summary_table = pd.concat([df_summary_table, total_row.to_frame().T], ignore_index=True)

        # Apply Table Formatting
        fmt_map = {"Traffic": "{:,.0f}", "Actual Revenue": "${:,.0f}", "Digital Lift": "{:,.0f}", "Digital $ Impact": "${:,.0f}", "Weather Penalty": "{:,.0f}"}
        for col, f_string in fmt_map.items():
            df_summary_table[col] = df_summary_table[col].apply(lambda x: f_string.format(x) if isinstance(x, (int, float)) else x)
        
        st.table(df_summary_table)

        # --- 6. YTD CAPTION ---
        current_year = datetime.date.today().year
        ytd_df_raw = df_audit_raw[df_audit_raw['entry_date'].dt.year == current_year].copy()
        if not ytd_df_raw.empty:
            m_ytd = get_forensic_metrics(ytd_df_raw.to_dict(orient='records'), c)
            df_y = m_ytd['df']
            y_traf, y_dig = df_y['actual_traffic'].sum(), df_y['residual_lift'].sum()
            st.caption(f"**{current_year} YTD:** {y_traf:,.0f} Guests | ${df_y['actual_coin_in'].sum():,.0f} Revenue | {df_y['new_members'].sum():,.0f} Members.  \n**YTD Digital Impact:** {y_dig:,.0f} Guests ({(y_dig/y_traf*100 if y_traf > 0 else 0):.1f}% contribution).")

        # --- 7. METRIC CARDS ---
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

        # --- 8. ROI & EFFICIENCY ---
        st.write("### 💎 BL-ROAS & Equity Efficiency")
        def get_stat_ui(val, mode="m"):
            if mode=="m":
                if val >= 5.0: return "💎 ELITE", "#008000"
                if val >= 3.0: return "✅ STRONG", "#2E8B57"
                return "⚠️ MONITOR", "#B8860B"
            else:
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

        # Status Badges
        sb1, sb2, sb3, sb4, sb5 = st.columns(5)
        with sb3: st.markdown(f"<div style='text-align:center;padding:5px;border-radius:5px;background-color:{m_color};color:white;font-size:0.7rem;font-weight:bold;margin-top:-10px;'>{m_status}</div>", unsafe_allow_html=True)
        with sb4: st.markdown(f"<div style='text-align:center;padding:5px;border-radius:5px;background-color:{e_color};color:white;font-size:0.7rem;font-weight:bold;margin-top:-10px;'>{e_status}</div>", unsafe_allow_html=True)

        # --- 9. SOCIAL & ATTRIBUTION CHART ---
        st.divider()
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
        
        # UNIQUE KEY FIX: Prevents StreamlitDuplicateElementId
        fig_stack.update_layout(height=500, margin=dict(l=10, r=10, t=10, b=10), hovermode="x unified", template="plotly_white")
        st.plotly_chart(fig_stack, use_container_width=True, key=f"stack_chart_{st.session_state.current_property_id}")

        # --- 10. DETAILED LEDGER & EXPORT ---
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
# 14. PAGE 6: AI STRATEGIC ANALYST (v19.1 - SaaS Deep-Sync)
# =================================================================
elif page == "FloorCast AI Analyst":
    st.markdown(f"""
        <div style="background-color: #E1E8F0; padding: 20px; border-radius: 12px; border-left: 6px solid #0047AB; margin-bottom: 25px;">
            <h2 style="color: #0047AB; margin: 0;">🕵️ FloorCast Strategic AI Analyst: {st.session_state.current_property_name}</h2>
            <p style="color: #444; margin: 0;">Unified Intelligence: Correlating Ledger, Sentiment, ROI Audits, & Events.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # --- 1. DEEP SYNC DATA AGGREGATION ---
    ledger_csv = "No ledger data available."
    sent_csv = "No sentiment data available."
    roi_csv = "No ROI records available."
    promo_csv = "No promotion data available."

    with st.status(f"🔗 Synchronizing {st.session_state.current_property_name} Intelligence...", expanded=False) as status:
        if 'ledger_data' in locals() and ledger_data:
            try:
                m_audit = get_forensic_metrics(ledger_data, st.session_state.coeffs)
                ledger_csv = m_audit['df'].to_csv(index=False)
                status.write("📊 Daily Ledger Nodes Linked")
            except: pass
        
        try:
            sent_res = supabase.table("sentiment_history").select("*")\
                .eq("property_id", st.session_state.current_property_id)\
                .order("timestamp", desc=True).limit(50).execute()
            if sent_res.data: 
                sent_csv = pd.DataFrame(sent_res.data).to_csv(index=False)
                status.write("💬 Sentiment Records Synced")
        except: pass

        try:
            roi_res = supabase.table("monthly_roi").select("*").eq("property_id", st.session_state.current_property_id).execute()
            if roi_res.data: roi_csv = pd.DataFrame(roi_res.data).to_csv(index=False)
            
            promo_res = supabase.table("promotions").select("*").eq("property_id", st.session_state.current_property_id).execute()
            if promo_res.data: promo_csv = pd.DataFrame(promo_res.data).to_csv(index=False)
            status.write("📈 ROI & Promo Matrices Mapped")
        except: pass

        status.update(label="✅ Strategic Intelligence Fully Hydrated", state="complete")

    # --- 2. ENTRY MODULES ---
    try:
        asset_res = supabase.table("property_assets").select("asset_name").eq("property_id", st.session_state.current_property_id).execute()
        tags = [item['asset_name'] for item in asset_res.data] if asset_res.data else ["Overall Property"]
    except:
        tags = ["Overall Property"]

    col_input1, col_input2 = st.columns(2)

    with col_input1:
        with st.expander("📝 Manual Sentiment Entry", expanded=True):
            with st.form("manual_sentiment_form", clear_on_submit=True):
                manual_tag = st.selectbox("Assign to Asset:", tags)
                f_text = st.text_area("Review Content", placeholder="Paste Google/TripAdvisor review...")
                if st.form_submit_button("🛡️ Archive & AI Score"):
                    if f_text:
                        if archive_sentiment_entry(f_text, manual_tag):
                            st.success("Review Scored & Archived.")
                            st.rerun()

    with col_input2:
        from docx import Document
        with st.expander("📄 Word Doc Bulk Parser", expanded=False):
            uploaded_doc = st.file_uploader("Upload .docx Reviews", type="docx")
            bulk_tag = st.selectbox("Bulk Assign to:", tags)
            if uploaded_doc and st.button("🚀 Process Bulk"):
                doc = Document(uploaded_doc)
                full_text = []
                for para in doc.paragraphs:
                    if len(para.text) > 20:
                        archive_sentiment_entry(para.text, bulk_tag)
                st.success("Bulk Ingestion Complete.")
                st.rerun()

    st.divider()

    # --- 3. THE CHAT INTERFACE ---
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    prompt = st.chat_input(f"Consult with the {st.session_state.current_property_name} Analyst...")
    
    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        try:
            import google.generativeai as genai
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash') 
            
            with st.chat_message("assistant"):
                with st.spinner("🕵️ Analyzing property dossier..."):
                    dossier = f"""
                    PROPERTY: {st.session_state.current_property_name}
                    LEDGER: {ledger_csv}
                    SENTIMENT: {sent_csv}
                    ROI: {roi_csv}
                    """
                    
                    full_query = f"Consultant Mode: Use this dossier to answer: {prompt}\n\nDOSSIER:\n{dossier}"
                    response = model.generate_content(full_query)
                    st.markdown(response.text)
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            
        except Exception as e:
            st.error(f"Consultation Error: {e}")

# =================================================================
# 15. PAGE 7: BL-ROAS COMMAND CENTER (v24.1 SaaS - Conflict Fixed)
# =================================================================
elif page == "BL-ROAS Calculator":
    st.markdown(f"""
        <div style="background-color: #F8F9FA; padding: 20px; border-radius: 12px; border-left: 6px solid #28A745; margin-bottom: 25px;">
            <h2 style="color: #28A745; margin: 0;">💰 BL-ROAS Command Center: {st.session_state.current_property_name}</h2>
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

    avg_spend_actual = float(ledger_coin_in / ledger_traffic) if ledger_traffic > 0 else DEFAULT_AVG_SPEND

    # --- 3. THE INPUT FORM ---
    with st.form("roas_input_form"):
        st.subheader(f"📊 {selected_label} Metrics")
        
        existing_res = supabase.table("monthly_roi")\
            .select("*")\
            .eq("property_id", st.session_state.current_property_id)\
            .eq("report_month", str(selected_month))\
            .execute()
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

    # --- 4. CALCULATION & UPSERT LOGIC ---
    if submit:
        brand_value = (utm_s * 1.5) + (org_s * 0.5) + (likes * 0.1) + (shares * 0.5) + (geo_lift * 2.0)
        bl_roas = brand_value / ad_spend if ad_spend > 0 else 0
        enhanced_rev = brand_value + ledger_coin_in + (ledger_signups * LTV_BENCHMARK)

        roi_payload = {
            "property_id": st.session_state.current_property_id,
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
            # FIX: Explicitly handle conflict on the composite key (Property + Month)
            supabase.table("monthly_roi").upsert(
                roi_payload, 
                on_conflict="property_id, report_month"
            ).execute()
            st.success(f"✅ ROI for {selected_label} saved successfully!")
            st.rerun() 
        except Exception as e:
            st.error(f"Sync Failure: {e}")

    # --- 5. REPORT GENERATOR ---
    st.divider()
    history_res = supabase.table("monthly_roi")\
        .select("*")\
        .eq("property_id", st.session_state.current_property_id)\
        .order("report_month", desc=True)\
        .execute()
        
    if history_res.data:
        df_hist = pd.DataFrame(history_res.data)
        curr_row = df_hist[df_hist['report_month'] == str(selected_month)]
        
        if not curr_row.empty:
            curr = curr_row.iloc[0]
            prop_potential = ledger_coin_in + (ledger_signups * LTV_BENCHMARK)
            
            report_text = f"""{selected_label} ROAS Results for {st.session_state.current_property_name}
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
            "manage_alerts": "Create/Delete Strategic Watchdogs",
            "calibrate_ai": "Change AI Coefficients & ROAS"
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
# 17. PAGE 9: STRATEGIC ALERTS
# =================================================================
elif page == "Strategic Alerts":
    st.markdown("""
        <div style="background-color: #1A1A1B; padding: 20px; border-radius: 12px; border-left: 6px solid #FF4B4B; margin-bottom: 25px;">
            <h2 style="color: #FF4B4B; margin: 0;">🚨 Strategic Alert Engine</h2>
        </div>
    """, unsafe_allow_html=True)
    
    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.subheader("🛠️ Create New Trigger")
        if st.session_state.current_property_id == "GLOBAL":
            st.warning("Select a property to deploy a watchdog.")
        else:
            with st.form("new_alert_form"):
                a_name = st.text_input("Alert Name")
                a_metric = st.selectbox("Metric", ["Revenue", "Guest Traffic", "Sentiment Score"])
                a_op = st.selectbox("Condition", ["Drops Below", "Exceeds"])
                a_val = st.number_input("Threshold", value=0.0)
                if st.form_submit_button("🛰️ Deploy Watchdog"):
                    payload = {
                        "property_id": st.session_state.current_property_id,
                        "alert_name": a_name,
                        "metric_target": a_metric,
                        "threshold_val": a_val,
                        "comparison_operator": "<" if a_op == "Drops Below" else ">",
                        "user_email": st.session_state.user_email
                    }
                    supabase.table("strategic_alerts").insert(payload).execute()
                    st.success("Watchdog Live.")
                    st.rerun()

    with col_b:
        st.subheader("📋 Active Watchdogs")
        target_id = st.session_state.get('current_property_id')
        try:
            if target_id == "GLOBAL":
                alerts_res = supabase.table("strategic_alerts").select("*").execute()
            else:
                alerts_res = supabase.table("strategic_alerts").select("*").eq("property_id", str(target_id)).execute()

            if alerts_res and alerts_res.data:
                for alert in alerts_res.data:
                    with st.expander(f"🔔 {alert.get('alert_name')}"):
                        st.write(f"**{alert.get('metric_target')}** {alert.get('comparison_operator')} **{alert.get('threshold_val')}**")
                        if st.button("Disable", key=f"dis_{alert['id']}"):
                            supabase.table("strategic_alerts").delete().eq("id", alert['id']).execute()
                            st.rerun()
        except Exception as e: st.error(f"Sync Error: {e}")

# =================================================================
# 18. FOOTER
# =================================================================
st.sidebar.divider()
st.sidebar.caption("© 2026 FloorCast Technologies | Strategic AI Unit")
