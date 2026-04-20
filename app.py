import streamlit as st
import pandas as pd
import datetime
import json
import asyncio
import numpy as np
from env_canada import ECWeather
import google.generativeai as genai
from supabase import create_client

# 1. PAGE CONFIG (Must be the very first Streamlit command)
st.set_page_config(page_title="FloorCast", layout="wide")

# 2. THE UNIFIED FORENSIC ENGINE (Single Source of Truth)
def get_forensic_metrics(df, coeffs):
    """Calculates KPIs once for the entire app. Safely handles missing columns."""
    if df is None or df.empty:
        return {"predictability": "0.0%", "digital_lift": "0.0%", "heartbeats": {}}

    df = df.copy()
    
    # Standardize and protect against missing columns
    cols_to_ensure = {
        'ad_clicks': ['ad_clicks', 'Clicks', 'Ad_Clicks'],
        'social_impressions': ['social_impressions', 'Impressions', 'Social_Imp'],
        'actual_traffic': ['actual_traffic', 'Traffic'],
        'actual_coin_in': ['actual_coin_in', 'Revenue', 'Coin_In']
    }

    for target, aliases in cols_to_ensure.items():
        existing = next((c for c in aliases if c in df.columns), None)
        if existing:
            df.rename(columns={existing: target}, inplace=True)
        if target not in df.columns:
            df[target] = 0 
        df[target] = pd.to_numeric(df[target], errors='coerce').fillna(0)

    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['day_name'] = df['entry_date'].dt.day_name()
    
    # Calculate DOW Heartbeats
    heartbeats = df.groupby('day_name')['actual_traffic'].mean().to_dict()
    
    # Weights from Engine
    c_clicks = coeffs.get('Clicks', 0.02)
    c_social = coeffs.get('Social_Imp', 0.0002)
    
    # Forensic 1: Digital Lift %
    total_traffic = df['actual_traffic'].sum()
    marketing_impact = (df['ad_clicks'].sum() * c_clicks) + (df['social_impressions'].sum() * c_social)
    lift_val = (marketing_impact / total_traffic * 100) if total_traffic > 0 else 0

    # Forensic 2: AI Predictability (1 - MAPE)
    df['expected'] = df.apply(lambda x: heartbeats.get(x['day_name'], 0) + 
                             (x['ad_clicks'] * c_clicks) + 
                             (x['social_impressions'] * c_social), axis=1)

    df_filtered = df[df['actual_traffic'] > 0].copy()
    if df_filtered.empty:
        return {"predictability": "0.0%", "digital_lift": f"{lift_val:.1f}%", "heartbeats": heartbeats}
        
    mape = (np.abs(df_filtered['actual_traffic'] - df_filtered['expected']) / df_filtered['actual_traffic']).mean()
    pred_val = (1 - mape) * 100 if not np.isnan(mape) else 0

    return {
        "predictability": f"{pred_val:.1f}%",
        "digital_lift": f"{lift_val:.1f}%",
        "heartbeats": heartbeats
    }

# 3. INITIALIZE CLIENTS
url = st.secrets["SUPABASE_URL"]
key = st.secrets["SUPABASE_KEY"]
supabase = create_client(url, key)

if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

# 4. WEATHER & AUTH LOGIC
async def fetch_live_ec_data():
    try:
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {"current": ec.conditions, "forecast": ec.daily_forecasts, "alerts": ec.alerts}
    except:
        return {"error": "Weather Unavailable"}

if 'weather_data' not in st.session_state:
    st.session_state.weather_data = asyncio.run(fetch_live_ec_data())

if 'user_authenticated' not in st.session_state:
    st.session_state.user_authenticated = False

# 5. GATEKEEPER (Login UI)
if not st.session_state.user_authenticated:
    st.markdown("<div style='text-align:center; padding:50px;'><h1 style='color:#FFCC00;'>🎰 FloorCast</h1><h3>Digital Lift & Predictor Login</h3></div>", unsafe_allow_html=True)
    with st.container(border=True):
        email = st.text_input("Email")
        pw = st.text_input("Password", type="password")
        if st.button("Login", use_container_width=True):
            try:
                res = supabase.auth.sign_in_with_password({"email": email, "password": pw})
                if res.user:
                    st.session_state.user_authenticated = True
                    st.rerun()
            except:
                st.error("Invalid credentials.")
    st.stop()

# 6. HYDRATE ENGINE WEIGHTS & DATA
if 'coeffs' not in st.session_state:
    try:
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        if response.data:
            st.session_state.coeffs = response.data[0]
        else:
            st.session_state.coeffs = {"id": 1, "Intercept": 3250, "Avg_Coin_In": 112.50, "Clicks": 0.02, "Social_Imp": 0.0002}
    except:
        st.session_state.coeffs = {"Intercept": 3250, "Avg_Coin_In": 112.50}

@st.cache_data(ttl=600)
def fetch_ledger_data():
    try:
        res = supabase.table("ledger").select("*").execute()
        return res.data if res.data else []
    except:
        return []

ledger_data = fetch_ledger_data()

# --- MODERN UI STYLING (The CSS) ---
st.markdown("""
    <style>
    /* Main Background - Clean Professional Grey */
    .stApp {
        background-color: #f4f7f9;
    }
    
    /* Bento Box / Card Effect for Tab Content */
    div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column;"] > div[data-testid="stVerticalBlock"] {
        border: 1px solid #e6e9ef;
        border-radius: 12px;
        padding: 25px;
        background-color: #ffffff;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        margin-bottom: 20px;
    }

    /* Tab Navigation Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: transparent;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff;
        border: 1px solid #e6e9ef;
        border-radius: 8px 8px 0px 0px;
        padding: 12px 24px;
        font-weight: 600;
        transition: all 0.3s ease;
    }

    /* Active Tab - Hard Rock Gold */
    .stTabs [aria-selected="true"] {
        background-color: #FFCC00 !important; 
        color: #000000 !important;
        border-bottom: 3px solid #000000;
    }

    /* Metric Styling */
    [data-testid="stMetricValue"] {
        color: #111;
        font-weight: 700;
    }
    </style>
    """, unsafe_allow_html=True)

# --- HEADER & NAVIGATION LOGOUT ---
header_col1, header_col2 = st.columns([4, 1])

with header_col1:
    st.markdown("<h1 style='color: #FFCC00; margin:0;'>🎰 FloorCast</h1>", unsafe_allow_html=True)

with header_col2:
    # A small, clean logout button aligned to the right
    if st.button("🔓 Logout", use_container_width=True):
        supabase.auth.sign_out()
        st.session_state.user_authenticated = False
        # Clear sensitive chat history on logout
        if 'messages' in st.session_state:
            st.session_state.messages = [] 
        st.rerun()

st.divider() # Creates the visual 'NAV bar' line

# 8. MAIN NAVIGATION
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 Executive Overview", "📑 Ledger Management", "📊 Property Analytics", 
    "⚙️ Engine Control", "🧠 FloorCast Analyst", "📋 Master Report", "🧪 Forecast Sandbox"
])

# --- TAB 1: EXECUTIVE OVERVIEW (FORENSIC DASHBOARD) ---
with tab1:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📈 Executive Performance</h2>
            <p style="color: #888; margin: 0;">Forensic Reality: Visualizing attribution and current month performance.</p>
        </div>
    """, unsafe_allow_html=True)

    if not ledger_data:
        st.info("👋 Welcome. Please upload your ledger in **Tab 2** to see your property performance metrics.")
    else:
        # 1. PULL UNIFIED TRUTH
        df_ledger = pd.DataFrame(ledger_data)
        
        # Call the unified function we placed at the top of the script
        metrics = get_forensic_metrics(df_ledger, st.session_state.coeffs)

        # 2. TOP-LEVEL KPI TILES
        kpi1, kpi2, kpi3 = st.columns(3)
        
        with kpi1:
            st.metric(
                label="AI Predictability", 
                value=metrics['predictability'],
                help="Measure of how accurately the engine explains traffic variations based on history."
            )
        
        with kpi2:
            st.metric(
                label="Digital Lift", 
                value=metrics['digital_lift'],
                help="The specific percentage of traffic attributed to Digital Ads and Social efforts."
            )
            
        with kpi3:
            # Safely calculate total revenue by finding the right column
            rev_col = next((c for c in ['actual_coin_in', 'Revenue', 'Coin_In'] if c in df_ledger.columns), None)
            total_rev = pd.to_numeric(df_ledger[rev_col], errors='coerce').sum() if rev_col else 0
            st.metric(label="YTD Ledger Revenue", value=f"${total_rev:,.0f}")

        st.divider()

        # 3. FORENSIC VISUALIZATION (Actual vs. Expected)
        st.write("### 🔍 Model Accuracy: Actual vs. Predicted Traffic")
        
        df_viz = df_ledger.copy()
        df_viz['entry_date'] = pd.to_datetime(df_viz['entry_date'])
        df_viz['day_name'] = df_viz['entry_date'].dt.day_name()
        
        # Ensure we use the exact same logic as the metrics for the chart line
        heartbeats = metrics['heartbeats']
        c_clicks = st.session_state.coeffs.get('Clicks', 0.02)
        c_social = st.session_state.coeffs.get('Social_Imp', 0.0002)

        # Map internal column names safely for the visualization
        click_col = next((c for c in ['ad_clicks', 'Clicks'] if c in df_viz.columns), None)
        imp_col = next((c for c in ['social_impressions', 'Impressions'] if c in df_viz.columns), None)
        traffic_col = next((c for c in ['actual_traffic', 'Traffic'] if c in df_viz.columns), None)
        
        # Lambda function that handles missing columns without crashing
        def calculate_prediction(row):
            base = heartbeats.get(row['day_name'], 0)
            clicks = pd.to_numeric(row[click_col], errors='coerce') if click_col else 0
            imps = pd.to_numeric(row[imp_col], errors='coerce') if imp_col else 0
            return base + (np.nan_to_num(clicks) * c_clicks) + (np.nan_to_num(imps) * c_social)

        df_viz['Predicted'] = df_viz.apply(calculate_prediction, axis=1)
        
        if traffic_col:
            df_viz = df_viz.rename(columns={traffic_col: 'Actual Traffic'})
            df_viz = df_viz.sort_values('entry_date')
            
            st.line_chart(
                df_viz, 
                x='entry_date', 
                y=['Actual Traffic', 'Predicted'], 
                color=["#FFCC00", "#555555"]
            )
        else:
            st.error("Missing Traffic column in ledger data.")

        st.divider()

        # 4. CURRENT MONTH PERFORMANCE TABLE
        st.write(f"### 🗓️ Performance Ledger: {datetime.date.today().strftime('%B %Y')}")
        
        current_month = datetime.date.today().month
        current_year = datetime.date.today().year
        
        df_current = df_viz[
            (df_viz['entry_date'].dt.month == current_month) & 
            (df_viz['entry_date'].dt.year == current_year)
        ].copy()

        if not df_current.empty:
            df_current = df_current.sort_values(by='entry_date', ascending=False)
            
            # Formatting for display
            display_cols = ['entry_date', 'Actual Traffic', rev_col, click_col, imp_col]
            final_cols = [c for c in display_cols if c is not None and c in df_current.columns]
            
            st.dataframe(
                df_current[final_cols].style.format({
                    "Actual Traffic": "{:,.0f}",
                    rev_col: "${:,.2f}" if rev_col else "{:}",
                    click_col: "{:,.0f}" if click_col else "{:}",
                    imp_col: "{:,.0f}" if imp_col else "{:}"
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info(f"No entries logged yet for {datetime.date.today().strftime('%B %Y')}.")
# --- TAB 2: LEDGER MANAGEMENT ---
with tab2:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📑 Ledger Management</h2>
            <p style="color: #888; margin: 0;">Update property performance and sync with the Vault.</p>
        </div>
    """, unsafe_allow_html=True)

    col_a, col_b = st.columns([1, 1])

    with col_a:
        st.write("### ✍️ Manual Daily Entry")
        with st.form("manual_entry", clear_on_submit=True):
            entry_date = st.date_input("Date", datetime.date.today())
            traffic = st.number_input("Total Traffic (Headcount)", min_value=0)
            coin_in = st.number_input("Total Coin-In ($)", min_value=0.0, format="%.2f")
            
            st.divider()
            st.write("**Marketing Metrics**")
            clicks = st.number_input("Ad Clicks", min_value=0)
            impressions = st.number_input("Ad Impressions", min_value=0)
            social = st.number_input("Social Engagements", min_value=0)
            
            if st.form_submit_button("💾 Save to Vault"):
                # MATCHING YOUR EXACT SUPABASE SCHEMA:
                new_row = {
                    "entry_date": entry_date.isoformat(),
                    "actual_traffic": traffic,
                    "actual_coin_in": coin_in,
                    "ad-clicks": clicks,           # FIXED: Uses the dash
                    "ad_impressions": impressions, # FIXED: Specific name
                    "social_engagements": social   # FIXED: Specific name
                }
                try:
                    supabase.table("ledger").insert([new_row]).execute()
                    st.success(f"Success! Data for {entry_date} saved.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed. Check your column names: {e}")

    with col_b:
        st.write("### 📤 Bulk CSV Upload")
        uploaded_file = st.file_uploader("Drop your ledger CSV here", type="csv")
        
        if uploaded_file is not None:
            df_upload = pd.read_csv(uploaded_file)
            if st.button("🚀 Push to Vault", use_container_width=True):
                try:
                    # Rename CSV columns to match the specific DB naming
                    df_upload.rename(columns={
                        'clicks': 'ad-clicks',
                        'impressions': 'ad_impressions',
                        'social': 'social_engagements'
                    }, inplace=True)
                    
                    data_dict = df_upload.to_dict(orient='records')
                    supabase.table("ledger").insert(data_dict).execute()
                    st.success("Bulk upload complete!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Upload failed: {e}")

    # --- LEDGER EDITOR ---
    st.divider()
    st.write("### 📜 Ledger Editor")
    
    if ledger_data:
        df_history = pd.DataFrame(ledger_data)
        
        if 'entry_date' in df_history.columns:
            df_history['entry_date'] = pd.to_datetime(df_history['entry_date'])
            df_history = df_history.sort_values(by='entry_date', ascending=False)
        
        # Display Mapping
        editor_config = {
            "id": None,
            "entry_date": st.column_config.DateColumn("Date", disabled=True),
            "actual_traffic": st.column_config.NumberColumn("Traffic"),
            "actual_coin_in": st.column_config.NumberColumn("Coin-In", format="$%.2f"),
            "ad-clicks": st.column_config.NumberColumn("Ad Clicks"),
            "ad_impressions": st.column_config.NumberColumn("Ad Impressions"),
            "social_engagements": st.column_config.NumberColumn("Social Engagements")
        }

        edited_df = st.data_editor(df_history, column_config=editor_config, use_container_width=True, hide_index=True)

        if st.button("✅ Confirm & Sync Changes"):
            try:
                for _, row in edited_df.iterrows():
                    up_data = row.to_dict()
                    up_data['entry_date'] = up_data['entry_date'].strftime('%Y-%m-%d')
                    supabase.table("ledger").update(up_data).eq("id", up_data['id']).execute()
                st.success("Vault Updated!")
                st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")

# --- TAB 3: STRATEGY & ROI ---
with tab3:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🚀 Strategy & Digital ROI</h2>
            <p style="color: #888; margin: 0;">Calculating the financial lift of digital marketing efforts.</p>
        </div>
    """, unsafe_allow_html=True)

    # Pull latest coefficients
    c = st.session_state.coeffs
    click_weight = c.get('Clicks', 0)
    promo_lift = c.get('Promo', 0)
    avg_spend = c.get('Avg_Coin_In', 1200)

    # 1. ROI CALCULATOR
    st.write("### Digital Lift Analysis")
    col_input, col_result = st.columns([1, 1])
    
    with col_input:
        st.info("Current Engine Weights applied:")
        st.write(f"* **Weight per Click:** {click_weight}")
        st.write(f"* **Promo Base Lift:** {promo_lift} guests")
        st.write(f"* **Revenue Value:** ${avg_spend:,.2f} / head")

    with col_result:
        # Example Calculation based on 1,000 clicks
        test_clicks = 1000
        attributed_traffic = (test_clicks * click_weight) + promo_lift
        attributed_rev = attributed_traffic * avg_spend
        
        st.success(f"**Attributed Revenue per 1k Clicks**")
        st.header(f"${attributed_rev:,.2f}")
        st.caption(f"Estimated {attributed_traffic:,.0f} additional guests driven by digital.")

    # 2. HISTORICAL ROI TREND
    df_strat = pd.DataFrame(ledger_data)
    if not df_strat.empty:
        # Calculate daily attributed revenue
        df_strat['Digital_Revenue_Lift'] = ((df_strat['ad_clicks'] * click_weight) + 
                                           (df_strat['active_promo'].astype(int) * promo_lift)) * avg_spend
        
        st.write("### Historical Digital Revenue Contribution")
        st.area_chart(df_strat.set_index('entry_date')['Digital_Revenue_Lift'])

# --- TAB 4: ENGINE CONTROL (CALIBRATION) ---
with tab4:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Calibration</h2>
            <p style="color: #888; margin: 0;">Adjust marketing multipliers based on industry benchmarks and forensic reality.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. INDUSTRY BENCHMARK REFERENCE
    with st.expander("📊 View Casino Industry Benchmarks"):
        st.write("""
        | Metric | Industry Average (Gaming) | Your Current Weight |
        | :--- | :--- | :--- |
        | **Social Impressions** | 0.0001 - 0.0005 (1 guest per 2k-10k views) | {social_w:.4f} |
        | **Ad Clicks** | 0.02 - 0.08 (2% - 8% conversion to floor) | {click_w:.2f} |
        | **Major Promo** | 1,000 - 5,000 (Flat guest lift for major events) | {promo_w:,.0f} |
        | **Weather Friction** | -20 to -60 (Guests lost per cm of snow) | {weather_w:,.0f} |
        """.format(
            social_w=st.session_state.coeffs.get('Social_Imp', 0.0002),
            click_w=st.session_state.coeffs.get('Clicks', 0.02),
            promo_w=st.session_state.coeffs.get('Promo', 450.0),
            weather_w=st.session_state.coeffs.get('Snow_cm', -45.0)
        ))

    # 2. MANUAL CALIBRATION FORM
    with st.form("engine_settings"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("### 📣 Marketing Multipliers")
            
            # Safeguard Clamping
            curr_clicks = float(st.session_state.coeffs.get('Clicks', 0.02))
            new_clicks = st.slider("Click Conversion (Weight)", 0.00, 0.20, 
                                 value=max(min(curr_clicks, 0.20), 0.00))
            
            curr_social = float(st.session_state.coeffs.get('Social_Imp', 0.0002))
            new_social = st.number_input("Social Impression Weight", 0.0000, 0.0100, 
                                      value=max(min(curr_social, 0.0100), 0.0000), format="%.4f")
            
            curr_promo = int(st.session_state.coeffs.get('Promo', 450))
            new_promo = st.number_input("Promo Lift (Flat Guest Count)", 0, 10000, 
                                     value=max(min(curr_promo, 10000), 0))

        with col2:
            st.write("### ❄️ Environmental Friction")
            
            curr_snow = int(st.session_state.coeffs.get('Snow_cm', -45))
            new_snow = st.slider("Snow Friction (Guests lost per cm)", -1000, 0, 
                                value=max(min(curr_snow, 0), -1000))
            
            curr_rain = int(st.session_state.coeffs.get('Rain_mm', -12))
            new_rain = st.slider("Rain Friction (Guests lost per mm)", -500, 0, 
                                value=max(min(curr_rain, 0), -500))
            
            curr_coin = float(st.session_state.coeffs.get('Avg_Coin_In', 112.50))
            new_coin = st.number_input("Avg Spend Per Head ($)", 0.0, 5000.0, 
                                     value=max(min(curr_coin, 5000.0), 0.0))

        # 3. CLEAN SYNC LOGIC
        if st.form_submit_button("💾 Save Calibration to Vault"):
            st.session_state.coeffs.update({
                'Clicks': new_clicks,
                'Social_Imp': new_social,
                'Promo': new_promo,
                'Snow_cm': new_snow,
                'Rain_mm': new_rain,
                'Avg_Coin_In': new_coin
            })
            
            db_schema_columns = ['id', 'Intercept', 'Avg_Coin_In', 'Temp_C', 'Snow_cm', 'Rain_mm', 'Promo', 'Clicks', 'Impressions']
            sync_payload = {k: v for k, v in st.session_state.coeffs.items() if k in db_schema_columns}
            sync_payload['Impressions'] = st.session_state.coeffs.get('Social_Imp', 0.0002)

            try:
                supabase.table("coefficients").update(sync_payload).eq("id", 1).execute()
                st.success("✅ Engine Calibrated! Data synced to Vault.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Sync failed: {e}")

    # 4. ENGINE INTEGRITY (Appears only once)
    st.divider()
    st.write("### 🔍 Engine Integrity")
    
    # Validation Logic
    if st.session_state.coeffs.get('Clicks', 0) < 0.015:
        st.warning("⚠️ **Low Attribution Warning:** Your Click Weight is significantly below industry averages. This may cause 'Digital Lift' in Tab 1 to appear undervalued.")
    
    if st.session_state.coeffs.get('Avg_Coin_In', 0) > 400:
        st.info("ℹ️ **Premium Spend Profile:** Your Avg. Spend per head is set for a high-limit environment. If this includes non-gaming revenue, ensure the ledger reflects total property spend.")

    # --- 4. DATA HEALTH CHECK ---
    st.divider()
    st.write("### 🔍 Engine Integrity")
    if st.session_state.coeffs.get('Clicks', 0) < 0.01:
        st.warning("⚠️ Warning: Click Weight is below industry floor (0.02). This will cause Digital Lift to appear artificially low.")
    if abs(st.session_state.coeffs.get('Snow_cm', 0)) > 150:
        st.info("ℹ️ Snow friction is set high. Ensure this accounts for road closures, not just property discomfort.")
# --- TAB 5: FORENSIC ANALYST & PRODUCT EXPERT ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧠 Forensic Consultant & App Expert</h2>
            <p style="color: #888; margin: 0;">Real-time KPI calculation and strategic guidance.</p>
        </div>
    """, unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 1. THE FORENSIC DATA VAULT
    vault_metrics = {}
    if ledger_data:
        df_vault = pd.DataFrame(ledger_data).copy()
        df_vault['entry_date'] = pd.to_datetime(df_vault['entry_date'])
        
        # --- DYNAMIC COLUMN NORMALIZATION ---
        col_map = {
            'social_impressions': ['social_impressions', 'Impressions', 'Social_Imp'],
            'ad_clicks': ['ad_clicks', 'Clicks', 'Ad_Clicks'],
            'actual_traffic': ['actual_traffic', 'Traffic'],
            'actual_coin_in': ['actual_coin_in', 'Revenue']
        }
        
        for target, aliases in col_map.items():
            existing_col = next((c for c in aliases if c in df_vault.columns), None)
            if existing_col:
                df_vault.rename(columns={existing_col: target}, inplace=True)
                df_vault[target] = pd.to_numeric(df_vault[target], errors='coerce').fillna(0)
            else:
                df_vault[target] = 0

        # Basic Averages
        avg_imp = float(df_vault['social_impressions'].mean())
        df_vault['day_name'] = df_vault['entry_date'].dt.day_name()
        heartbeats = df_vault.groupby('day_name')['actual_traffic'].mean().to_dict()
        
        # --- FORENSIC KPI CALCULATIONS ---
        clean_weights = {k: v for k, v in st.session_state.coeffs.items() if k not in ['Intercept', 'DOW_Profiles']}
        
        # Digital Lift: Attribution of marketing vs Total Traffic
        total_traffic = df_vault['actual_traffic'].sum()
        marketing_impact = (df_vault['ad_clicks'].sum() * clean_weights.get('Clicks', 0.02)) + \
                           (df_vault['social_impressions'].sum() * clean_weights.get('Social_Imp', 0.0002))
        digital_lift_pct = (marketing_impact / total_traffic) * 100 if total_traffic > 0 else 0

        # AI Predictability: (1 - Mean Absolute Percentage Error)
        df_vault['expected'] = df_vault.apply(lambda x: heartbeats.get(x['day_name'], 0) + 
                                            (x['ad_clicks'] * clean_weights.get('Clicks', 0.02)) + 
                                            (x['social_impressions'] * clean_weights.get('Social_Imp', 0.0002)), axis=1)
        
        import numpy as np
        mape = (np.abs(df_vault['actual_traffic'] - df_vault['expected']) / df_vault['actual_traffic']).replace([np.inf, -np.inf], np.nan).dropna().mean()
        predictability_score = (1 - mape) * 100 if not np.isnan(mape) else 0

        vault_metrics = {
            "avg_social_imp": avg_imp,
            "heartbeats": heartbeats,
            "digital_lift": f"{digital_lift_pct:.1f}%",
            "predictability": f"{predictability_score:.1f}%"
        }

    # 2. CHAT INPUT
    prompt = st.chat_input("Ask about KPIs, predictability, or how to use the app...")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            import google.generativeai as genai
            import json
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            history_payload = []
            for m in st.session_state.messages[:-1]:
                role = "model" if m["role"] == "assistant" else "user"
                history_payload.append({"role": role, "parts": [m["content"]]})
            
            # THE FORENSIC BRAIN
            sys_context = f"""
            SYSTEM ROLE: Chief Strategy Officer & Product Expert.
            
            CORE KPIs (ACTIVE DATA):
            - AI Predictability: {vault_metrics.get('predictability', 'N/A')}
            - Digital Lift: {vault_metrics.get('digital_lift', 'N/A')}

            PRODUCT KNOWLEDGE:
            - Tab 1 (Dashboard): Shows Digital Lift and Predictability forensic views.
            - Tab 2 (Ledger): Database for CSV uploads and manual data entry.
            - Tab 3 (Sandbox): Simulation tool for 'What-If' scenarios.
            - Tab 4 (Engine): Calibration of weights using industry guardrails.
            - Tab 5 (Consultant): Real-time analysis and user training.

            MATH RULES:
            1. DOW Heartbeats are the ONLY baseline. 
            2. NEVER add the Global Intercept (4365) to the Heartbeat.
            3. Use {vault_metrics.get('heartbeats', {})} for baseline analysis.

            MANDATE: 
            - Answer product questions like a Help Guide.
            - Answer data questions as a Forensic CSO (Concise Executive Summary first).
            - End with 1 strategic follow-up question.
            """

            chat = model.start_chat(history=history_payload)
            response = chat.send_message(f"{sys_context}\n\nUSER MESSAGE: {prompt}")
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()

        except Exception as e:
            st.error(f"Consultation Error: {e}")

    # 3. REVERSED FEED
    for message in reversed(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if st.button("🗑️ Reset Forensic Session", use_container_width=True):
        st.session_state.messages = []
        st.rerun()
# --- TAB 6: MASTER ANALYTICS & FORENSIC REPORT ---
with tab6:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Master Forensic Report</h2>
            <p style="color: #888; margin: 0;">Accounting-grade analysis of property performance and marketing ROI.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. PULL ENGINE CONSTANTS
    c = st.session_state.coeffs
    avg_spend = c.get('Avg_Coin_In', 1200)
    click_weight = c.get('Clicks', 0)
    promo_lift = c.get('Promo', 0)
    intercept = c.get('Intercept', 0)

    df_rep = pd.DataFrame(ledger_data).copy()
    
    if not df_rep.empty:
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        # --- THE CALCULATION ENGINE (Hard Math Only) ---
        # A. Marketing Attribution
        df_rep['attr_traffic'] = (df_rep['ad_clicks'] * click_weight) + (df_rep['active_promo'].astype(int) * promo_lift)
        df_rep['attr_revenue'] = df_rep['attr_traffic'] * avg_spend
        
        # B. Efficiency & Variance Metrics
        df_rep['actual_spend_avg'] = df_rep['actual_coin_in'] / df_rep['actual_traffic']
        df_rep['rev_variance'] = df_rep['actual_coin_in'] - (df_rep['actual_traffic'] * avg_spend)
        
        # C. Global Aggregates
        total_rev = df_rep['actual_coin_in'].sum()
        total_vis = df_rep['actual_traffic'].sum()
        total_attr_rev = df_rep['attr_revenue'].sum()
        total_days = len(df_rep)

        # 2. TOP-LEVEL PERFORMANCE TILES
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total YTD Coin-In", f"${total_rev:,.0f}")
        with col2:
            st.metric("Marketing ROI (Est)", f"${total_attr_rev:,.0f}")
        with col3:
            st.metric("Base Traffic Avg", f"{total_vis / total_days:,.0f}")
        with col4:
            st.metric("Ledger Spend Avg", f"${total_rev / total_vis:,.2f}")

        st.write("---")

        # 3. THE MASTER FORENSIC DATA TABLE
        st.write("### 🔍 Daily Performance Breakdown")
        
        # Build the final exportable dataframe
        master_df = df_rep[[
            'entry_date', 'actual_traffic', 'actual_coin_in', 'ad_clicks'
        ]].copy()
        
        master_df['Digital Traffic'] = df_rep['attr_traffic']
        master_df['Digital Revenue'] = df_rep['attr_revenue']
        master_df['Actual $/Head'] = df_rep['actual_spend_avg']
        master_df['vs. Engine Target'] = df_rep['rev_variance']
        
        st.dataframe(
            master_df.sort_values('entry_date', ascending=False),
            column_config={
                "entry_date": "Date",
                "actual_traffic": st.column_config.NumberColumn("Total Traffic", format="%d"),
                "actual_coin_in": st.column_config.NumberColumn("Total Revenue", format="$%d"),
                "Digital Revenue": st.column_config.NumberColumn("Digital Lift", format="$%d"),
                "Actual $/Head": st.column_config.NumberColumn("Avg Spend", format="$%.2f"),
                "vs. Engine Target": st.column_config.NumberColumn("Variance", format="$%d")
            },
            use_container_width=True,
            hide_index=True
        )

        # 4. RATIO & VOLATILITY ANALYSIS
        st.write("### 📉 Operational Efficiency")
        c1, c2, c3 = st.columns(3)
        
        with c1:
            with st.container(border=True):
                st.write("**Marketing Contribution**")
                ratio = (total_attr_rev / total_rev) * 100 if total_rev > 0 else 0
                st.title(f"{ratio:.1f}%")
                st.caption("Percentage of YTD Revenue driven by Digital weights.")

        with c2:
            with st.container(border=True):
                st.write("**Revenue Volatility**")
                std_dev = df_rep['actual_coin_in'].std()
                st.title(f"${std_dev:,.0f}")
                st.caption("Standard deviation (Daily Revenue Risk).")

        with c3:
            with st.container(border=True):
                st.write("**Ad Click Efficiency**")
                # Revenue per individual click based on current weights
                rev_per_click = click_weight * avg_spend
                st.title(f"${rev_per_click:.2f}")
                st.caption("Revenue value of a single Ad Click.")

        # 5. DATA EXPORT
        st.write("---")
        st.download_button(
            label="📥 Export Forensic Report to CSV",
            data=master_df.to_csv(index=False),
            file_name=f"HR_Ottawa_Forensic_Report_{datetime.date.today()}.csv",
            mime="text/csv",
            use_container_width=True
        )

    else:
        st.warning("No data found in ledger. Add entries in the Input tab to generate reports.")

# --- TAB 7: SYNCHRONIZED FORECAST SANDBOX ---
with tab7:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧪 Forecast Sandbox</h2>
            <p style="color: #888; margin: 0;">Fully Synchronized: Aligned with Tab 5 Strategy & Live Environment Canada Data.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. DATE RANGE SELECTION
    today = datetime.date.today()
    date_range = st.date_input(
        "Select Simulation Window:",
        value=(today, today + datetime.timedelta(days=2)),
        help="The Sandbox will pull specific daily forecasts for this entire window."
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        num_days = (end_date - start_date).days + 1
        
        # Pull live data fetched at top of app
        live_forecast = st.session_state.get('weather_data', {}).get('forecast', [])

        # 2. SCENARIO INPUTS
        st.write(f"### 🎛️ Simulation Parameters ({num_days} Days)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Marketing & Social**")
            s_promo = st.checkbox("Active Promotion?", value=False)
            s_clicks = st.number_input("Daily Ad Clicks", value=500)
            s_imp = st.number_input("Daily Social Impressions", value=10000)
            s_eng = st.number_input("Daily Social Engagement", value=500)
        
        with col2:
            st.write("**Scenario Toggles**")
            weather_mode = st.radio("Weather Source:", ["Live EC Forecast", "Manual Overrides"])
            m_temp = st.slider("Manual Temp (°C)", -30, 40, 15, disabled=(weather_mode == "Live EC Forecast"))
            m_rain = st.slider("Manual Rain (mm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))
            m_snow = st.slider("Manual Snow (cm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))

        with col3:
            st.write("**Engine Baseline**")
            c = st.session_state.coeffs
            st.metric("Spend Anchor", f"${c.get('Avg_Coin_In', 1200):,.2f}")
            st.info("The Sandbox is now triangulating historical DOW averages per day.")

        # 3. UNIFIED CALCULATION LOOP (Crucial for closing the disconnect)
        total_range_traffic = 0
        total_range_revenue = 0
        
        # Load Ledger for DOW Averages
        df_sb = pd.DataFrame(ledger_data)
        df_sb['entry_date'] = pd.to_datetime(df_sb['entry_date'])
        df_sb['day_name'] = df_sb['entry_date'].dt.day_name()
        dow_profiles = df_sb.groupby('day_name')['actual_traffic'].mean().to_dict()

        current_date = start_date
        while current_date <= end_date:
            day_str = current_date.strftime("%Y-%m-%d")
            day_name = current_date.strftime("%A")
            
            # Identify Weather for THIS specific day
            if weather_mode == "Live EC Forecast":
                ec_day = next((item for item in live_forecast if day_str in str(item.get('datetime'))), None)
                day_temp = ec_day.get('temperature', 15.0) if ec_day else m_temp
                day_rain = m_rain # Fallback or map from EC
                day_snow = m_snow
            else:
                day_temp, day_rain, day_snow = m_temp, m_rain, m_snow

            # THE MATH (Aligned with Tab 4 & 5)
            # 1. Start with the historical heartbeat for THIS specific day
            base_traffic = dow_profiles.get(day_name, c.get('Intercept', 1000))
            
            # 2. Add Marketing Lifts
            promo_lift = c['Promo'] if s_promo else 0
            social_lift = (s_imp * c.get('Social_Imp', 0.0002)) + (s_eng * c.get('Social_Eng', 0.01))
            marketing_lift = (s_clicks * c.get('Clicks', 0.001))
            
            # 3. Apply Weather Friction
            weather_friction = (day_temp * c.get('Temp_C', 0)) + (day_rain * c.get('Rain_mm', -2.0)) + (day_snow * c.get('Snow_cm', -4.0))
            
            # 4. Aggregate
            daily_total = base_traffic + promo_lift + social_lift + marketing_lift + weather_friction
            total_range_traffic += daily_total
            total_range_revenue += (daily_total * c.get('Avg_Coin_In', 1200.0))
            
            current_date += datetime.timedelta(days=1)

        # 4. OUTPUTS
        st.write("---")
        res1, res2, res3 = st.columns(3)
        res1.metric("Predicted Traffic", f"{int(total_range_traffic):,} Guests")
        res2.metric("Total Window Revenue", f"${total_range_revenue:,.2f}")
        res3.metric("Daily Avg Volume", f"{int(total_range_traffic / num_days)} / day")
