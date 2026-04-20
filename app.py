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
st.set_page_config(page_title="FloorCast | Hard Rock Ottawa", layout="wide", page_icon="🎰")

# 2. THE UNIFIED FORENSIC ENGINE (Single Source of Truth)
def get_forensic_metrics(df, coeffs):
    """Calculates KPIs once for the entire app. Safely handles missing columns."""
    if df is None or len(df) == 0:
        return {"predictability": "0.0%", "digital_lift": "0.0%", "heartbeats": {}}

    df = pd.DataFrame(df).copy()
    
    # Standardize and protect against missing columns
    cols_to_ensure = {
        'ad_clicks': ['ad_clicks', 'Clicks', 'Ad_Clicks'],
        'ad_impressions': ['ad_impressions', 'Impressions', 'Social_Imp'],
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
    
    # Calculate DOW Heartbeats (Historical Baseline)
    heartbeats = df.groupby('day_name')['actual_traffic'].mean().to_dict()
    
    # Weights from Engine
    c_clicks = coeffs.get('Clicks', 0.02)
    c_social = coeffs.get('Social_Imp', 0.0002)

    # --- UPDATED OOH LOGIC ---
    # Static boards own 100% of the time
    c_static = coeffs.get('Static_Weight', 50.0)
    n_static = coeffs.get('Static_Count', 2)
    
    # Digital boards are shared (usually 10% Share of Voice)
    c_digital_ooh = coeffs.get('Digital_OOH_Weight', 10.0) 
    n_digital_ooh = coeffs.get('Digital_OOH_Count', 4)
    
    total_ooh_lift = (c_static * n_static) + (c_digital_ooh * n_digital_ooh)
    
    # Forensic 1: Digital Lift %
    total_traffic = df['actual_traffic'].sum()
    marketing_impact = (df['ad_clicks'].sum() * c_clicks) + (df['ad_impressions'].sum() * c_social)
    lift_val = (marketing_impact / total_traffic * 100) if total_traffic > 0 else 0

    # Forensic 2: AI Predictability (Including OOH Lift)
    df['expected'] = df.apply(lambda x: heartbeats.get(x['day_name'], 0) + 
                               (x['ad_clicks'] * c_clicks) + 
                               (x['ad_impressions'] * c_social) +
                               total_ooh_lift, axis=1)

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

# 4. WEATHER LOGIC (Environment Canada Sync)
async def fetch_live_ec_data():
    try:
        # Coordinates for Hard Rock Ottawa area
        ec = ECWeather(coordinates=(45.33, -75.71))
        await ec.update()
        return {"current": ec.conditions, "forecast": ec.daily_forecasts, "alerts": ec.alerts}
    except:
        return {"error": "Weather Unavailable"}

if 'weather_data' not in st.session_state:
    st.session_state.weather_data = asyncio.run(fetch_live_ec_data())

if 'user_authenticated' not in st.session_state:
    st.session_state.user_authenticated = False

# 5. GATEKEEPER (Secure Login UI)
if not st.session_state.user_authenticated:
    st.markdown("<div style='text-align:center; padding:50px;'><h1 style='color:#FFCC00;'>🎰 FloorCast</h1><h3>Hard Rock Ottawa | Strategic Engine</h3></div>", unsafe_allow_html=True)
    
    with st.container(border=True):
        email = st.text_input("Email")
        pw = st.text_input("Password", type="password")
        
        # Use a unique key for the button to avoid state collisions
        if st.button("Access Engine", use_container_width=True, key="login_btn"):
            try:
                # 1. Check with Supabase
                res = supabase.auth.sign_in_with_password({"email": email, "password": pw})
                
                if res.user:
                    # 2. Update status
                    st.session_state.user_authenticated = True
                    # 3. FORCE REFRESH: This bypasses the st.stop() on the next lines
                    st.rerun() 
                else:
                    st.error("Authentication failed.")
            except Exception as e:
                st.error("Invalid credentials or connection error.")
    
    # This is the line that causes the double-click if st.rerun() isn't called above
    st.stop()

# 6. HYDRATE ENGINE WEIGHTS & DATA
if 'coeffs' not in st.session_state:
    try:
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        if response.data:
            st.session_state.coeffs = response.data[0]
            # Safety Fallbacks: If these columns don't exist in Supabase yet, create them in memory
            if 'OOH_Weight' not in st.session_state.coeffs: st.session_state.coeffs['OOH_Weight'] = 150.0
            if 'OOH_Count' not in st.session_state.coeffs: st.session_state.coeffs['OOH_Count'] = 0
        else:
            st.session_state.coeffs = {
                "id": 1, "Intercept": 3250, "Avg_Coin_In": 112.50, 
                "Clicks": 0.02, "Social_Imp": 0.0002, 
                "OOH_Weight": 150.0, "OOH_Count": 0
            }
    except:
        st.session_state.coeffs = {"Intercept": 3250, "Avg_Coin_In": 450, "OOH_Weight": 150.0, "OOH_Count": 0}

# Fetch the Ledger (Defining it here prevents the NameError)
def fetch_ledger_data():
    try:
        res = supabase.table("ledger").select("*").execute()
        return res.data if res.data else []
    except:
        return []

ledger_data = fetch_ledger_data()

# Ensure ledger_data is defined BEFORE any logic checks it
def fetch_ledger_data():
    try:
        res = supabase.table("ledger").select("*").execute()
        return res.data if res.data else []
    except:
        return []

ledger_data = fetch_ledger_data()

# 7. MODERN UI STYLING (Minimalist Executive Theme)
st.markdown("""
    <style>
    /* Clean Page Background */
    .stApp {
        background-color: #f4f7f9;
        color: #111111;
    }
    
    /* Remove the 'Bento Box' borders and shadows */
    div[data-testid="stVerticalBlock"] > div {
        border: none !important;
        background-color: transparent !important;
        box-shadow: none !important;
        margin-bottom: 0px !important;
        padding: 0px !important;
    }

    /* Keep the Tabs professional */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: #e6e9ef;
        border-radius: 4px 4px 0px 0px;
        padding: 10px 20px;
        color: #333;
    }

    /* Active Tab - Hard Rock Gold */
    .stTabs [aria-selected="true"] {
        background-color: #FFCC00 !important; 
        color: #000000 !important;
        font-weight: bold;
    }

    /* Standardize Metric spacing */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #e6e9ef;
    }
    </style>
    """, unsafe_allow_html=True)

# 8. TOP NAVIGATION BAR
h1, h2 = st.columns([4, 1])
with h1:
    st.markdown("<h1 style='color: #FFCC00; margin:0;'>🎰 FloorCast</h1>", unsafe_allow_html=True)
with h2:
    if st.button("🔓 Logout", use_container_width=True):
        supabase.auth.sign_out()
        st.session_state.user_authenticated = False
        st.rerun()

st.divider()

# 9. MAIN TAB NAVIGATION
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
            <p style="color: #888; margin: 0;">Update property performance. Newest entries will appear at the top of the editor below.</p>
        </div>
    """, unsafe_allow_html=True)

    col_a, col_b = st.columns([1, 1])

    # --- SECTION A: MANUAL FORM ---
    with col_a:
        st.write("### ✍️ Manual Results Entry")
        with st.form("manual_entry_v3"):
            entry_date = st.date_input("Select Date", datetime.date.today())
            
            c1, c2 = st.columns(2)
            with c1: traffic = st.number_input("Traffic (Headcount)", min_value=0)
            with c2: coin_in = st.number_input("Coin-In ($)", min_value=0.0, format="%.2f")
            
            st.divider()
            st.write("**Marketing Metrics**")
            m1, m2, m3 = st.columns(3)
            with m1: clicks = st.number_input("Ad Clicks", min_value=0)
            with m2: imps = st.number_input("Ad Impressions", min_value=0)
            with m3: social = st.number_input("Social Engagements", min_value=0)
            
            # This button is inside the form, so it's safe from duplicate ID errors
            submit_form = st.form_submit_button("💾 Sync Results to Vault", use_container_width=True)
            
            if submit_form:
                with st.spinner("Writing to Vault..."):
                    date_str = entry_date.isoformat()
                    new_row = {
                        "entry_date": date_str,
                        "actual_traffic": traffic,
                        "actual_coin_in": coin_in,
                        "ad_clicks": clicks,
                        "ad_impressions": imps,
                        "social_engagements": social
                    }
                    try:
                        # UPSERT handles backfilling missed weekend dates or updating existing ones
                        supabase.table("ledger").upsert(new_row, on_conflict="entry_date").execute()
                        st.success(f"✅ Data for {date_str} is now in the Vault.")
                        
                        # FORCE REFRESH: This clears the cache and re-fetches the latest data
                        import time
                        time.sleep(1)
                        st.rerun() 
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

    # --- SECTION B: BULK UPLOAD ---
    with col_b:
        st.write("### 📤 Bulk CSV Upload")
        uploaded_file = st.file_uploader("Upload Ledger CSV", type="csv", key="csv_uploader_t2")
        if uploaded_file is not None:
            df_upload = pd.read_csv(uploaded_file)
            st.dataframe(df_upload.head(3), use_container_width=True)
            
            if st.button("🚀 Push CSV to Vault", use_container_width=True, key="btn_csv_push_t2"):
                try:
                    data_dict = df_upload.to_dict(orient='records')
                    supabase.table("ledger").upsert(data_dict, on_conflict="entry_date").execute()
                    st.success("Bulk upload complete!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Upload failed: {e}")

    # --- SECTION C: THE LEDGER EDITOR (Indented correctly) ---
    st.divider()
    st.write("### 📜 Ledger Editor")
    st.caption("Newest entries are forced to the top. Double-click cells to edit directly.")
    
    if ledger_data:
        # 1. Convert to DataFrame
        df_history = pd.DataFrame(ledger_data)
        
        # 2. FORCE SORTING: This solves the "where is my data" problem
        if 'entry_date' in df_history.columns:
            df_history['entry_date'] = pd.to_datetime(df_history['entry_date'])
            df_history = df_history.sort_values(by='entry_date', ascending=False)
        
        # 3. RENDER THE EDITOR
        # key="ledger_editor_unique_t2" prevents the 'Duplicate ID' error
        edited_df = st.data_editor(
            df_history, 
            key="ledger_editor_unique_t2", 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "entry_date": st.column_config.DateColumn("Date", disabled=True),
                "actual_traffic": st.column_config.NumberColumn("Traffic", format="%d"),
                "actual_coin_in": st.column_config.NumberColumn("Coin-In ($)", format="$%.2f")
            }
        )

        # 4. SYNC BUTTON (Indented so it stays in Tab 2 only)
        if st.button("✅ Confirm & Sync Edits", key="btn_sync_ledger_t2", use_container_width=True):
            with st.spinner("Updating Vault records..."):
                try:
                    for _, row in edited_df.iterrows():
                        up_data = row.to_dict()
                        # Format date for Supabase match
                        d_key = pd.to_datetime(up_data['entry_date']).strftime('%Y-%m-%d')
                        up_data['entry_date'] = d_key
                        
                        # Clean payload
                        if 'id' in up_data: del up_data['id']
                        
                        supabase.table("ledger").update(up_data).eq("entry_date", d_key).execute()
                    
                    st.success("Vault Successfully Updated!")
                    st.rerun() # Refresh the view
                except Exception as e:
                    st.error(f"Manual sync failed: {e}")
    else:
        st.info("The Vault is currently empty. Use the form above to add your first entry.")

# --- TAB 3: PROPERTY ANALYTICS ---
with tab3:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Property Performance Analytics</h2>
            <p style="color: #888; margin: 0;">Forensic trend analysis of digital vs. physical results.</p>
        </div>
    """, unsafe_allow_html=True)

    if ledger_data:
        df_analysis = pd.DataFrame(ledger_data)
        df_analysis['entry_date'] = pd.to_datetime(df_analysis['entry_date'])
        df_analysis = df_analysis.sort_values('entry_date')

        # 1. VISUAL TREND SELECTION
        st.write("### 📈 Performance Trends")
        # Added _t3 to the key to prevent collisions
        metric_choice = st.pills("Select Metric to Analyze", 
                                ["Traffic", "Coin-In", "Ad Clicks"], 
                                selection_mode="single",
                                default="Traffic",
                                key="analysis_pills_t3") 

        if metric_choice == "Traffic":
            st.area_chart(df_analysis.set_index('entry_date')['actual_traffic'], color="#FFCC00")
        elif metric_choice == "Coin-In":
            st.line_chart(df_analysis.set_index('entry_date')['actual_coin_in'], color="#2ecc71")
        else:
            st.bar_chart(df_analysis.set_index('entry_date')['ad_clicks'], color="#00CCFF")

        # 2. READ-ONLY DATA VIEW (Safer & Faster)
        st.divider()
        st.write("### 📜 Detailed Analytics View")
        
        # We use a standard dataframe here instead of an editor to prevent ID errors
        st.dataframe(
            df_analysis[['entry_date', 'actual_traffic', 'actual_coin_in', 'ad_clicks', 'ad_impressions', 'social_engagements']],
            column_config={
                "entry_date": st.column_config.DateColumn("Date"),
                "actual_traffic": st.column_config.NumberColumn("Traffic"),
                "actual_coin_in": st.column_config.NumberColumn("Coin-In ($)", format="$%.2f"),
                "ad_clicks": st.column_config.NumberColumn("Ad Clicks"),
                "ad_impressions": st.column_config.NumberColumn("Ad Impressions"),
                "social_engagements": st.column_config.NumberColumn("Social Engagements")
            },
            use_container_width=True,
            hide_index=True
        )
        
        st.caption("💡 To edit these values, please use the **Ledger Management** tab.")

    else:
        st.info("No data found in the Vault. Please backfill results in Tab 2 to see analytics.")
# --- TAB 4: ENGINE CONTROL (CALIBRATION) ---
with tab4:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Calibration</h2>
            <p style="color: #888; margin: 0;">Calibrate Static vs. Shared Digital OOH impact.</p>
        </div>
    """, unsafe_allow_html=True)

    # Safety check for coefficients
    if 'coeffs' not in st.session_state:
        st.error("Engine coefficients not found. Please refresh.")
        st.stop()

    # 1. MANUAL CALIBRATION FORM
    with st.form("engine_settings_refined_v2"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("### 🖼️ Static Billboards (24/7)")
            # Pull values with high safety ceilings to prevent 'AboveMax' errors
            s_weight = float(st.session_state.coeffs.get('Static_Weight', 50.0))
            new_static_w = st.slider("Lift per Static Board", 0.0, 500.0, value=float(s_weight))
            new_static_c = st.number_input("Active Static Boards", 0, 20, value=int(st.session_state.coeffs.get('Static_Count', 2)))

            st.write("### 📺 Digital OOH (Shared Loop)")
            d_weight = float(st.session_state.coeffs.get('Digital_OOH_Weight', 10.0))
            new_digital_w = st.slider("Lift per Digital Board (Shared)", 0.0, 200.0, value=float(d_weight))
            new_digital_c = st.number_input("Active Digital Boards", 0, 50, value=int(st.session_state.coeffs.get('Digital_OOH_Count', 4)))

        with col2:
            st.write("### 📣 Marketing & Environment")
            # Click Weight
            c_clicks = float(st.session_state.coeffs.get('Clicks', 0.02))
            new_clicks = st.slider("Click Weight", 0.00, 0.50, value=float(c_clicks))
            
            # Snow Friction
            c_snow = float(st.session_state.coeffs.get('Snow_cm', -45.0))
            new_snow = st.slider("Snow Friction (Guests/cm)", -1000.0, 0.0, value=float(c_snow))
            
            # Financial Anchor - Raised to $5,000 to prevent 'AboveMax' crashes
            c_coin = float(st.session_state.coeffs.get('Avg_Coin_In', 112.50))
            new_coin = st.number_input("Avg Spend Per Head ($)", 0.0, 5000.0, value=float(c_coin))

        st.divider()
        
        # THE SUBMIT BUTTON - Must be inside the 'with st.form' block
        submit_cal = st.form_submit_button("💾 Save Calibration to Vault", use_container_width=True)

        if submit_cal:
            sync_payload = {
                'Static_Weight': new_static_w,
                'Static_Count': new_static_c,
                'Digital_OOH_Weight': new_digital_w,
                'Digital_OOH_Count': new_digital_c,
                'Clicks': new_clicks,
                'Snow_cm': new_snow,
                'Avg_Coin_In': new_coin
            }
            try:
                # Update Supabase
                supabase.table("coefficients").update(sync_payload).eq("id", 1).execute()
                # Update Session State immediately for UI consistency
                st.session_state.coeffs.update(sync_payload)
                st.success("✅ OOH Calibration Updated & Vault Synced!")
                import time
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")

    # 2. FORENSIC SUMMARY
    st.write("### 🔍 Current Campaign Pressure")
    total_static = new_static_w * new_static_c
    total_digital = new_digital_w * new_digital_c
    total_ooh = total_static + total_digital
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Static OOH Lift", f"{total_static:,.0f}")
    c2.metric("Digital OOH Lift", f"{total_digital:,.0f}")
    c3.metric("Total OOH Impact", f"{total_ooh:,.0f}", help="Estimated daily guest lift from all boards")
# --- TAB 5: FORENSIC ANALYST & PRODUCT EXPERT ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧠 Forensic Consultant & App Expert</h2>
            <p style="color: #888; margin: 0;">Real-time KPI calculation and strategic guidance powered by Gemini.</p>
        </div>
    """, unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 1. THE FORENSIC DATA VAULT (Synced to Tab 2 Schema)
    vault_metrics = {}
    if ledger_data:
        df_vault = pd.DataFrame(ledger_data).copy()
        df_vault['entry_date'] = pd.to_datetime(df_vault['entry_date'])
        
        # --- DYNAMIC COLUMN NORMALIZATION ---
        # Updated to match: ad_clicks, ad_impressions, social_engagements
        col_map = {
            'ad_impressions': ['ad_impressions', 'Impressions', 'social_impressions'],
            'ad_clicks': ['ad_clicks', 'Clicks'],
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

        # Calculation logic
        df_vault['day_name'] = df_vault['entry_date'].dt.day_name()
        heartbeats = df_vault.groupby('day_name')['actual_traffic'].mean().to_dict()
        
        # Pull weights from Tab 4 settings
        w_clicks = st.session_state.coeffs.get('Clicks', 0.02)
        w_social = st.session_state.coeffs.get('Social_Imp', 0.0002)
        
        # Calculate Digital Lift (How much of your traffic is marketing-driven?)
        total_traffic = df_vault['actual_traffic'].sum()
        marketing_impact = (df_vault['ad_clicks'].sum() * w_clicks) + \
                           (df_vault['ad_impressions'].sum() * w_social)
        digital_lift_pct = (marketing_impact / total_traffic) * 100 if total_traffic > 0 else 0

        # AI Predictability
        df_vault['expected'] = df_vault.apply(lambda x: heartbeats.get(x['day_name'], 0) + 
                                            (x['ad_clicks'] * w_clicks) + 
                                            (x['ad_impressions'] * w_social), axis=1)
        
        import numpy as np
        mape = (np.abs(df_vault['actual_traffic'] - df_vault['expected']) / df_vault['actual_traffic']).replace([np.inf, -np.inf], np.nan).dropna().mean()
        predictability_score = (1 - mape) * 100 if not np.isnan(mape) else 85.0 # Default fallback

        vault_metrics = {
            "heartbeats": heartbeats,
            "digital_lift": f"{digital_lift_pct:.1f}%",
            "predictability": f"{predictability_score:.1f}%",
            "avg_spend": f"${df_vault['actual_coin_in'].mean():,.2f}"
        }

    # 2. CHAT INPUT
    prompt = st.chat_input("Ask about your Digital Lift, weekend results, or how to use Tab 3...")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            import google.generativeai as genai
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            # FIXED: Changed model to 2.5-flash
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            history_payload = []
            for m in st.session_state.messages[:-1]:
                role = "model" if m["role"] == "assistant" else "user"
                history_payload.append({"role": role, "parts": [m["content"]]})
            
            # THE FORENSIC BRAIN CONTEXT
            sys_context = f"""
            SYSTEM ROLE: Chief Strategy Officer at Hard Rock Ottawa. 
            TONE: Professional, Data-Driven, Strategic.

            LIVE KPI VAULT:
            - AI Predictability (Model Accuracy): {vault_metrics.get('predictability', 'N/A')}
            - Digital Lift (Marketing Impact): {vault_metrics.get('digital_lift', 'N/A')}
            - Avg. Property Spend: {vault_metrics.get('avg_spend', 'N/A')}
            - Baseline DOW Heartbeats: {vault_metrics.get('heartbeats', {})}

            PRODUCT GUIDE:
            - Tab 1: Executive Overview (The Big Picture)
            - Tab 2: Ledger Management (Where you enter Friday-Sunday data)
            - Tab 3: Property Analytics (Trend charts & Correlations)
            - Tab 4: Engine Control (Calibration of multipliers)
            - Tab 5: This Consultant Tab

            STRATEGY RULE: 
            If Predictability is < 80%, suggest the user check their multipliers in Tab 4 or check for missed Promos in Tab 2.
            
            Always end with one sharp strategic question.
            """

            chat = model.start_chat(history=history_payload)
            response = chat.send_message(f"{sys_context}\n\nUSER MESSAGE: {prompt}")
            
            st.session_state.messages.append({"role": "assistant", "content": response.text})
            st.rerun()

        except Exception as e:
            st.error(f"Consultation Error: {e}")

    # 3. DISPLAY FEED (Reversed for modern chat feel)
    for message in reversed(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if st.button("🗑️ Reset Forensic Session", key="reset_chat_t5"):
        st.session_state.messages = []
        st.rerun()
# --- TAB 6: MASTER ANALYTICS & FORENSIC REPORT ---
with tab6:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Master Forensic Report</h2>
            <p style="color: #888; margin: 0;">Accounting-grade analysis of Hard Rock Ottawa performance and marketing ROI.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. PULL ENGINE CONSTANTS (Hard-coded to your Session State)
    c = st.session_state.coeffs
    avg_spend_target = c.get('Avg_Coin_In', 112.50)
    click_weight = c.get('Clicks', 0.02)
    social_weight = c.get('Social_Imp', 0.0002)
    promo_lift = c.get('Promo', 450.0)

    if ledger_data:
        df_rep = pd.DataFrame(ledger_data).copy()
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        # --- THE CALCULATION ENGINE (Accounting Logic) ---
        
        # Safety: Ensure ad_clicks exists and is numeric
        if 'ad_clicks' not in df_rep.columns: df_rep['ad_clicks'] = 0
        df_rep['ad_clicks'] = pd.to_numeric(df_rep['ad_clicks']).fillna(0)
        
        # A. Marketing Attribution (Social + Clicks)
        # We calculate the 'Lift' generated by digital efforts
        df_rep['attr_traffic'] = (df_rep['ad_clicks'] * click_weight)
        
        # Add Social Impact if column exists
        if 'ad_impressions' in df_rep.columns:
            df_rep['attr_traffic'] += (df_rep['ad_impressions'].fillna(0) * social_weight)
            
        df_rep['attr_revenue'] = df_rep['attr_traffic'] * avg_spend_target
        
        # B. Efficiency & Variance Metrics
        df_rep['actual_spend_avg'] = df_rep['actual_coin_in'] / df_rep['actual_traffic']
        df_rep['rev_variance'] = df_rep['actual_coin_in'] - (df_rep['actual_traffic'] * avg_spend_target)
        
        # C. Global Aggregates
        total_rev = df_rep['actual_coin_in'].sum()
        total_vis = df_rep['actual_traffic'].sum()
        total_attr_rev = df_rep['attr_revenue'].sum()
        total_days = len(df_rep)

        # 2. EXECUTIVE PERFORMANCE TILES
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total YTD Coin-In", f"${total_rev:,.0f}")
        with col2:
            # ROI is essentially your Digital Revenue
            st.metric("Digital Lift (Rev)", f"${total_attr_rev:,.0f}")
        with col3:
            st.metric("Avg Daily Guests", f"{total_vis / total_days:,.0f}")
        with col4:
            st.metric("Avg $/Head (Actual)", f"${total_rev / total_vis:,.2f}")

        st.divider()

        # 3. THE MASTER FORENSIC DATA TABLE
        st.write("### 🔍 Daily Performance Breakdown")
        
        # Build the final exportable dataframe
        master_df = df_rep[[
            'entry_date', 'actual_traffic', 'actual_coin_in', 'ad_clicks'
        ]].copy()
        
        master_df['Digital Traffic'] = df_rep['attr_traffic'].round(0)
        master_df['Digital Revenue'] = df_rep['attr_revenue'].round(2)
        master_df['Actual $/Head'] = df_rep['actual_spend_avg'].round(2)
        master_df['vs. Target Variance'] = df_rep['rev_variance'].round(2)
        
        st.dataframe(
            master_df.sort_values('entry_date', ascending=False),
            column_config={
                "entry_date": "Date",
                "actual_traffic": st.column_config.NumberColumn("Total Traffic", format="%d"),
                "actual_coin_in": st.column_config.NumberColumn("Total Revenue", format="$%d"),
                "Digital Revenue": st.column_config.NumberColumn("Digital Lift", format="$%d"),
                "Actual $/Head": st.column_config.NumberColumn("Avg Spend", format="$%.2f"),
                "vs. Target Variance": st.column_config.NumberColumn("Variance", format="$%d")
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
                st.caption("Percentage of Total Revenue driven by Digital Calibration.")

        with c2:
            with st.container(border=True):
                st.write("**Revenue Volatility**")
                std_dev = df_rep['actual_coin_in'].std()
                st.title(f"${std_dev:,.0f}")
                st.caption("Standard deviation of daily floor revenue (Risk Metric).")

        with c3:
            with st.container(border=True):
                st.write("**Click Value**")
                # Revenue per individual click based on current weights
                rev_per_click = click_weight * avg_spend_target
                st.title(f"${rev_per_click:.2f}")
                st.caption("Attributed revenue generated by a single Ad Click.")

        # 5. DATA EXPORT
        st.write("---")
        st.download_button(
            label="📥 Download Forensic Report for Executive Review (CSV)",
            data=master_df.to_csv(index=False),
            file_name=f"HardRock_Ottawa_Forensic_{datetime.date.today()}.csv",
            mime="text/csv",
            use_container_width=True,
            key="btn_export_tab6" # Added unique key
        )

    else:
        st.warning("No data found in the Vault. Please enter weekend results in Tab 2 to generate this report.")

# --- TAB 7: SYNCHRONIZED FORECAST SANDBOX ---
with tab7:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🧪 Forecast Sandbox</h2>
            <p style="color: #888; margin: 0;">Fully Synchronized: Triangulating Historical Baselines & Live Weather Forecasts.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. DATE RANGE SELECTION
    today = datetime.date.today()
    date_range = st.date_input(
        "Select Simulation Window:",
        value=(today, today + datetime.timedelta(days=2)),
        help="The Sandbox will pull specific daily forecasts for this entire window.",
        key="sb_date_range"
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        num_days = (end_date - start_date).days + 1
        
        # Pull live data from session state
        live_forecast = st.session_state.get('weather_data', {}).get('forecast', [])

        # 2. SCENARIO INPUTS
        st.write(f"### 🎛️ Simulation Parameters ({num_days} Days)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Marketing & Social**")
            s_promo = st.checkbox("Active Major Promotion?", value=False)
            s_clicks = st.number_input("Est. Daily Ad Clicks", value=500)
            s_imp = st.number_input("Est. Daily Social Impressions", value=10000)
        
        with col2:
            st.write("**Environment**")
            weather_mode = st.radio("Weather Source:", ["Live EC Forecast", "Manual Overrides"])
            m_temp = st.slider("Manual Temp (°C)", -30, 40, 15, disabled=(weather_mode == "Live EC Forecast"))
            m_rain = st.slider("Manual Rain (mm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))
            m_snow = st.slider("Manual Snow (cm)", 0, 50, 0, disabled=(weather_mode == "Live EC Forecast"))

        with col3:
            st.write("**Engine Baseline**")
            c = st.session_state.coeffs
            st.metric("Spend Anchor", f"${c.get('Avg_Coin_In', 112.50):,.2f}")
            st.info("Simulation pulls baseline 'Heartbeats' from your Ledger history.")

        # 3. UNIFIED CALCULATION LOOP
        total_range_traffic = 0
        total_range_revenue = 0
        
        # Calculate DOW Averages from Ledger for the "Heartbeat"
        if ledger_data:
            df_sb = pd.DataFrame(ledger_data)
            df_sb['entry_date'] = pd.to_datetime(df_sb['entry_date'])
            df_sb['day_name'] = df_sb['entry_date'].dt.day_name()
            dow_profiles = df_sb.groupby('day_name')['actual_traffic'].mean().to_dict()
        else:
            dow_profiles = {}

        current_date = start_date
        while current_date <= end_date:
            day_str = current_date.strftime("%Y-%m-%d")
            day_name = current_date.strftime("%A")
            
            # --- WEATHER ATTRIBUTION ---
            if weather_mode == "Live EC Forecast" and live_forecast:
                # Find weather matching the date
                ec_day = next((item for item in live_forecast if str(item.get('datetime')).startswith(day_str)), None)
                day_temp = ec_day.get('temperature', 15.0) if ec_day else m_temp
                # Use overrides for precip if not in current EC feed
                day_rain = m_rain 
                day_snow = m_snow
            else:
                day_temp, day_rain, day_snow = m_temp, m_rain, m_snow

            # --- THE FORENSIC MATH ---
            # 1. Start with the historical DOW heartbeat or the global intercept
            base_traffic = dow_profiles.get(day_name, c.get('Intercept', 4365))
            
            # 2. Add Marketing Lifts from Tab 4 Coefficients
            p_lift = c.get('Promo', 450.0) if s_promo else 0
            m_lift = (s_clicks * c.get('Clicks', 0.02)) + (s_imp * c.get('Social_Imp', 0.0002))
            
            # 3. Apply Weather Friction
            w_friction = (day_rain * c.get('Rain_mm', -12.0)) + (day_snow * c.get('Snow_cm', -45.0))
            
            # 4. Final Aggregation for the Day
            daily_total = base_traffic + p_lift + m_lift + w_friction
            
            total_range_traffic += daily_total
            total_range_revenue += (daily_total * c.get('Avg_Coin_In', 112.50))
            
            current_date += datetime.timedelta(days=1)

        # 4. RESULTS DISPLAY
        st.divider()
        res1, res2, res3 = st.columns(3)
        
        with res1:
            st.metric("Predicted Total Traffic", f"{int(total_range_traffic):,} Guests")
        with res2:
            st.metric("Projected Window Revenue", f"${total_range_revenue:,.2f}")
        with res3:
            st.metric("Daily Avg Volume", f"{int(total_range_traffic / num_days)} / day")

        # 5. STRATEGIC INSIGHT
        st.write("---")
        if total_range_traffic / num_days > (c.get('Intercept', 4365) * 1.2):
            st.success("🔥 **High Volume Scenario:** This configuration suggests a 20%+ lift over baseline. Ensure floor staffing is optimized.")
        elif total_range_traffic / num_days < (c.get('Intercept', 4365) * 0.8):
            st.warning("📉 **Low Volume Warning:** Forecast is significantly below average. Consider boosting digital ad spend.")

    else:
        st.info("Please select a valid date range to start the simulation.")
