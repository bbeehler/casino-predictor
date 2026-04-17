import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
import google.generativeai as genai
from sklearn.linear_model import LinearRegression
import numpy as np

# 1. PAGE CONFIG (Must be the very first Streamlit command)
st.set_page_config(page_title="FloorCast", layout="wide")

# 2. MODERN UI STYLING (The CSS)
st.markdown("""
    <style>
    /* Main Background */
    .stApp {
        background-color: #f4f7f9;
    }
    
    /* Bento Box Card Effect */
    div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column;"] > div[data-testid="stVerticalBlock"] {
        border: 1px solid #e6e9ef;
        border-radius: 12px;
        padding: 20px;
        background-color: #ffffff;
        box-shadow: 0 4px 12px rgba(0,0,0,0.03);
    }

    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff;
        border: 1px solid #e6e9ef;
        border-radius: 8px 8px 0px 0px;
        padding: 10px 20px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FFCC00 !important; /* Hard Rock Gold */
        color: #000000 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# 3. DATABASE & AI SETUP (Using your existing secrets)
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

# 4. INITIALIZE SESSION STATE
if 'coeffs' not in st.session_state:
    st.session_state.coeffs = {
        'Intercept': 3250.0,
        'DOW_Mon': -450.0, 'DOW_Tue': -380.0, 'DOW_Wed': -210.0, 'DOW_Thu': 150.0,
        'DOW_Fri': 1200.0, 'DOW_Sat': 2100.0, 'DOW_Sun': 850.0,
        'Temp_C': 2.5, 'Snow_cm': -45.0, 'Rain_mm': -12.0, 'Alert': -500.0,
        'Promo': 450.0, 'Impressions': 0.0002, 'Engagements': 0.15, 'Clicks': 0.85,
        'Avg_Coin_In': 112.50
    }

# 5. FETCH DATA
def fetch_data():
    try:
        response = supabase.table("ledger").select("*").execute()
        return response.data
    except:
        return []

ledger_data = fetch_data()

# 6. APP TABS
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Executive Dashboard", 
    "📝 Daily Tracker & Forecast", 
    "📈 Reporting & ROI", 
    "⚙️ Admin Engine", 
    "💬 Ask AI"
])

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🏢 Executive Strategy Command</h2>
            <p style="color: #888; margin: 0;">Real-time Property Performance vs. AI Baselines (YTD 2026).</p>
        </div>
    """, unsafe_allow_html=True)
    
    if ledger_data:
        # 1. DATA PREP
        df_exec = pd.DataFrame(ledger_data).copy()
        df_exec['entry_date'] = pd.to_datetime(df_exec['entry_date'])
        
        # Ensure we are looking at numbers
        df_exec['actual_traffic'] = pd.to_numeric(df_exec['actual_traffic'], errors='coerce').fillna(0)
        df_exec['actual_coin_in'] = pd.to_numeric(df_exec['actual_coin_in'], errors='coerce').fillna(0)
        
        # 2. FIND THE LATEST VALID DATA (The 'Deep Scan')
        # We look for the most recent row where traffic is GREATER THAN zero
        df_valid = df_exec[df_exec['actual_traffic'] > 0].sort_values('entry_date', ascending=False)
        
        if not df_valid.empty:
            latest_row = df_valid.iloc[0]
            
            # 3. LIVE AI ACCURACY MATH
            c = st.session_state.coeffs
            def get_live_val(row):
                dow_key = f"DOW_{pd.to_datetime(row['entry_date']).strftime('%a')}"
                base = c['Intercept'] + c.get(dow_key, 0)
                weather = (row.get('temp_c', 0) * c['Temp_C'])
                promo = c['Promo'] if row.get('active_promo', False) else 0
                return base + weather + promo

            # Apply prediction to the whole valid dataset to get average accuracy
            df_valid['live_pred'] = df_valid.apply(get_live_val, axis=1)
            df_valid['error'] = abs(df_valid['actual_traffic'] - df_valid['live_pred']) / df_valid['actual_traffic']
            accuracy_pct = max(0, (1 - df_valid['error'].mean()) * 100)

            # 4. EXECUTIVE KPI BENTO BOX
            col1, col2, col3 = st.columns(3)
            with col1:
                with st.container(border=True):
                    st.markdown("🎯 **AI Model Accuracy**")
                    st.metric("System Reliability", f"{accuracy_pct:.1f}%")
                    st.progress(min(max(accuracy_pct / 100, 0.0), 1.0))
            with col2:
                with st.container(border=True):
                    st.markdown("💰 **Latest Revenue**")
                    st.metric("Daily Revenue", f"${latest_row['actual_coin_in']:,.0f}")
                    st.caption(f"Date: {latest_row['entry_date'].strftime('%Y-%m-%d')}")
            with col3:
                with st.container(border=True):
                    st.markdown("🚶 **Floor Traffic**")
                    st.metric("Actual Visitors", f"{latest_row['actual_traffic']:,.0f}")
                    st.caption("Property Foot Traffic")

            st.markdown("---")

            # 5. TREND VISUALIZATION
            t1, t2 = st.columns([2, 1])
            with t1:
                st.markdown("#### 📈 7-Day Performance vs. AI Baseline")
                chart_data = df_valid.head(7).copy().sort_values('entry_date')
                chart_data = chart_data.rename(columns={'actual_traffic': 'Actual', 'live_pred': 'AI Prediction', 'entry_date': 'Date'})
                st.area_chart(chart_data.set_index('Date')[['Actual', 'AI Prediction']], color=["#FFCC00", "#555555"])
            with t2:
                st.markdown("#### 🤖 AI Analyst Status")
                st.info(f"The model is currently syncing with {len(df_valid)} historical records. Accuracy is based on Live Coefficients.")
        else:
            st.warning("⚠️ Database connected, but no rows with Traffic > 0 were found. Please check your data in the Admin Tab.")
    else:
        st.info("No data found in the Ledger.")
# --- TAB 2: DAILY TRACKER & FORECAST ---
with tab2:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🕹️ FloorPace Control Panel</h2>
            <p style="color: #888; margin: 0;">Log Daily Actuals, Simulate Forecasts, and Audit Historical Data.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Create the two-column "Bento" layout
    col_input, col_sandbox = st.columns([1, 1.2])
    
    with col_input:
        with st.container(border=True):
            st.subheader("📝 1. Log Actuals")
            with st.form("entry_form", clear_on_submit=True):
                date_in = st.date_input("Date", datetime.date.today())
                act_traf = st.number_input("Actual Traffic", min_value=0, step=100)
                act_coin = st.number_input("Actual Coin-In ($)", min_value=0, step=1000)
                
                st.divider()
                st.markdown("**Environment & Marketing**")
                w1, w2 = st.columns(2)
                temp = w1.slider("Temp (°C)", -30, 40, 15)
                snow = w2.slider("Snow (cm)", 0, 50, 0)
                
                promo = st.checkbox("Active Promotion")
                
                with st.expander("Detailed Digital Metrics"):
                    imp = st.number_input("Ad Impressions", 0, 1000000, 300000)
                    clks = st.number_input("Ad Clicks", 0, 5000, 200)

                if st.form_submit_button("💾 Save to FloorPace Ledger", use_container_width=True):
                    c = st.session_state.coeffs
                    dow_key = f"DOW_{date_in.strftime('%a')}"
                    
                    # Math for Prediction
                    base_v = float(c['Intercept'] + c.get(dow_key, 0))
                    weather_v = float((temp * c['Temp_C']) + (snow * c['Snow_cm']))
                    dig_lift_v = float((promo * c['Promo']) + (imp * c['Impressions']) + (clks * c['Clicks']))
                    final_pred = float(base_v + weather_v + dig_lift_v)
                    
                    # CLEAN PAYLOAD: Removing 'variance' to match SQL Schema
                    data = {
                        "entry_date": str(date_in),
                        "actual_traffic": int(act_traf),
                        "actual_coin_in": float(act_coin),
                        "predicted_traffic": int(final_pred),
                        "temp_c": float(temp),
                        "snow_cm": float(snow),
                        "active_promo": bool(promo),
                        "ad_impressions": int(imp),
                        "ad_clicks": int(clks)
                    }
                    
                    try:
                        supabase.table("ledger").upsert(data, on_conflict="entry_date").execute()
                        st.toast("✅ Record saved successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Database Sync Error: {e}")

    with col_sandbox:
        with st.container(border=True):
            st.subheader("🔮 2. Forecast Sandbox")
            st.write("Simulate future dates to see projected revenue.")
            f_range = st.date_input("Forecast Range", [datetime.date.today(), datetime.date.today() + datetime.timedelta(days=7)])
            
            s1, s2 = st.columns(2)
            sim_temp = s1.slider("Simulated Temp", -30, 40, 15)
            sim_promo = s2.checkbox("Apply Promo to all dates?")
            
            if len(f_range) == 2:
                dates = pd.date_range(f_range[0], f_range[1])
                c = st.session_state.coeffs
                f_list = []
                for d in dates:
                    dk = f"DOW_{d.strftime('%a')}"
                    p = c['Intercept'] + c.get(dk, 0) + (sim_temp * c['Temp_C']) + (c['Promo'] if sim_promo else 0)
                    f_list.append({"Date": d.strftime("%a %d"), "Visitors": int(p), "Revenue": p * c['Avg_Coin_In']})
                
                df_f = pd.DataFrame(f_list)
                st.metric("Est. Total Revenue", f"${df_f['Revenue'].sum():,.0f}")
                st.line_chart(df_f.set_index("Date")["Visitors"], color="#FFCC00")

    # --- SECTION 3: AUDIT & FULL-FIELD EDIT ---
    st.markdown("---")
    st.markdown("### 🔍 3. Historical Ledger Audit & Corrections")
    if ledger_data:
        df_edit = pd.DataFrame(ledger_data)
        df_edit['entry_date'] = pd.to_datetime(df_edit['entry_date'])
        
        with st.container(border=True):
            st.markdown("**Find & Fix a Specific Entry**")
            edit_col1, edit_col2 = st.columns([1, 2])
            
            with edit_col1:
                search_date = st.date_input("Select Date to Edit", value=df_edit['entry_date'].max().date())
                search_str = search_date.strftime("%Y-%m-%d")
            
            found = df_edit[df_edit['entry_date'].dt.strftime('%Y-%m-%d') == search_str]
            
            with edit_col2:
                if not found.empty:
                    record = found.iloc[0]
                    if st.toggle(f"🔓 Unlock ALL FIELDS for {search_str}"):
                        with st.form(f"full_edit_{search_str}"):
                            ec1, ec2, ec3 = st.columns(3)
                            with ec1:
                                up_t = st.number_input("Traffic", value=int(record.get('actual_traffic', 0)))
                                up_c = st.number_input("Coin-In ($)", value=float(record.get('actual_coin_in', 0.0)))
                                up_p = st.number_input("Pred. Traffic", value=int(record.get('predicted_traffic', 0)))
                            with ec2:
                                up_temp = st.number_input("Temp", value=float(record.get('temp_c', 0.0)))
                                up_snow = st.number_input("Snow", value=float(record.get('snow_cm', 0.0)))
                            with ec3:
                                up_promo = st.checkbox("Promo", value=bool(record.get('active_promo', False)))
                                up_imp = st.number_input("Impressions", value=int(record.get('ad_impressions', 0)))
                                up_clk = st.number_input("Clicks", value=int(record.get('ad_clicks', 0)))

                            if st.form_submit_button("💾 Save All Changes", use_container_width=True):
                                try:
                                    # STRICT TYPES: Ints for traffic/impressions, Floats for money/temp
                                    supabase.table("ledger").update({
                                        "actual_traffic": int(up_t), 
                                        "actual_coin_in": float(up_c), 
                                        "predicted_traffic": int(up_p),
                                        "temp_c": float(up_temp), 
                                        "snow_cm": float(up_snow), 
                                        "active_promo": bool(up_promo), 
                                        "ad_impressions": int(up_imp), 
                                        "ad_clicks": int(up_clk)
                                    }).eq("entry_date", search_str).execute()
                                    st.toast(f"Record for {search_str} updated!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Update Error: {e}")
                else:
                    st.info("No record found for this date.")

        st.markdown("**Full Historical Ledger**")
        display_df = df_edit.sort_values('entry_date', ascending=False)
        display_df['entry_date'] = display_df['entry_date'].dt.strftime('%Y-%m-%d')
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# --- TAB 3: STRATEGIC REPORTING & ROI ---
with tab3:
    # 1. HEADER WITH ACCENT (Fixed the parameter name here)
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Strategic Performance & Digital ROI</h2>
            <p style="color: #888; margin: 0;">Correlating Digital Matters Now metrics with Property Floor Reality.</p>
        </div>
    """, unsafe_allow_html=True)
    
    if ledger_data:
        df_rep = pd.DataFrame(ledger_data).copy()
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        df_rep = df_rep.sort_values('entry_date', ascending=False)

        # 2. DATE FILTER
        with st.container(border=True):
            f1, f2, f3 = st.columns([1, 1, 1])
            start_rep = f1.date_input("📅 Report Start", df_rep['entry_date'].min().date())
            end_rep = f2.date_input("📅 Report End", df_rep['entry_date'].max().date())
            f3.write("##") 
            if f3.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()
        
        mask = (df_rep['entry_date'].dt.date >= start_rep) & (df_rep['entry_date'].dt.date <= end_rep)
        df_filtered = df_rep.loc[mask].copy()

        if not df_filtered.empty:
            st.write("##")
            
            # 3. DIGITAL PERFORMANCE BENTO
            st.markdown("#### 📱 Digital Impact Metrics")
            d1, d2, d3, d4 = st.columns(4)
            
            # Ensure numeric conversion for sums
            df_filtered['ad_impressions'] = pd.to_numeric(df_filtered['ad_impressions'], errors='coerce').fillna(0)
            df_filtered['ad_clicks'] = pd.to_numeric(df_filtered['ad_clicks'], errors='coerce').fillna(0)
            df_filtered['social_engagements'] = pd.to_numeric(df_filtered['social_engagements'], errors='coerce').fillna(0)
            df_filtered['actual_coin_in'] = pd.to_numeric(df_filtered['actual_coin_in'], errors='coerce').fillna(0)

            total_imps = df_filtered['ad_impressions'].sum()
            total_clks = df_filtered['ad_clicks'].sum()
            total_engs = df_filtered['social_engagements'].sum()
            total_rev = df_filtered['actual_coin_in'].sum()
            
            with d1:
                with st.container(border=True):
                    st.metric("Ad Impressions", f"{total_imps:,.0f}")
            with d2:
                with st.container(border=True):
                    st.metric("Ad Clicks", f"{total_clks:,.0f}")
            with d3:
                with st.container(border=True):
                    st.metric("Engagements", f"{total_engs:,.0f}")
            with d4:
                with st.container(border=True):
                    rpc = total_rev / total_clks if total_clks > 0 else 0
                    st.metric("Rev per Click", f"${rpc:.2f}")

            st.write("##")

            # 4. CHARTING & SUMMARY
            t_col, c_col = st.columns([2.5, 1])
            with t_col:
                with st.container(border=True):
                    st.markdown("#### 📈 Actual Traffic vs. AI Predicted Baseline")
                    c = st.session_state.coeffs
                    df_filtered['ai_baseline'] = df_filtered.apply(lambda row: 
                        c['Intercept'] + c.get(f"DOW_{row['entry_date'].strftime('%a')}", 0) + 
                        (row.get('temp_c', 0) * c['Temp_C']) + (c['Promo'] if row.get('active_promo', False) else 0), axis=1)
                    
                    chart_rep = df_filtered.sort_values('entry_date')
                    chart_rep = chart_rep.rename(columns={'actual_traffic': 'Floor Reality', 'ai_baseline': 'AI Baseline'})
                    st.area_chart(chart_rep.set_index('entry_date')[['Floor Reality', 'AI Baseline']], color=["#FFCC00", "#555555"])
            
            with c_col:
                with st.container(border=True):
                    st.markdown("#### 📝 Executive Summary")
                    total_var = df_filtered['actual_traffic'].sum() - df_filtered['ai_baseline'].sum()
                    perf_color = "#28a745" if total_var > 0 else "#dc3545"
                    st.markdown(f"""
                        <div style="text-align: center; padding: 10px;">
                            <h1 style="color: {perf_color}; margin: 0;">{total_var:+,.0f}</h1>
                            <p style="color: #888;">Net Traffic Variance</p>
                        </div>
                    """, unsafe_allow_html=True)
                    st.info(f"During this period, digital efforts influenced {total_clks:,.0f} clicks to the property.")

            st.write("##")
            st.download_button(
                label="📥 Export Hard Rock ROI Report (CSV)",
                data=df_filtered.to_csv(index=False),
                file_name=f"FloorPace_ROI_{start_rep}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.warning("No data found for the selected date range.")

# --- TAB 4: ADMIN ENGINE (MASTER CONTROL & IMPORTER) ---
with tab4:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Control & Data Management</h2>
            <p style="color: #888; margin: 0;">Tune AI Coefficients, Sync Models, and Manage Bulk CSV Integrations.</p>
        </div>
    """, unsafe_allow_html=True)
    # ... rest of your Tab 4 code ...
    
    # 1. THE ADVANCED AI OPTIMIZER
    with st.container(border=True):
        c_left, c_right = st.columns([2, 1])
        with c_left:
            st.subheader("🤖 Multivariate AI Sync")
            st.write("Recalibrate coefficients based on Weather, Promotions, and Digital Performance.")
        with c_right:
            st.write("##")
            if st.button("🚀 Sync AI to Reality", type="primary", use_container_width=True):
                if len(ledger_data) > 7:
                    df_ml = pd.DataFrame(ledger_data)
                    # Clean and Force Types
                    df_ml['actual_traffic'] = pd.to_numeric(df_ml['actual_traffic'], errors='coerce').fillna(0)
                    df_ml['temp_c'] = pd.to_numeric(df_ml['temp_c'], errors='coerce').fillna(0)
                    df_ml['promo_val'] = df_ml['active_promo'].astype(int)
                    df_ml['ad_clicks'] = pd.to_numeric(df_ml['ad_clicks'], errors='coerce').fillna(0)
                    
                    # Only train on valid history
                    df_train = df_ml[df_ml['actual_traffic'] > 0].copy()
                    
                    if len(df_train) > 5:
                        from sklearn.linear_model import LinearRegression
                        # We include Click-weight in the AI Sync now
                        X = df_train[['temp_c', 'promo_val', 'ad_clicks']].values
                        y = df_train['actual_traffic'].values
                        
                        model = LinearRegression().fit(X, y)
                        
                        # Update State
                        st.session_state.coeffs['Intercept'] = round(float(model.intercept_), 2)
                        st.session_state.coeffs['Temp_C'] = round(float(model.coef_[0]), 2)
                        st.session_state.coeffs['Promo'] = round(float(model.coef_[1]), 2)
                        st.session_state.coeffs['Clicks'] = round(float(model.coef_[2]), 4)
                        
                        # Sync Avg Revenue per Head
                        df_train['actual_coin_in'] = pd.to_numeric(df_train['actual_coin_in'], errors='coerce').fillna(0)
                        if df_train['actual_traffic'].sum() > 0:
                            st.session_state.coeffs['Avg_Coin_In'] = round(df_train['actual_coin_in'].sum() / df_train['actual_traffic'].sum(), 2)

                        st.success("🎯 AI recalibrated successfully!")
                        st.rerun()
                else:
                    st.error("Need more historical data (8+ days) to Sync.")

    st.divider()

    # 2. MASTER MANUAL OVERRIDES
    with st.form("admin_settings_final"):
        st.markdown("#### 🛠️ Manual Coefficient Overrides")
        
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            new_intercept = st.number_input("Base Traffic", value=float(st.session_state.coeffs['Intercept']))
        with f2:
            new_coin = st.number_input("Avg Spend ($)", value=float(st.session_state.coeffs['Avg_Coin_In']))
        with f3:
            new_temp = st.number_input("Temp Impact", value=float(st.session_state.coeffs['Temp_C']))
        with f4:
            new_snow = st.number_input("Snow Penalty", value=float(st.session_state.coeffs['Snow_cm']))

        st.markdown("**Day of the Week Adjustments**")
        d1, d2, d3, d4, d5, d6, d7 = st.columns(7)
        new_mon = d1.number_input("Mon", value=float(st.session_state.coeffs['DOW_Mon']))
        new_tue = d2.number_input("Tue", value=float(st.session_state.coeffs['DOW_Tue']))
        new_wed = d3.number_input("Wed", value=float(st.session_state.coeffs['DOW_Wed']))
        new_thu = d4.number_input("Thu", value=float(st.session_state.coeffs['DOW_Thu']))
        new_fri = d5.number_input("Fri", value=float(st.session_state.coeffs['DOW_Fri']))
        new_sat = d6.number_input("Sat", value=float(st.session_state.coeffs['DOW_Sat']))
        new_sun = d7.number_input("Sun", value=float(st.session_state.coeffs['DOW_Sun']))

        st.markdown("**Digital & Promo Weights**")
        m1, m2, m3 = st.columns(3)
        new_promo = m1.number_input("Promotion Lift", value=float(st.session_state.coeffs['Promo']))
        new_imp = m2.number_input("Impression Weight", value=float(st.session_state.coeffs['Impressions']), format="%.6f")
        new_clk = m3.number_input("Click Weight", value=float(st.session_state.coeffs['Clicks']), format="%.4f")

        if st.form_submit_button("💾 Save All Engine Changes", use_container_width=True):
            st.session_state.coeffs.update({
                'Intercept': new_intercept, 'Avg_Coin_In': new_coin, 'Temp_C': new_temp, 'Snow_cm': new_snow,
                'DOW_Mon': new_mon, 'DOW_Tue': new_tue, 'DOW_Wed': new_wed, 'DOW_Thu': new_thu,
                'DOW_Fri': new_fri, 'DOW_Sat': new_sat, 'DOW_Sun': new_sun,
                'Promo': new_promo, 'Impressions': new_imp, 'Clicks': new_clk
            })
            st.success("Coefficients locked in.")
            st.rerun()

    st.divider()

    # 3. THE HIGH-CAPACITY IMPORTER
    st.subheader("📥 Bulk Data Importer")
    with st.container(border=True):
        st.write("Headers: `entry_date`, `actual_traffic`, `actual_coin_in`, `temp_c`, `ad_impressions`, `ad_clicks`, `social_engagements`")
        uploaded_file = st.file_uploader("Upload Historical CSV", type="csv")
        
        if uploaded_file is not None:
            df_upload = pd.read_csv(uploaded_file)
            if st.button("🚀 Process & Sync to Ledger", use_container_width=True):
                progress_bar = st.progress(0)
                success_count = 0
                
                def clean(val, is_float=False):
                    try:
                        if pd.isna(val) or str(val).strip() == "": return 0.0 if is_float else 0
                        c = str(val).replace(',', '').replace('$', '').strip()
                        return float(c) if is_float else int(float(c))
                    except: return 0.0 if is_float else 0

                for i, row in df_upload.iterrows():
                    progress_bar.progress((i + 1) / len(df_upload))
                    payload = {
                        "entry_date": str(row.get('entry_date', row.get('date'))),
                        "actual_traffic": clean(row.get('actual_traffic', 0)),
                        "actual_coin_in": clean(row.get('actual_coin_in', 0.0), is_float=True),
                        "temp_c": clean(row.get('temp_c', 0), is_float=True),
                        "active_promo": bool(row.get('active_promo', False)),
                        "ad_impressions": clean(row.get('ad_impressions', row.get('impressions', 0))),
                        "ad_clicks": clean(row.get('ad_clicks', row.get('clicks', 0))),
                        "social_engagements": clean(row.get('social_engagements', row.get('engagements', 0)))
                    }
                    try:
                        supabase.table("ledger").upsert(payload, on_conflict="entry_date").execute()
                        success_count += 1
                    except Exception as e:
                        st.error(f"Row {i+1} failed: {e}")

                if success_count > 0:
                    st.success(f"✅ Successfully integrated {success_count} records!")
                    st.rerun()
# --- TAB 5: ASK FLOORCAST ---
with tab5:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🤖 Ask FloorCast</h2>
            <p style="color: #888; margin: 0;">Query your property data using natural language for instant insights.</p>
        </div>
    """, unsafe_allow_html=True)

    st.info("💡 **Try asking:** 'How did the snow last Tuesday impact our coin-in?' or 'What is the correlation between ad clicks and Friday traffic?'")

    # Chat Interface
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat Input
    if prompt := st.chat_input("Ask FloorCast anything about your property performance..."):
        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Generate FloorCast Response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing Ledger..."):
                # Here we pass the ledger context to the AI
                # For now, a placeholder logic; in your full build, this connects to your LLM
                response = f"FloorCast Analysis: I'm currently reviewing the ledger for '{prompt}'. (AI connection active)"
                st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response})
