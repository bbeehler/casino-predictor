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
    st.markdown("### 🏢 Executive Strategy Command")
    
    if ledger_data:
        # 1. DATA PREP & SORTING
        df_exec = pd.DataFrame(ledger_data).copy()
        df_exec['entry_date'] = pd.to_datetime(df_exec['entry_date'])
        # Sort by date so index 0 is always the absolute latest entry
        df_exec = df_exec.sort_values('entry_date', ascending=False).reset_index(drop=True)
        
        # 2. THE AI ENGINE SYNC (Live Accuracy Calculation)
        c = st.session_state.coeffs
        def get_live_val(row):
            dow_key = f"DOW_{pd.to_datetime(row['entry_date']).strftime('%a')}"
            base = c['Intercept'] + c.get(dow_key, 0)
            weather = (row.get('temp_c', 0) * c['Temp_C'])
            promo = c['Promo'] if row.get('active_promo', False) else 0
            return base + weather + promo

        df_exec['live_pred'] = df_exec.apply(get_live_val, axis=1)
        df_exec['actual_traffic'] = pd.to_numeric(df_exec['actual_traffic'], errors='coerce').fillna(0)
        
        # Calculate Accuracy (MAPE)
        df_calc = df_exec[df_exec['actual_traffic'] > 0].copy()
        if not df_calc.empty:
            df_calc['error'] = abs(df_calc['actual_traffic'] - df_calc['live_pred']) / df_calc['actual_traffic']
            accuracy_pct = max(0, (1 - df_calc['error'].mean()) * 100)
        else:
            accuracy_pct = 0.0

        # 3. EXECUTIVE KPI BENTO BOX
        col1, col2, col3 = st.columns(3)
        
        # Select the latest record safely
        latest_row = df_exec.iloc[0] if not df_exec.empty else None

        with col1:
            with st.container(border=True):
                st.markdown("🎯 **AI Model Accuracy**")
                st.metric("System Reliability", f"{accuracy_pct:.1f}%")
                st.progress(min(max(accuracy_pct / 100, 0.0), 1.0))
        
        with col2:
            with st.container(border=True):
                st.markdown("💰 **Latest Revenue**")
                rev_val = pd.to_numeric(latest_row['actual_coin_in'], errors='coerce') if latest_row is not None else 0
                st.metric("Daily Coin-In", f"${float(rev_val):,.0f}")
                date_str = latest_row['entry_date'].strftime('%Y-%m-%d') if latest_row is not None else "N/A"
                st.caption(f"Data Date: {date_str}")
        
        with col3:
            with st.container(border=True):
                st.markdown("🚶 **Floor Traffic**")
                tix_val = pd.to_numeric(latest_row['actual_traffic'], errors='coerce') if latest_row is not None else 0
                st.metric("Actual Visitors", f"{float(tix_val):,.0f}")
                st.caption("Property Foot Traffic")

        st.markdown("---")

        # 4. TREND VISUALIZATION & INSIGHTS
        t1, t2 = st.columns([2, 1])
        
        with t1:
            st.markdown("#### 📈 7-Day Performance vs. AI Baseline")
            # Show the last 7 entries
            chart_data = df_exec.head(7).copy().sort_values('entry_date')
            chart_data = chart_data.rename(columns={
                'actual_traffic': 'Actual Traffic', 
                'live_pred': 'AI Prediction', 
                'entry_date': 'Date'
            })
            st.area_chart(chart_data.set_index('Date')[['Actual Traffic', 'AI Prediction']], 
                          color=["#FFCC00", "#555555"])

        with t2:
            st.markdown("#### 🤖 AI Analyst Status")
            if accuracy_pct > 85:
                st.success(f"Model Integrity: High. The AI has successfully correlated your DOW and Weather patterns to real traffic.")
            elif accuracy_pct > 60:
                st.warning("Model Integrity: Fair. Consider a 'Sync AI to Reality' in the Admin tab to tighten the prediction gap.")
            else:
                st.error("Model Integrity: Low. Current coefficients do not match property reality. Recalibration required.")
                
    else:
        st.info("No data found. Please log an entry or upload a CSV to activate the Executive Dashboard.")

# --- TAB 2: DAILY TRACKER & FORECAST ---
with tab2:
    st.markdown("### 🕹️ FloorPace Control Panel")
    
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
                    eng = st.number_input("Social Engagements", 0, 10000, 500)
                    clks = st.number_input("Ad Clicks", 0, 5000, 200)

                if st.form_submit_button("💾 Save to FloorPace Ledger", use_container_width=True):
                    c = st.session_state.coeffs
                    dow_key = f"DOW_{date_in.strftime('%a')}"
                    
                    # Math for Prediction
                    base_v = float(c['Intercept'] + c.get(dow_key, 0))
                    weather_v = float((temp * c['Temp_C']) + (snow * c['Snow_cm']))
                    dig_lift_v = float((promo * c['Promo']) + (imp * c['Impressions']) + (eng * c['Engagements']) + (clks * c['Clicks']))
                    final_pred = float(base_v + weather_v + dig_lift_v)
                    
                    data = {
                        "entry_date": str(date_in),
                        "day_of_week": str(date_in.strftime("%A")),
                        "actual_traffic": int(act_traf),
                        "actual_coin_in": float(act_coin),
                        "predicted_traffic": int(final_pred),
                        "variance": int(int(act_traf) - int(final_pred)),
                        "temp_c": int(temp),
                        "snow_cm": float(snow),
                        "rain_mm": 0.0,
                        "active_promo": bool(promo),
                        "ad_impressions": int(imp),
                        "social_engagements": int(eng),
                        "ad_clicks": int(clks),
                        "digital_lift_visitors": int(dig_lift_v),
                        "digital_revenue_impact": float(float(dig_lift_v) * float(c['Avg_Coin_In'])),
                        "weather_alert": False
                    }
                    
                    try:
                        supabase.table("ledger").upsert(data, on_conflict="entry_date").execute()
                        st.toast("✅ Record saved to FloorPace Ledger")
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
                                up_temp = st.number_input("Temp", value=int(record.get('temp_c', 0)))
                                up_snow = st.number_input("Snow", value=float(record.get('snow_cm', 0.0)))
                                up_alert = st.checkbox("Alert", value=bool(record.get('weather_alert', False)))
                            with ec3:
                                up_promo = st.checkbox("Promo", value=bool(record.get('active_promo', False)))
                                up_imp = st.number_input("Impressions", value=int(record.get('ad_impressions', 0)))
                                up_clk = st.number_input("Clicks", value=int(record.get('ad_clicks', 0)))

                            if st.form_submit_button("💾 Save All Changes"):
                                supabase.table("ledger").update({
                                    "actual_traffic": up_t, "actual_coin_in": up_c, "predicted_traffic": up_p,
                                    "temp_c": up_temp, "snow_cm": up_snow, "weather_alert": up_alert,
                                    "active_promo": up_promo, "ad_impressions": up_imp, "ad_clicks": up_clk,
                                    "variance": int(up_t - up_p)
                                }).eq("entry_date", search_str).execute()
                                st.toast("Full record updated!")
                                st.rerun()
                else:
                    st.info("No record found for this date.")

        st.markdown("**Full Historical Ledger**")
        display_df = df_edit.sort_values('entry_date', ascending=False)
        display_df['entry_date'] = display_df['entry_date'].dt.strftime('%Y-%m-%d')
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# --- TAB 3: REPORTING & ROI ---
with tab3:
    st.markdown("### 📊 Performance Analytics & ROI")
    
    if ledger_data:
        df_rep = pd.DataFrame(ledger_data).copy()
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        # Pull master coefficients from your Admin Tab (Excel constants)
        c = st.session_state.coeffs
        
        # 1. Date Range Filter
        min_d, max_d = df_rep['entry_date'].min().date(), df_rep['entry_date'].max().date()
        rep_range = st.date_input("Select Reporting Period", [min_d, max_d])
        
        if len(rep_range) == 2:
            start_date, end_date = pd.to_datetime(rep_range[0]), pd.to_datetime(rep_range[1])
            mask = (df_rep['entry_date'] >= start_date) & (df_rep['entry_date'] <= end_date)
            df_filtered = df_rep.loc[mask].copy().sort_values('entry_date')
            
            if not df_filtered.empty:
                # --- 2. THE EXCEL FORMULA ENGINE ---
                # This mirrors your spreadsheet: = (Base + DOW + Weather + Promo) * AvgSpend
                def get_excel_baseline(row):
                    # Get Day of Week (Mon, Tue, etc.)
                    dow_key = f"DOW_{pd.to_datetime(row['entry_date']).strftime('%a')}"
                    
                    # 1. Base Traffic + Day of Week Offset
                    base_traffic = float(c['Intercept'] + c.get(dow_key, 0))
                    
                    # 2. Weather Impact (Temp + Snow)
                    weather_impact = float((row.get('temp_c', 0) * c['Temp_C']) + (row.get('snow_cm', 0) * c['Snow_cm']))
                    
                    # 3. Promotion Lift
                    promo_lift = float(c['Promo'] if row.get('active_promo', False) else 0)
                    
                    # Total Predicted Traffic (The 'Excel Forecast' column)
                    total_traffic_forecast = base_traffic + weather_impact + promo_lift
                    
                    # Return Revenue: Forecast * Avg Spend
                    return total_traffic_forecast * float(c['Avg_Coin_In'])

                # Apply the formula to every row to create the 'AI Baseline' column
                df_filtered['excel_baseline_rev'] = df_filtered.apply(get_excel_baseline, axis=1)
                
                # --- 3. METRIC CALCULATIONS ---
                actual_revenue = pd.to_numeric(df_filtered['actual_coin_in'], errors='coerce').fillna(0).sum()
                baseline_revenue = df_filtered['excel_baseline_rev'].sum()
                
                variance = actual_revenue - baseline_revenue
                pct_variance = (variance / baseline_revenue * 100) if baseline_revenue != 0 else 0.0

                # --- 4. DISPLAY (EXECUTIVE VIEW) ---
                m1, m2, m3 = st.columns(3)
                with m1:
                    with st.container(border=True):
                        st.metric("Actual Total Revenue", f"${actual_revenue:,.0f}")
                with m2:
                    with st.container(border=True):
                        st.metric("AI Baseline Revenue", f"${baseline_revenue:,.0f}")
                with m3:
                    with st.container(border=True):
                        st.metric("Revenue Variance", f"${variance:,.0f}", delta=f"{pct_variance:.1f}%")

                st.divider()

                # --- 5. THE CHART ---
                st.markdown("**Revenue vs AI Prediction Baseline (Excel Logic)**")
                chart_df = df_filtered.copy()
                chart_df = chart_df.rename(columns={'actual_coin_in': 'Actual Revenue', 'entry_date': 'Date'})
                
                st.area_chart(chart_df.set_index('Date')[['Actual Revenue', 'excel_baseline_rev']], 
                              color=["#FFCC00", "#555555"])
            else:
                st.warning("Select a date range that contains data.")

# --- TAB 4: ADMIN ENGINE (THE BRAIN) ---
with tab4:
    st.markdown("### ⚙️ Engine Control & AI Optimization")
    
    # 1. THE ADVANCED ML OPTIMIZER
    with st.container(border=True):
        c_left, c_right = st.columns([2, 1])
        with c_left:
            st.subheader("🤖 Multivariate ML Optimization")
            st.write("Automatically synchronizes all coefficients by analyzing correlations between Weather, DOW, and Promotions.")
        with c_right:
            st.write("##")
            if st.button("🚀 Sync AI to Reality", type="primary", use_container_width=True):
                if len(ledger_data) > 10:
                    df_ml = pd.DataFrame(ledger_data)
                    # Prepare the math features
                    df_ml['actual_traffic'] = pd.to_numeric(df_ml['actual_traffic'], errors='coerce').fillna(0)
                    df_ml['temp_c'] = pd.to_numeric(df_ml['temp_c'], errors='coerce').fillna(0)
                    df_ml['promo_val'] = df_ml['active_promo'].astype(int)
                    
                    # Create Day of Week "Dummy" variables (just like Excel regression)
                    df_ml['dt'] = pd.to_datetime(df_ml['entry_date'])
                    df_ml['dow'] = df_ml['dt'].dt.day_name()
                    df_dummies = pd.get_dummies(df_ml['dow'])
                    
                    # Combine all features
                    X = pd.concat([df_ml[['temp_c', 'promo_val']], df_dummies], axis=1)
                    y = df_ml['actual_traffic']
                    
                    from sklearn.linear_model import LinearRegression
                    model = LinearRegression().fit(X, y)
                    
                    # UPDATE ALL COEFFICIENTS AUTOMATICALLY
                    st.session_state.coeffs['Intercept'] = round(model.intercept_, 2)
                    st.session_state.coeffs['Temp_C'] = round(model.coef_[0], 2)
                    st.session_state.coeffs['Promo'] = round(model.coef_[1], 2)
                    
                    # Update DOW Offsets
                    dow_map = {'Monday': 'DOW_Mon', 'Tuesday': 'DOW_Tue', 'Wednesday': 'DOW_Wed', 
                               'Thursday': 'DOW_Thu', 'Friday': 'DOW_Fri', 'Saturday': 'DOW_Sat', 'Sunday': 'DOW_Sun'}
                    
                    for i, col_name in enumerate(df_dummies.columns):
                        if col_name in dow_map:
                            st.session_state.coeffs[dow_map[col_name]] = round(model.coef_[i+2], 2)

                    st.success("🎯 Global Optimization Complete! Accuracy recalculated.")
                    st.rerun()
                else:
                    st.error("Need at least 10 days of historical data for a Multivariate Sync.")

    st.markdown("---")

    # 2. THE SETTINGS FORM (Now serves as a 'Review' of what the AI learned)
    with st.form("admin_settings_full"):
        st.markdown("#### 🛠️ Current Learned Coefficients")
        col_fin, col_wea = st.columns(2)
        with col_fin:
            new_intercept = st.number_input("Base Daily Traffic", value=float(st.session_state.coeffs['Intercept']))
            new_coin = st.number_input("Avg Revenue per Head ($)", value=float(st.session_state.coeffs['Avg_Coin_In']))
        with col_wea:
            new_temp = st.number_input("Temp Impact", value=float(st.session_state.coeffs['Temp_C']))
            new_promo = st.number_input("Promotion Lift", value=float(st.session_state.coeffs['Promo']))

        st.divider()
        d1, d2, d3, d4, d5, d6, d7 = st.columns(7)
        new_mon = d1.number_input("Mon", value=float(st.session_state.coeffs['DOW_Mon']))
        new_tue = d2.number_input("Tue", value=float(st.session_state.coeffs['DOW_Tue']))
        new_wed = d3.number_input("Wed", value=float(st.session_state.coeffs['DOW_Wed']))
        new_thu = d4.number_input("Thu", value=float(st.session_state.coeffs['DOW_Thu']))
        new_fri = d5.number_input("Fri", value=float(st.session_state.coeffs['DOW_Fri']))
        new_sat = d6.number_input("Sat", value=float(st.session_state.coeffs['DOW_Sat']))
        new_sun = d7.number_input("Sun", value=float(st.session_state.coeffs['DOW_Sun']))

        if st.form_submit_button("💾 Finalize Manual Tweaks"):
            st.session_state.coeffs.update({'Intercept': new_intercept, 'Avg_Coin_In': new_coin, 'Temp_C': new_temp, 'Promo': new_promo,
                'DOW_Mon': new_mon, 'DOW_Tue': new_tue, 'DOW_Wed': new_wed, 'DOW_Thu': new_thu, 'DOW_Fri': new_fri, 'DOW_Sat': new_sat, 'DOW_Sun': new_sun})
            st.rerun()

# --- TAB 5: ASK AI DATA ANALYST ---
with tab5:
    st.markdown("### 💬 AI Data Analyst")
    st.write("Ask questions about your property's performance, weather impacts, or marketing ROI.")

    # 1. Initialize Chat History
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 2. Display Chat History with Modern Styling
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # 3. The Chat Input
    if prompt := st.chat_input("Ex: How did rain affect our revenue last week?"):
        # Add user message to chat
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 4. Generate AI Response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing ledger and generating insights..."):
                try:
                    # Provide the AI with the full ledger context (condensed)
                    if ledger_data:
                        df_context = pd.DataFrame(ledger_data).tail(90).to_csv(index=False)
                    else:
                        df_context = "No data available in the ledger yet."

                    model = genai.GenerativeModel('models/gemini-2.5-flash')
                    
                    full_prompt = f"""
                    You are a world-class Casino Data Analyst for Hard Rock Ottawa. 
                    You have access to the last 60 days of property data:
                    {df_context}
                    
                    User Question: {prompt}
                    
                    Guidelines:
                    - Be specific. Use dollar amounts and traffic numbers from the data.
                    - If asked about weather, correlate it to the 'variance' or 'actual_traffic'.
                    - Maintain a professional, executive-ready tone.
                    - If the data doesn't contain the answer, say so.
                    """

                    response = model.generate_content(full_prompt)
                    st.markdown(response.text)
                    
                    # Add assistant response to history
                    st.session_state.messages.append({"role": "assistant", "content": response.text})
                except Exception as e:
                    st.error(f"Assistant Error: {e}")

    # 5. Clear Chat Option
    if st.button("Clear Conversation", type="secondary"):
        st.session_state.messages = []
        st.rerun()
