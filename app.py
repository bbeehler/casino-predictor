import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
import google.generativeai as genai
from sklearn.linear_model import LinearRegression
import numpy as np

# 1. PAGE CONFIG (Must be the very first Streamlit command)
st.set_page_config(page_title="Hard Rock Strategic Engine", layout="wide")

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
    # 1. THE HERO CARD (AI Strategy)
    st.markdown("### 🤖 Strategic Intelligence")
    
    if ledger_data:
        df_dash = pd.DataFrame(ledger_data)
        df_dash = df_dash[df_dash['actual_traffic'] > 0].copy()
        
        with st.container(border=True):
            if not df_dash.empty:
                st.write("Click below to have Gemini synthesize your data into a strategic narrative.")
                if st.button("✨ Generate AI Executive Briefing", use_container_width=True):
                    with st.spinner("AI is analyzing performance..."):
                        try:
                            df_dash['entry_date'] = pd.to_datetime(df_dash['entry_date'])
                            recent_30 = df_dash.sort_values('entry_date', ascending=False).head(30).to_csv(index=False)
                            
                            c = st.session_state.coeffs
                            f_outlook = ""
                            for i in range(1, 8):
                                d = datetime.date.today() + datetime.timedelta(days=i)
                                d_key = f"DOW_{d.strftime('%a')}"
                                base = c['Intercept'] + c.get(d_key, 0)
                                f_outlook += f"{d.strftime('%a %d')}: Est. {int(base)} visitors; "

                            model = genai.GenerativeModel('models/gemini-2.5-flash')
                            prompt = f"Senior Strategy Lead for Hard Rock Ottawa. Summarize this data: {recent_30}. Outlook: {f_outlook}. Max 200 words."
                            
                            response = model.generate_content(prompt)
                            st.markdown("---")
                            st.markdown(response.text)
                        except Exception as e:
                            st.error(f"AI Error: {e}")
            else:
                st.info("Log daily entries to see AI insights.")

        # 2. THE METRIC GRID (Small Cards)
        st.markdown("---")
        st.markdown("### 📊 Performance Pulse")
        m1, m2, m3 = st.columns(3)

        with m1:
            with st.container(border=True):
                st.metric("Total Revenue (YTD)", f"${df_dash['actual_coin_in'].sum():,.0f}")
        with m2:
            with st.container(border=True):
                st.metric("Total Traffic", f"{int(df_dash['actual_traffic'].sum()):,}")
        with m3:
            with st.container(border=True):
                df_dash['error'] = abs(df_dash['actual_traffic'] - df_dash['predicted_traffic'])
                mape = (df_dash['error'] / df_dash['actual_traffic']).mean()
                st.metric("AI Accuracy", f"{(1 - mape) * 100:.1f}%")

        # 3. ANALYTICS CARDS (Mixed Sizes)
        st.markdown("---")
        col_chart, col_side = st.columns([2, 1])

        with col_chart:
            with st.container(border=True):
                st.subheader("📈 Revenue Trend")
                chart_data = df_dash.set_index('entry_date')[['actual_coin_in']]
                st.area_chart(chart_data, color="#FFCC00")

        with col_side:
            with st.container(border=True):
                st.subheader("📱 Digital Lift")
                st.write(f"**Impact:**")
                st.title(f"${df_dash['digital_revenue_impact'].sum():,.0f}")
                st.metric("Visitor Lift", f"{int(df_dash['digital_lift_visitors'].sum()):,}")

        # 4. RAW DATA
        st.markdown("---")
        with st.expander("📂 View Full Ledger"):
            st.dataframe(df_dash.sort_values('entry_date', ascending=False), use_container_width=True)
    else:
        st.info("Database is empty. Log entries in Tab 2 to populate dashboard.")

# --- TAB 2: DAILY TRACKER & FORECAST ---
with tab2:
    st.markdown("### 🕹️ Property Control Panel")
    
    # We create a 2-column layout for the "Bento Box" feel
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

                if st.form_submit_button("💾 Save Daily Records", use_container_width=True):
                    c = st.session_state.coeffs
                    dow_key = f"DOW_{date_in.strftime('%a')}"
                    
                    # Math for Prediction
                    base = c['Intercept'] + c.get(dow_key, 0)
                    weather = (temp * c['Temp_C']) + (snow * c['Snow_cm'])
                    dig_lift = (promo * c['Promo']) + (imp * c['Impressions']) + (eng * c['Engagements']) + (clks * c['Clicks'])
                    final_pred = base + weather + dig_lift
                    
                    data = {
                        "entry_date": str(date_in), "day_of_week": date_in.strftime("%A"),
                        "actual_traffic": act_traf, "actual_coin_in": float(act_coin),
                        "predicted_traffic": int(final_pred), "variance": int(act_traf - final_pred),
                        "temp_c": int(temp), "snow_cm": float(snow), "active_promo": promo,
                        "ad_impressions": int(imp), "social_engagements": int(eng), "ad_clicks": int(clks),
                        "digital_lift_visitors": int(dig_lift), "digital_revenue_impact": float(dig_lift * c['Avg_Coin_In'])
                    }
                    supabase.table("ledger").upsert(data, on_conflict="entry_date").execute()
                    st.toast("✅ Record saved to Ledger")
                    st.rerun()

    with col_sandbox:
        with st.container(border=True):
            st.subheader("🔮 2. Forecast Sandbox")
            st.write("Simulate future dates to see projected revenue.")
            
            f_range = st.date_input("Forecast Range", [datetime.date.today() + datetime.timedelta(days=1), datetime.date.today() + datetime.timedelta(days=7)])
            
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
                
                v1, v2 = st.columns(2)
                v1.metric("Est. Total Visitors", f"{df_f['Visitors'].sum():,}")
                v2.metric("Est. Total Revenue", f"${df_f['Revenue'].sum():,.0f}")
                
                st.line_chart(df_f.set_index("Date")["Visitors"], color="#FFCC00")

   # --- SECTION B: LEDGER RANGE SEARCH ---
    st.markdown("### 🔍 3. Historical Ledger Audit")
    with st.container(border=True):
        if ledger_data:
            df_edit = pd.DataFrame(ledger_data)
            df_edit['entry_date'] = pd.to_datetime(df_edit['entry_date'])
            
            # 1. Date Range Picker
            search_range = st.date_input(
                "Select Date Range to Audit",
                value=[df_edit['entry_date'].max().date() - datetime.timedelta(days=7), 
                       df_edit['entry_date'].max().date()],
                help="Select the start and end date to view all logged data for that period."
            )
            
            # Ensure we have both a start and end date before filtering
            if isinstance(search_range, list) or isinstance(search_range, tuple):
                if len(search_range) == 2:
                    start_search, end_search = search_range
                    
                    # Filter the dataframe
                    mask = (df_edit['entry_date'].dt.date >= start_search) & (df_edit['entry_date'].dt.date <= end_search)
                    found_range = df_edit.loc[mask].sort_values('entry_date', ascending=False)
                    
                    if not found_range.empty:
                        st.success(f"Displaying {len(found_range)} records from {start_search} to {end_search}")
                        
                        # 2. Summary Metrics for the Selected Range
                        r1, r2, r3 = st.columns(3)
                        avg_acc = (1 - (abs(found_range['actual_traffic'] - found_range['predicted_traffic']) / found_range['actual_traffic']).mean()) * 100
                        
                        r1.metric("Range Total Revenue", f"${found_range['actual_coin_in'].sum():,.0f}")
                        r2.metric("Range Total Traffic", f"{int(found_range['actual_traffic'].sum()):,}")
                        r3.metric("Avg. Model Accuracy", f"{avg_acc:.1f}%")
                        
                        # 3. The Full Data Table
                        st.write("---")
                        # We format the date column back to a clean string for the table
                        found_range['entry_date'] = found_range['entry_date'].dt.strftime('%Y-%m-%d')
                        st.dataframe(found_range, use_container_width=True, hide_index=True)
                        
                        # 4. CSV Download for the range
                        csv = found_range.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Selected Range as CSV",
                            data=csv,
                            file_name=f"HardRock_Audit_{start_search}_to_{end_search}.csv",
                            mime='text/csv',
                        )
                    else:
                        st.warning("No records found for this specific date range.")
        else:
            st.info("Database is empty. Log entries above to see them here.")
# --- TAB 3: REPORTING & ROI ---
with tab3:
    st.markdown("### 📈 Strategic Reporting & ROI")
    
    if ledger_data:
        df_rep = pd.DataFrame(ledger_data)
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        # 1. THE CONTROL CARD (Range Selector)
        with st.container(border=True):
            r_col1, r_col2 = st.columns(2)
            # Default to the full range of data available
            rep_start = r_col1.date_input("Report Start", df_rep['entry_date'].min().date())
            rep_end = r_col2.date_input("Report End", df_rep['entry_date'].max().date())
            
            mask = (df_rep['entry_date'].dt.date >= rep_start) & (df_rep['entry_date'].dt.date <= rep_end)
            df_f = df_rep.loc[mask].sort_values('entry_date')

        if not df_f.empty:
            # 2. THE PERFORMANCE CARD (Actual vs AI Baseline)
            st.markdown("#### AI Prediction vs. Actual Revenue")
            with st.container(border=True):
                avg_coin = st.session_state.coeffs['Avg_Coin_In']
                df_f['AI_Pred_Rev'] = df_f['predicted_traffic'] * avg_coin
                
                t_act_rev = df_f['actual_coin_in'].sum()
                t_pre_rev = df_f['AI_Pred_Rev'].sum()
                rev_var = t_act_rev - t_pre_rev
                rev_pct = (rev_var / t_pre_rev) * 100 if t_pre_rev != 0 else 0
                
                v1, v2, v3 = st.columns(3)
                v1.metric("Actual Revenue", f"${t_act_rev:,.0f}")
                v2.metric("AI Baseline", f"${t_pre_rev:,.0f}")
                v3.metric("Revenue Variance", f"${rev_var:+.0f}", delta=f"{rev_pct:+.1f}%")
                
                # Big Chart Comparison
                st.area_chart(df_f.set_index('entry_date')[['actual_coin_in', 'AI_Pred_Rev']], color=["#FFCC00", "#333333"])

            # 3. THE TUG-OF-WAR (Weather vs. Digital)
            st.markdown("#### Impact Analysis")
            c_left, c_right = st.columns(2)
            
            with c_left:
                with st.container(border=True):
                    st.subheader("⛈️ Weather Penalty")
                    c = st.session_state.coeffs
                    # Calculate loss based on variables in the ledger
                    w_loss = ((df_f['snow_cm']*c['Snow_cm']) + (df_f['rain_mm']*c['Rain_mm']) + (df_f['weather_alert'].astype(int)*c['Alert'])) * avg_coin
                    st.title(f"${abs(w_loss.sum()):,.0f}")
                    st.caption("Estimated revenue lost to environmental factors.")
            
            with c_right:
                with st.container(border=True):
                    st.subheader("📱 Digital Marketing Gain")
                    # Use the pre-calculated digital revenue impact from the DB
                    p_gain = df_f['digital_revenue_impact'].sum()
                    st.title(f"${p_gain:,.0f}")
                    st.caption("Total revenue contribution from digital marketing lift.")

            # 4. DIGITAL ROI & DATA AUDIT
            st.markdown("#### Digital Channel Efficiency & Raw Ledger")
            with st.container(border=True):
                total_eng = df_f['social_engagements'].sum()
                rev_per_eng = p_gain / total_eng if total_eng > 0 else 0
                
                d1, d2, d3 = st.columns(3)
                d1.metric("Total Impressions", f"{int(df_f['ad_impressions'].sum()):,}")
                d2.metric("Total Engagements", f"{int(total_eng):,}")
                d3.metric("Rev Per Engagement", f"${rev_per_eng:.2f}")

                st.divider()
                # Full data view for this range
                st.write("**Range Audit Ledger**")
                st.dataframe(df_f, use_container_width=True, hide_index=True)
                
                # Range CSV Download
                csv_data = df_f.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Report as CSV",
                    data=csv_data,
                    file_name=f"HardRock_Report_{rep_start}_to_{rep_end}.csv",
                    mime='text/csv'
                )
        else:
            st.warning("No data found for this range.")
    else:
        st.info("Database is empty. Analytics will populate once daily entries are logged.")

# --- TAB 4: ADMIN ENGINE (FULL CONTROL) ---
with tab4:
    st.markdown("### ⚙️ Engine Control & Coefficient Tuning")
    
    # 1. THE AI RETRAINING CARD (Now Fully Functional)
    with st.container(border=True):
        c_left, c_right = st.columns([2, 1])
        with c_left:
            st.subheader("⚡ Machine Learning Auto-Tune")
            st.write("Analyze historical ledger data to automatically recalibrate all weights based on actual performance.")
        with c_right:
            st.write("##")
            if st.button("Run ML Recalibration", type="primary", use_container_width=True):
                if len(ledger_data) < 10:
                    st.error("Need at least 10 days of data to recalibrate accurately.")
                else:
                    with st.spinner("Calculating optimal coefficients..."):
                        # Prepare Data
                        df_ml = pd.DataFrame(ledger_data)
                        
                        # Create DOW dummies (Mon, Tue, etc)
                        df_ml['date_dt'] = pd.to_datetime(df_ml['entry_date'])
                        df_ml['dow'] = df_ml['date_dt'].dt.strftime('%a')
                        dow_dummies = pd.get_dummies(df_ml['dow'], prefix='DOW')
                        
                        # Define Features (X) and Target (y)
                        features = ['temp_c', 'snow_cm', 'active_promo', 'ad_impressions', 'social_engagements', 'ad_clicks']
                        X = pd.concat([df_ml[features], dow_dummies], axis=1).fillna(0)
                        y = df_ml['actual_traffic']
                        
                        # Run Linear Regression
                        model = LinearRegression()
                        model.fit(X, y)
                        
                        # Map new weights back to session state
                        new_weights = dict(zip(X.columns, model.coef_))
                        
                        # Update the Session State
                        st.session_state.coeffs['Intercept'] = float(model.intercept_)
                        for key in new_weights:
                            if key in st.session_state.coeffs:
                                st.session_state.coeffs[key] = float(new_weights[key])
                        
                        # Recalculate Average Revenue per Head based on history
                        st.session_state.coeffs['Avg_Coin_In'] = float(df_ml['actual_coin_in'].sum() / df_ml['actual_traffic'].sum())
                        
                        st.success("Model recalibrated! Accuracy is now optimized to your real-world data.")
                        st.rerun()

    # 2. THE MASTER SETTINGS FORM
    with st.form("admin_settings_full"):
        st.markdown("#### 🛠️ Manual Coefficient Overrides")
        
        # Row 1: Core Financials & Weather
        col_fin, col_wea = st.columns(2)
        with col_fin:
            st.markdown("**Core Baselines**")
            new_intercept = st.number_input("Base Daily Traffic (Intercept)", value=st.session_state.coeffs['Intercept'], step=50.0)
            new_coin = st.number_input("Avg Revenue per Head ($)", value=st.session_state.coeffs['Avg_Coin_In'], step=1.0)
        
        with col_wea:
            st.markdown("**Environmental Impacts**")
            new_temp = st.number_input("Temp Impact (per °C)", value=st.session_state.coeffs['Temp_C'], format="%.2f")
            new_snow = st.number_input("Snow Penalty (per cm)", value=st.session_state.coeffs['Snow_cm'], format="%.2f")
            new_rain = st.number_input("Rain Penalty (per mm)", value=st.session_state.coeffs['Rain_mm'], format="%.2f")
            new_alert = st.number_input("Severe Weather Alert Penalty", value=st.session_state.coeffs['Alert'], step=50.0)

        st.divider()

        # Row 2: Day of the Week (The "DOW" Multipliers)
        st.markdown("**Day of the Week Adjustments (Traffic +/-)**")
        d1, d2, d3, d4, d5, d6, d7 = st.columns(7)
        new_mon = d1.number_input("Mon", value=st.session_state.coeffs['DOW_Mon'], step=10.0)
        new_tue = d2.number_input("Tue", value=st.session_state.coeffs['DOW_Tue'], step=10.0)
        new_wed = d3.number_input("Wed", value=st.session_state.coeffs['DOW_Wed'], step=10.0)
        new_thu = d4.number_input("Thu", value=st.session_state.coeffs['DOW_Thu'], step=10.0)
        new_fri = d5.number_input("Fri", value=st.session_state.coeffs['DOW_Fri'], step=10.0)
        new_sat = d6.number_input("Sat", value=st.session_state.coeffs['DOW_Sat'], step=10.0)
        new_sun = d7.number_input("Sun", value=st.session_state.coeffs['DOW_Sun'], step=10.0)

        st.divider()

        # Row 3: Marketing & Digital Lift
        st.markdown("**Marketing & Digital Correlation Weights**")
        m1, m2, m3, m4 = st.columns(4)
        new_promo = m1.number_input("Promotion Lift", value=st.session_state.coeffs['Promo'], step=10.0)
        new_imp = m2.number_input("Ad Impressions", value=st.session_state.coeffs['Impressions'], format="%.6f")
        new_eng = m3.number_input("Engagements", value=st.session_state.coeffs['Engagements'], format="%.4f")
        new_clk = m4.number_input("Ad Clicks", value=st.session_state.coeffs['Clicks'], format="%.4f")

        st.write("---")
        if st.form_submit_button("💾 Save All Engine Changes", use_container_width=True):
            # Update the Session State
            st.session_state.coeffs.update({
                'Intercept': new_intercept, 'Avg_Coin_In': new_coin,
                'Temp_C': new_temp, 'Snow_cm': new_snow, 'Rain_mm': new_rain, 'Alert': new_alert,
                'DOW_Mon': new_mon, 'DOW_Tue': new_tue, 'DOW_Wed': new_wed, 'DOW_Thu': new_thu,
                'DOW_Fri': new_fri, 'DOW_Sat': new_sat, 'DOW_Sun': new_sun,
                'Promo': new_promo, 'Impressions': new_imp, 'Engagements': new_eng, 'Clicks': new_clk
            })
            st.success("Engine recalibrated successfully!")
            st.rerun()

    # 3. EXPORT SETTINGS
    with st.expander("📥 Backup Engine Configuration"):
        st.json(st.session_state.coeffs)

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
