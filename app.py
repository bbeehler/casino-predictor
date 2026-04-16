import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
import google.generativeai as genai

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

                            model = genai.GenerativeModel('models/gemini-1.5-flash')
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

   # --- SECTION B: LEDGER SEARCH & EDIT (FULL COLUMN VIEW) ---
    st.markdown("### 🔍 3. Search & Edit Historical Ledger")
    with st.container(border=True):
        if ledger_data:
            df_edit = pd.DataFrame(ledger_data)
            df_edit['entry_date'] = df_edit['entry_date'].astype(str)
            
            search_date = st.date_input("Search a past date to verify all data points", 
                                       value=pd.to_datetime(df_edit['entry_date']).max().date())
            
            search_str = search_date.strftime("%Y-%m-%d")
            found = df_edit[df_edit['entry_date'] == search_str]
            
            if not found.empty:
                row = found.iloc[0]
                st.success(f"**Full Record Found for {search_str}**")
                
                # 1. THE SUMMARY CARDS (Top Level)
                r1, r2, r3, r4 = st.columns(4)
                r1.metric("Actual Traffic", f"{int(row['actual_traffic']):,}")
                r2.metric("Actual Revenue", f"${row['actual_coin_in']:,.0f}")
                r3.metric("Digital Lift", f"{int(row.get('digital_lift_visitors', 0)):,}")
                
                # Calculate Accuracy for this specific day
                error = abs(row['actual_traffic'] - row['predicted_traffic'])
                acc = (1 - (error / row['actual_traffic'])) * 100 if row['actual_traffic'] > 0 else 0
                r4.metric("AI Accuracy", f"{acc:.1f}%")
                
                # 2. THE FULL AUDIT (Every Column)
                st.write("---")
                st.markdown("**Detailed Variable Audit:**")
                # We transpose the single row so it looks like a clean vertical list of every column
                audit_df = found.T
                audit_df.columns = ["Value"]
                st.table(audit_df)
                
            else:
                st.warning(f"No entry exists for {search_str}.")
# --- TAB 3: REPORTING & ROI ---
with tab3:
    st.header("📈 Strategic Reporting Center")
    
    if ledger_data:
        df_rep = pd.DataFrame(ledger_data)
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        # --- 1. DATE RANGE SELECTOR ---
        st.subheader("🗓️ Select Report Range")
        col_d1, col_d2 = st.columns(2)
        start_date = col_d1.date_input("Start Date", df_rep['entry_date'].min())
        end_date = col_d2.date_input("End Date", df_rep['entry_date'].max())
        
        # Filter data based on selection
        mask = (df_rep['entry_date'].dt.date >= start_date) & (df_rep['entry_date'].dt.date <= end_date)
        df_filtered = df_rep.loc[mask].sort_values('entry_date')
        
        if not df_filtered.empty:
            # --- 2. REPORT MODULE: TRAFFIC & REVENUE VARIANCE ---
            st.divider()
            st.subheader("💰 Traffic & Revenue: AI Prediction vs. Actual")
            
            # Core Math
            total_act_traf = df_filtered['actual_traffic'].sum()
            total_pred_traf = df_filtered['predicted_traffic'].sum()
            
            # Revenue Math
            # Actual Revenue is from the database; Predicted Revenue = Predicted Traffic * Avg Coin-In
            total_act_rev = df_filtered['actual_coin_in'].sum()
            avg_coin = st.session_state.coeffs['Avg_Coin_In']
            total_pred_rev = total_pred_traf * avg_coin
            
            rev_variance = total_act_rev - total_pred_rev
            rev_var_pct = (rev_variance / total_pred_rev) * 100 if total_pred_rev > 0 else 0

            # Traffic Row
            st.write("**Foot Traffic Performance**")
            t1, t2, t3 = st.columns(3)
            t1.metric("Actual Traffic", f"{total_act_traf:,}")
            t2.metric("AI Predicted", f"{total_pred_traf:,.0f}")
            t3.metric("Traffic Variance", f"{total_act_traf - total_pred_traf:+.0f}")

            # Revenue Row
            st.write("**Revenue Performance (Actual vs. AI Baseline)**")
            r1, r2, r3 = st.columns(3)
            r1.metric("Actual Revenue", f"${total_act_rev:,.0f}")
            r2.metric("AI Predicted Revenue", f"${total_pred_rev:,.0f}")
            r3.metric("Revenue Variance", f"${rev_variance:+.0f}", delta=f"{rev_var_pct:+.1f}%")
            
            # Dual Axis Style Chart (or simplified comparison)
            st.write("**Revenue Trend vs. AI Expectation**")
            # Create a comparison dataframe for the chart
            df_filtered['Predicted_Revenue'] = df_filtered['predicted_traffic'] * avg_coin
            chart_data_rev = df_filtered.set_index('entry_date')[['actual_coin_in', 'Predicted_Revenue']]
            chart_data_rev.columns = ['Actual Revenue', 'AI Predicted Revenue']
            st.area_chart(chart_data_rev)

            # --- 3. REPORT MODULE: DIGITAL ROI ---
            st.divider()
            st.subheader("📱 Digital Marketing ROI Deep-Dive")
            
            d_rev = df_filtered['digital_revenue_impact'].sum()
            d_lift = df_filtered['digital_lift_visitors'].sum()
            # Calculate "Cost Per Visit" if you eventually add spend data, for now we use "Revenue Per Engagement"
            total_eng = df_filtered['social_engagements'].sum()
            rev_per_eng = d_rev / total_eng if total_eng > 0 else 0
            
            r1, r2, r3 = st.columns(3)
            r1.metric("Digital Revenue Impact", f"${d_rev:,.0f}")
            r2.metric("Total Digital Lift", f"{d_lift:,.0f} Visitors")
            r3.metric("Rev per Engagement", f"${rev_per_eng:.2f}")
            
            # Correlation chart: Social Engagements vs. Digital Lift
            st.bar_chart(df_filtered.set_index('entry_date')[['digital_lift_visitors', 'social_engagements']])

            # --- 4. REPORT MODULE: THE "OUTSIDE FORCES" REPORT ---
            st.divider()
            st.subheader("⛈️ Environmental & Promo Impact")
            
            # Calculate dynamic impacts based on coefficients
            c = st.session_state.coeffs
            df_filtered['weather_loss'] = (
                (df_filtered['snow_cm'] * c['Snow_cm']) + 
                (df_filtered['rain_mm'] * c['Rain_mm']) + 
                (df_filtered['weather_alert'].astype(int) * c['Alert'])
            ) * c['Avg_Coin_In']
            
            df_filtered['promo_gain'] = (df_filtered['active_promo'].astype(int) * c['Promo']) * c['Avg_Coin_In']
            
            total_weather_loss = abs(df_filtered['weather_loss'].sum())
            total_promo_gain = df_filtered['promo_gain'].sum()
            
            e1, e2 = st.columns(2)
            e1.metric("Revenue Lost to Weather", f"-${total_weather_loss:,.0f}", delta_color="inverse")
            e2.metric("Revenue Gained via Promos", f"+${total_promo_gain:,.0f}")
            
            # Comparison of Forces
            impact_data = pd.DataFrame({
                'Category': ['Weather Impact', 'Promo Impact'],
                'Dollar Value': [-total_weather_loss, total_promo_gain]
            })
            st.bar_chart(impact_data.set_index('Category'))

            # --- 5. EXPORT OPTIONS ---
            st.divider()
            csv = df_filtered.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download This Report as CSV",
                data=csv,
                file_name=f"HardRock_Report_{start_date}_to_{end_date}.csv",
                mime='text/csv',
                use_container_width=True
            )

        else:
            st.warning("No data found for the selected date range.")
    else:
        st.info("Database is empty. Please add data to generate reports.")

# --- TAB 4: ADMIN ENGINE ---
with tab4:
    st.header("Coefficient Control Center")
    
    st.subheader("🤖 AI Auto-Calibration")
    st.markdown("Click below to train a Multiple Linear Regression model on your historical database. This will automatically find the mathematically optimal weights for all variables based on your actual past performance.")
    
    if st.button("⚡ Run Machine Learning Auto-Tune", type="primary", use_container_width=True):
        if ledger_data and len(ledger_data) > 10:
            df_ml = pd.DataFrame(ledger_data)
            ml_cols = ['actual_traffic', 'day_of_week', 'temp_c', 'snow_cm', 'rain_mm', 'weather_alert', 'active_promo', 'ad_impressions', 'social_engagements', 'ad_clicks', 'actual_coin_in']
            
            if all(c in df_ml.columns for c in ml_cols):
                df_clean = df_ml.dropna(subset=ml_cols).copy()
                
                df_clean['weather_alert'] = df_clean['weather_alert'].astype(int)
                df_clean['active_promo'] = df_clean['active_promo'].astype(int)
                
                dow_dummies = pd.get_dummies(df_clean['day_of_week'], prefix='DOW')
                days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                for day in days:
                    if f'DOW_{day}' not in dow_dummies.columns:
                        dow_dummies[f'DOW_{day}'] = 0
                
                df_clean = pd.concat([df_clean, dow_dummies], axis=1)
                
                features = [f'DOW_{d}' for d in days] + ['temp_c', 'snow_cm', 'rain_mm', 'weather_alert', 'active_promo', 'ad_impressions', 'social_engagements', 'ad_clicks']
                X = df_clean[features]
                y = df_clean['actual_traffic']
                
                model = LinearRegression()
                model.fit(X, y)
                
                st.session_state.coeffs['Intercept'] = float(model.intercept_)
                st.session_state.coeffs['DOW_Mon'] = float(model.coef_[0])
                st.session_state.coeffs['DOW_Tue'] = float(model.coef_[1])
                st.session_state.coeffs['DOW_Wed'] = float(model.coef_[2])
                st.session_state.coeffs['DOW_Thu'] = float(model.coef_[3])
                st.session_state.coeffs['DOW_Fri'] = float(model.coef_[4])
                st.session_state.coeffs['DOW_Sat'] = float(model.coef_[5])
                st.session_state.coeffs['DOW_Sun'] = float(model.coef_[6])
                st.session_state.coeffs['Temp_C'] = float(model.coef_[7])
                st.session_state.coeffs['Snow_cm'] = float(model.coef_[8])
                st.session_state.coeffs['Rain_mm'] = float(model.coef_[9])
                st.session_state.coeffs['Alert'] = float(model.coef_[10])
                st.session_state.coeffs['Promo'] = float(model.coef_[11])
                st.session_state.coeffs['Impressions'] = float(model.coef_[12])
                st.session_state.coeffs['Engagements'] = float(model.coef_[13])
                st.session_state.coeffs['Clicks'] = float(model.coef_[14])
                
                total_traffic = df_clean['actual_traffic'].sum()
                total_coin = df_clean['actual_coin_in'].sum()
                if total_traffic > 0:
                    st.session_state.coeffs['Avg_Coin_In'] = float(total_coin / total_traffic)

                # Send a popup notification and FORCE the screen to refresh instantly
                st.toast("✅ AI Model retrained! Screen refreshing...")
                st.rerun() 
                
            else:
                st.error("Missing columns. Make sure the database migration was completed.")
        else:
            st.warning("You need at least 10 days of historical data to train the AI.")

    st.divider()

    with st.form("coeff_form"):
        st.subheader("Core Baselines (Manual Override)")
        c1, c2 = st.columns(2)
        c_coin = c1.number_input("Average Coin-In per Visitor ($)", value=float(st.session_state.coeffs['Avg_Coin_In']))
        c_int = c2.number_input("Base Traffic (Intercept)", value=float(st.session_state.coeffs['Intercept']))
        
        st.subheader("Day of Week Modifiers")
        row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
        c_mon = row1_col1.number_input("Monday", value=float(st.session_state.coeffs['DOW_Mon']))
        c_tue = row1_col2.number_input("Tuesday", value=float(st.session_state.coeffs['DOW_Tue']))
        c_wed = row1_col3.number_input("Wednesday", value=float(st.session_state.coeffs['DOW_Wed']))
        c_thu = row1_col4.number_input("Thursday", value=float(st.session_state.coeffs['DOW_Thu']))
        
        row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)
        c_fri = row2_col1.number_input("Friday", value=float(st.session_state.coeffs['DOW_Fri']))
        c_sat = row2_col2.number_input("Saturday", value=float(st.session_state.coeffs['DOW_Sat']))
        c_sun = row2_col3.number_input("Sunday", value=float(st.session_state.coeffs['DOW_Sun']))
        with row2_col4:
            st.empty() 
            
        st.subheader("Weather Modifiers")
        w1, w2, w3, w4 = st.columns(4)
        c_temp = w1.number_input("Temp (per °C)", value=float(st.session_state.coeffs['Temp_C']), format="%.4f")
        c_snow = w2.number_input("Snow (per cm)", value=float(st.session_state.coeffs['Snow_cm']), format="%.4f")
        c_rain = w3.number_input("Rain (per mm)", value=float(st.session_state.coeffs['Rain_mm']), format="%.4f")
        c_alert = w4.number_input("Weather Alert Penalty", value=float(st.session_state.coeffs['Alert']), format="%.4f")

        st.subheader("Digital Marketing Lifts")
        d1, d2, d3, d4 = st.columns(4)
        c_promo = d1.number_input("Active Promo Lift", value=float(st.session_state.coeffs['Promo']), format="%.4f")
        # Expanded Impressions to 8 decimal places so tiny AI weights show up
        c_imp = d2.number_input("Impressions (per 1)", value=float(st.session_state.coeffs['Impressions']), format="%.8f", step=0.000001)
        c_eng = d3.number_input("Engagements (per 1)", value=float(st.session_state.coeffs['Engagements']), format="%.4f", step=0.01)
        c_clicks = d4.number_input("Clicks (per 1)", value=float(st.session_state.coeffs['Clicks']), format="%.4f", step=0.01)
        
        submit = st.form_submit_button("Lock In & Update Engine Parameters")
        if submit:
            st.session_state.coeffs['Avg_Coin_In'] = c_coin
            st.session_state.coeffs['Intercept'] = c_int
            st.session_state.coeffs['DOW_Mon'] = c_mon
            st.session_state.coeffs['DOW_Tue'] = c_tue
            st.session_state.coeffs['DOW_Wed'] = c_wed
            st.session_state.coeffs['DOW_Thu'] = c_thu
            st.session_state.coeffs['DOW_Fri'] = c_fri
            st.session_state.coeffs['DOW_Sat'] = c_sat
            st.session_state.coeffs['DOW_Sun'] = c_sun
            st.session_state.coeffs['Temp_C'] = c_temp
            st.session_state.coeffs['Snow_cm'] = c_snow
            st.session_state.coeffs['Rain_mm'] = c_rain
            st.session_state.coeffs['Alert'] = c_alert
            st.session_state.coeffs['Promo'] = c_promo
            st.session_state.coeffs['Impressions'] = c_imp
            st.session_state.coeffs['Engagements'] = c_eng
            st.session_state.coeffs['Clicks'] = c_clicks
            
            st.success("Parameters saved! The predictive model is fully updated.")

# --- TAB 5: ASK AI ---
with tab5:
    st.header("💬 Ask the Data Analyst")
    st.markdown("Ask natural language questions about your property's historical performance, weather impacts, or digital marketing ROI.")
    
    # Simple Reset button for the chat
    if st.button("Clear Chat History", key="clear_chat"):
        st.session_state.messages = []
        st.rerun()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat Input
    if prompt := st.chat_input("e.g., 'What was our best day for traffic last month?'"):
        
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            if "GEMINI_API_KEY" not in st.secrets:
                st.error("API Key missing! Please add GEMINI_API_KEY to your Streamlit secrets.")
            elif not ledger_data:
                st.warning("Your database is empty. Add data first so I have something to analyze!")
            else:
                with st.spinner("Analyzing database..."):
                    try:
                        # Prepare data context
                        df_ai = pd.DataFrame(ledger_data)
                        data_context = df_ai.to_csv(index=False)
                        
                        system_prompt = f"""
                        You are an expert Data Analyst for a Casino/Hotel property. 
                        I am providing you with our raw historical daily database below in CSV format. 
                        Please answer the user's question accurately based ONLY on this data. 
                        Keep your answers concise, professional, and highlight key metrics.
                        
                        DATABASE:
                        {data_context}
                        
                        USER QUESTION:
                        {prompt}
                        """
                        
                        # We use the full model path which we confirmed works during the dropdown test
                        model = genai.GenerativeModel('models/gemini-2.5-flash')
                        
                        response = model.generate_content(system_prompt)
                        
                        st.markdown(response.text)
                        st.session_state.messages.append({"role": "assistant", "content": response.text})
                    except Exception as e:
                        st.error(f"Error communicating with AI: {e}")
