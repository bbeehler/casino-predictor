import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
from sklearn.linear_model import LinearRegression
import google.generativeai as genai # NEW: Google AI Library

# --- MODERN UI STYLING ---
st.markdown("""
    <style>
    /* Main background and font */
    .stApp {
        background-color: #f8f9fa;
        font-family: 'Inter', sans-serif;
    }
    /* Card-style containers */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
        color: #1a1a1a;
    }
    div[data-testid="metric-container"] {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 15px 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.02);
    }
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: #ffffff;
        padding: 10px;
        border-radius: 15px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        white-space: pre-wrap;
        background-color: #f1f3f5;
        border-radius: 8px;
        color: #495057;
        gap: 0px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #000000 !important;
        color: #ffffff !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- CONFIGURE AI ---
if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

st.set_page_config(page_title="Casino Traffic Predictor", layout="wide")
st.title("🎰 Property Traffic Engine - VERSION 2")

# --- DATABASE CONNECTION ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase: Client = init_connection()

# --- INITIALIZE COEFFICIENTS ---
if 'coeffs' not in st.session_state:
    st.session_state.coeffs = {
        'Intercept': 4606.16, 
        'DOW_Mon': -1837.23, 'DOW_Tue': -1810.69, 'DOW_Wed': -7.65,
        'DOW_Thu': -410.40, 'DOW_Fri': 1032.13, 'DOW_Sat': 2912.14, 'DOW_Sun': 121.70,
        'Temp_C': 0.82, 'Snow_cm': -53.11, 'Rain_mm': -9.55, 'Alert': -49.37,
        'Promo': 99.74, 'Impressions': 0.000881, 'Engagements': 0.0943, 'Clicks': 0.244,
        'Avg_Coin_In': 1335.00
    }

# Fetch Ledger Data from Supabase
@st.cache_data(ttl=60)
def load_ledger():
    response = supabase.table("ledger").select("*").order("entry_date", desc=True).execute()
    return response.data

ledger_data = load_ledger()

# --- APP NAVIGATION ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Executive Dashboard", "📝 Daily Tracker", "📈 Reporting & ROI", "⚙️ Admin Engine", "💬 Ask AI"])

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.header("Executive Summary & AI Briefing")
    
    if ledger_data:
        df_dash = pd.DataFrame(ledger_data)
        # Ensure we only analyze days where actual data exists
        df_dash = df_dash[df_dash['actual_traffic'] > 0].copy()
        
        if not df_dash.empty:
            df_dash['entry_date'] = pd.to_datetime(df_dash['entry_date'])
            c = st.session_state.coeffs
            
            # --- 1. AI STRATEGIC BRIEFING ---
            with st.container(border=True):
                st.subheader("🤖 AI Strategic Analyst")
                if st.button("✨ Generate Executive Briefing"):
                    with st.spinner("Gemini is synthesizing historical data and forecasting..."):
                        try:
                            # Contextual data prep
                            recent_30 = df_dash.sort_values('entry_date', ascending=False).head(30).to_csv(index=False)
                            
                            # Future 7-day outlook context
                            f_dates = [datetime.date.today() + datetime.timedelta(days=x) for x in range(1, 8)]
                            f_outlook = ""
                            for d in f_dates:
                                d_key = f"DOW_{d.strftime('%a')}"
                                base = c['Intercept'] + c.get(d_key, 0)
                                f_outlook += f"{d.strftime('%a %d')}: Est. {int(base)} visitors; "

                            model = genai.GenerativeModel('models/gemini-2.5-flash')
                            prompt = f"""
                            You are the Senior Strategy Lead for Hard Rock Hotel & Casino Ottawa. 
                            Write a highly professional, data-driven Executive Summary for the General Manager.
                            
                            DATA SNAPSHOT (Last 30 Days):
                            {recent_30}
                            
                            PREDICTIVE 7-DAY OUTLOOK:
                            {f_outlook}
                            
                            STRUCTURE:
                            1. HIGHLIGHTS: Mention top performing days and any surprising Digital ROI.
                            2. RISK/OPPORTUNITY: Analyze the upcoming week's forecast.
                            3. ACTION: One specific recommendation for the marketing or floor team.
                            
                            Keep it under 250 words. Be sharp, executive-focused, and prioritize revenue impact.
                            """
                            
                            response = model.generate_content(prompt)
                            st.markdown(response.text)
                        except Exception as e:
                            st.error(f"AI Briefing Error: {e}")

            st.divider()

            # --- 2. CORE PERFORMANCE METRICS ---
            st.subheader("💰 Financial & Traffic Performance")
            
            # Math for MoM (Month over Month)
            latest_date = df_dash['entry_date'].max()
            curr_m, curr_y = latest_date.month, latest_date.year
            prev_m = curr_m - 1 if curr_m > 1 else 12
            prev_y = curr_y if curr_m > 1 else curr_y - 1
            
            df_curr = df_dash[(df_dash['entry_date'].dt.month == curr_m) & (df_dash['entry_date'].dt.year == curr_y)]
            df_prev = df_dash[(df_dash['entry_date'].dt.month == prev_m) & (df_dash['entry_date'].dt.year == prev_y)]
            
            def get_mom_metrics(df):
                if df.empty: return 0, 0, 0
                return df['actual_coin_in'].sum(), df['actual_traffic'].sum(), df['digital_revenue_impact'].sum()

            c_rev, c_traf, c_dig = get_mom_metrics(df_curr)
            p_rev, p_traf, p_dig = get_mom_metrics(df_prev)

            def format_delta(curr, prev):
                if prev == 0: return "N/A"
                pct = ((curr - prev) / prev) * 100
                return f"{pct:+.1f}% vs Last Month"

            m1, m2, m3 = st.columns(3)
            m1.metric("Current Month Revenue", f"${c_rev:,.0f}", delta=format_delta(c_rev, p_rev))
            m2.metric("Current Month Traffic", f"{int(c_traf):,}", delta=format_delta(c_traf, p_traf))
            m3.metric("Digital Rev Impact", f"${c_dig:,.0f}", delta=format_delta(c_dig, p_dig))

            st.divider()

            # --- 3. AI PRECISION & TRENDS ---
            st.subheader("🎯 Model Integrity & Trends")
            
            # AI Accuracy (MAPE)
            df_dash['abs_error'] = abs(df_dash['actual_traffic'] - df_dash['predicted_traffic'])
            mape = (df_dash['abs_error'] / df_dash['actual_traffic']).mean()
            accuracy_score = (1 - mape) * 100
            
            # Digital Lift (Visitors)
            total_lift = df_dash['digital_lift_visitors'].sum()
            
            t1, t2 = st.columns(2)
            t1.metric("Overall Prediction Accuracy", f"{accuracy_score:.1f}%", help="Higher is better. Based on mean absolute percentage error.")
            t2.metric("Total Digital Visitor Lift", f"{int(total_lift):,}")

            # Cumulative Revenue Area Chart
            st.write("**Revenue Progression vs. Forecast Range**")
            df_dash['Cumulative_Revenue'] = df_dash['actual_coin_in'].cumsum()
            st.area_chart(df_dash.set_index('entry_date')['Cumulative_Revenue'])

        else:
            st.warning("Insufficient actual data to generate dashboard metrics.")
    else:
        st.info("No data found in database. Dashboard will populate once daily entries are logged.")

# --- TAB 2: DAILY TRACKER & FORECAST ---
with tab2:
    st.header("Daily Performance & Predictive Sandbox")
    
    # --- SECTION A: TOP ROW (ENTRY + FORECAST) ---
    col_entry, col_forecast = st.columns([1, 1.2])
    
    with col_entry:
        st.subheader("📝 1. Log Daily Actuals")
        with st.form("entry_form"):
            entry_date = st.date_input("Date", datetime.date.today())
            actual_traffic = st.number_input("Actual Foot Traffic", min_value=0, step=100)
            actual_coinin = st.number_input("Actual Coin-In ($)", min_value=0, step=1000)
            
            st.markdown("**Contextual Variables**")
            w1, w2 = st.columns(2)
            temp = w1.slider("Temp (°C)", -30, 40, 15)
            snow = w2.slider("Snow (cm)", 0, 50, 0)
            
            d1, d2 = st.columns(2)
            promo = d1.checkbox("Active Promotion")
            alert = d2.checkbox("Weather Alert")
            
            with st.expander("Digital Input Details"):
                rain = st.number_input("Rain (mm)", 0, 50, 0)
                impressions = st.number_input("Ad Impressions", 0, 1000000, 300000)
                engagements = st.number_input("Social Engagements", 0, 10000, 500)
                clicks = st.number_input("Ad Clicks", 0, 5000, 200)

            if st.form_submit_button("💾 Save Daily Entry"):
                c = st.session_state.coeffs
                dow_key = f"DOW_{entry_date.strftime('%a')}"
                
                # Math
                base = c['Intercept'] + c.get(dow_key, 0)
                weather = (temp * c['Temp_C']) + (snow * c['Snow_cm']) + (rain * c['Rain_mm']) + (alert * c['Alert'])
                dig_lift = (promo * c['Promo']) + (impressions * c['Impressions']) + (engagements * c['Engagements']) + (clicks * c['Clicks'])
                total_pred = base + weather + dig_lift
                
                entry = {
                    "entry_date": entry_date.strftime("%Y-%m-%d"),
                    "day_of_week": entry_date.strftime("%A"),
                    "actual_traffic": actual_traffic,
                    "predicted_traffic": int(total_pred),
                    "variance": int(actual_traffic - total_pred),
                    "digital_lift_visitors": int(dig_lift),
                    "digital_revenue_impact": float(dig_lift * c['Avg_Coin_In']),
                    "actual_coin_in": float(actual_coinin),
                    "temp_c": int(temp), "snow_cm": float(snow), "rain_mm": float(rain),
                    "weather_alert": alert, "active_promo": promo,
                    "ad_impressions": int(impressions), "social_engagements": int(engagements), "ad_clicks": int(clicks)
                }
                supabase.table("ledger").upsert(entry, on_conflict="entry_date").execute()
                st.toast("✅ Database Updated!")
                st.rerun()

    with col_forecast:
        st.subheader("🔮 2. Strategic Forecast Sandbox")
        st.markdown("Select a future range and simulate conditions.")
        
        f_col1, f_col2 = st.columns(2)
        f_start = f_col1.date_input("Forecast Start", datetime.date.today() + datetime.timedelta(days=1))
        f_end = f_col2.date_input("Forecast End", datetime.date.today() + datetime.timedelta(days=7))
        
        st.write("---")
        s1, s2, s3 = st.columns(3)
        sim_temp = s1.slider("Simulated Temp (°C)", -30, 40, 15)
        sim_snow = s2.number_input("Simulated Snow (cm)", 0.0)
        sim_promo = s3.checkbox("Apply Promo to Range?")
        
        if f_start <= f_end:
            forecast_range = pd.date_range(start=f_start, end=f_end)
            c = st.session_state.coeffs
            f_data = []
            
            for d in forecast_range:
                dow_key = f"DOW_{d.strftime('%a')}"
                pred = c['Intercept'] + c.get(dow_key, 0) + (sim_temp * c['Temp_C']) + (sim_snow * c['Snow_cm'])
                if sim_promo: pred += c['Promo']
                f_data.append({"Date": d.strftime('%a, %b %d'), "Visitors": int(pred), "Revenue": pred * c['Avg_Coin_In']})
            
            df_f = pd.DataFrame(f_data)
            
            # Summary Metrics
            m_col1, m_col2 = st.columns(2)
            m_col1.metric("Projected Visitors", f"{df_f['Visitors'].sum():,}")
            m_col2.metric("Projected Revenue", f"${df_f['Revenue'].sum():,.0f}")
            
            st.line_chart(df_f.set_index("Date")["Visitors"])
        else:
            st.error("Start date must be before end date.")

# --- SECTION B: LEDGER SEARCH & EDIT ---
    st.divider()
    st.subheader("🔍 3. Search & Edit Historical Ledger")
    
    if ledger_data:
        df_ledger = pd.DataFrame(ledger_data)
        
        # Search controls
        search_col, status_col = st.columns([1, 2])
        with search_col:
            enable_search = st.toggle("Filter & Edit Specific Date")
            if enable_search:
                search_date = st.date_input("Select Date to Edit", value=pd.to_datetime(df_ledger['entry_date']).max().date())
                search_date_str = search_date.strftime("%Y-%m-%d")
        
        if enable_search:
            found = df_ledger[df_ledger['entry_date'] == search_date_str]
            
            with status_col:
                if not found.empty:
                    existing = found.iloc[0]
                    st.success(f"Found record for {search_date_str}. Use the editor below.")
                    
                    # THE SELF-CONTAINED EDIT FORM
                    with st.expander(f"✏️ Editing Record: {search_date_str}", expanded=True):
                        with st.form(key=f"edit_form_{search_date_str}"):
                            e1, e2, e3 = st.columns(3)
                            
                            with e1:
                                st.markdown("**Core Metrics**")
                                up_traffic = st.number_input("Actual Traffic", value=int(existing.get('actual_traffic', 0)))
                                up_coin = st.number_input("Actual Coin-In ($)", value=float(existing.get('actual_coin_in', 0.0)))
                                up_pred = st.number_input("Predicted Traffic", value=int(existing.get('predicted_traffic', 0)))
                            
                            with e2:
                                st.markdown("**Weather**")
                                up_temp = st.number_input("Temp (°C)", value=int(existing.get('temp_c', 0)))
                                up_snow = st.number_input("Snow (cm)", value=float(existing.get('snow_cm', 0.0)))
                                up_alert = st.checkbox("Weather Alert", value=bool(existing.get('weather_alert', False)))
                            
                            with e3:
                                st.markdown("**Digital**")
                                up_promo = st.checkbox("Active Promo", value=bool(existing.get('active_promo', False)))
                                up_imp = st.number_input("Impressions", value=int(existing.get('ad_impressions', 0)))
                                up_eng = st.number_input("Engagements", value=int(existing.get('social_engagements', 0)))
                                up_clks = st.number_input("Clicks", value=int(existing.get('ad_clicks', 0)))

                            # THE MISSING SUBMIT BUTTON
                            if st.form_submit_button("💾 Save Changes to Database"):
                                # Recalculate variance for accuracy
                                new_var = int(up_traffic) - int(up_pred)
                                
                                supabase.table("ledger").update({
                                    "actual_traffic": int(up_traffic),
                                    "predicted_traffic": int(up_pred),
                                    "actual_coin_in": float(up_coin),
                                    "variance": int(new_var),
                                    "temp_c": int(up_temp),
                                    "snow_cm": float(up_snow),
                                    "weather_alert": up_alert,
                                    "active_promo": up_promo,
                                    "ad_impressions": int(up_imp),
                                    "social_engagements": int(up_eng),
                                    "ad_clicks": int(up_clks)
                                }).eq("entry_date", search_date_str).execute()
                                
                                st.toast("✅ Database record updated!")
                                st.rerun()
                else:
                    st.warning(f"No entry found for {search_date_str}.")

        # Display the main table
        st.dataframe(df_ledger.sort_values('entry_date', ascending=False), use_container_width=True, hide_index=True)

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
