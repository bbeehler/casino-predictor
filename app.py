import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
from sklearn.linear_model import LinearRegression
import google.generativeai as genai # NEW: Google AI Library

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
    st.header("Executive Overview & MoM Trends")
    
    if ledger_data:
        df_dash = pd.DataFrame(ledger_data)
        df_dash = df_dash[df_dash['actual_traffic'] > 0].copy()
        
        if not df_dash.empty:
            # --- 1. DATA PREP & ON-THE-FLY MATH ---
            df_dash['entry_date'] = pd.to_datetime(df_dash['entry_date'])
            
            # Calculate Weather Impact dynamically based on current AI coefficients
            c = st.session_state.coeffs
            df_dash['weather_impact_vis'] = (
                (df_dash.get('temp_c', 0) * c['Temp_C']) + 
                (df_dash.get('snow_cm', 0) * c['Snow_cm']) + 
                (df_dash.get('rain_mm', 0) * c['Rain_mm']) + 
                (df_dash.get('weather_alert', False).astype(int) * c['Alert'])
            )
            df_dash['weather_impact_rev'] = df_dash['weather_impact_vis'] * c['Avg_Coin_In']
            
            # Determine Current vs Previous Month
            latest_date = df_dash['entry_date'].max()
            curr_m, curr_y = latest_date.month, latest_date.year
            prev_m = curr_m - 1 if curr_m > 1 else 12
            prev_y = curr_y if curr_m > 1 else curr_y - 1
            
            df_curr = df_dash[(df_dash['entry_date'].dt.month == curr_m) & (df_dash['entry_date'].dt.year == curr_y)]
            df_prev = df_dash[(df_dash['entry_date'].dt.month == prev_m) & (df_dash['entry_date'].dt.year == prev_y)]
            
            # Helper function to extract totals
            def get_metrics(df):
                rev = df['actual_coin_in'].sum()
                traf = df['actual_traffic'].sum()
                rev_pp = rev / traf if traf > 0 else 0
                dig_vis = df['digital_lift_visitors'].sum()
                dig_rev = df['digital_revenue_impact'].sum()
                wea_vis = df['weather_impact_vis'].sum()
                wea_rev = df['weather_impact_rev'].sum()
                return rev, traf, rev_pp, dig_vis, dig_rev, wea_vis, wea_rev

            # YTD Totals
            y_rev, y_traf, y_rev_pp, y_dig_vis, y_dig_rev, y_wea_vis, y_wea_rev = get_metrics(df_dash)
            # Current Month Totals
            c_rev, c_traf, c_rev_pp, c_dig_vis, c_dig_rev, c_wea_vis, c_wea_rev = get_metrics(df_curr)
            # Previous Month Totals
            p_rev, p_traf, p_rev_pp, p_dig_vis, p_dig_rev, p_wea_vis, p_wea_rev = get_metrics(df_prev)
            
            # Helper function for safe percentage calculation
            def calc_mom(curr, prev):
                if prev == 0 and curr == 0: return "0.0% MoM"
                if prev == 0: return "N/A (No prior data)"
                pct = ((curr - prev) / abs(prev)) * 100
                return f"{pct:+.1f}% MoM"

            # --- 2. ROW 1: CORE FINANCIALS ---
            st.subheader("💰 Core Financials & Traffic (YTD)")
            r1c1, r1c2, r1c3 = st.columns(3)
            r1c1.metric("Total Revenue", f"${y_rev:,.0f}", delta=calc_mom(c_rev, p_rev))
            r1c2.metric("Foot Traffic", f"{y_traf:,.0f}", delta=calc_mom(c_traf, p_traf))
            r1c3.metric("Avg Rev per Person", f"${y_rev_pp:,.2f}", delta=calc_mom(c_rev_pp, p_rev_pp))
            
            st.divider()
            
            # --- 3. ROW 2: DIGITAL VS WEATHER IMPACT ---
            st.subheader("⚖️ Driving Forces: Digital vs. Weather (YTD)")
            r2c1, r2c2, r2c3, r2c4 = st.columns(4)
            r2c1.metric("Digital Lift (Visits)", f"{y_dig_vis:,.0f}", delta=calc_mom(c_dig_vis, p_dig_vis))
            r2c2.metric("Digital Contribution ($)", f"${y_dig_rev:,.0f}", delta=calc_mom(c_dig_rev, p_dig_rev))
            r2c3.metric("Weather Impact (Visits)", f"{y_wea_vis:,.0f}", delta=calc_mom(c_wea_vis, p_wea_vis), delta_color="inverse")
            r2c4.metric("Weather Impact ($)", f"${y_wea_rev:,.0f}", delta=calc_mom(c_wea_rev, p_wea_rev), delta_color="inverse")
            
            st.divider()

            # --- 4. ROW 3: AI ACCURACY & SOCIAL METRICS ---
            st.subheader("🤖 Engine Accuracy & Social Performance")
            
            # Calculate AI Accuracy
            df_dash['abs_error'] = abs(df_dash['actual_traffic'] - df_dash['predicted_traffic'])
            mape = (df_dash['abs_error'] / df_dash['actual_traffic']).mean()
            model_accuracy = (1 - mape) * 100
            
            # Safe Social Metric Calculations
            def get_soc(df, col): return int(df.get(col, pd.Series([0])).sum())
            y_imp, c_imp, p_imp = get_soc(df_dash, 'ad_impressions'), get_soc(df_curr, 'ad_impressions'), get_soc(df_prev, 'ad_impressions')
            y_eng, c_eng, p_eng = get_soc(df_dash, 'social_engagements'), get_soc(df_curr, 'social_engagements'), get_soc(df_prev, 'social_engagements')
            y_clk, c_clk, p_clk = get_soc(df_dash, 'ad_clicks'), get_soc(df_curr, 'ad_clicks'), get_soc(df_prev, 'ad_clicks')

            r3c1, r3c2, r3c3, r3c4 = st.columns(4)
            r3c1.metric("🎯 AI Prediction Accuracy", f"{model_accuracy:.1f}%", help="Based on Mean Absolute Percentage Error (MAPE)")
            r3c2.metric("Ad Impressions", f"{y_imp:,.0f}", delta=calc_mom(c_imp, p_imp))
            r3c3.metric("Social Engagements", f"{y_eng:,.0f}", delta=calc_mom(c_eng, p_eng))
            r3c4.metric("Ad Clicks", f"{y_clk:,.0f}", delta=calc_mom(c_clk, p_clk))

        else:
            st.info("No completed actuals found. Save some daily entries to generate YTD metrics.")
    else:
        st.info("Database is empty. Add data to see the Executive Dashboard.")
# --- TAB 2: DAILY TRACKER (DATA ENTRY) ---
with tab2:
    st.header("Daily Entry & Validation")
    col_input, col_output = st.columns([1, 1])
    
    with col_input:
        st.subheader("1. Log Actuals")
        entry_date = st.date_input("Date", datetime.date(2026, 4, 15))
        actual_traffic = st.number_input("Actual Foot Traffic", min_value=0, value=0, step=100)
        actual_coinin = st.number_input("Actual Coin-In ($)", min_value=0, value=0, step=1000)
        
        st.subheader("2. Validate Variables")
        temp = st.slider("Temperature (°C)", -30, 40, 15)
        snow = st.slider("Snow (cm)", 0, 50, 0)
        rain = st.slider("Rain (mm)", 0, 50, 0)
        alert = st.checkbox("Severe Weather Alert")
        promo = st.checkbox("Active Promotion")
        impressions = st.number_input("Ad Impressions", 0, 1000000, 300000, step=10000)
        engagements = st.number_input("Social Engagements", 0, 10000, 500, step=100)
        clicks = st.number_input("Ad Clicks", 0, 5000, 200, step=50)

    with col_output:
        c = st.session_state.coeffs
        dow_name = entry_date.strftime("%A")
        dow_key = f"DOW_{dow_name[:3]}"
        
        baseline = c['Intercept'] + c.get(dow_key, 0)
        weather_impact = (temp * c['Temp_C']) + (snow * c['Snow_cm']) + (rain * c['Rain_mm']) + (alert * c['Alert'])
        digital_lift = (promo * c['Promo']) + (impressions * c['Impressions']) + (engagements * c['Engagements']) + (clicks * c['Clicks'])
        
        total_pred = baseline + weather_impact + digital_lift
        variance = actual_traffic - total_pred if actual_traffic > 0 else 0
        
        st.subheader("Prediction vs Actual")
        m1, m2, m3 = st.columns(3)
        m1.metric("Model Predicted", f"{int(total_pred):,}")
        m2.metric("Actual Traffic", f"{int(actual_traffic):,}")
        m3.metric("Variance", f"{int(variance):,}", delta=int(variance))
        
        st.divider()
        st.markdown(f"**Digital Lift Calculation:** {int(digital_lift):,} Visitors")
        estimated_digital_rev = digital_lift * c['Avg_Coin_In']
        st.success(f"Estimated Revenue from Digital Marketing today: **${estimated_digital_rev:,.2f}**")
        
        if st.button("💾 Save Daily Entry to Database", use_container_width=True):
            entry = {
                "entry_date": entry_date.strftime("%Y-%m-%d"),
                "day_of_week": dow_name,
                "actual_traffic": actual_traffic,
                "predicted_traffic": int(total_pred),
                "variance": int(variance),
                "digital_lift_visitors": int(digital_lift),
                "digital_revenue_impact": float(estimated_digital_rev),
                "actual_coin_in": float(actual_coinin),
                "temp_c": int(temp),
                "snow_cm": float(snow),
                "rain_mm": float(rain),
                "weather_alert": alert,
                "active_promo": promo,
                "ad_impressions": int(impressions),
                "social_engagements": int(engagements),
                "ad_clicks": int(clicks)
            }
            supabase.table("ledger").insert(entry).execute()
            st.toast("✅ Saved securely to Database!")
            st.cache_data.clear()

    st.divider()
    st.subheader("🔍 Search & Edit Ledger")
    
    if ledger_data:
        df_ledger = pd.DataFrame(ledger_data)
        
        # --- FIX: Added ALL the new columns to the display list so they unhide in the UI ---
        display_cols = [
            'entry_date', 'day_of_week', 'actual_traffic', 'predicted_traffic', 'variance', 
            'actual_coin_in', 'digital_lift_visitors', 'digital_revenue_impact',
            'temp_c', 'snow_cm', 'rain_mm', 'weather_alert', 'active_promo', 
            'ad_impressions', 'social_engagements', 'ad_clicks'
        ]
        
        available_cols = [col for col in display_cols if col in df_ledger.columns]
        display_df = df_ledger[available_cols]
        
        search_col, result_col = st.columns([1, 2])
        
        with search_col:
            enable_search = st.toggle("Filter & Edit Specific Date")
            if enable_search:
                min_date = pd.to_datetime(df_ledger['entry_date']).min().date()
                max_date = pd.to_datetime(df_ledger['entry_date']).max().date()
                search_date = st.date_input("Select Date", min_value=min_date, max_value=max_date, value=max_date)
        
        if enable_search:
            search_date_str = search_date.strftime("%Y-%m-%d")
            found_record = display_df[display_df['entry_date'] == search_date_str]
            
            with result_col:
                if found_record.empty:
                    st.warning(f"No records found for {search_date_str}.")
                else:
                    st.success(f"Record found for {search_date_str}!")
                    
                    existing_data = found_record.iloc[0]
                    
                    with st.expander("✏️ Edit this Record", expanded=True):
                        with st.form("edit_form"):
                            safe_day_name = existing_data['day_of_week']
                            st.markdown(f"**Editing Date:** {search_date_str} ({safe_day_name})")
                            
                            # --- FIX: Expanded to 3 columns to fit all the new inputs ---
                            edit_col1, edit_col2, edit_col3 = st.columns(3)
                            
                            with edit_col1:
                                st.markdown("**Core Metrics**")
                                new_traffic = st.number_input("Actual Traffic", value=int(existing_data.get('actual_traffic', 0)), step=100)
                                new_coin_in = st.number_input("Actual Coin-In ($)", value=float(existing_data.get('actual_coin_in', 0.0)), step=1000.0)
                                new_pred = st.number_input("Predicted Traffic", value=int(existing_data.get('predicted_traffic', 0)), step=100)
                                new_dig_lift = st.number_input("Digital Lift (Visits)", value=int(existing_data.get('digital_lift_visitors', 0)), step=50)
                                new_dig_rev = st.number_input("Digital Rev ($)", value=float(existing_data.get('digital_revenue_impact', 0.0)), step=500.0)
                            
                            with edit_col2:
                                st.markdown("**Weather**")
                                new_temp = st.number_input("Temp (°C)", value=int(existing_data.get('temp_c', 0)))
                                new_snow = st.number_input("Snow (cm)", value=float(existing_data.get('snow_cm', 0.0)))
                                new_rain = st.number_input("Rain (mm)", value=float(existing_data.get('rain_mm', 0.0)))
                                new_alert = st.checkbox("Weather Alert", value=bool(existing_data.get('weather_alert', False)))

                            with edit_col3:
                                st.markdown("**Digital Inputs**")
                                new_promo = st.checkbox("Active Promo", value=bool(existing_data.get('active_promo', False)))
                                new_imp = st.number_input("Ad Impressions", value=int(existing_data.get('ad_impressions', 0)), step=10000)
                                new_eng = st.number_input("Social Engagements", value=int(existing_data.get('social_engagements', 0)), step=100)
                                new_clicks = st.number_input("Ad Clicks", value=int(existing_data.get('ad_clicks', 0)), step=50)

                            submit_update = st.form_submit_button("Save All Changes to Database")
                            
                            if submit_update:
                                new_variance = int(new_traffic) - int(new_pred)
                                
                                # --- FIX: Tell Supabase to overwrite ALL fields ---
                                supabase.table("ledger").update({
                                    "actual_traffic": int(new_traffic),
                                    "predicted_traffic": int(new_pred),
                                    "actual_coin_in": float(new_coin_in),
                                    "digital_lift_visitors": int(new_dig_lift),
                                    "digital_revenue_impact": float(new_dig_rev),
                                    "variance": int(new_variance),
                                    "temp_c": int(new_temp),
                                    "snow_cm": float(new_snow),
                                    "rain_mm": float(new_rain),
                                    "weather_alert": new_alert,
                                    "active_promo": new_promo,
                                    "ad_impressions": int(new_imp),
                                    "social_engagements": int(new_eng),
                                    "ad_clicks": int(new_clicks)
                                }).eq("entry_date", search_date_str).execute()
                                
                                st.success("All fields updated successfully in the database!")
                                st.cache_data.clear()
                                st.rerun()

        # Display the Table (We drop digital lift here just to save horizontal space, but all others will show!)
        if 'digital_lift_visitors' in display_df.columns:
            visual_df = display_df.drop(columns=['digital_lift_visitors'])
        else:
            visual_df = display_df
            
        st.dataframe(visual_df, use_container_width=True, hide_index=True)
        
    else:
        st.info("Your database is currently empty. Save an entry to start tracking!")
# --- TAB 3: REPORTING & ROI ---
with tab3:
    st.header("Historical Reporting & Revenue Implications")
    
    if ledger_data:
        df_ledger = pd.DataFrame(ledger_data)
        
        if 'actual_traffic' in df_ledger.columns:
            total_actual = df_ledger['actual_traffic'].sum()
            total_pred = df_ledger['predicted_traffic'].sum()
            total_digital_rev = df_ledger['digital_revenue_impact'].sum()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Period Actual Traffic", f"{total_actual:,}")
            c2.metric("Period Predicted Traffic", f"{total_pred:,}")
            c3.metric("Total Digital Revenue ROI", f"${total_digital_rev:,.2f}")
            
            st.subheader("Traffic Trends: Actual vs Model")
            chart_data = df_ledger[['entry_date', 'actual_traffic', 'predicted_traffic']].set_index('entry_date')
            st.line_chart(chart_data)
    else:
        st.warning("Save entries in the Daily Tracker to generate reports!")

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
    
    # Initialize chat history so the conversation stays on screen
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display previous chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # The Chat Input Box
    if prompt := st.chat_input("e.g., 'What was our best day for foot traffic last month?'"):
        
        # 1. Display user message in chat message container
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        # 2. Call the AI
        with st.chat_message("assistant"):
            if "GEMINI_API_KEY" not in st.secrets:
                st.error("API Key missing! Please add GEMINI_API_KEY to your Streamlit secrets.")
            elif not ledger_data:
                st.warning("Your database is empty. Add data first so I have something to analyze!")
            else:
                with st.spinner("Analyzing database..."):
                    try:
                        # Convert your entire database into a clean string format for the AI to read
                        df_ai = pd.DataFrame(ledger_data)
                        data_context = df_ai.to_csv(index=False)
                        
                        # Build the master prompt (Giving the AI a persona + the data + the question)
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
                        
                        # Initialize the model and generate the response
                        model = genai.GenerativeModel('gemini-pro')
                        response = model.generate_content(system_prompt)
                        
                        # Display the answer
                        st.markdown(response.text)
                        st.session_state.messages.append({"role": "assistant", "content": response.text})
                    except Exception as e:
                        st.error(f"An error occurred: {e}")
