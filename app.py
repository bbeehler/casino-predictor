import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client

st.set_page_config(page_title="Casino Traffic Predictor", layout="wide")
st.title("🎰 Property Traffic & Digital Lift Engine")

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
tab1, tab2, tab3, tab4 = st.tabs(["📊 Executive Dashboard", "📝 Daily Tracker", "📈 Reporting & ROI", "⚙️ Admin Engine"])

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.header("YTD Property Overview")
    
    col1, col2, col3 = st.columns(3)
    col1.metric(label="Total Foot Traffic", value="452,193", delta="+5.2% vs Last Month")
    col2.metric(label="Digital Contribution", value="43,053 Visitors", delta="+4.2% vs Last Month")
    col3.metric(label="Weather Penalty", value="-109,872 Visitors", delta="-12% vs Last Month", delta_color="inverse")
    
    st.divider()
    
    st.subheader("📱 Digital Marketing Performance")
    soc1, soc2, soc3 = st.columns(3)
    soc1.metric(label="Total Ad Impressions", value="14.2M", delta="+8.1% vs Last Month")
    soc2.metric(label="Total Engagements", value="458,200", delta="+3.4% vs Last Month")
    soc3.metric(label="Total Ad Clicks", value="112,450", delta="+5.5% vs Last Month")
    
    st.divider()
    
    st.subheader("Estimated Coin-In YTD: $57.7M")
    st.progress(0.75)

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
        
        # Pull columns for the display and edit logic
        display_cols = ['entry_date', 'day_of_week', 'actual_traffic', 'predicted_traffic', 'variance', 'actual_coin_in', 'digital_lift_visitors', 'digital_revenue_impact']
        # Only pull columns that actually exist in the dataframe to prevent errors if migration isn't run yet
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
                            
                            edit_col1, edit_col2 = st.columns(2)
                            
                            with edit_col1:
                                st.markdown("**Actuals**")
                                new_traffic = st.number_input("Actual Traffic", value=int(existing_data.get('actual_traffic', 0)), step=100)
                                new_coin_in = st.number_input("Actual Coin-In ($)", value=float(existing_data.get('actual_coin_in', 0.0)), step=1000.0)
                            
                            with edit_col2:
                                st.markdown("**Model Estimates**")
                                new_pred = st.number_input("Predicted Traffic", value=int(existing_data.get('predicted_traffic', 0)), step=100)
                                new_dig_lift = st.number_input("Digital Lift (Visitors)", value=int(existing_data.get('digital_lift_visitors', 0)), step=50)
                                new_dig_rev = st.number_input("Digital Revenue ($)", value=float(existing_data.get('digital_revenue_impact', 0.0)), step=500.0)
                            
                            submit_update = st.form_submit_button("Save All Changes to Database")
                            
                            if submit_update:
                                new_variance = int(new_traffic) - int(new_pred)
                                
                                supabase.table("ledger").update({
                                    "actual_traffic": int(new_traffic),
                                    "predicted_traffic": int(new_pred),
                                    "actual_coin_in": float(new_coin_in),
                                    "digital_lift_visitors": int(new_dig_lift),
                                    "digital_revenue_impact": float(new_dig_rev),
                                    "variance": int(new_variance)
                                }).eq("entry_date", search_date_str).execute()
                                
                                st.success("All fields updated successfully in the database!")
                                st.cache_data.clear()
                                st.rerun()

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
    with st.form("coeff_form"):
        st.subheader("Financial Metrics")
        c_coin = st.number_input("Average Coin-In per Visitor ($)", value=st.session_state.coeffs['Avg_Coin_In'])
        
        st.subheader("Day of Week Baselines")
        row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
        c_mon = row1_col1.number_input("Monday", value=st.session_state.coeffs['DOW_Mon'])
        c_tue = row1_col2.number_input("Tuesday", value=st.session_state.coeffs['DOW_Tue'])
        c_wed = row1_col3.number_input("Wednesday", value=st.session_state.coeffs['DOW_Wed'])
        c_thu = row1_col4.number_input("Thursday", value=st.session_state.coeffs['DOW_Thu'])
        
        row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)
        c_fri = row2_col1.number_input("Friday", value=st.session_state.coeffs['DOW_Fri'])
        c_sat = row2_col2.number_input("Saturday", value=st.session_state.coeffs['DOW_Sat'])
        c_sun = row2_col3.number_input("Sunday", value=st.session_state.coeffs['DOW_Sun'])
        with row2_col4:
            st.empty() 
            
        st.subheader("Weather Penalties")
        w1, w2 = st.columns(2)
        c_snow = w1.number_input("Snow (per cm)", value=st.session_state.coeffs['Snow_cm'])
        c_alert = w2.number_input("Weather Alert", value=st.session_state.coeffs['Alert'])
        
        submit = st.form_submit_button("Update Engine Parameters")
        if submit:
            st.session_state.coeffs['Avg_Coin_In'] = c_coin
            st.session_state.coeffs['DOW_Mon'] = c_mon
            st.session_state.coeffs['DOW_Tue'] = c_tue
            st.session_state.coeffs['DOW_Wed'] = c_wed
            st.session_state.coeffs['DOW_Thu'] = c_thu
            st.session_state.coeffs['DOW_Fri'] = c_fri
            st.session_state.coeffs['DOW_Sat'] = c_sat
            st.session_state.coeffs['DOW_Sun'] = c_sun
            st.session_state.coeffs['Snow_cm'] = c_snow
            st.session_state.coeffs['Alert'] = c_alert
            st.success("Parameters updated! The model has been recalibrated.")
