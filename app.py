import streamlit as st
import pandas as pd
import datetime

st.set_page_config(page_title="Casino Traffic Predictor", layout="wide")
st.title("🎰 Property Traffic & Digital Lift Engine")

# --- DATABASE / STATE INITIALIZATION ---
if 'coeffs' not in st.session_state:
    st.session_state.coeffs = {
        'Intercept': 4606.16, 'DOW_Mon': -1837.23, 'DOW_Tue': -1810.69, 'DOW_Wed': -7.65,
        'DOW_Thu': -410.40, 'DOW_Fri': 1032.13, 'DOW_Sat': 2912.14, 'DOW_Sun': 121.70,
        'Temp_C': 0.82, 'Snow_cm': -53.11, 'Rain_mm': -9.55, 'Alert': -49.37,
        'Promo': 99.74, 'Impressions': 0.000881, 'Engagements': 0.0943, 'Clicks': 0.244,
        'Avg_Coin_In': 1335.00 # Historical V2 Average
    }

if 'ledger' not in st.session_state:
    st.session_state.ledger = []

# --- APP NAVIGATION ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 Executive Dashboard", "📝 Daily Tracker", "📈 Reporting & ROI", "⚙️ Admin Engine"])

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.header("YTD Property Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric(label="Total Foot Traffic YTD", value="452,193", delta="12% vs Last Year")
    col2.metric(label="Digital Contribution", value="43,053 Visitors", delta="+ 4.2%")
    col3.metric(label="Weather Penalty", value="-109,872 Visitors", delta="Severe Winter", delta_color="inverse")
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
        
        if st.button("💾 Save Daily Entry to Ledger", use_container_width=True):
            entry = {
                "Date": entry_date.strftime("%Y-%m-%d"),
                "DOW": dow_name,
                "Actual_Traffic": actual_traffic,
                "Predicted_Traffic": int(total_pred),
                "Variance": int(variance),
                "Digital_Lift_Visitors": int(digital_lift),
                "Digital_Revenue_Impact": estimated_digital_rev,
                "Actual_CoinIn": actual_coinin
            }
            st.session_state.ledger.append(entry)
            st.toast("Entry saved successfully!")

    st.divider()
    st.subheader("Recent Ledger Entries")
    if len(st.session_state.ledger) > 0:
        df_ledger = pd.DataFrame(st.session_state.ledger)
        st.dataframe(df_ledger, use_container_width=True)
    else:
        st.info("No daily entries yet. Save an entry above to start building the ledger.")

# --- TAB 3: REPORTING & ROI ---
with tab3:
    st.header("Historical Reporting & Revenue Implications")
    
    if len(st.session_state.ledger) > 0:
        df_ledger = pd.DataFrame(st.session_state.ledger)
        
        total_actual = df_ledger['Actual_Traffic'].sum()
        total_pred = df_ledger['Predicted_Traffic'].sum()
        total_digital_rev = df_ledger['Digital_Revenue_Impact'].sum()
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Period Actual Traffic", f"{total_actual:,}")
        c2.metric("Period Predicted Traffic", f"{total_pred:,}")
        c3.metric("Total Digital Revenue ROI", f"${total_digital_rev:,.2f}")
        
        st.subheader("Traffic Trends: Actual vs Model")
        chart_data = df_ledger[['Date', 'Actual_Traffic', 'Predicted_Traffic']].set_index('Date')
        st.line_chart(chart_data)
    else:
        st.warning("Go to the Daily Tracker tab and save some entries to generate reports!")

# --- TAB 4: ADMIN ENGINE ---
with tab4:
    st.header("Coefficient Control Center")
    with st.form("coeff_form"):
        st.subheader("Financial Metrics")
        c_coin = st.number_input("Average Coin-In per Visitor ($)", value=st.session_state.coeffs['Avg_Coin_In'])
        
        st.subheader("Day of Week Baselines")
        c1, c2, c3, c4 = st.columns(4)
        c_mon = c1.number_input("Monday", value=st.session_state.coeffs['DOW_Mon'])
        c_fri = c2.number_input("Friday", value=st.session_state.coeffs['DOW_Fri'])
        c_sat = c3.number_input("Saturday", value=st.session_state.coeffs['DOW_Sat'])
        c_sun = c4.number_input("Sunday", value=st.session_state.coeffs['DOW_Sun'])
        
        submit = st.form_submit_button("Update Engine Parameters")
        if submit:
            st.session_state.coeffs['Avg_Coin_In'] = c_coin
            st.session_state.coeffs['DOW_Mon'] = c_mon
            st.session_state.coeffs['DOW_Fri'] = c_fri
            st.session_state.coeffs['DOW_Sat'] = c_sat
            st.session_state.coeffs['DOW_Sun'] = c_sun
            st.success("Parameters updated!")
