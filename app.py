import streamlit as st
import pandas as pd

# 1. Page Configuration
st.set_page_config(page_title="Casino Traffic Predictor", layout="wide")
st.title("🎰 Property Traffic & Digital Lift Engine")

# 2. Initialize the "Database" (Session State)
# In V2, we will connect this to an actual database. For now, it holds state in the browser.
if 'coeffs' not in st.session_state:
    st.session_state.coeffs = {
        'Intercept': 4606.16,
        'DOW_Mon': -1837.23, 'DOW_Tue': -1810.69, 'DOW_Wed': -7.65,
        'DOW_Thu': -410.40, 'DOW_Fri': 1032.13, 'DOW_Sat': 2912.14, 'DOW_Sun': 121.70,
        'Temp_C': 0.82, 'Snow_cm': -53.11, 'Rain_mm': -9.55, 'Alert': -49.37,
        'Promo': 99.74, 'Impressions': 0.000881, 'Engagements': 0.0943, 'Clicks': 0.244
    }

# 3. App Navigation
tab1, tab2, tab3 = st.tabs(["📊 Executive Dashboard", "🔮 Daily Predictor", "⚙️ Admin Engine"])

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.header("YTD Property Overview")
    st.markdown("*(Sample V2 Data)*")
    
    col1, col2, col3 = st.columns(3)
    col1.metric(label="Total Foot Traffic YTD", value="452,193", delta="12% vs Last Year")
    col2.metric(label="Digital Contribution", value="43,053 Visitors", delta="+ 4.2%")
    col3.metric(label="Weather Penalty", value="-109,872 Visitors", delta="Severe Winter", delta_color="inverse")
    
    st.divider()
    st.subheader("Estimated Coin-In YTD: $57.7M")
    st.progress(0.75)

# --- TAB 2: DAILY PREDICTOR ---
with tab2:
    st.header("Forecast Simulator")
    
    col_input, col_output = st.columns([1, 1])
    
    with col_input:
        st.subheader("Daily Variables")
        dow = st.selectbox("Day of Week", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
        
        st.markdown("**Weather**")
        temp = st.slider("Temperature (°C)", -30, 40, 15)
        snow = st.slider("Snow (cm)", 0, 50, 0)
        rain = st.slider("Rain (mm)", 0, 50, 0)
        alert = st.checkbox("Severe Weather Alert")
        
        st.markdown("**Marketing**")
        promo = st.checkbox("Active On-Site Promotion")
        impressions = st.number_input("Ad Impressions", 0, 1000000, 300000, step=10000)
        engagements = st.number_input("Social Engagements", 0, 10000, 500, step=100)
        clicks = st.number_input("Ad Clicks", 0, 5000, 200, step=50)

    with col_output:
        # Fetch current coefficients
        c = st.session_state.coeffs
        
        # Calculate Math
        dow_key = f"DOW_{dow[:3]}"
        baseline = c['Intercept'] + c[dow_key]
        weather_impact = (temp * c['Temp_C']) + (snow * c['Snow_cm']) + (rain * c['Rain_mm']) + (alert * c['Alert'])
        digital_lift = (promo * c['Promo']) + (impressions * c['Impressions']) + (engagements * c['Engagements']) + (clicks * c['Clicks'])
        
        total_pred = baseline + weather_impact + digital_lift
        
        st.subheader("Projected Traffic")
        st.metric("Total Expected Visitors", f"{int(total_pred):,}")
        
        st.markdown("### Traffic Breakdown")
        st.metric("1. Baseline (Property + DOW)", f"{int(baseline):,}")
        st.metric("2. Weather Impact", f"{int(weather_impact):,}")
        st.metric("3. Marketing & Digital Lift", f"+ {int(digital_lift):,}")

# --- TAB 3: ADMIN ENGINE ---
with tab3:
    st.header("Coefficient Control Center")
    st.markdown("Adjust the weights below. Changes will instantly update the Predictor Math.")
    
    with st.form("coeff_form"):
        st.subheader("Day of Week Baselines")
        c1, c2, c3, c4 = st.columns(4)
        c_mon = c1.number_input("Monday", value=st.session_state.coeffs['DOW_Mon'])
        c_fri = c2.number_input("Friday", value=st.session_state.coeffs['DOW_Fri'])
        c_sat = c3.number_input("Saturday", value=st.session_state.coeffs['DOW_Sat'])
        c_sun = c4.number_input("Sunday", value=st.session_state.coeffs['DOW_Sun'])
        
        st.subheader("Weather Penalties")
        w1, w2 = st.columns(2)
        c_snow = w1.number_input("Snow (per cm)", value=st.session_state.coeffs['Snow_cm'])
        c_alert = w2.number_input("Weather Alert", value=st.session_state.coeffs['Alert'])
        
        submit = st.form_submit_button("Update Engine Parameters")
        
        if submit:
            st.session_state.coeffs['DOW_Mon'] = c_mon
            st.session_state.coeffs['DOW_Fri'] = c_fri
            st.session_state.coeffs['DOW_Sat'] = c_sat
            st.session_state.coeffs['DOW_Sun'] = c_sun
            st.session_state.coeffs['Snow_cm'] = c_snow
            st.session_state.coeffs['Alert'] = c_alert
            st.success("Engine weights updated successfully! Check the Predictor tab.")
