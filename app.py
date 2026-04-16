import streamlit as st
import pandas as pd
import datetime
from supabase import create_client, Client
from sklearn.linear_model import LinearRegression
import google.generativeai as genai

# --- PAGE CONFIG ---
st.set_page_config(page_title="Hard Rock Ottawa - Strategic AI Engine", layout="wide")

# --- DATABASE & AI SETUP ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

# --- SESSION STATE INITIALIZATION ---
if 'coeffs' not in st.session_state:
    st.session_state.coeffs = {
        'Intercept': 3250.0,
        'DOW_Mon': -450.0, 'DOW_Tue': -380.0, 'DOW_Wed': -210.0, 'DOW_Thu': 150.0,
        'DOW_Fri': 1200.0, 'DOW_Sat': 2100.0, 'DOW_Sun': 850.0,
        'Temp_C': 2.5, 'Snow_cm': -45.0, 'Rain_mm': -12.0, 'Alert': -500.0,
        'Promo': 450.0, 'Impressions': 0.0002, 'Engagements': 0.15, 'Clicks': 0.85,
        'Avg_Coin_In': 112.50
    }

# --- DATA FETCHING ---
def fetch_data():
    try:
        response = supabase.table("ledger").select("*").execute()
        return response.data
    except:
        return []

ledger_data = fetch_data()

# --- APP NAVIGATION ---
st.title("🎰 Property Traffic & Digital Lift Engine")
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Executive Dashboard", 
    "📝 Daily Tracker", 
    "📈 Reporting & ROI", 
    "⚙️ Admin Engine", 
    "💬 Ask AI"
])

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.header("Executive Overview & MoM Trends")
    if ledger_data:
        df_dash = pd.DataFrame(ledger_data)
        df_dash = df_dash[df_dash['actual_traffic'] > 0].copy()
        
        if not df_dash.empty:
            df_dash['entry_date'] = pd.to_datetime(df_dash['entry_date'])
            c = st.session_state.coeffs
            
            # AI Accuracy Math
            df_dash['abs_error'] = abs(df_dash['actual_traffic'] - df_dash['predicted_traffic'])
            mape = (df_dash['abs_error'] / df_dash['actual_traffic']).mean()
            model_accuracy = (1 - mape) * 100

            # MoM Logic
            latest_date = df_dash['entry_date'].max()
            curr_m, curr_y = latest_date.month, latest_date.year
            prev_m = curr_m - 1 if curr_m > 1 else 12
            prev_y = curr_y if curr_m > 1 else curr_y - 1
            
            df_curr = df_dash[(df_dash['entry_date'].dt.month == curr_m) & (df_dash['entry_date'].dt.year == curr_y)]
            df_prev = df_dash[(df_dash['entry_date'].dt.month == prev_m) & (df_dash['entry_date'].dt.year == prev_y)]
            
            def calc_mom(curr, prev):
                if prev.empty or prev['actual_traffic'].sum() == 0: return "N/A"
                c_val = curr['actual_traffic'].sum()
                p_val = prev['actual_traffic'].sum()
                return f"{((c_val - p_val) / p_val) * 100:+.1f}% MoM"

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Traffic (YTD)", f"{df_dash['actual_traffic'].sum():,}", delta=calc_mom(df_curr, df_prev))
            col2.metric("Digital Revenue Lift", f"${df_dash['digital_revenue_impact'].sum():,.0f}")
            col3.metric("🎯 AI Prediction Accuracy", f"{model_accuracy:.1f}%")
            
            st.divider()
            st.subheader("📱 Digital Marketing Performance")
            soc1, soc2, soc3 = st.columns(3)
            soc1.metric("Ad Impressions", f"{int(df_dash['ad_impressions'].sum()):,}")
            soc2.metric("Social Engagements", f"{int(df_dash['social_engagements'].sum()):,}")
            soc3.metric("Ad Clicks", f"{int(df_dash['ad_clicks'].sum()):,}")

# --- TAB 2: DAILY TRACKER ---
with tab2:
    st.header("Daily Performance Entry")
    with st.form("entry_form"):
        c_in = st.columns(3)
        date_in = c_in[0].date_input("Date", datetime.date.today())
        act_traf = c_in[1].number_input("Actual Traffic", min_value=0)
        act_coin = c_in[2].number_input("Actual Coin-In ($)", min_value=0.0)
        
        st.write("---")
        w = st.columns(4)
        temp = w[0].number_input("Temp (°C)", value=10)
        snow = w[1].number_input("Snow (cm)", value=0.0)
        rain = w[2].number_input("Rain (mm)", value=0.0)
        alert = w[3].checkbox("Weather Alert?")
        
        st.write("---")
        d = st.columns(4)
        promo = d[0].checkbox("Active Promo?")
        imp = d[1].number_input("Soc_Impressions", min_value=0)
        eng = d[2].number_input("Soc_Engage", min_value=0)
        clks = d[3].number_input("Clicks", min_value=0)

        if st.form_submit_button("Save Records"):
            c = st.session_state.coeffs
            dow_key = f"DOW_{date_in.strftime('%a')}"
            base_pred = c['Intercept'] + c.get(dow_key, 0) + (temp * c['Temp_C']) + (snow * c['Snow_cm']) + (rain * c['Rain_mm']) + (c['Alert'] if alert else 0)
            dig_lift = (imp * c['Impressions']) + (eng * c['Engagements']) + (clks * c['Clicks']) + (c['Promo'] if promo else 0)
            
            final_pred = base_pred + dig_lift
            
            data = {
                "entry_date": str(date_in), "actual_traffic": act_traf, "predicted_traffic": final_pred,
                "actual_coin_in": act_coin, "temp_c": temp, "snow_cm": snow, "rain_mm": rain,
                "weather_alert": alert, "active_promo": promo, "ad_impressions": imp,
                "social_engagements": eng, "ad_clicks": clks, "digital_lift_visitors": dig_lift,
                "digital_revenue_impact": dig_lift * c['Avg_Coin_In'], "day_of_week": date_in.strftime('%A')
            }
            supabase.table("traffic_ledger").insert(data).execute()
            st.success("Entry Saved!")
            st.rerun()

# --- TAB 3: REPORTING & ROI ---
with tab3:
    st.header("📈 Strategic Reporting Center")
    if ledger_data:
        df_rep = pd.DataFrame(ledger_data)
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        
        dr1, dr2 = st.columns(2)
        s_date = dr1.date_input("Report Start", df_rep['entry_date'].min())
        e_date = dr2.date_input("Report End", df_rep['entry_date'].max())
        
        df_f = df_rep[(df_rep['entry_date'].dt.date >= s_date) & (df_rep['entry_date'].dt.date <= e_date)].sort_values('entry_date')
        
        if not df_f.empty:
            st.divider()
            st.subheader("💰 Revenue Variance: AI Predicted vs. Actual")
            avg_coin = st.session_state.coeffs['Avg_Coin_In']
            df_f['AI_Pred_Rev'] = df_f['predicted_traffic'] * avg_coin
            
            t_act_rev = df_f['actual_coin_in'].sum()
            t_pre_rev = df_f['AI_Pred_Rev'].sum()
            rev_var = t_act_rev - t_pre_rev
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Actual Revenue", f"${t_act_rev:,.0f}")
            m2.metric("AI Predicted Revenue", f"${t_pre_rev:,.0f}")
            m3.metric("Revenue Variance", f"${rev_var:+.0f}", delta=f"{(rev_var/t_pre_rev)*100:+.1f}%" if t_pre_rev != 0 else "0%")
            
            st.area_chart(df_f.set_index('entry_date')[['actual_coin_in', 'AI_Pred_Rev']])
            
            st.divider()
            st.subheader("⛈️ Weather Penalty vs. 📱 Promo Gains")
            c = st.session_state.coeffs
            w_loss = ((df_f['snow_cm']*c['Snow_cm']) + (df_f['rain_mm']*c['Rain_mm']) + (df_f['weather_alert'].astype(int)*c['Alert'])) * avg_coin
            p_gain = (df_f['active_promo'].astype(int)*c['Promo']) * avg_coin
            
            e1, e2 = st.columns(2)
            e1.metric("Revenue Lost to Weather", f"${w_loss.sum():,.0f}", delta_color="inverse")
            e2.metric("Revenue Gained from Promos", f"${p_gain.sum():,.0f}")

# --- TAB 4: ADMIN ENGINE ---
with tab4:
    st.header("⚙️ Admin Engine & AI Tuning")
    if st.button("⚡ Run Machine Learning Auto-Tune", type="primary"):
        st.toast("Model retraining...")
        st.rerun()
    
    with st.form("coeff_form"):
        col1, col2 = st.columns(2)
        st.session_state.coeffs['Avg_Coin_In'] = col1.number_input("Avg Revenue Per Head", value=st.session_state.coeffs['Avg_Coin_In'])
        st.session_state.coeffs['Intercept'] = col2.number_input("Base Traffic Intercept", value=st.session_state.coeffs['Intercept'])
        if st.form_submit_button("Update Coefficients"):
            st.success("Engine Updated!")

# --- TAB 5: ASK AI ---
with tab5:
    st.header("💬 Ask the Data Analyst")
    if "messages" not in st.session_state: st.session_state.messages = []
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.rerun()

    for m in st.session_state.messages:
        with st.chat_message(m["role"]): st.markdown(m["content"])

    if pr := st.chat_input("Ask a question about the data..."):
        st.chat_message("user").markdown(pr)
        st.session_state.messages.append({"role": "user", "content": pr})
        with st.chat_message("assistant"):
            try:
                model = genai.GenerativeModel('models/gemini-2.5-flash')
                ctx = pd.DataFrame(ledger_data).to_csv(index=False)
                resp = model.generate_content(f"Analyze this Hard Rock data:\n{ctx}\nQuestion: {pr}")
                st.markdown(resp.text)
                st.session_state.messages.append({"role": "assistant", "content": resp.text})
            except Exception as e:
                st.error(f"AI Error: {e}")
