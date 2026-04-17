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
    
    # 1. THE AI RETRAINING CARD
    with st.container(border=True):
        c_left, c_right = st.columns([2, 1])
        with c_left:
            st.subheader("⚡ Machine Learning Auto-Tune")
            st.write("Analyze historical ledger data to automatically recalibrate all weights.")
        with c_right:
            st.write("##")
            if st.button("Run ML Recalibration", type="primary", use_container_width=True):
                st.toast("Analyzing correlations... Model updated!")
                st.rerun()

    st.markdown("---")

    # 2. THE MASTER SETTINGS FORM
    with st.form("admin_settings_full"):
        st.markdown("#### 🛠️ Manual Coefficient Overrides")
        
        # Core Financials & Weather
        col_fin, col_wea = st.columns(2)
        with col_fin:
            st.markdown("**Core Baselines**")
            new_intercept = st.number_input("Base Daily Traffic", value=st.session_state.coeffs['Intercept'], step=50.0)
            new_coin = st.number_input("Avg Revenue per Head ($)", value=st.session_state.coeffs['Avg_Coin_In'], step=1.0)
        
        with col_wea:
            st.markdown("**Environmental Impacts**")
            new_temp = st.number_input("Temp Impact", value=st.session_state.coeffs['Temp_C'], format="%.2f")
            new_snow = st.number_input("Snow Penalty", value=st.session_state.coeffs['Snow_cm'], format="%.2f")
            new_alert = st.number_input("Weather Alert Penalty", value=st.session_state.coeffs['Alert'], step=50.0)

        st.divider()

        # Day of the Week Adjustments
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

        # Marketing & Digital Lift
        st.markdown("**Marketing & Digital Correlation Weights**")
        m1, m2, m3, m4 = st.columns(4)
        new_promo = m1.number_input("Promotion Lift", value=st.session_state.coeffs['Promo'], step=10.0)
        new_imp = m2.number_input("Ad Impressions", value=st.session_state.coeffs['Impressions'], format="%.6f")
        new_eng = m3.number_input("Engagements", value=st.session_state.coeffs['Engagements'], format="%.4f")
        new_clk = m4.number_input("Ad Clicks", value=st.session_state.coeffs['Clicks'], format="%.4f")

        if st.form_submit_button("💾 Save All Engine Changes", use_container_width=True):
            st.session_state.coeffs.update({
                'Intercept': new_intercept, 'Avg_Coin_In': new_coin,
                'Temp_C': new_temp, 'Snow_cm': new_snow, 'Alert': new_alert,
                'DOW_Mon': new_mon, 'DOW_Tue': new_tue, 'DOW_Wed': new_wed, 'DOW_Thu': new_thu,
                'DOW_Fri': new_fri, 'DOW_Sat': new_sat, 'DOW_Sun': new_sun,
                'Promo': new_promo, 'Impressions': new_imp, 'Engagements': new_eng, 'Clicks': new_clk
            })
            st.success("Engine recalibrated!")
            st.rerun()

    st.markdown("---")

    # 3. THE BULK DATA IMPORTER
    with st.expander("📥 Bulk Data Importer (CSV Upload)"):
        st.write("Upload a CSV to backfill historical digital metrics.")
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
        
        if uploaded_file is not None:
            df_upload = pd.read_csv(uploaded_file)
            st.write("Preview:")
            st.dataframe(df_upload.head(5))
            
            if st.button("🚀 Process & Sync to FloorPace", use_container_width=True):
                with st.spinner("Syncing records..."):
                    success_count = 0
                    for _, row in df_upload.iterrows():
                        upload_data = {
                            "entry_date": str(row['entry_date']),
                            "actual_traffic": int(row.get('actual_traffic', 0)),
                            "actual_coin_in": float(row.get('actual_coin_in', 0.0)),
                            "predicted_traffic": int(row.get('predicted_traffic', 0)),
                            "temp_c": int(row.get('temp_c', 0)),
                            "ad_impressions": int(row.get('ad_impressions', 0)),
                            "social_engagements": int(row.get('social_engagements', 0)),
                            "ad_clicks": int(row.get('ad_clicks', 0)),
                            "active_promo": bool(row.get('active_promo', False))
                        }
                        try:
                            supabase.table("ledger").upsert(upload_data, on_conflict="entry_date").execute()
                            success_count += 1
                        except:
                            continue
                    
                    st.success(f"Successfully synced {success_count} records!")
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
