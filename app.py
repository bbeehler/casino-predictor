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

# --- 1. INITIAL DATA HYDRATION (Run at Startup) ---
if 'coeffs' not in st.session_state:
    try:
        # Pull the master record (ID 1) from Supabase
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        
        if response.data:
            # Load saved weights into session state
            st.session_state.coeffs = response.data[0]
        else:
            # Fallback if the table is empty
            st.session_state.coeffs = {
                "id": 1, "Intercept": 1000, "Temp_C": 0, "Snow_cm": 0, 
                "Rain_mm": 0, "Promo": 0, "Clicks": 0, 
                "Impressions": 0, "Avg_Coin_In": 1200
            }
    except Exception as e:
        st.error(f"Failed to load Engine Weights: {e}")

# --- TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🏢 Executive Strategy Command</h2>
            <p style="color: #888; margin: 0;">Real-time Property Performance vs. AI Baselines (YTD 2026).</p>
        </div>
    """, unsafe_allow_html=True)
    
    if ledger_data:
        df_exec = pd.DataFrame(ledger_data).copy()
        df_exec['entry_date'] = pd.to_datetime(df_exec['entry_date'])
        
        # 1. PREP DATA & CALCULATE ACCURACY
        c = st.session_state.coeffs
        current_year = 2026 
        df_ytd = df_exec[df_exec['entry_date'].dt.year == current_year].copy()
        
        # Numeric Safety
        for col in ['actual_traffic', 'actual_coin_in', 'ad_clicks', 'ad_impressions']:
            df_ytd[col] = pd.to_numeric(df_ytd[col], errors='coerce').fillna(0)

        # Baseline & Accuracy Math
        def get_pred(row):
            dow_key = f"DOW_{pd.to_datetime(row['entry_date']).strftime('%a')}"
            return c['Intercept'] + c.get(dow_key, 0) + (row.get('temp_c', 0) * c['Temp_C']) + (c['Promo'] if row.get('active_promo', False) else 0)

        df_ytd['ai_baseline'] = df_ytd.apply(get_pred, axis=1)
        
        # Accuracy Calculation (Available for all cards now)
        df_calc = df_ytd[df_ytd['actual_traffic'] > 0].copy()
        accuracy_pct = 0.0
        if not df_calc.empty:
            df_calc['error'] = abs(df_calc['actual_traffic'] - df_calc['ai_baseline']) / df_calc['actual_traffic']
            accuracy_pct = max(0, (1 - df_calc['error'].mean()) * 100)

        # 2. DIGITAL LIFT MATH
        df_ytd['digital_lift'] = df_ytd.apply(lambda row: 
            (c['Promo'] if row.get('active_promo', False) else 0) + 
            (row.get('ad_clicks', 0) * c['Clicks']) + 
            (row.get('ad_impressions', 0) * c['Impressions']), axis=1)

        total_lift = df_ytd['digital_lift'].sum()
        total_traffic = df_ytd['actual_traffic'].sum()
        lift_percentage = (total_lift / total_traffic * 100) if total_traffic > 0 else 0
        digital_rev_impact = total_lift * c['Avg_Coin_In']

        # 3. EXECUTIVE KPI BENTO BOX
        col1, col2, col3 = st.columns(3)
        
        with col1:
            with st.container(border=True):
                st.markdown("💰 **YTD Total Revenue**")
                st.metric("Total Coin-In and Table Drop", f"${df_ytd['actual_coin_in'].sum():,.0f}")
                st.caption(f"Jan 1 - {df_ytd['entry_date'].max().strftime('%b %d')}")
        
        with col2:
            with st.container(border=True):
                st.markdown("🚶 **YTD Floor Traffic**")
                st.metric("Total Visitors", f"{total_traffic:,.0f}")
                st.caption(f"Actual Property Attendance")

        with col3:
            with st.container(border=True):
                st.markdown("🚀 **YTD Digital Lift Contribution**")
                st.metric("Lift Visitors", f"{total_lift:,.0f}", f"{lift_percentage:.1f}% of Total")
                
                # Attributed Revenue Impact Section
                st.markdown(f"""
                    <div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #333;">
                        <p style="color: #888; font-size: 0.8rem; margin: 0;">Attributed Revenue Impact</p>
                        <h3 style="color: #FFCC00; margin: 0;">${digital_rev_impact:,.0f}</h3>
                    </div>
                """, unsafe_allow_html=True)

        st.markdown("---")
        
        # 4. SUMMARY ROW
        sum_col1, sum_col2 = st.columns(2)
        with sum_col1:
            with st.container(border=True):
                st.markdown("#### 🤖 FloorCast Executive Summary")
                days_tracked = len(df_ytd) if len(df_ytd) > 0 else 1
                st.write(f"Digital strategy contributes an average lift of **{int(total_lift / days_tracked)}** visitors daily.")
                st.write(f"At an average spend of **${c['Avg_Coin_In']}**, marketing is a key revenue driver.")
        
        with sum_col2:
            with st.container(border=True):
                st.markdown("#### 🎯 Model Performance")
                st.write(f"Current Model Accuracy: **{accuracy_pct:.1f}%**")
                st.info("The AI Baseline factors in weather, calendar cycles, and your digital weights.")
    else:
        st.info("No ledger data detected. Please upload data in the Admin tab.")

# --- TAB 2: DAILY TRACKER & FORECAST ---
with tab2:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🕹️ FloorPace Control Panel</h2>
            <p style="color: #888; margin: 0;">Log Daily Actuals, Simulate Forecasts, and Audit Historical Data.</p>
        </div>
    """, unsafe_allow_html=True)
    
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
                    clks = st.number_input("Ad Clicks", 0, 5000, 200)

                if st.form_submit_button("💾 Save to FloorPace Ledger", use_container_width=True):
                    c = st.session_state.coeffs
                    dow_key = f"DOW_{date_in.strftime('%a')}"
                    
                    # Math for Prediction
                    base_v = float(c['Intercept'] + c.get(dow_key, 0))
                    weather_v = float((temp * c['Temp_C']) + (snow * c['Snow_cm']))
                    dig_lift_v = float((promo * c['Promo']) + (imp * c['Impressions']) + (clks * c['Clicks']))
                    final_pred = float(base_v + weather_v + dig_lift_v)
                    
                    # CLEAN PAYLOAD: Removing 'variance' to match SQL Schema
                    data = {
                        "entry_date": str(date_in),
                        "actual_traffic": int(act_traf),
                        "actual_coin_in": float(act_coin),
                        "predicted_traffic": int(final_pred),
                        "temp_c": float(temp),
                        "snow_cm": float(snow),
                        "active_promo": bool(promo),
                        "ad_impressions": int(imp),
                        "ad_clicks": int(clks)
                    }
                    
                    try:
                        supabase.table("ledger").upsert(data, on_conflict="entry_date").execute()
                        st.toast("✅ Record saved successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Database Sync Error: {e}")

    with col_sandbox:
        with st.container(border=True):
            st.subheader("🔮 2. Forecast Sandbox")
            st.write("Simulate future dates with full environmental and digital variables.")
            
            # 1. DATE & PRIMARY ENVIRONMENT
            f_range = st.date_input("Forecast Range", [datetime.date.today(), datetime.date.today() + datetime.timedelta(days=7)])
            
            s1, s2, s3 = st.columns(3)
            sim_temp = s1.slider("Sim. Temp (°C)", -30, 40, 15)
            sim_rain = s2.slider("Sim. Rain (mm)", 0, 50, 0)
            sim_snow = s3.slider("Sim. Snow (cm)", 0, 50, 0)
            
            # 2. ALERTS & PROMOS
            a1, a2 = st.columns(2)
            sim_promo = a1.checkbox("Apply Promotion?")
            sim_alert = a2.checkbox("Simulate Weather Alert?")
            
            # 3. DIGITAL CAMPAIGN SIMULATOR
            st.markdown("**Digital Campaign Simulation**")
            sd1, sd2 = st.columns(2)
            sim_imp = sd1.number_input("Est. Impressions", value=300000, step=10000)
            sim_clk = sd2.number_input("Est. Ad Clicks", value=500, step=50)
            
            if len(f_range) == 2:
                dates = pd.date_range(f_range[0], f_range[1])
                c = st.session_state.coeffs
                f_list = []
                
                for d in dates:
                    dk = f"DOW_{d.strftime('%a')}"
                    
                    # ENHANCED MULTIVARIATE MATH
                    # We assume a -15% 'Alert Penalty' if a Weather Alert is simulated
                    alert_penalty = 0.85 if sim_alert else 1.0
                    
                    p_traffic = (
                        c['Intercept'] + 
                        c.get(dk, 0) + 
                        (sim_temp * c.get('Temp_C', 0)) + 
                        (sim_snow * c.get('Snow_cm', 0)) +
                        (sim_rain * c.get('Rain_mm', -2.5)) + # Fallback penalty for rain
                        (c.get('Promo', 0) if sim_promo else 0) +
                        (sim_imp * c.get('Impressions', 0)) + 
                        (sim_clk * c.get('Clicks', 0))
                    ) * alert_penalty
                    
                    p_revenue = p_traffic * c['Avg_Coin_In']
                    
                    f_list.append({
                        "Date": d.strftime("%a %d"), 
                        "Visitors": int(max(0, p_traffic)), 
                        "Revenue": float(max(0, p_revenue))
                    })
                
                df_f = pd.DataFrame(f_list)
                
                # 4. DUAL METRIC DISPLAY
                m_col1, m_col2 = st.columns(2)
                m_col1.metric("Est. Total Visitors", f"{df_f['Visitors'].sum():,.0f}")
                m_col2.metric("Est. Total Revenue", f"${df_f['Revenue'].sum():,.0f}")
                
                # 5. VISUAL TREND
                st.line_chart(df_f.set_index("Date")["Visitors"], color="#FFCC00")
                
                if sim_alert:
                    st.warning("⚠️ Projections reflect a 15% reduction due to Simulated Weather Alert.")

    # --- SECTION 3: AUDIT & FULL-FIELD EDIT ---
    st.markdown("---")
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
                                up_temp = st.number_input("Temp", value=float(record.get('temp_c', 0.0)))
                                up_snow = st.number_input("Snow", value=float(record.get('snow_cm', 0.0)))
                            with ec3:
                                up_promo = st.checkbox("Promo", value=bool(record.get('active_promo', False)))
                                up_imp = st.number_input("Impressions", value=int(record.get('ad_impressions', 0)))
                                up_clk = st.number_input("Clicks", value=int(record.get('ad_clicks', 0)))

                            if st.form_submit_button("💾 Save All Changes", use_container_width=True):
                                try:
                                    # STRICT TYPES: Ints for traffic/impressions, Floats for money/temp
                                    supabase.table("ledger").update({
                                        "actual_traffic": int(up_t), 
                                        "actual_coin_in": float(up_c), 
                                        "predicted_traffic": int(up_p),
                                        "temp_c": float(up_temp), 
                                        "snow_cm": float(up_snow), 
                                        "active_promo": bool(up_promo), 
                                        "ad_impressions": int(up_imp), 
                                        "ad_clicks": int(up_clk)
                                    }).eq("entry_date", search_str).execute()
                                    st.toast(f"Record for {search_str} updated!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Update Error: {e}")
                else:
                    st.info("No record found for this date.")

        st.markdown("**Full Historical Ledger**")
        display_df = df_edit.sort_values('entry_date', ascending=False)
        display_df['entry_date'] = display_df['entry_date'].dt.strftime('%Y-%m-%d')
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# --- TAB 3: STRATEGIC REPORTING & ROI ---
with tab3:
    # 1. HEADER WITH ACCENT (Fixed the parameter name here)
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Strategic Performance & Digital ROI</h2>
            <p style="color: #888; margin: 0;">Correlating Digital Matters Now metrics with Property Floor Reality.</p>
        </div>
    """, unsafe_allow_html=True)
    
    if ledger_data:
        df_rep = pd.DataFrame(ledger_data).copy()
        df_rep['entry_date'] = pd.to_datetime(df_rep['entry_date'])
        df_rep = df_rep.sort_values('entry_date', ascending=False)

        # 2. DATE FILTER
        with st.container(border=True):
            f1, f2, f3 = st.columns([1, 1, 1])
            start_rep = f1.date_input("📅 Report Start", df_rep['entry_date'].min().date())
            end_rep = f2.date_input("📅 Report End", df_rep['entry_date'].max().date())
            f3.write("##") 
            if f3.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()
        
        mask = (df_rep['entry_date'].dt.date >= start_rep) & (df_rep['entry_date'].dt.date <= end_rep)
        df_filtered = df_rep.loc[mask].copy()

        if not df_filtered.empty:
            st.write("##")
            
            # 3. DIGITAL PERFORMANCE BENTO
            st.markdown("#### 📱 Digital Impact Metrics")
            d1, d2, d3, d4 = st.columns(4)
            
            # Ensure numeric conversion for sums
            df_filtered['ad_impressions'] = pd.to_numeric(df_filtered['ad_impressions'], errors='coerce').fillna(0)
            df_filtered['ad_clicks'] = pd.to_numeric(df_filtered['ad_clicks'], errors='coerce').fillna(0)
            df_filtered['social_engagements'] = pd.to_numeric(df_filtered['social_engagements'], errors='coerce').fillna(0)
            df_filtered['actual_coin_in'] = pd.to_numeric(df_filtered['actual_coin_in'], errors='coerce').fillna(0)

            total_imps = df_filtered['ad_impressions'].sum()
            total_clks = df_filtered['ad_clicks'].sum()
            total_engs = df_filtered['social_engagements'].sum()
            total_rev = df_filtered['actual_coin_in'].sum()
            
            with d1:
                with st.container(border=True):
                    st.metric("Ad Impressions", f"{total_imps:,.0f}")
            with d2:
                with st.container(border=True):
                    st.metric("Ad Clicks", f"{total_clks:,.0f}")
            with d3:
                with st.container(border=True):
                    st.metric("Engagements", f"{total_engs:,.0f}")
            with d4:
                with st.container(border=True):
                    rpc = total_rev / total_clks if total_clks > 0 else 0
                    st.metric("Rev per Click", f"${rpc:.2f}")

            st.write("##")

            # 4. CHARTING & SUMMARY
            t_col, c_col = st.columns([2.5, 1])
            with t_col:
                with st.container(border=True):
                    st.markdown("#### 📈 Actual Traffic vs. AI Predicted Baseline")
                    c = st.session_state.coeffs
                    df_filtered['ai_baseline'] = df_filtered.apply(lambda row: 
                        c['Intercept'] + c.get(f"DOW_{row['entry_date'].strftime('%a')}", 0) + 
                        (row.get('temp_c', 0) * c['Temp_C']) + (c['Promo'] if row.get('active_promo', False) else 0), axis=1)
                    
                    chart_rep = df_filtered.sort_values('entry_date')
                    chart_rep = chart_rep.rename(columns={'actual_traffic': 'Floor Reality', 'ai_baseline': 'AI Baseline'})
                    st.area_chart(chart_rep.set_index('entry_date')[['Floor Reality', 'AI Baseline']], color=["#FFCC00", "#555555"])
            
            with c_col:
                with st.container(border=True):
                    st.markdown("#### 📝 Executive Summary")
                    total_var = df_filtered['actual_traffic'].sum() - df_filtered['ai_baseline'].sum()
                    perf_color = "#28a745" if total_var > 0 else "#dc3545"
                    st.markdown(f"""
                        <div style="text-align: center; padding: 10px;">
                            <h1 style="color: {perf_color}; margin: 0;">{total_var:+,.0f}</h1>
                            <p style="color: #888;">Net Traffic Variance</p>
                        </div>
                    """, unsafe_allow_html=True)
                    st.info(f"During this period, digital efforts influenced {total_clks:,.0f} clicks to the property.")

            st.write("##")
            st.download_button(
                label="📥 Export Hard Rock ROI Report (CSV)",
                data=df_filtered.to_csv(index=False),
                file_name=f"FloorPace_ROI_{start_rep}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.warning("No data found for the selected date range.")

import google.generativeai as genai

import google.generativeai as genai
import json

import google.generativeai as genai
import json
import google.generativeai as genai
import json
import pandas as pd

import google.generativeai as genai
import json
import pandas as pd

import google.generativeai as genai
import json
import pandas as pd

# --- TAB 4: ADMIN ENGINE & DATA MANAGEMENT ---
with tab4:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">⚙️ Engine Control & Data Management</h2>
            <p style="color: #888; margin: 0;">Accounting-Verified Baseline with AI Variance Calibration.</p>
        </div>
    """, unsafe_allow_html=True)

    # 1. THE "HARD MATH" AUTO-CALIBRATION
    if st.button("🤖 Auto-Calibrate Engine weights with AI", use_container_width=True):
        with st.spinner("Calculating Baseline & Revenue Reality..."):
            try:
                df_calc = pd.DataFrame(ledger_data).copy()
                
                # PURE ARITHMETIC PILLARS
                total_vis = df_calc['actual_traffic'].sum()
                num_days = len(df_calc)
                math_intercept = total_vis / num_days if num_days > 0 else 0
                
                total_rev = df_calc['actual_coin_in'].sum()
                math_avg_spend = total_rev / total_vis if total_vis > 0 else 0

                # AI VARIANCE MODELING (Concise Prompt for Speed)
                genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                model = genai.GenerativeModel('gemini-2.5-flash')
                
                prompt = f"""
                SYSTEM: Statistical Auditor. 
                CONSTANTS: Intercept={math_intercept:.2f}, Avg_Coin_In={math_avg_spend:.2f}.
                DATA: {df_calc.tail(100).to_csv(index=False)}
                TASK: Return raw JSON coefficients for: Promo, Clicks, Impressions, Temp_C, Snow_cm, Rain_mm.
                NO MARKDOWN.
                """
                
                response = model.generate_content(prompt)
                clean_json = response.text.replace("```json", "").replace("```", "").strip()
                suggestion = json.loads(clean_json)
                
                # Force Accounting Overrides into Session State
                suggestion['Intercept'] = math_intercept
                suggestion['Avg_Coin_In'] = math_avg_spend
                
                # Update session state directly
                st.session_state.coeffs.update(suggestion)
                st.success(f"🎯 Math Verified: Spend at ${math_avg_spend:,.2f}. Review values below.")
                # Removed st.rerun() here to prevent hanging
                
            except Exception as e:
                st.error(f"Calibration failed: {e}")

    st.write("##")
    # Pull coefficients from session state for the input fields
    c = st.session_state.coeffs

# 3. BENTO CONTROL CENTER (With Type-Safety Fix)
    c = st.session_state.coeffs
    
    # Helper function to prevent TypeErrors
    def safe_float(val):
        try:
            return float(pd.to_numeric(val, errors='coerce')) if val is not None else 0.0
        except:
            return 0.0

    col_fin, col_dig, col_env = st.columns(3)

    with col_fin:
        with st.container(border=True):
            st.markdown("💰 **Financial & Baseline**")
            new_intercept = st.number_input("Base Daily Traffic", value=safe_float(c.get('Intercept', 0)))
            new_avg_spend = st.number_input("Avg. Spend per Head ($)", value=safe_float(c.get('Avg_Coin_In', 0)))
            st.caption("Baseline floor performance.")

    with col_dig:
        with st.container(border=True):
            st.markdown("🚀 **Digital Marketing Weights**")
            new_promo = st.number_input("Promo Flat Lift", value=safe_float(c.get('Promo', 0)))
            new_clicks = st.number_input("Weight / Ad Click", value=safe_float(c.get('Clicks', 0)))
            new_imps = st.number_input("Weight / 1k Imps", value=safe_float(c.get('Impressions', 0)), format="%.4f")
            st.caption("Weights driving Marketing ROI.")

    with col_env:
        with st.container(border=True):
            st.markdown("☁️ **Environmental Impact**")
            new_temp = st.number_input("Temp Impact (°C)", value=safe_float(c.get('Temp_C', 0)))
            new_snow = st.number_input("Snow Impact (cm)", value=safe_float(c.get('Snow_cm', 0)))
            new_rain = st.number_input("Rain Impact (mm)", value=safe_float(c.get('Rain_mm', 0)))
            st.caption("Ottawa weather adjustments.")

    # 3. PERMANENT DATABASE SYNC
    st.write("##")
    if st.button("💾 Save All Engine Changes", use_container_width=True):
        try:
            updated_values = {
                "id": 1, "Intercept": new_intercept, "Temp_C": new_temp, "Snow_cm": new_snow, 
                "Rain_mm": new_rain, "Promo": new_promo, "Clicks": new_clicks, 
                "Impressions": new_imps, "Avg_Coin_In": new_avg_spend
            }
            # This is the line that makes it survive a refresh
            supabase.table("coefficients").upsert(updated_values).execute()
            st.session_state.coeffs.update(updated_values)
            st.success("✅ Changes Saved to Database.")
            st.rerun() # Rerun is only here to "lock in" the save
        except Exception as e:
            st.error(f"Save failed: {e}")

# --- TAB 5: ASK FLOORCAST ---
with tab5:
    # 1. BRANDED HEADER
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 10px;">
            <h2 style="color: #FFCC00; margin: 0;">🤖 Ask FloorCast</h2>
            <p style="color: #888; margin: 0;">Proprietary analyst for Hard Rock Ottawa performance data.</p>
        </div>
    """, unsafe_allow_html=True)

    # 2. THE CLEAR COMMANDS
    # This button sits right under the header
    if st.button("🧹 Clear Chat History", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

    # 3. API CONFIGURATION
    if "GEMINI_API_KEY" not in st.secrets:
        st.error("🛑 GEMINI_API_KEY missing from Secrets.")
        st.stop()
    
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
    
    # Safety Override for Casino/Financial data
    safety_settings = [
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    ]

    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        safety_settings=safety_settings,
        generation_config={"temperature": 0.2, "max_output_tokens": 2048}
    )

    # 4. CHAT STATE MANAGEMENT
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display History
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # 5. DATA CONTEXT (Last 30 Days)
    if ledger_data:
        df_context = pd.DataFrame(ledger_data).tail(30)
        csv_context = df_context.to_csv(index=False)
    else:
        csv_context = "No data available."

    # 6. CHAT INPUT
    if prompt := st.chat_input("Query FloorCast..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Consulting FloorPace Ledger..."):
                try:
                    full_query = f"""
                    You are FloorCast, Lead Data Analyst for Hard Rock Hotel & Casino Ottawa.
                    
                    LEDGER DATA:
                    {csv_context}
                    
                    USER SETTINGS:
                    {st.session_state.coeffs}
                    
                    QUESTION: {prompt}
                    
                    INSTRUCTIONS:
                    - Reference specific numbers from the ledger.
                    - Provide an executive summary.
                    - Do not truncate the response.
                    """
                    
                    response = model.generate_content(full_query)
                    
                    if response.text:
                        st.markdown(response.text)
                        st.session_state.messages.append({"role": "assistant", "content": response.text})
                    else:
                        st.warning("Analysis complete, but no text response was generated.")
                
                except Exception as e:
                    st.error(f"Analysis failed: {str(e)}")
