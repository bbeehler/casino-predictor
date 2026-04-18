import streamlit as st
import pandas as pd
import google.generativeai as genai
import datetime
import json
from supabase import create_client

# --- 1. CONFIG & STYLING ---
st.set_page_config(page_title="Hard Rock Ottawa | FloorCast", layout="wide")

st.markdown("""
    <style>
    .main { background-color: #000000; }
    [data-testid="stMetricValue"] { color: #FFCC00 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 20px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #111; border-radius: 5px; color: white; padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] { border-bottom: 3px solid #FFCC00 !important; }
    </style>
""", unsafe_allow_html=True)

# --- 2. DATABASE & HYDRATION ---
url = st.secrets["SUPABASE_URL"]
key = st.secrets["SUPABASE_KEY"]
supabase = create_client(url, key)

# Ensure Coefficients are loaded into session
if 'coeffs' not in st.session_state:
    try:
        res = supabase.table("coefficients").select("*").eq("id", 1).execute()
        if res.data:
            st.session_state.coeffs = res.data[0]
        else:
            st.session_state.coeffs = {"Intercept": 1000, "Avg_Coin_In": 1200, "Clicks": 0.5, "Promo": 500, "Temp_C": -5, "Snow_cm": -20, "Rain_mm": -10}
    except:
        st.session_state.coeffs = {"Intercept": 1000, "Avg_Coin_In": 1200}

# Load Global Ledger Context
try:
    ledger_res = supabase.table("ledger").select("*").order("entry_date", ascending=True).execute()
    ledger_data = ledger_res.data
except:
    ledger_data = []

# --- 3. TABS NAVIGATION ---
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏛️ Executive", "📥 Input", "🚀 Strategy", "⚙️ Admin", "🔍 Analyst", "📊 Master Report"
])

# --- 4. TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🏛️ Executive Property Overview</h2>
            <p style="color: #888; margin: 0;">YTD Ledger Performance & Model Accuracy.</p>
        </div>
    """, unsafe_allow_html=True)

    c = st.session_state.coeffs
    avg_spend = c.get('Avg_Coin_In', 1200)
    
    df_exec = pd.DataFrame(ledger_data)
    if not df_exec.empty:
        # Safety Check
        for col in ['ad_clicks', 'active_promo', 'temp_c', 'snow_cm', 'rain_mm']:
            if col not in df_exec.columns: df_exec[col] = 0
        
        df_exec['entry_date'] = pd.to_datetime(df_exec['entry_date'])
        
        # Calculations
        total_traffic = df_exec['actual_traffic'].sum()
        total_revenue = df_exec['actual_coin_in'].sum()
        ytd_avg_actual = total_revenue / total_traffic if total_traffic > 0 else 0
        
        df_exec['lift'] = (df_exec['ad_clicks'] * c.get('Clicks', 0)) + (df_exec['active_promo'].astype(int) * c.get('Promo', 0))
        total_lift_rev = df_exec['lift'].sum() * avg_spend

        # Predictability Logic
        df_exec['expected'] = c.get('Intercept', 0) + df_exec['lift'] + (df_exec['temp_c'] * c.get('Temp_C', 0)) + (df_exec['snow_cm'] * c.get('Snow_cm', 0))
        df_exec['error'] = abs(df_exec['actual_traffic'] - df_exec['expected']) / df_exec['actual_traffic']
        accuracy = max(0, (1 - df_exec['error'].mean()) * 100)
        score_color = "#00FF00" if accuracy > 85 else "#FFCC00"

        # Bento Cards Row 1
        r1c1, r1c2 = st.columns(2)
        with r1c1:
            st.markdown(f'<div style="background-color:#1a1a1a;padding:30px;border-radius:15px;border-top:5px solid #FFCC00;text-align:center;"><p style="color:#888;font-size:12px;text-transform:uppercase;">Total YTD Revenue</p><h1 style="color:#FFF;margin:0;">${total_revenue:,.0f}</h1></div>', unsafe_allow_html=True)
        with r1c2:
            st.markdown(f'<div style="background-color:#1a1a1a;padding:30px;border-radius:15px;border-top:5px solid #FFCC00;text-align:center;"><p style="color:#888;font-size:12px;text-transform:uppercase;">Digital ROI YTD</p><h1 style="color:#FFF;margin:0;">${total_lift_rev:,.0f}</h1></div>', unsafe_allow_html=True)

        st.write("##")
        # Bento Cards Row 2
        r2c1, r2c2 = st.columns(2)
        with r2c1:
            st.markdown(f'<div style="background-color:#1a1a1a;padding:30px;border-radius:15px;border-left:10px solid #FFCC00;text-align:center;"><p style="color:#888;font-size:12px;text-transform:uppercase;">Actual YTD $/Head</p><h1 style="color:#FFF;margin:0;">${ytd_avg_actual:,.2f}</h1></div>', unsafe_allow_html=True)
        with r2c2:
            st.markdown(f'<div style="background-color:#1a1a1a;padding:30px;border-radius:15px;border-left:10px solid {score_color};text-align:center;"><p style="color:#888;font-size:12px;text-transform:uppercase;">AI Predictability Score</p><h1 style="color:{score_color};margin:0;">{accuracy:.1f}%</h1></div>', unsafe_allow_html=True)

# --- 5. TAB 4: ADMIN ENGINE (YTD CALIBRATION) ---
with tab4:
    st.markdown("### ⚙️ Engine Calibration")
    if st.button("🤖 Auto-Calibrate (YTD Accounting Mode)", use_container_width=True):
        df_calc = pd.DataFrame(ledger_data)
        if not df_calc.empty:
            math_avg_spend = df_calc['actual_coin_in'].sum() / df_calc['actual_traffic'].sum()
            st.session_state.coeffs['Avg_Coin_In'] = math_avg_spend
            st.success(f"Anchored Spend to YTD Reality: ${math_avg_spend:,.2f}")
            st.rerun()

    c_edit = st.session_state.coeffs
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        new_spend = st.number_input("Avg. Spend per Head ($)", value=float(c_edit.get('Avg_Coin_In', 1200)))
        new_clicks = st.number_input("Weight / Ad Click", value=float(c_edit.get('Clicks', 0.5)))
    with col_b:
        new_base = st.number_input("Base Daily Traffic", value=float(c_edit.get('Intercept', 1000)))
        new_promo = st.number_input("Promo Flat Lift", value=float(c_edit.get('Promo', 500)))
    with col_c:
        new_temp = st.number_input("Temp Impact (°C)", value=float(c_edit.get('Temp_C', 0)))
        new_snow = st.number_input("Snow Impact (cm)", value=float(c_edit.get('Snow_cm', 0)))

    if st.button("💾 Save All Engine Changes", use_container_width=True):
        upd = {"id": 1, "Avg_Coin_In": new_spend, "Clicks": new_clicks, "Intercept": new_base, "Promo": new_promo, "Temp_C": new_temp, "Snow_cm": new_snow}
        supabase.table("coefficients").upsert(upd).execute()
        st.session_state.coeffs.update(upd)
        st.success("Database Updated Successfully.")
        st.rerun()

# --- 6. TAB 6: MASTER FORENSIC REPORT ---
with tab6:
    st.markdown("### 📊 Master Forensic Report")
    df_rep = pd.DataFrame(ledger_data).copy()
    if not df_rep.empty:
        df_rep['digital_lift'] = (df_rep['ad_clicks'] * c.get('Clicks', 0)) + (df_rep['active_promo'].astype(int) * c.get('Promo', 0))
        df_rep['attributed_rev'] = df_rep['digital_lift'] * avg_spend
        df_rep['true_spend'] = df_rep['actual_coin_in'] / df_rep['actual_traffic']
        df_rep['variance'] = df_rep['actual_coin_in'] - (df_rep['actual_traffic'] * avg_spend)

        st.dataframe(df_rep.sort_values('entry_date', ascending=False), column_config={
            "actual_coin_in": st.column_config.NumberColumn("Actual Rev", format="$%d"),
            "attributed_rev": st.column_config.NumberColumn("Digital ROI", format="$%d"),
            "true_spend": st.column_config.NumberColumn("$/Head", format="$%.2f"),
            "variance": st.column_config.NumberColumn("vs. Engine Target", format="$%d")
        }, use_container_width=True, hide_index=True)
        st.download_button("📥 Export Report", df_rep.to_csv(index=False), "HR_Forensic_Report.csv", use_container_width=True)
