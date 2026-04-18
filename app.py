import streamlit as st
import pandas as pd
import google.generativeai as genai
import datetime
import json
from supabase import create_client

# --- 1. INITIALIZATION & DATABASE SYNC ---
# Ensure these are in your st.secrets
url = st.secrets["SUPABASE_URL"]
key = st.secrets["SUPABASE_KEY"]
supabase = create_client(url, key)

# Hydrate Coefficients (Source of Truth)
if 'coeffs' not in st.session_state:
    try:
        response = supabase.table("coefficients").select("*").eq("id", 1).execute()
        if response.data:
            st.session_state.coeffs = response.data[0]
        else:
            # Fallback Baseline
            st.session_state.coeffs = {
                "id": 1, "Intercept": 1000, "Avg_Coin_In": 1200, 
                "Clicks": 0.5, "Promo": 500, "Temp_C": 0, "Snow_cm": 0, "Rain_mm": 0
            }
    except:
        st.session_state.coeffs = {"Intercept": 1000, "Avg_Coin_In": 1200}

# Load Ledger Data once for all tabs
try:
    ledger_response = supabase.table("ledger").select("*").order("entry_date").execute()
    ledger_data = ledger_response.data
except:
    ledger_data = []

# --- 2. TABS DEFINITION ---
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏛️ Executive", "📥 Input", "🚀 Strategy", "⚙️ Admin", "🔍 Analyst", "📊 Master Report"
])

# --- 3. TAB 1: EXECUTIVE DASHBOARD ---
with tab1:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">🏛️ Executive Property Overview</h2>
            <p style="color: #888; margin: 0;">Live YTD Ledger Performance & Model Accuracy.</p>
        </div>
    """, unsafe_allow_html=True)

    c = st.session_state.coeffs
    avg_spend = c.get('Avg_Coin_In', 1200)
    
    df_exec = pd.DataFrame(ledger_data)
    
    if not df_exec.empty:
        # Safety Column Sync
        for col in ['ad_clicks', 'active_promo', 'temp_c', 'snow_cm', 'rain_mm']:
            if col not in df_exec.columns: df_exec[col] = 0
            
        df_exec['entry_date'] = pd.to_datetime(df_exec['entry_date'])
        
        # Financial Totals
        total_traffic = df_exec['actual_traffic'].sum()
        total_revenue = df_exec['actual_coin_in'].sum()
        
        # Digital ROI Logic
        df_exec['daily_lift'] = (df_exec['ad_clicks'] * c.get('Clicks', 0)) + (df_exec['active_promo'].astype(int) * c.get('Promo', 0))
        total_lift_rev = df_exec['daily_lift'].sum() * avg_spend

        # Predictability Score (Actual vs. Engine Guess)
        df_exec['expected'] = c.get('Intercept', 0) + df_exec['daily_lift'] + (df_exec['temp_c'] * c.get('Temp_C', 0))
        df_exec['error'] = abs(df_exec['actual_traffic'] - df_exec['expected']) / df_exec['actual_traffic']
        accuracy = max(0, (1 - df_exec['error'].mean()) * 100)

        # UI: Top Row Bento Cards
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-top: 5px solid #FFCC00; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">Total YTD Revenue</p>
                <h1 style="color: #FFF; margin: 0;">${total_revenue:,.0f}</h1>
                <p style="color: #FFCC00; font-size: 11px; margin-top: 10px;">Actual Ledger Coin-In</p>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-top: 5px solid #FFCC00; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">Digital ROI YTD</p>
                <h1 style="color: #FFF; margin: 0;">${total_lift_rev:,.0f}</h1>
                <p style="color: #FFCC00; font-size: 11px; margin-top: 10px;">Value of Marketing Lift</p>
            </div>""", unsafe_allow_html=True)

        st.write("##")
        
        # UI: Bottom Row Bento Cards
        col3, col4 = st.columns(2)
        with col3:
            ytd_avg_actual = total_revenue / total_traffic if total_traffic > 0 else 0
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-left: 10px solid #FFCC00; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">True YTD Spend/Head</p>
                <h1 style="color: #FFF; margin: 0;">${ytd_avg_actual:,.2f}</h1>
                <p style="color: #888; font-size: 11px; margin-top: 10px;">Engine Setting: ${avg_spend:,.2f}</p>
            </div>""", unsafe_allow_html=True)
        with col4:
            score_color = "#00FF00" if accuracy > 85 else "#FFCC00" if accuracy > 70 else "#FF0000"
            st.markdown(f"""<div style="background-color: #1a1a1a; padding: 30px; border-radius: 15px; border-left: 10px solid {score_color}; text-align: center;">
                <p style="color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 5px;">AI Predictability</p>
                <h1 style="color: {score_color}; margin: 0;">{accuracy:.1f}%</h1>
                <p style="color: #FFF; font-size: 11px; margin-top: 10px;">Model Confidence Score</p>
            </div>""", unsafe_allow_html=True)

# --- 4. TAB 4: ADMIN ENGINE (YTD CALIBRATION) ---
with tab4:
    st.markdown("### ⚙️ Engine Calibration")
    
    if st.button("🤖 Auto-Calibrate (YTD Accounting Mode)", use_container_width=True):
        with st.spinner("Locking Revenue Reality..."):
            df_calc = pd.DataFrame(ledger_data)
            if not df_calc.empty:
                # HARDEST MATH: YTD Ledger Averaging
                total_vis = df_calc['actual_traffic'].sum()
                total_rev = df_calc['actual_coin_in'].sum()
                math_avg_spend = total_rev / total_vis if total_vis > 0 else 1200
                
                # Update Session State
                st.session_state.coeffs['Avg_Coin_In'] = math_avg_spend
                st.success(f"Engine Anchored: YTD Spend per Head is ${math_avg_spend:,.2f}")
                st.rerun()

    # Manual Control Grid
    col_a, col_b = st.columns(2)
    with col_a:
        new_spend = st.number_input("Avg. Spend per Head ($)", value=float(c.get('Avg_Coin_In', 1200)))
        new_clicks = st.number_input("Weight / Ad Click", value=float(c.get('Clicks', 0)))
    with col_b:
        new_base = st.number_input("Base Daily Traffic", value=float(c.get('Intercept', 1000)))
        new_promo = st.number_input("Promo Flat Lift", value=float(c.get('Promo', 0)))

    if st.button("💾 Save All Engine Changes", use_container_width=True):
        updates = {"id": 1, "Avg_Coin_In": new_spend, "Clicks": new_clicks, "Intercept": new_base, "Promo": new_promo}
        supabase.table("coefficients").upsert(updates).execute()
        st.session_state.coeffs.update(updates)
        st.success("Changes saved to Database.")
        st.rerun()

# --- 5. TAB 6: MASTER FORENSIC REPORT ---
with tab6:
    st.markdown("""
        <div style="background-color: #111; padding: 20px; border-radius: 10px; border-left: 5px solid #FFCC00; margin-bottom: 25px;">
            <h2 style="color: #FFCC00; margin: 0;">📊 Master Forensic Report</h2>
            <p style="color: #888; margin: 0;">Full Ledger Analysis vs. Engine Targets.</p>
        </div>
    """, unsafe_allow_html=True)

    df_rep = pd.DataFrame(ledger_data).copy()
    
    if not df_rep.empty:
        df_rep['digital_lift'] = (df_rep['ad_clicks'] * c.get('Clicks', 0)) + (df_rep['active_promo'].astype(int) * c.get('Promo', 0))
        df_rep['attributed_rev'] = df_rep['digital_lift'] * avg_spend
        df_rep['true_spend'] = df_rep['actual_coin_in'] / df_rep['actual_traffic']
        df_rep['variance'] = df_rep['actual_coin_in'] - (df_rep['actual_traffic'] * avg_spend)

        st.dataframe(
            df_rep.sort_values('entry_date', ascending=False),
            column_config={
                "entry_date": "Date",
                "actual_traffic": "Traffic",
                "actual_coin_in": st.column_config.NumberColumn("Actual Rev", format="$%d"),
                "attributed_rev": st.column_config.NumberColumn("Digital ROI", format="$%d"),
                "true_spend": st.column_config.NumberColumn("$/Head", format="$%.2f"),
                "variance": st.column_config.NumberColumn("vs. Engine Mean", format="$%d")
            },
            use_container_width=True, hide_index=True
        )

        st.download_button("📥 Export Report", df_rep.to_csv(index=False), "Forensic_Report.csv", use_container_width=True)
