# --- TAB 4: ADMIN ENGINE ---
with tab4:
    st.header("Coefficient Control Center")
    with st.form("coeff_form"):
        st.subheader("Financial Metrics")
        c_coin = st.number_input("Average Coin-In per Visitor ($)", value=st.session_state.coeffs['Avg_Coin_In'])
        
        st.subheader("Day of Week Baselines")
        # Split into two rows so it looks clean on the screen
        row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
        c_mon = row1_col1.number_input("Monday", value=st.session_state.coeffs['DOW_Mon'])
        c_tue = row1_col2.number_input("Tuesday", value=st.session_state.coeffs['DOW_Tue'])
        c_wed = row1_col3.number_input("Wednesday", value=st.session_state.coeffs['DOW_Wed'])
        c_thu = row1_col4.number_input("Thursday", value=st.session_state.coeffs['DOW_Thu'])
        
        row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)
        c_fri = row2_col1.number_input("Friday", value=st.session_state.coeffs['DOW_Fri'])
        c_sat = row2_col2.number_input("Saturday", value=st.session_state.coeffs['DOW_Sat'])
        c_sun = row2_col3.number_input("Sunday", value=st.session_state.coeffs['DOW_Sun'])
        # Empty column to keep layout even
        with row2_col4:
            st.empty() 
        
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
            st.success("Parameters updated! The model has been recalibrated.")
