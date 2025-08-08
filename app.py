import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import warnings

warnings.filterwarnings('ignore')

# Expected template columns (63 columns)
EXPECTED_COLUMNS = [
    'name', 'BAG_LOT_NO_BAG_MIRROR', 'MEGA_BAG_LOT_NO_BAG', 'BAG_LOT_NO_BAG_MIRROR_FNL',
    'KICO_MINE_LOADING_MONTH_BAG', 'EXPORT_MINE_LOADING_MONTH_BAG', 'BAG_EXPORT_MONTH', 'BAG_PRN_MONTH',
    'TRUCK_TYPE_BAG_MIRROR', 'SUB_BUYER_BAG_MIRROR', 'TRUCK_LOADING_POINT_BAG_MIRROR', 'LSP_NAME_BAG_MIRROR',
    'ROUTE_TYPE_BAG_MIRROR', 'BAG_FLAG_STATUS_UPL', 'BAG_FLAG_STATUS_DETAIL', 'LIVE_CURRENT_ACTIVITY',
    'LIVE_CURRENT_ACTIVITY_1', 'LIVE_CURRENT_ACTIVITY_2', 'ROUTE_BAG_ETA_CALC', 'EST_PRN_RECEIVED_DATE',
    'EST_PRN_RECEIVE_DATE_GROUPED', 'OFFLOADING_TRUCK_ID', 'BAG_GROSS_WET_KG_INCL_SAMPLE_WMT',
    'BAG_GROSS_EXCL_SAMPLE_WMT', 'BAG_NET_EXCL_SAMPLE_WMT', 'BAG_GROSS_WET_KG_INCL_SAMPLE_KG',
    'BAG_GROSS_EXCL_SAMPLE_KG', 'DRC DATA - NET WT EXCL. SAMPLE (KG)', 'MINE_LOADING_TS_BAG_MIRROR',
    'MINE_LOADING_TS_EXPORT_BAG_MIRROR', 'LOADED_TRUCK_POLYTRA_ARRIVAL_TS_BAG_MIRROR',
    'LOADED_TRUCK_POLYTRA_EXIT_TS_BAG_MIRROR', 'MINE_EXIT_TS_BAG_MIRROR', 'SHUNT_TRK_OFFL_TS_BAG_MIRROR',
    'BAG_EXPORT_TS', 'ROUTE_CONSIGNEE_1_BAG_MIRROR', 'GRN_WH_GROSS_WEIGHT', 'GRN_WH_NET_WEIGHT',
    'GRN_WAREHOUSE_NAME', 'GRN_RECEIVED_DATE', 'GDN_LOADED_DATE', 'GDN_DISPATCH_DATE', 'PRN_ARRIVAL_DATE',
    'ROUTE_PORT_WAREHOUSE_BAG_MIRROR', 'PRN_WAREHOUSE_NAME_SCOPE_2', 'ROUTE_PORT_DESTINATION_BAG_MIRROR',
    'ROUTE_FINAL_DESTINATION_BAG_MIRROR', 'PRN_WH_GROSS_WEIGHT_SCOPE_2', 'PRN_WH_NET_WEIGHT',
    'PRN_RECEIVED_DATE_SCOPE_2', 'PDN_LOADED_DATE', 'PDN_DISPATCH_DATE', 'PDN_BC_NUMBER', 'PDN_VESSEL_NAME',
    'EXPORT_TRUCK_ID_BAG_MIRROR', 'SHUNT_TRUCK_ID_BAG_MIRROR', 'DRC_WAGON_ID_BAG_MIRROR',
    'WG_TRAIN_NO_BAG_MIRROR', 'ZAM_TRUCK_ID_BAG_MIRROR', 'BAG_SEAL_NO',
    'DMS_APPVL_PROC_STATUS_AUTO_BAG_MIRROR', 'FINAL_INCOTERM', 'STOCK_COMMENTS'
]


# Helper functions
def safe_str(value):
    """Safely convert value to string, handling None/NaN"""
    return "" if pd.isna(value) or value is None else str(value)


def safe_title(value):
    """Safely apply .title() to a value"""
    return "" if pd.isna(value) or value is None else str(value).title()


def is_not_null(value):
    """Check if value is not null/NaN"""
    return pd.notna(value) and value is not None


def is_null(value):
    """Check if value is null/NaN"""
    return pd.isna(value) or value is None


# Calculation functions
def calculate_live_current_activity(row):
    """Calculate LIVE_CURRENT_ACTIVITY"""
    if row['BAG_FLAG_STATUS_UPL'] == "Insurance Claim":
        return safe_str(row['BAG_FLAG_STATUS_DETAIL'])
    elif is_not_null(row['PDN_DISPATCH_DATE']):
        return "Sailed"
    elif (is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) and is_not_null(row['DRC_WAGON_ID_BAG_MIRROR']) and
          is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and is_null(row['BAG_EXPORT_TS'])):
        return "Loaded (Export)"
    elif is_null(row['BAG_EXPORT_TS']) and is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']):
        return "Loaded (Export)"
    elif (is_not_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR']) or "K3W5" in safe_str(row['name'])) and is_null(
            row['BAG_EXPORT_TS']):
        return "In Stock - (Mega Terminal)"
    elif is_not_null(row['GRN_RECEIVED_DATE']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']) and is_null(
            row['GDN_LOADED_DATE']):
        return "In Stock - Zambia"
    elif is_not_null(row['GDN_LOADED_DATE']) and is_null(row['GDN_DISPATCH_DATE']):
        return "Loaded - Zambia"
    elif is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']) and is_not_null(row['PRN_WAREHOUSE_NAME_SCOPE_2']):
        return "In Stock - Port"
    elif is_not_null(row['BAG_EXPORT_TS']) or is_not_null(row['GDN_DISPATCH_DATE']):
        return "En-Route"
    elif is_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR']) and is_not_null(row['MINE_LOADING_TS_BAG_MIRROR']):
        return "Loaded (Shunt Truck)"
    else:
        return ""


def calculate_live_current_activity_1(row):
    """Calculate LIVE_CURRENT_ACTIVITY_1"""
    if row['BAG_FLAG_STATUS_UPL'] == "Insurance Claim":
        return safe_str(row['BAG_FLAG_STATUS_DETAIL'])
    elif is_not_null(row['PDN_DISPATCH_DATE']):
        return "Sailed - " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR'])
    elif is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']) and is_not_null(row['PRN_WAREHOUSE_NAME_SCOPE_2']):
        return "In Stock - " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR'])
    elif (is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) and is_not_null(row['DRC_WAGON_ID_BAG_MIRROR']) and
          is_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and is_null(row['BAG_EXPORT_TS'])):
        return "Allocated to Train (at Mega Terminal)"
    elif ((is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) or row[
        'TRUCK_LOADING_POINT_BAG_MIRROR'] == "MEGA TERMINAL") and
          is_not_null(row['EXPORT_TRUCK_ID_BAG_MIRROR']) and is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and
          is_null(row['BAG_EXPORT_TS'])):
        return "Loaded (at Mega Terminal)"
    elif (is_not_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR']) or "K3W5" in safe_str(row['name'])) and is_null(
            row['BAG_EXPORT_TS']):
        return "In Stock - (Mega Terminal)"
    elif (is_not_null(row['GRN_RECEIVED_DATE']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']) and
          is_null(row['GDN_LOADED_DATE'])):
        return "In Stock - Zambia"
    elif is_not_null(row['GDN_LOADED_DATE']) and is_null(row['GDN_DISPATCH_DATE']):
        return "Loaded - Zambia"
    elif is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']) and is_not_null(row['PRN_WAREHOUSE_NAME_SCOPE_2']):
        return "In Stock - Port"
    elif (is_not_null(row['PRN_ARRIVAL_DATE']) and
          (is_not_null(row['BAG_EXPORT_TS']) or is_not_null(row['GDN_DISPATCH_DATE'])) and
          (is_null(row['GRN_RECEIVED_DATE']) or is_null(row['PRN_RECEIVED_DATE_SCOPE_2']))):
        return "Arrived " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR']) + " Not Offloaded"
    elif (row['ROUTE_TYPE_BAG_MIRROR'] != "INDIRECT" and is_not_null(row['BAG_EXPORT_TS']) and
          is_null(row['PRN_RECEIVED_DATE_SCOPE_2'])):
        return "Direct"
    elif (row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and is_not_null(row['BAG_EXPORT_TS']) and
          is_null(row['GRN_RECEIVED_DATE'])):
        return "1st Leg"
    elif (row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and is_not_null(row['GDN_DISPATCH_DATE']) and
          is_null(row['PRN_RECEIVED_DATE_SCOPE_2'])):
        return "2nd Leg"
    elif (is_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR']) and is_not_null(row['MINE_EXIT_TS_BAG_MIRROR']) and
          is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR'])):
        return "Loaded (On Route to Mega Terminal)"
    elif is_not_null(row['MINE_LOADING_TS_BAG_MIRROR']) and is_not_null(row['EXPORT_TRUCK_ID_BAG_MIRROR']):
        return "Loaded (at the Mine)"
    elif (is_not_null(row['MINE_LOADING_TS_BAG_MIRROR']) and is_null(row['MINE_EXIT_TS_BAG_MIRROR']) and
          is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR'])):
        return "Loaded (at the Mine)"
    else:
        return ""


def calculate_live_current_activity_2(row):
    """Calculate LIVE_CURRENT_ACTIVITY_2 (depends on LIVE_CURRENT_ACTIVITY and LIVE_CURRENT_ACTIVITY_1)"""
    if row['BAG_FLAG_STATUS_UPL'] == "Insurance Claim":
        return safe_str(row['BAG_FLAG_STATUS_DETAIL'])
    elif is_not_null(row['PDN_DISPATCH_DATE']):
        return f"Sailed {safe_str(row['PDN_VESSEL_NAME'])}/{safe_str(row['PDN_BC_NUMBER'])}"
    elif (row['LIVE_CURRENT_ACTIVITY'] == "En-Route" and is_not_null(row['PRN_ARRIVAL_DATE']) and
          is_null(row['PRN_RECEIVED_DATE_SCOPE_2'])):
        return "Arrived " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR']) + " Not Offloaded"

    # CONSISTENCY CHECKS: Align with previous calculations
    elif row['LIVE_CURRENT_ACTIVITY_1'] == "Allocated to Train (at Mega Terminal)":
        return "Allocated to Train (at Mega Terminal)"
    elif row['LIVE_CURRENT_ACTIVITY'] == "In Stock - (Mega Terminal)":
        return "In Stock (Mega Terminal)"
    elif row['LIVE_CURRENT_ACTIVITY'] == "In Stock - Port":
        return "In Stock (" + safe_title(row['PRN_WAREHOUSE_NAME_SCOPE_2']) + ")" if is_not_null(
            row['PRN_WAREHOUSE_NAME_SCOPE_2']) else "In Stock (Port)"
    elif row['LIVE_CURRENT_ACTIVITY'] == "In Stock - Zambia":
        return "In Stock (" + safe_title(row['GRN_WAREHOUSE_NAME']) + ")" if is_not_null(
            row['GRN_WAREHOUSE_NAME']) else "In Stock (Zambia)"

    # Transit conditions
    elif row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and is_not_null(row['BAG_EXPORT_TS']) and is_null(
            row['GRN_RECEIVED_DATE']):
        return "In Transit (Kipushi - Zambia Warehouse)"
    elif row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and is_not_null(row['GDN_DISPATCH_DATE']) and is_null(
            row['PRN_RECEIVED_DATE_SCOPE_2']):
        return "In Transit (Zambia Warehouse - " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR']) + ")"
    elif row['ROUTE_TYPE_BAG_MIRROR'] == "DIRECT" and is_not_null(row['BAG_EXPORT_TS']) and is_null(
            row['PRN_RECEIVED_DATE_SCOPE_2']):
        return "In Transit (Kipushi - " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR']) + ")"

    # Loaded conditions
    elif (row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and row['LIVE_CURRENT_ACTIVITY'] == "Loaded - Zambia"):
        return "Loaded (" + safe_title(row['ROUTE_CONSIGNEE_1_BAG_MIRROR']) + ")"
    elif (is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) and is_not_null(row['DRC_WAGON_ID_BAG_MIRROR']) and
          is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and is_null(row['BAG_EXPORT_TS'])):
        return "Loaded - Wagon currently at Mega Terminal"
    elif (is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and is_null(row['MINE_EXIT_TS_BAG_MIRROR']) and
          (is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) or row[
              'TRUCK_LOADING_POINT_BAG_MIRROR'] == "MEGA TERMINAL") and
          is_null(row['BAG_EXPORT_TS'])):
        return "Loaded - Truck currently at Mega Terminal"
    elif (is_not_null(row['LOADED_TRUCK_POLYTRA_ARRIVAL_TS_BAG_MIRROR']) and
          is_not_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_not_null(row['EXPORT_TRUCK_ID_BAG_MIRROR']) and
          is_null(row['LOADED_TRUCK_POLYTRA_EXIT_TS_BAG_MIRROR'])):
        return "Loaded - Truck currently at Offsite"
    elif is_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_not_null(row['MINE_LOADING_TS_BAG_MIRROR']):
        return "Loaded - Truck currently at Mine"
    elif (is_not_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_null(
            row['LOADED_TRUCK_POLYTRA_ARRIVAL_TS_BAG_MIRROR'])) or (
            is_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR'])):
        return "Loaded - Truck Exited P2 Parking, Waiting on Convoy"
    else:
        return ""


def validate_template(df):
    """Validate if uploaded file matches expected template"""
    df_cols = list(df.columns)

    if len(df_cols) != len(EXPECTED_COLUMNS):
        return False, f"Expected {len(EXPECTED_COLUMNS)} columns, but found {len(df_cols)}"

    missing_cols = []
    wrong_order = []

    for i, expected_col in enumerate(EXPECTED_COLUMNS):
        if i >= len(df_cols):
            missing_cols.append(expected_col)
        elif df_cols[i] != expected_col:
            wrong_order.append(f"Position {i + 1}: Expected '{expected_col}', found '{df_cols[i]}'")

    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    if wrong_order:
        return False, f"Column order issues: {wrong_order[:5]}"

    return True, "Template validation successful"


def process_data(df):
    """Process data with proper dependency chain: ACTIVITY → ACTIVITY_1 → ACTIVITY_2"""
    df_processed = df.copy()

    # Step 1: Calculate LIVE_CURRENT_ACTIVITY
    df_processed['LIVE_CURRENT_ACTIVITY_CORRECTED'] = df_processed.apply(calculate_live_current_activity, axis=1)
    df_processed['LIVE_CURRENT_ACTIVITY'] = df_processed['LIVE_CURRENT_ACTIVITY_CORRECTED']

    # Step 2: Calculate LIVE_CURRENT_ACTIVITY_1 (depends on corrected LIVE_CURRENT_ACTIVITY)
    df_processed['LIVE_CURRENT_ACTIVITY_1_CORRECTED'] = df_processed.apply(calculate_live_current_activity_1, axis=1)
    df_processed['LIVE_CURRENT_ACTIVITY_1'] = df_processed['LIVE_CURRENT_ACTIVITY_1_CORRECTED']

    # Step 3: Calculate LIVE_CURRENT_ACTIVITY_2 (depends on both corrected values)
    df_processed['LIVE_CURRENT_ACTIVITY_2_CORRECTED'] = df_processed.apply(calculate_live_current_activity_2, axis=1)

    return df_processed


def create_comparison_df(original_df, processed_df):
    """Create comparison dataframe at name level"""
    comparison_data = []
    target_cols = ['LIVE_CURRENT_ACTIVITY', 'LIVE_CURRENT_ACTIVITY_1', 'LIVE_CURRENT_ACTIVITY_2']

    for idx, row in original_df.iterrows():
        name = safe_str(row['name'])
        bag_lot_no = safe_str(row['BAG_LOT_NO_BAG_MIRROR'])

        for col in target_cols:
            original_val = safe_str(row[col])
            corrected_val = safe_str(processed_df.loc[idx, f'{col}_CORRECTED'])

            if original_val != corrected_val:
                comparison_data.append({
                    'Name': name,
                    'BAG_LOT_NO': bag_lot_no,
                    'Column': col,
                    'Original_Value': original_val,
                    'Corrected_Value': corrected_val
                })

    return pd.DataFrame(comparison_data)


def create_excel_download(processed_df):
    """Create Excel file for download"""
    final_df = processed_df.copy()
    final_df['LIVE_CURRENT_ACTIVITY'] = final_df['LIVE_CURRENT_ACTIVITY_CORRECTED']
    final_df['LIVE_CURRENT_ACTIVITY_1'] = final_df['LIVE_CURRENT_ACTIVITY_1_CORRECTED']
    final_df['LIVE_CURRENT_ACTIVITY_2'] = final_df['LIVE_CURRENT_ACTIVITY_2_CORRECTED']

    # Remove temporary columns
    cols_to_remove = ['LIVE_CURRENT_ACTIVITY_CORRECTED', 'LIVE_CURRENT_ACTIVITY_1_CORRECTED',
                      'LIVE_CURRENT_ACTIVITY_2_CORRECTED']
    final_df = final_df.drop(columns=cols_to_remove)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Corrected_Data')

    return output.getvalue()


# Streamlit App
st.set_page_config(page_title="Active Bag Report Calculator", page_icon="📊", layout="wide")

st.title("📊 Active Bag Report Calculator")
st.markdown("Upload your Active Bag Report CSV file to recalculate LIVE_CURRENT_ACTIVITY columns")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)
        st.success(f"File uploaded successfully! {len(df)} rows, {len(df.columns)} columns")

        is_valid, message = validate_template(df)

        if not is_valid:
            st.error(f"❌ Template validation failed: {message}")
            st.markdown("### Expected Template Structure")
            st.markdown("Please ensure your CSV file has exactly these columns in this order:")

            col_display = pd.DataFrame({
                'Position': range(1, len(EXPECTED_COLUMNS) + 1),
                'Column Name': EXPECTED_COLUMNS
            })
            st.dataframe(col_display, height=400)
        else:
            st.success("✅ Template validation passed!")

            with st.spinner("Processing data and calculating corrections..."):
                processed_df = process_data(df)
                comparison_df = create_comparison_df(df, processed_df)

            # Summary statistics
            st.markdown("## 📈 Summary Statistics")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Corrections Made", len(comparison_df))
            with col3:
                correction_rate = (len(comparison_df) / (len(df) * 3)) * 100 if len(df) > 0 else 0
                st.metric("Correction Rate", f"{correction_rate:.1f}%")
            with col4:
                affected_names = len(comparison_df['Name'].unique()) if len(comparison_df) > 0 else 0
                st.metric("Affected Names", affected_names)

            # Show corrections
            if len(comparison_df) > 0:
                st.markdown("## 🔄 Corrections Made")
                st.dataframe(comparison_df, height=300)

                st.markdown("### Corrections by Column")
                col_breakdown = comparison_df['Column'].value_counts()

                breakdown_col1, breakdown_col2, breakdown_col3 = st.columns(3)
                for i, (col, count) in enumerate(col_breakdown.items()):
                    with [breakdown_col1, breakdown_col2, breakdown_col3][i % 3]:
                        st.metric(col, count)
            else:
                st.info("🎉 No corrections needed! All values are already correct.")

            # Download section
            st.markdown("## 📥 Download Corrected File")
            excel_data = create_excel_download(processed_df)

            st.download_button(
                label="📊 Download Corrected Excel File",
                data=excel_data,
                file_name="corrected_active_bag_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Preview
            if st.checkbox("Show corrected data preview"):
                st.markdown("### Corrected Data Preview")
                preview_df = processed_df.copy()
                preview_df['LIVE_CURRENT_ACTIVITY'] = preview_df['LIVE_CURRENT_ACTIVITY_CORRECTED']
                preview_df['LIVE_CURRENT_ACTIVITY_1'] = preview_df['LIVE_CURRENT_ACTIVITY_1_CORRECTED']
                preview_df['LIVE_CURRENT_ACTIVITY_2'] = preview_df['LIVE_CURRENT_ACTIVITY_2_CORRECTED']

                preview_cols = ['name', 'BAG_LOT_NO_BAG_MIRROR', 'LIVE_CURRENT_ACTIVITY', 'LIVE_CURRENT_ACTIVITY_1',
                                'LIVE_CURRENT_ACTIVITY_2']
                st.dataframe(preview_df[preview_cols], height=400)

    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")
        st.markdown("Please ensure your CSV file is properly formatted and matches the expected template.")

else:
    st.info("👆 Please upload a CSV file to get started")

    st.markdown("## 📋 Template Requirements")
    st.markdown("Your CSV file must contain exactly **63 columns** in the following order:")

    col_display = pd.DataFrame({
        'Position': range(1, len(EXPECTED_COLUMNS) + 1),
        'Column Name': EXPECTED_COLUMNS
    })
    st.dataframe(col_display, height=400)

st.markdown("---")
st.markdown(
    "*This application recalculates LIVE_CURRENT_ACTIVITY, LIVE_CURRENT_ACTIVITY_1, and LIVE_CURRENT_ACTIVITY_2 columns based on business logic requirements.*")