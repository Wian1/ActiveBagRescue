import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import warnings
from datetime import datetime, timedelta
import calendar

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
def calculate_route_bag_eta_calc(row):
    """Calculate ROUTE_BAG_ETA_CALC (float days)"""
    # If already received at port, ETA is 0
    if is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        return 0.0
    
    # Calculate ETA based on current activity and route type
    if row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Export)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'INDIRECT':
        return 51.0
    elif row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Export)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'DIRECT':
        return 38.0
    elif row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Shunt Truck)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'INDIRECT':
        return 51.0
    elif row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Shunt Truck)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'DIRECT':
        return 38.0
    elif row['LIVE_CURRENT_ACTIVITY'] == 'In Stock - (Mega Terminal)':
        return 0.0
    elif row['LIVE_CURRENT_ACTIVITY_1'] == '1st Leg':
        return 39.0
    elif row['LIVE_CURRENT_ACTIVITY_1'] == 'Direct':
        return 26.0
    elif row['LIVE_CURRENT_ACTIVITY_1'] == '2nd Leg':
        return 15.0
    elif is_not_null(row['PRN_ARRIVAL_DATE']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        return 2.0
    else:
        return 0.0

def safe_date_add_days(date_value, days):
    """Safely add days to a date - simple and robust"""
    if is_null(date_value) or days == 0:
        return None
    
    try:
        # Handle string dates - extract first 10 characters for simple parsing
        if isinstance(date_value, str):
            # Extract just the date part: "2025-08-29 11:11:19.788000+02:00" -> "2025-08-29"
            date_part = date_value[:10]
            try:
                parsed_date = datetime.strptime(date_part, '%Y-%m-%d')
                result = parsed_date + timedelta(days=days)
                # Return timezone-naive datetime for Excel compatibility
                return result.replace(tzinfo=None) if result.tzinfo else result
            except:
                pass
        
        # Handle pandas Timestamp - convert to timezone-naive
        elif isinstance(date_value, pd.Timestamp):
            # Convert to timezone-naive
            if date_value.tz is not None:
                date_value = date_value.tz_convert(None)
            result = date_value + pd.Timedelta(days=days)
            return result.replace(tzinfo=None) if hasattr(result, 'tz') and result.tz else result
        
        # Handle datetime objects - convert to timezone-naive
        elif isinstance(date_value, datetime):
            # Remove timezone info for Excel compatibility
            naive_date = date_value.replace(tzinfo=None)
            return naive_date + timedelta(days=days)
        
        # Fallback: try pandas parsing and make timezone-naive
        else:
            try:
                parsed_date = pd.to_datetime(date_value)
                if hasattr(parsed_date, 'tz') and parsed_date.tz is not None:
                    parsed_date = parsed_date.tz_convert(None)
                result = parsed_date + pd.Timedelta(days=days)
                return result.replace(tzinfo=None) if hasattr(result, 'tz') and result.tz else result
            except:
                pass
                
    except Exception as e:
        print(f"Date parsing failed for {date_value}: {e}")
    
    return None

def calculate_est_prn_received_date(row):
    """Calculate EST_PRN_RECEIVED_DATE (datetime)"""
    # If already received at port, return None/0
    if is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        return None
    
    # Get the ETA days and base date for calculation
    eta_days = row['ROUTE_BAG_ETA_CALC']
    base_date = None
    
    if row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Export)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'INDIRECT':
        base_date = row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']
    elif row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Export)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'DIRECT':
        base_date = row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']
    elif row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Shunt Truck)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'INDIRECT':
        base_date = row['MINE_LOADING_TS_BAG_MIRROR']
    elif row['LIVE_CURRENT_ACTIVITY'] == 'Loaded (Shunt Truck)' and row['ROUTE_TYPE_BAG_MIRROR'] == 'DIRECT':
        base_date = row['MINE_LOADING_TS_BAG_MIRROR']
    elif row['LIVE_CURRENT_ACTIVITY'] == 'In Stock - (Mega Terminal)':
        return None  # No calculation needed
    elif row['LIVE_CURRENT_ACTIVITY_1'] == '1st Leg':
        base_date = row['BAG_EXPORT_TS']
    elif row['LIVE_CURRENT_ACTIVITY_1'] == 'Direct':
        base_date = row['BAG_EXPORT_TS']
    elif row['LIVE_CURRENT_ACTIVITY'] == 'In Stock - Zambia':
        base_date = row['GRN_RECEIVED_DATE']
        eta_days = 29.0  # Special case for Zambia
    elif row['LIVE_CURRENT_ACTIVITY_1'] == '2nd Leg':
        base_date = row['GDN_DISPATCH_DATE']
    elif is_not_null(row['PRN_ARRIVAL_DATE']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        base_date = row['PRN_ARRIVAL_DATE']
    
    return safe_date_add_days(base_date, eta_days)

def calculate_est_prn_receive_date_grouped(row):
    """Calculate EST_PRN_RECEIVE_DATE_GROUPED (string)"""
    # If already received at port, return current activity
    if is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        return row['LIVE_CURRENT_ACTIVITY']
    
    # If in stock at mega terminal, return current activity  
    if row['LIVE_CURRENT_ACTIVITY'] == 'In Stock - (Mega Terminal)':
        return row['LIVE_CURRENT_ACTIVITY']
    
    # Red flag check
    if row['BAG_FLAG_STATUS_UPL'] != "Normal Cargo":
        return "Red Flag"
    
    est_date = row['EST_PRN_RECEIVED_DATE']
    
    # Debug: Check if we have a valid ETA but no estimated date
    if row['ROUTE_BAG_ETA_CALC'] > 0 and is_null(est_date):
        print(f"DEBUG: Row has ETA {row['ROUTE_BAG_ETA_CALC']} but EST_PRN_RECEIVED_DATE is null for {row.get('name', 'unknown')}")
    
    # If no estimated date, return current activity only for specific cases
    if is_null(est_date):
        return row['LIVE_CURRENT_ACTIVITY']
    
    try:
        # Convert to datetime if needed
        if isinstance(est_date, str):
            est_date = pd.to_datetime(est_date)
        elif hasattr(est_date, 'to_pydatetime'):
            est_date = est_date.to_pydatetime()
        
        current_time = datetime.now()
        
        # Check if overdue
        if current_time > est_date:
            return "Investigate"
        
        # Get month name and year
        month_name = est_date.strftime('%B')  # Full month name
        year = est_date.year
        
        # Get days in month
        days_in_month = calendar.monthrange(year, est_date.month)[1]
        
        # Group by date ranges
        if est_date.day <= 15:
            return f"1 - 15 {month_name} {year}"
        elif days_in_month == 28:
            return f"16 - 28 {month_name} {year}"
        elif days_in_month == 30:
            return f"16 - 30 {month_name} {year}"
        elif days_in_month == 31:
            return f"16 - 31 {month_name} {year}"
        else:
            return row['LIVE_CURRENT_ACTIVITY']
    
    except Exception as e:
        print(f"DEBUG: Date grouping failed for {est_date}: {e}")
        return row['LIVE_CURRENT_ACTIVITY']

def calculate_offloading_truck_id(row):
    """Calculate OFFLOADING_TRUCK_ID based on priority order"""
    if is_not_null(row['ZAM_TRUCK_ID_BAG_MIRROR']):
        return safe_str(row['ZAM_TRUCK_ID_BAG_MIRROR'])
    elif is_not_null(row['DRC_WAGON_ID_BAG_MIRROR']):
        return safe_str(row['DRC_WAGON_ID_BAG_MIRROR'])
    elif is_not_null(row['EXPORT_TRUCK_ID_BAG_MIRROR']):
        return safe_str(row['EXPORT_TRUCK_ID_BAG_MIRROR'])
    elif is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']):
        return safe_str(row['SHUNT_TRUCK_ID_BAG_MIRROR'])
    else:
        return ""

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
    elif (is_not_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR']) or "K3W5" in safe_str(row['name'])) and is_null(row['BAG_EXPORT_TS']):
        return "In Stock - (Mega Terminal)"
    elif is_not_null(row['GRN_RECEIVED_DATE']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']) and is_null(row['GDN_LOADED_DATE']):
        return "In Stock - Zambia"
    elif is_not_null(row['GDN_LOADED_DATE']) and is_null(row['GDN_DISPATCH_DATE']):
        return "Loaded - Zambia"
    elif is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
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
    elif is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        return "In Stock - " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR'])
    elif (is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) and is_not_null(row['DRC_WAGON_ID_BAG_MIRROR']) and
          is_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and is_null(row['BAG_EXPORT_TS'])):
        return "Allocated to Train (at Mega Terminal)"
    elif ((is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) or row['TRUCK_LOADING_POINT_BAG_MIRROR'] == "MEGA TERMINAL") and
          is_not_null(row['EXPORT_TRUCK_ID_BAG_MIRROR']) and is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and
          is_null(row['BAG_EXPORT_TS'])):
        return "Loaded (at Mega Terminal)"
    elif (is_not_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR']) or "K3W5" in safe_str(row['name'])) and is_null(row['BAG_EXPORT_TS']):
        return "In Stock - (Mega Terminal)"
    elif (is_not_null(row['GRN_RECEIVED_DATE']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']) and
          is_null(row['GDN_LOADED_DATE'])):
        return "In Stock - Zambia"
    elif is_not_null(row['GDN_LOADED_DATE']) and is_null(row['GDN_DISPATCH_DATE']):
        return "Loaded - Zambia"
    elif is_not_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
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
        # Use PRN_WAREHOUSE_NAME_SCOPE_2 first, fallback to ROUTE_PORT_WAREHOUSE_BAG_MIRROR
        warehouse_name = row['PRN_WAREHOUSE_NAME_SCOPE_2'] if is_not_null(row['PRN_WAREHOUSE_NAME_SCOPE_2']) else row['ROUTE_PORT_WAREHOUSE_BAG_MIRROR']
        return "In Stock (" + safe_title(warehouse_name) + ")" if is_not_null(warehouse_name) else "In Stock (Port)"
    elif row['LIVE_CURRENT_ACTIVITY'] == "In Stock - Zambia":
        return "In Stock (" + safe_title(row['GRN_WAREHOUSE_NAME']) + ")" if is_not_null(row['GRN_WAREHOUSE_NAME']) else "In Stock (Zambia)"

    # Transit conditions
    elif row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and is_not_null(row['BAG_EXPORT_TS']) and is_null(row['GRN_RECEIVED_DATE']):
        return "In Transit (Kipushi - Zambia Warehouse)"
    elif row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and is_not_null(row['GDN_DISPATCH_DATE']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        return "In Transit (Zambia Warehouse - " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR']) + ")"
    elif row['ROUTE_TYPE_BAG_MIRROR'] == "DIRECT" and is_not_null(row['BAG_EXPORT_TS']) and is_null(row['PRN_RECEIVED_DATE_SCOPE_2']):
        return "In Transit (Kipushi - " + safe_title(row['ROUTE_PORT_DESTINATION_BAG_MIRROR']) + ")"

    # Loaded conditions
    elif (row['ROUTE_TYPE_BAG_MIRROR'] == "INDIRECT" and row['LIVE_CURRENT_ACTIVITY'] == "Loaded - Zambia"):
        return "Loaded (" + safe_title(row['ROUTE_CONSIGNEE_1_BAG_MIRROR']) + ")"
    elif (is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) and is_not_null(row['DRC_WAGON_ID_BAG_MIRROR']) and
          is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and is_null(row['BAG_EXPORT_TS'])):
        return "Loaded - Wagon currently at Mega Terminal"
    elif (is_not_null(row['MINE_LOADING_TS_EXPORT_BAG_MIRROR']) and is_null(row['MINE_EXIT_TS_BAG_MIRROR']) and
          (is_not_null(row['SHUNT_TRUCK_ID_BAG_MIRROR']) or row['TRUCK_LOADING_POINT_BAG_MIRROR'] == "MEGA TERMINAL") and
          is_null(row['BAG_EXPORT_TS'])):
        return "Loaded - Truck currently at Mega Terminal"
    elif (is_not_null(row['LOADED_TRUCK_POLYTRA_ARRIVAL_TS_BAG_MIRROR']) and
          is_not_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_not_null(row['EXPORT_TRUCK_ID_BAG_MIRROR']) and
          is_null(row['LOADED_TRUCK_POLYTRA_EXIT_TS_BAG_MIRROR'])):
        return "Loaded - Truck currently at Offsite"
    elif is_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_not_null(row['MINE_LOADING_TS_BAG_MIRROR']):
        return "Loaded - Truck currently at Mine"
    elif (is_not_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_null(row['LOADED_TRUCK_POLYTRA_ARRIVAL_TS_BAG_MIRROR'])) or (is_null(row['MINE_EXIT_TS_BAG_MIRROR']) and is_null(row['SHUNT_TRK_OFFL_TS_BAG_MIRROR'])):
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
            wrong_order.append(f"Position {i+1}: Expected '{expected_col}', found '{df_cols[i]}'")
    
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    if wrong_order:
        return False, f"Column order issues: {wrong_order[:5]}"
    
    return True, "Template validation successful"

def process_data(df):
    """Process data with proper dependency chain: OFFLOADING_TRUCK_ID → ACTIVITY → ACTIVITY_1 → ACTIVITY_2 → ETA calculations"""
    df_processed = df.copy()
    
    # Step 1: Calculate OFFLOADING_TRUCK_ID (independent)
    df_processed['OFFLOADING_TRUCK_ID_CORRECTED'] = df_processed.apply(calculate_offloading_truck_id, axis=1)
    df_processed['OFFLOADING_TRUCK_ID'] = df_processed['OFFLOADING_TRUCK_ID_CORRECTED']
    
    # Step 2: Calculate LIVE_CURRENT_ACTIVITY
    df_processed['LIVE_CURRENT_ACTIVITY_CORRECTED'] = df_processed.apply(calculate_live_current_activity, axis=1)
    df_processed['LIVE_CURRENT_ACTIVITY'] = df_processed['LIVE_CURRENT_ACTIVITY_CORRECTED']
    
    # Step 3: Calculate LIVE_CURRENT_ACTIVITY_1 (depends on corrected LIVE_CURRENT_ACTIVITY)
    df_processed['LIVE_CURRENT_ACTIVITY_1_CORRECTED'] = df_processed.apply(calculate_live_current_activity_1, axis=1)
    df_processed['LIVE_CURRENT_ACTIVITY_1'] = df_processed['LIVE_CURRENT_ACTIVITY_1_CORRECTED']
    
    # Step 4: Calculate LIVE_CURRENT_ACTIVITY_2 (depends on both corrected values)
    df_processed['LIVE_CURRENT_ACTIVITY_2_CORRECTED'] = df_processed.apply(calculate_live_current_activity_2, axis=1)
    df_processed['LIVE_CURRENT_ACTIVITY_2'] = df_processed['LIVE_CURRENT_ACTIVITY_2_CORRECTED']
    
    # Step 5: Calculate ETA-related columns (depend on corrected activity values)
    df_processed['ROUTE_BAG_ETA_CALC_CORRECTED'] = df_processed.apply(calculate_route_bag_eta_calc, axis=1)
    df_processed['ROUTE_BAG_ETA_CALC'] = df_processed['ROUTE_BAG_ETA_CALC_CORRECTED']
    
    df_processed['EST_PRN_RECEIVED_DATE_CORRECTED'] = df_processed.apply(calculate_est_prn_received_date, axis=1)
    df_processed['EST_PRN_RECEIVED_DATE'] = df_processed['EST_PRN_RECEIVED_DATE_CORRECTED']
    
    df_processed['EST_PRN_RECEIVE_DATE_GROUPED_CORRECTED'] = df_processed.apply(calculate_est_prn_receive_date_grouped, axis=1)
    
    return df_processed

def normalize_datetime_for_comparison(value):
    """Normalize datetime values for accurate comparison"""
    if pd.isna(value) or value is None:
        return ""
    
    # If it's already a string, try to extract just the date part
    if isinstance(value, str):
        # Handle timezone-aware datetime strings
        if '+' in value and len(value) > 10:
            try:
                # Extract date part from "2025-09-23 08:48:47+02:00" -> "2025-09-23"
                return value[:10]
            except:
                return str(value)
        return str(value)
    
    # If it's a datetime object, format as date string
    elif isinstance(value, (datetime, pd.Timestamp)):
        try:
            return value.strftime('%Y-%m-%d')
        except:
            return str(value)
    
    return str(value)

def create_comparison_df(original_df, processed_df):
    """Create comparison dataframe at name level"""
    comparison_data = []
    target_cols = ['OFFLOADING_TRUCK_ID', 'LIVE_CURRENT_ACTIVITY', 'LIVE_CURRENT_ACTIVITY_1', 'LIVE_CURRENT_ACTIVITY_2', 'ROUTE_BAG_ETA_CALC', 'EST_PRN_RECEIVED_DATE', 'EST_PRN_RECEIVE_DATE_GROUPED']
    
    for idx, row in original_df.iterrows():
        name = safe_str(row['name'])
        bag_lot_no = safe_str(row['BAG_LOT_NO_BAG_MIRROR'])
        
        for col in target_cols:
            original_val = row[col]
            corrected_val = processed_df.loc[idx, f'{col}_CORRECTED']
            
            # Special handling for datetime columns
            if col in ['EST_PRN_RECEIVED_DATE']:
                original_normalized = normalize_datetime_for_comparison(original_val)
                corrected_normalized = normalize_datetime_for_comparison(corrected_val)
                
                # Only report as correction if the actual date changed
                if original_normalized != corrected_normalized:
                    comparison_data.append({
                        'Name': name,
                        'BAG_LOT_NO': bag_lot_no,
                        'Column': col,
                        'Original_Value': original_normalized,
                        'Corrected_Value': corrected_normalized
                    })
            else:
                # Regular string comparison for non-datetime columns
                original_str = safe_str(original_val)
                corrected_str = safe_str(corrected_val)
                
                if original_str != corrected_str:
                    comparison_data.append({
                        'Name': name,
                        'BAG_LOT_NO': bag_lot_no,
                        'Column': col,
                        'Original_Value': original_str,
                        'Corrected_Value': corrected_str
                    })
    
    return pd.DataFrame(comparison_data)

def create_excel_download(processed_df):
    """Create Excel file for download with production column aliases"""
    final_df = processed_df.copy()
    
    # Update corrected values
    final_df['OFFLOADING_TRUCK_ID'] = final_df['OFFLOADING_TRUCK_ID_CORRECTED']
    final_df['LIVE_CURRENT_ACTIVITY'] = final_df['LIVE_CURRENT_ACTIVITY_CORRECTED']
    final_df['LIVE_CURRENT_ACTIVITY_1'] = final_df['LIVE_CURRENT_ACTIVITY_1_CORRECTED']
    final_df['LIVE_CURRENT_ACTIVITY_2'] = final_df['LIVE_CURRENT_ACTIVITY_2_CORRECTED']
    final_df['ROUTE_BAG_ETA_CALC'] = final_df['ROUTE_BAG_ETA_CALC_CORRECTED']
    final_df['EST_PRN_RECEIVED_DATE'] = final_df['EST_PRN_RECEIVED_DATE_CORRECTED']
    final_df['EST_PRN_RECEIVE_DATE_GROUPED'] = final_df['EST_PRN_RECEIVE_DATE_GROUPED_CORRECTED']
    
    # Define column mapping: original_name -> production_alias
    column_mapping = {
        'name': 'BAG ID',
        'BAG_LOT_NO_BAG_MIRROR': 'KICO LOT NO',
        'MEGA_BAG_LOT_NO_BAG': 'MEGA TERMINAL LOT NO',
        'BAG_LOT_NO_BAG_MIRROR_FNL': 'ACTIVE LOT NO',
        'KICO_MINE_LOADING_MONTH_BAG': 'LOADING MONTH - KICO',
        'EXPORT_MINE_LOADING_MONTH_BAG': 'LOADING MONTH - EXPORT',
        'BAG_EXPORT_MONTH': 'DRC EXPORT MONTH',
        'BAG_PRN_MONTH': 'PORT RECEIVED MONTH',
        'TRUCK_TYPE_BAG_MIRROR': 'SHUNT / EXPORT',
        'SUB_BUYER_BAG_MIRROR': 'OFFTAKER',
        'TRUCK_LOADING_POINT_BAG_MIRROR': 'LOADING POINT',
        'LSP_NAME_BAG_MIRROR': 'LSP NAME',
        'ROUTE_TYPE_BAG_MIRROR': 'ROUTE TYPE',
        'BAG_FLAG_STATUS_UPL': 'BAG FLAG STATUS',
        'LIVE_CURRENT_ACTIVITY': 'CURRENT ACTIVITY',
        'LIVE_CURRENT_ACTIVITY_1': 'ACTIVITY CRITERIA 1',
        'LIVE_CURRENT_ACTIVITY_2': 'ACTIVITY CRITERIA 2',
        'ROUTE_BAG_ETA_CALC': 'ETA TO PORT',
        'EST_PRN_RECEIVED_DATE': 'ESTIMATED PORT WAREHOUSE RECEIVE DATE',
        'EST_PRN_RECEIVE_DATE_GROUPED': 'ESTIMATED PORT WAREHOUSE RECEIVE DATE (GROUPED)',
        'OFFLOADING_TRUCK_ID': 'OFFLOADING / CURRENT REG ID',
        'BAG_GROSS_WET_KG_INCL_SAMPLE_WMT': 'DRC DATA - GROSS WT INCL. SAMPLE (TONS)',
        'BAG_GROSS_EXCL_SAMPLE_WMT': 'DRC DATA - GROSS WEIGHT EXCL. SAMPLE (TONS)',
        'BAG_NET_EXCL_SAMPLE_WMT': 'DRC DATA - NET WEIGHT EXCL. SAMPLE (TONS)',
        'BAG_GROSS_WET_KG_INCL_SAMPLE_KG': 'DRC DATA - GROSS WT INCL. SAMPLE (KG)',
        'BAG_GROSS_EXCL_SAMPLE_KG': 'DRC DATA - GROSS WT EXCL. SAMPLE (KG)',
        'DRC DATA - NET WT EXCL. SAMPLE (KG)': 'DRC DATA - NET WT EXCL. SAMPLE (KG)',
        'MINE_LOADING_TS_BAG_MIRROR': 'DRC LOADED DATE',
        'MINE_EXIT_TS_BAG_MIRROR': 'MINE EXIT DATE',
        'BAG_EXPORT_TS': 'DRC EXPORT DATE',
        'ROUTE_CONSIGNEE_1_BAG_MIRROR': 'TRANSIT WAREHOUSE',
        'GRN_WH_GROSS_WEIGHT': 'ZM RECEIVING GW (KG)',
        'GRN_WH_NET_WEIGHT': 'ZM RECEIVING NET (KG)',
        'GRN_RECEIVED_DATE': 'ZM WHS RECEIVED DATE',
        'GDN_LOADED_DATE': 'ZM WHS LOADED DATE',
        'GDN_DISPATCH_DATE': 'ZM WHS DISPATCH DATE',
        'PRN_ARRIVAL_DATE': 'PORT WHS ARRIVAL DATE',
        'ROUTE_PORT_WAREHOUSE_BAG_MIRROR': 'INSTRUCTED PORT WHS',
        'PRN_WAREHOUSE_NAME_SCOPE_2': 'RECEIVED - PORT WAREHOUSE',
        'ROUTE_PORT_DESTINATION_BAG_MIRROR': 'INSTRUCTED PORT DESTINATION',
        'ROUTE_FINAL_DESTINATION_BAG_MIRROR': 'INSTRUCTED FINAL DESTINATION',
        'PRN_WH_GROSS_WEIGHT_SCOPE_2': 'PORT WHS GW (KG)',
        'PRN_WH_NET_WEIGHT': 'PORT WHS NET WT (KG)',
        'PRN_RECEIVED_DATE_SCOPE_2': 'PORT WHS RECEIVED DATE',
        'PDN_LOADED_DATE': 'PORT WHS LOADED DATE',
        'PDN_DISPATCH_DATE': 'PORT WHS DISPATCH DATE',
        'EXPORT_TRUCK_ID_BAG_MIRROR': 'EXPORT TRUCK ID',
        'SHUNT_TRUCK_ID_BAG_MIRROR': 'SHUNT TRUCK ID',
        'DRC_WAGON_ID_BAG_MIRROR': 'WAGON ID',
        'WG_TRAIN_NO_BAG_MIRROR': 'TRAIN NO',
        'ZAM_TRUCK_ID_BAG_MIRROR': 'ZAMBIA TRUCK ID',
        'BAG_SEAL_NO': 'DRC DATA - BAG SEAL NO',
        'DMS_APPVL_PROC_STATUS_AUTO_BAG_MIRROR': 'IVANHOE INVOICE STATUS',
        'FINAL_INCOTERM': 'FINAL INCOTERM',
        'STOCK_COMMENTS': 'DIARY OF EVENTS YYYY-MM-DD - (User Initial)'
    }
    
    # Columns to exclude from production output (calculation-only columns)
    exclude_columns = [
        'BAG_FLAG_STATUS_DETAIL',
        'MINE_LOADING_TS_EXPORT_BAG_MIRROR', 
        'LOADED_TRUCK_POLYTRA_ARRIVAL_TS_BAG_MIRROR',
        'LOADED_TRUCK_POLYTRA_EXIT_TS_BAG_MIRROR',
        'SHUNT_TRK_OFFL_TS_BAG_MIRROR',
        'GRN_WAREHOUSE_NAME',
        'PDN_BC_NUMBER',
        'PDN_VESSEL_NAME',
        # Remove temporary corrected columns
        'OFFLOADING_TRUCK_ID_CORRECTED',
        'LIVE_CURRENT_ACTIVITY_CORRECTED', 
        'LIVE_CURRENT_ACTIVITY_1_CORRECTED', 
        'LIVE_CURRENT_ACTIVITY_2_CORRECTED',
        'ROUTE_BAG_ETA_CALC_CORRECTED',
        'EST_PRN_RECEIVED_DATE_CORRECTED',
        'EST_PRN_RECEIVE_DATE_GROUPED_CORRECTED'
    ]
    
    # Remove excluded columns and ensure timezone-naive dates for Excel
    final_df = final_df.drop(columns=[col for col in exclude_columns if col in final_df.columns])
    
    # Convert any timezone-aware datetime columns to timezone-naive for Excel compatibility
    for col in final_df.columns:
        if final_df[col].dtype == 'datetime64[ns, UTC]' or final_df[col].dtype.name.startswith('datetime64[ns,'):
            final_df[col] = pd.to_datetime(final_df[col]).dt.tz_convert(None)
        elif final_df[col].dtype == 'object':
            # Check if column contains datetime-like strings and convert
            try:
                sample = final_df[col].dropna().iloc[0] if len(final_df[col].dropna()) > 0 else None
                if sample and isinstance(sample, str) and '+' in sample and ':' in sample:
                    # Likely timezone-aware datetime string
                    final_df[col] = pd.to_datetime(final_df[col], errors='ignore').dt.tz_convert(None) if pd.api.types.is_datetime64_any_dtype(pd.to_datetime(final_df[col], errors='coerce')) else final_df[col]
            except:
                pass
    
    # Rename columns to production aliases
    final_df = final_df.rename(columns=column_mapping)
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Active_Bag_Report')
    
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
                correction_rate = (len(comparison_df) / (len(df) * 7)) * 100 if len(df) > 0 else 0
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
                label="📊 Download Production Report (Excel)",
                data=excel_data,
                file_name="active_bag_report_corrected.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            # Preview
            if st.checkbox("Show corrected data preview"):
                st.markdown("### Corrected Data Preview")
                preview_df['OFFLOADING_TRUCK_ID'] = preview_df['OFFLOADING_TRUCK_ID_CORRECTED']
                preview_df['LIVE_CURRENT_ACTIVITY'] = preview_df['LIVE_CURRENT_ACTIVITY_CORRECTED']
                preview_df['LIVE_CURRENT_ACTIVITY_1'] = preview_df['LIVE_CURRENT_ACTIVITY_1_CORRECTED']
                preview_df['LIVE_CURRENT_ACTIVITY_2'] = preview_df['LIVE_CURRENT_ACTIVITY_2_CORRECTED']
                preview_df['ROUTE_BAG_ETA_CALC'] = preview_df['ROUTE_BAG_ETA_CALC_CORRECTED']
                preview_df['EST_PRN_RECEIVED_DATE'] = preview_df['EST_PRN_RECEIVED_DATE_CORRECTED']
                preview_df['EST_PRN_RECEIVE_DATE_GROUPED'] = preview_df['EST_PRN_RECEIVE_DATE_GROUPED_CORRECTED']
                
                preview_cols = ['name', 'BAG_LOT_NO_BAG_MIRROR', 'OFFLOADING_TRUCK_ID', 'LIVE_CURRENT_ACTIVITY', 'LIVE_CURRENT_ACTIVITY_1', 'LIVE_CURRENT_ACTIVITY_2', 'ROUTE_BAG_ETA_CALC', 'EST_PRN_RECEIVED_DATE', 'EST_PRN_RECEIVE_DATE_GROUPED']
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
st.markdown("*This application recalculates OFFLOADING_TRUCK_ID, LIVE_CURRENT_ACTIVITY columns, and ETA calculations based on business logic requirements.*")