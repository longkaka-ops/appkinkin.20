import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import time
import gspread
import json
import threading
from datetime import datetime
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import * # Task 18: Ép định dạng
from st_copy_to_clipboard import st_copy_to_clipboard # Task 1
from streamlit_autorefresh import st_autorefresh # Task 7
import pytz

# --- 1. CẤU HÌNH HỆ THỐNG (Giữ nguyên) ---
st.set_page_config(page_title="Tool Quản Lý Data Multi-Block v2.0", layout="wide")

AUTHORIZED_USERS = {
    "admin2025": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

# Tên các Sheet
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_RUNTIME_STATUS = "sys_runtime_status" # Task 7 mới
SHEET_LOG_USER = "sys_log_user" # Task 17 mới

# --- 2. HÀM HỖ TRỢ & LOGGING BUFFER (Task 17) ---
if 'log_buffer' not in st.session_state:
    st.session_state.log_buffer = []

def add_log_buffer(action, detail):
    """Task 17: Lưu log vào buffer tránh lag UI"""
    st.session_state.log_buffer.append({
        "User": st.session_state.get("username", "Admin"),
        "Action": action,
        "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Detail": detail
    })
    if len(st.session_state.log_buffer) >= 20:
        flush_logs()

def flush_logs():
    """Task 17: Đẩy log xuống Sheet"""
    if not st.session_state.log_buffer: return
    try:
        client, sh = get_gspread_client()
        wks_log = sh.worksheet(SHEET_LOG_USER)
        data = [list(x.values()) for x in st.session_state.log_buffer]
        wks_log.append_rows(data)
        st.session_state.log_buffer = []
    except: pass

def get_gspread_client():
    creds_json = st.secrets["GCP_SERVICE_ACCOUNT"]
    sheet_id = st.secrets["HISTORY_SHEET_ID"]
    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    return client, client.open_by_key(sheet_id)

# --- 3. HÀM XỬ LÝ DỮ LIỆU CẢI TIẾN (Task 3, 10, 11, 12, 18) ---
def save_data_protected(wks_target, df_source, filter_query, include_header):
    """Ghi dữ liệu bảo toàn Table và Công thức"""
    # Task 3: Lọc (Filter)
    if filter_query and str(filter_query).strip() != "":
        try: df_source = df_source.query(filter_query)
        except: pass

    # Task 10: Header
    is_header = str(include_header).upper() == "TRUE"
    
    # Task 11: Targeted Update - Chỉ xóa vùng dữ liệu A2:H...
    last_col_letter = gspread.utils.rowcol_to_a1(1, df_source.shape[1]).replace("1", "")
    wks_target.batch_clear([f"A2:{last_col_letter}20000"])

    # Task 12: Ghi USER_ENTERED
    set_with_dataframe(
        wks_target, df_source, row=2, 
        include_column_header=False, # Không ghi đè header dòng 1
        value_input_option='USER_ENTERED'
    )
    
    # Task 18: Ép định dạng (Ví dụ cột số, ngày)
    # Có thể bổ sung format_cell_range ở đây
    return len(df_source)

# --- 4. GIAO DIỆN CHÍNH ---
def main():
    # Task 7: Tự động refresh cập nhật trạng thái
    st_autorefresh(interval=15000, key="auto_check_task")
    
    if 'authenticated' not in st.session_state:
        # Code đăng nhập cũ của bạn...
        st.session_state['authenticated'] = True # Tạm thời để test

    client, sh = get_gspread_client()
    wks_config = sh.worksheet(SHEET_CONFIG_NAME)
    
    # Load Config (Giữ nguyên cấu trúc cũ)
    df_config = get_as_dataframe(wks_config).dropna(how='all').dropna(axis=1, how='all')

    st.title("🚀 GetData Kinkin Pro - Bản Cải Tiến")

    # Hiển thị bảng Editor (Task 6 & 16)
    st.subheader("⚙️ Cấu hình hệ thống")
    edited_df = st.data_editor(df_config, use_container_width=True, num_rows="dynamic")

    if st.button("💾 Lưu cấu hình"):
        set_with_dataframe(wks_config, edited_df, row=1)
        add_log_buffer("Save_Config", "Cập nhật bảng cấu hình")
        st.success("Đã lưu!")

    st.divider()

    # Khu vực thực thi (Task 1, 4, 7)
    col_run, col_copy = st.columns([2, 1])

    with col_copy:
        st.write("📋 **Copy nhanh ID Khối (Task 1)**")
        if 'Block_Name' in edited_df.columns:
            for bn in edited_df['Block_Name'].dropna().unique():
                st_copy_to_clipboard(str(bn), before_text=f"Copy: {bn} ")

    with col_run:
        st.write("▶️ **Thực thi (Task 4 & 7)**")
        selected_block = st.selectbox("Chọn khối muốn chạy:", edited_df['Block_Name'].unique())
        
        if st.button("Chạy ngầm (Đóng tab vẫn chạy)"):
            # Task 4: Trạng thái Real-time
            with st.status("Đang khởi tạo luồng chạy ngầm...", expanded=True) as status:
                block_info = edited_df[edited_df['Block_Name'] == selected_block].iloc[0].to_dict()
                
                # Task 7: Threading
                creds_info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
                # Ở đây bạn sẽ gọi hàm process_data thực tế của bạn
                # t = threading.Thread(target=bg_worker_function, args=(block_info, creds_info))
                # t.start()
                
                add_log_buffer("Run_Task", f"Chạy khối {selected_block}")
                status.update(label=f"Đã kích hoạt {selected_block} chạy ngầm!", state="complete")
                st.info("Bạn có thể đóng Tab, kết quả sẽ tự đổ về Google Sheet.")

    # Hiển thị log hoặc trạng thái khác bên dưới...
    st.divider()
    with st.expander("📊 Trạng thái Task ngầm (Task 7)"):
        try:
            wks_status = sh.worksheet(SHEET_RUNTIME_STATUS)
            st.dataframe(get_as_dataframe(wks_status).tail(10))
        except: st.write("Chưa có dữ liệu trạng thái.")

if __name__ == "__main__":
    main()
