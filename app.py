import streamlit as st
import pandas as pd
import gspread
import json
import time
import threading
from datetime import datetime
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from st_copy_to_clipboard import st_copy_to_clipboard
from streamlit_autorefresh import st_autorefresh

# --- 1. CẤU HÌNH TÊN SHEET HỆ THỐNG ---
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_RUNTIME_STATUS = "sys_runtime_status"
SHEET_LOG_USER = "sys_log_user"

st.set_page_config(page_title="GetData Kinkin Pro v2.0", layout="wide")

# --- 2. HÀM KẾT NỐI VÀ TỰ KHỞI TẠO ---
def get_gspread_client():
    try:
        # Xử lý Secrets (Chấp nhận cả String và Dict)
        creds_data = st.secrets["GCP_SERVICE_ACCOUNT"]
        info = dict(creds_data) if not isinstance(creds_data, str) else json.loads(creds_data)
        
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        )
        client = gspread.authorize(creds)
        sheet_id = st.secrets.get("HISTORY_SHEET_ID") or st.secrets.get("history_sheet_id")
        sh = client.open_by_key(sheet_id)
        return client, sh
    except Exception as e:
        st.error(f"❌ Lỗi kết nối: {str(e)}")
        st.stop()

def initialize_sheets(sh):
    """Tự động tạo các sheet nếu chưa tồn tại"""
    existing_sheets = [w.title for w in sh.worksheets()]
    
    # 1. Tạo sheet cấu hình chính
    if SHEET_CONFIG_NAME not in existing_sheets:
        headers = [
            "Block_Name", "Trạng thái", "Vùng lấy dữ liệu", "Tháng", 
            "Link file nguồn", "Sheet nguồn", "Link dữ liệu đích", 
            "Tên sheet dữ liệu đích", "Dòng dữ liệu", "Kết quả", 
            "Tần_suất_Phút", "Điều_kiện_lọc", "Lấy_tiêu_đề", "Ghi_chú", "ID_Dòng"
        ]
        wks = sh.add_worksheet(title=SHEET_CONFIG_NAME, rows="100", cols="20")
        wks.append_row(headers)
        st.success(f"✅ Đã tự động tạo sheet: {SHEET_CONFIG_NAME}")

    # 2. Tạo sheet trạng thái chạy ngầm (Task 7)
    if SHEET_RUNTIME_STATUS not in existing_sheets:
        headers = ["Block_ID", "Status", "Message", "Last_Update"]
        sh.add_worksheet(title=SHEET_RUNTIME_STATUS, rows="1000", cols="5")
        st.success(f"✅ Đã tự động tạo sheet: {SHEET_RUNTIME_STATUS}")

    # 3. Tạo sheet Log người dùng (Task 17)
    if SHEET_LOG_USER not in existing_sheets:
        headers = ["User", "Action", "Time", "Detail"]
        sh.add_worksheet(title=SHEET_LOG_USER, rows="5000", cols="5")
        st.success(f"✅ Đã tự động tạo sheet: {SHEET_LOG_USER}")

# --- 3. GIAO DIỆN CHÍNH ---
def main():
    st_autorefresh(interval=20000, key="global_refresh")
    client, sh = get_gspread_client()
    
    # Tự động kiểm tra và tạo sheet nếu thiếu
    initialize_sheets(sh)

    wks_config = sh.worksheet(SHEET_CONFIG_NAME)
    df_config = get_as_dataframe(wks_config).dropna(how='all').dropna(axis=1, how='all')

    st.title("🚀 Kinkin Automation - Hệ thống đã sẵn sàng")

    # Hiển thị bảng Editor để người dùng nhập liệu lần đầu
    st.subheader("⚙️ Quản lý Cấu hình (Auto-Sync)")
    edited_df = st.data_editor(df_config, use_container_width=True, num_rows="dynamic")
    
    if st.button("💾 Lưu và Cập nhật Master"):
        # Task 11: Targeted Update (Xóa vùng dữ liệu cũ dòng 2 trở đi)
        last_col = gspread.utils.rowcol_to_a1(1, edited_df.shape[1]).replace("1", "")
        wks_config.batch_clear([f"A2:{last_col}5000"])
        set_with_dataframe(wks_config, edited_df, row=1)
        st.success("Đã lưu dữ liệu vào Google Sheet!")

    st.divider()

    # Điều khiển thực thi
    col_run, col_copy = st.columns([2, 1])
    with col_copy:
        st.write("📋 **Copy nhanh ID (Task 1)**")
        if 'Block_Name' in df_config.columns:
            for val in df_config['Block_Name'].dropna().unique():
                st_copy_to_clipboard(str(val))

    with col_run:
        st.write("▶️ **Thực thi luồng**")
        if not df_config.empty and 'Block_Name' in df_config.columns:
            selected = st.selectbox("Chọn khối:", df_config['Block_Name'].unique())
            if st.button("Kích hoạt chạy ngầm (Task 7)"):
                st.info(f"Đã gửi lệnh chạy khối {selected} vào hàng đợi.")
        else:
            st.warning("Vui lòng thêm dữ liệu vào bảng cấu hình trước.")

    # Hiển thị log
    with st.expander("📊 Nhật ký hệ thống"):
        wks_status = sh.worksheet(SHEET_RUNTIME_STATUS)
        st.dataframe(get_as_dataframe(wks_status).tail(10), use_container_width=True)

if __name__ == "__main__":
    main()
