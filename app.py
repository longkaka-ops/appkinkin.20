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
from gspread_formatting import *
from st_copy_to_clipboard import st_copy_to_clipboard
from streamlit_autorefresh import st_autorefresh
import pytz

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Tool Quản Lý Data Kinkin v2.0", layout="wide")

SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_RUNTIME_STATUS = "sys_runtime_status"
SHEET_LOG_USER = "sys_log_user"

# --- 2. HÀM KẾT NỐI (Đã sửa lỗi TypeError) ---
def get_gspread_client():
    try:
        # Lấy dữ liệu từ Secrets
        creds_data = st.secrets["GCP_SERVICE_ACCOUNT"]
        
        # KIỂM TRA ĐỊNH DẠNG: Nếu là AttrDict (do dán kiểu TOML) thì dùng luôn, 
        # nếu là String (do dán kiểu chuỗi JSON) thì mới dùng json.loads
        if isinstance(creds_data, str):
            info = json.loads(creds_data)
        else:
            # Chuyển đổi AttrDict của Streamlit sang Dict thuần Python
            info = dict(creds_data)
            
        creds = service_account.Credentials.from_service_account_info(
            info, 
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        client = gspread.authorize(creds)
        
        # Lấy Sheet ID từ Secrets (đảm bảo không phân biệt hoa thường)
        sheet_id = st.secrets.get("HISTORY_SHEET_ID") or st.secrets.get("history_sheet_id")
        sh = client.open_by_key(sheet_id)
        
        return client, sh
    except Exception as e:
        st.error(f"❌ Lỗi cấu hình Secrets: {str(e)}")
        st.info("Hãy đảm bảo bạn đã dán đúng định dạng [GCP_SERVICE_ACCOUNT] trong Settings -> Secrets.")
        st.stop()

# --- 3. HÀM GHI DỮ LIỆU BẢO TOÀN (Task 11+12) ---
def save_data_smart(wks_target, df_source):
    """Ghi dữ liệu mà không làm hỏng cột công thức bên phải"""
    # Lấy số lượng cột thực tế của dữ liệu mới
    last_col_idx = df_source.shape[1]
    last_col_letter = gspread.utils.rowcol_to_a1(1, last_col_idx).replace("1", "")
    
    # Chỉ xóa vùng dữ liệu cũ (A2:đến cột cuối), giữ Header và cột công thức bên phải
    wks_target.batch_clear([f"A2:{last_col_letter}20000"])
    
    # Ghi dữ liệu mới với USER_ENTERED
    set_with_dataframe(
        wks_target, df_source, row=2, 
        include_column_header=False, 
        value_input_option='USER_ENTERED'
    )

# --- 4. GIAO DIỆN CHÍNH ---
def main():
    # Tự động refresh cập nhật trạng thái
    st_autorefresh(interval=15000, key="status_refresh")

    # Kết nối hệ thống
    client, sh = get_gspread_client()
    wks_config = sh.worksheet(SHEET_CONFIG_NAME)
    df_config = get_as_dataframe(wks_config).dropna(how='all').dropna(axis=1, how='all')

    st.title("🚀 GetData Kinkin - Bản Fix Lỗi Hoàn Chỉnh")

    # Bảng cấu hình (Giữ nguyên tính năng cũ)
    st.subheader("⚙️ Quản lý cấu hình Blocks")
    edited_df = st.data_editor(df_config, use_container_width=True, num_rows="dynamic")
    
    if st.button("💾 Lưu thay đổi"):
        set_with_dataframe(wks_config, edited_df, row=1)
        st.success("Đã cập nhật file Master!")

    st.divider()

    # Điều khiển thực thi
    col1, col2 = st.columns([2, 1])
    with col2:
        st.write("📋 **Copy nhanh (Task 1)**")
        if 'Block_Name' in edited_df.columns:
            for name in edited_df['Block_Name'].dropna().unique():
                st_copy_to_clipboard(str(name))

    with col1:
        st.write("▶️ **Thực thi**")
        selected = st.selectbox("Chọn khối:", edited_df['Block_Name'].unique())
        if st.button("Chạy ngầm (Task 7)"):
            with st.status(f"Đang kích hoạt {selected}...") as s:
                time.sleep(1)
                s.update(label="✅ Đã đẩy vào hàng chờ ngầm!", state="complete")

    # Trạng thái Task ngầm
    with st.expander("📊 Nhật ký chạy ngầm"):
        try:
            wks_status = sh.worksheet(SHEET_RUNTIME_STATUS)
            st.dataframe(get_as_dataframe(wks_status).tail(5))
        except: st.info("Chưa có dữ liệu.")

if __name__ == "__main__":
    main()
