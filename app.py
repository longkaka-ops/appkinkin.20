import streamlit as st
import pandas as pd
import gspread
import threading
import time
import uuid
from datetime import datetime
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import *
from google.oauth2 import service_account
from st_copy_to_clipboard import st_copy_to_clipboard
from streamlit_autorefresh import st_autorefresh

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="GetData Kinkin Pro", layout="wide")

# Google Sheet ID (Thay bằng ID của bạn hoặc dùng secrets)
HISTORY_SHEET_ID = st.secrets.get("HISTORY_SHEET_ID", "YOUR_SHEET_ID_HERE")
GCP_JSON = st.secrets.get("GCP_SERVICE_ACCOUNT")

# Tên các Sheet hệ thống
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_RUNTIME_STATUS = "sys_runtime_status"
SHEET_LOG_USER = "sys_log_user"

# --- 2. HÀM KẾT NỐI ---
def get_gspread_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

# --- 3. TASK 17: LOGGING BUFFER (NEAR REAL-TIME) ---
if 'log_buffer' not in st.session_state:
    st.session_state.log_buffer = []

def add_log(action, detail):
    log_entry = {
        "User": st.session_state.get("username", "Admin"),
        "Action": action,
        "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Detail": detail
    }
    st.session_state.log_buffer.append(log_entry)
    # Task 17: Flush khi đủ 20 dòng
    if len(st.session_state.log_buffer) >= 20:
        flush_logs()

def flush_logs():
    if not st.session_state.log_buffer: return
    try:
        client = get_gspread_client()
        sh = client.open_by_key(HISTORY_SHEET_ID)
        wks_log = sh.worksheet(SHEET_LOG_USER)
        data = [list(x.values()) for x in st.session_state.log_buffer]
        wks_log.append_rows(data)
        st.session_state.log_buffer = []
    except: pass

# --- 4. TASK 11+12: GHI BẢO TOÀN TABLE (TARGETED UPDATE) ---
def targeted_update(wks_target, df_source, filter_query, include_header):
    # Task 3: Filter
    if filter_query:
        try: df_source = df_source.query(filter_query)
        except: pass
    
    # Task 10: Header
    show_header = str(include_header).upper() == "TRUE"
    if not show_header:
        df_source = df_source.iloc[1:]

    # Task 11: Chỉ xóa vùng dữ liệu (A2:H...)
    last_col = gspread.utils.rowcol_to_a1(1, df_source.shape[1]).replace("1", "")
    wks_target.batch_clear([f"A2:{last_col}10000"])

    # Task 12: Ghi USER_ENTERED
    set_with_dataframe(wks_target, df_source, row=2, include_column_header=False, value_input_option='USER_ENTERED')
    return len(df_source)

# --- 5. TASK 7: CHẠY NGẦM (THREADING) ---
def background_worker(block_data, creds_info):
    # Hàm này chạy trong thread riêng, đóng tab vẫn chạy
    client = gspread.authorize(service_account.Credentials.from_service_account_info(creds_info))
    sh = client.open_by_key(HISTORY_SHEET_ID)
    wks_status = sh.worksheet(SHEET_RUNTIME_STATUS)
    
    block_id = block_data['ID_Dòng']
    # Cập nhật status: Running
    wks_status.append_row([block_id, "Running", "Đang xử lý...", datetime.now().isoformat()])
    
    try:
        # Giả lập logic lấy data (Bạn sẽ thay bằng logic gọi file nguồn thực tế)
        time.sleep(10) 
        # Cập nhật thành công
        wks_status.append_row([block_id, "Success", "Hoàn tất 100%", datetime.now().isoformat()])
    except Exception as e:
        wks_status.append_row([block_id, "Failed", str(e), datetime.now().isoformat()])

# --- 6. GIAO DIỆN CHÍNH (UI) ---
def main():
    st.title("🚀 Kinkin Data Automation Pro")
    
    # Auto-refresh mỗi 10s để check status chạy ngầm (Task 7)
    st_autorefresh(interval=10000, key="status_check")

    # Sidebar: Login & Tools
    with st.sidebar:
        st.header("Cấu hình & Nhật ký")
        if st.button("💾 Lưu Log ngay (Flush)"):
            flush_logs()
            st.success("Đã đẩy log!")

    # Tab quản lý
    tab_config, tab_monitor = st.tabs(["⚙️ Cấu hình Khối", "📊 Giám sát Task ngầm"])

    with tab_config:
        # Task 1 & 6: Hiển thị bảng cấu hình với tính năng sửa và copy
        st.subheader("Danh sách Khối Dữ liệu")
        
        # Giả lập đọc dữ liệu từ Sheet
        # df_config = load_config_from_gsheet() 
        df_sample = pd.DataFrame([
            {"ID_Dòng": "BK001", "Block_Name": "Doanh Thu HN", "Tần_suất_Phút": 15, "Link file nguồn": "https://..."},
            {"ID_Dòng": "BK002", "Block_Name": "Chi Phí HCM", "Tần_suất_Phút": 0, "Link file nguồn": "https://..."}
        ])

        col1, col2 = st.columns([4, 1])
        with col1:
            edited_df = st.data_editor(df_sample, use_container_width=True, num_rows="dynamic")
        
        with col2:
            st.write("📋 Copy ID nhanh")
            for id_val in df_sample["ID_Dòng"]:
                st_copy_to_clipboard(id_val, before_text=f"ID {id_val}: ")

        # Nút Chạy Task (Task 7)
        if st.button("▶️ Chạy Khối được chọn"):
            with st.status("Đang khởi tạo Task ngầm...", expanded=True) as status:
                add_log("Run_Task", f"Khởi chạy khối {df_sample['Block_Name'][0]}")
                
                # Khởi tạo Thread
                creds_info = json.loads(GCP_JSON)
                t = threading.Thread(target=background_worker, args=(df_sample.iloc[0].to_dict(), creds_info))
                t.start()
                
                status.update(label="Task đã được đẩy vào chạy ngầm. Bạn có thể đóng tab!", state="complete")
                st.info("Hệ thống đang xử lý dưới nền. Kết quả sẽ cập nhật trong tab Giám sát.")

    with tab_monitor:
        st.subheader("Trạng thái tiến trình (Task 7)")
        # Đọc từ SHEET_RUNTIME_STATUS và hiển thị
        st.info("Dữ liệu ở đây tự động cập nhật mỗi 10 giây từ Google Sheet hệ thống.")
        # st.table(load_runtime_status())

if __name__ == "__main__":
    main()
