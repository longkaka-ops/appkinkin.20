import streamlit as st
import pandas as pd
import time
import gspread
import json
import re
import pytz
import uuid
import numpy as np
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from datetime import datetime
from google.oauth2 import service_account
from collections import defaultdict
from st_copy_to_clipboard import st_copy_to_clipboard

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Kinkin Manager (V22 - Standard)", layout="wide", page_icon="🛡️")

AUTHORIZED_USERS = {
    "admin2025": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

# Tên Sheet
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_ACTIVITY_NAME = "log_hanh_vi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"
SHEET_NOTE_NAME = "database_ghi_chu"

# Cột Config
COL_BLOCK_NAME = "Block_Name"
COL_STATUS = "Trạng thái"
COL_SRC_LINK = "Link dữ liệu lấy dữ liệu"
COL_TGT_LINK = "Link dữ liệu đích"
COL_SRC_SHEET = "Tên sheet nguồn dữ liệu gốc"
COL_TGT_SHEET = "Tên sheet dữ liệu đích"
COL_DATA_RANGE = "Vùng lấy dữ liệu"
COL_MONTH = "Tháng"
COL_RESULT = "Kết quả"
COL_LOG_ROW = "Dòng dữ liệu"
# Cột Tính Năng
COL_FILTER = "Dieu_Kien_Loc"      
COL_HEADER = "Lay_Header"         
COL_MODE = "Che_Do_Ghi"           
COL_COPY_FLAG = "Copy_Flag" 

# Cột Note
NOTE_COL_ID = "ID"
NOTE_COL_BLOCK = "Tên Khối"
NOTE_COL_CONTENT = "Nội dung Note"

DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- 2. HÀM HỖ TRỢ ---
def col_name_to_index(col_name):
    col_name = col_name.upper()
    index = 0
    for char in col_name:
        index = index * 26 + (ord(char) - ord('A')) + 1
    return index - 1

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

def get_creds():
    raw_creds = st.secrets["gcp_service_account"]
    if isinstance(raw_creds, str):
        try: creds_info = json.loads(raw_creds)
        except: return None
    else: creds_info = dict(raw_creds)
    if "private_key" in creds_info: creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)

def get_sh_with_retry(creds, sheet_id_or_key):
    gc = gspread.authorize(creds)
    max_retries = 3
    for i in range(max_retries):
        try: return gc.open_by_key(sheet_id_or_key)
        except Exception as e:
            if i == max_retries - 1: raise e
            time.sleep((2 ** i) + 0.5) 
    return None

# --- LOG HÀNH VI ---
def log_user_action(creds, user_id, action, status=""):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_ACTIVITY_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_ACTIVITY_NAME, rows=1000, cols=4)
            wks.append_row(["Thời gian", "Người dùng", "Hành vi", "Trạng thái (Chi tiết)"])
        
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")
        wks.append_row([time_now, user_id, action, status])
    except Exception as e: print(f"Lỗi log: {e}")

def fetch_activity_logs(creds, limit=50):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_ACTIVITY_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df.empty: return pd.DataFrame()
        return df.tail(limit).iloc[::-1]
    except: return pd.DataFrame()

# --- SOI CHI TIẾT THAY ĐỔI ---
def row_to_string(row):
    cols = [COL_SRC_LINK, COL_TGT_LINK, COL_SRC_SHEET, COL_TGT_SHEET, COL_FILTER, COL_MODE]
    vals = [str(row.get(c, '')).strip().replace('nan', '') for c in cols]
    return "|".join(vals)

def format_full_row_info(row):
    info = []
    key_cols = [
        (COL_SRC_LINK, "Link Nguồn"), (COL_TGT_LINK, "Link Đích"),
        (COL_SRC_SHEET, "Sheet Nguồn"), (COL_TGT_SHEET, "Sheet Đích"),
        (COL_DATA_RANGE, "Range"), (COL_FILTER, "Filter"), (COL_MODE, "Mode")
    ]
    for col, label in key_cols:
        val = str(row.get(col, '')).strip().replace('nan', '')
        if val: info.append(f"{label}='{val}'")
    return ", ".join(info)

def detect_changes_detailed(df_old, df_new):
    changes = []
    old_records = df_old.to_dict('records')
    new_records = df_new.to_dict('records')
    
    if len(old_records) == len(new_records):
        for i in range(len(old_records)):
            r_old = old_records[i]; r_new = new_records[i]
            diffs = []
            cols_check = [COL_SRC_LINK, COL_TGT_LINK, COL_SRC_SHEET, COL_TGT_SHEET, COL_FILTER, COL_MODE, COL_DATA_RANGE, COL_STATUS, COL_MONTH, COL_HEADER]
            for col in cols_check:
                v_old = str(r_old.get(col, '')).strip().replace('nan', '')
                v_new = str(r_new.get(col, '')).strip().replace('nan', '')
                if v_old != v_new:
                    if len(v_old) > 20: v_old = "..." + v_old[-10:]
                    if len(v_new) > 20: v_new = "..." + v_new[-10:]
                    diffs.append(f"{col}: {v_old} -> {v_new}")
            if diffs: changes.append(f"✏️ Sửa dòng {i+1}: {'; '.join(diffs)}")
    else:
        new_sigs = [row_to_string(r) for r in new_records]
        for i, r_old in enumerate(old_records):
            sig_old = row_to_string(r_old)
            if sig_old not in new_sigs:
                full_info = format_full_row_info(r_old)
                changes.append(f"❌ Đã xóa dòng (STT {i+1} cũ): [{full_info}]")
        old_sigs = [row_to_string(r) for r in old_records]
        for i, r_new in enumerate(new_records):
            sig_new = row_to_string(r_new)
            if sig_new not in old_sigs:
                changes.append(f"➕ Thêm dòng mới tại vị trí {i+1}")

    if not changes: return "Lưu cấu hình (Không có thay đổi nội dung)"
    return "\n".join(changes)

# --- 4. HỆ THỐNG LOCK AN TOÀN ---
def get_system_lock_status(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
            wks.update([["is_locked", "user", "time_start"], ["FALSE", "", ""]])
            return False, "", ""
        val = wks.cell(2, 1).value
        user = wks.cell(2, 2).value
        time_str = wks.cell(2, 3).value
        if val == "TRUE":
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 300: return False, "", ""
            except: pass
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def acquire_lock(creds, user_id):
    is_locked, locking_user, t = get_system_lock_status(creds)
    if is_locked and locking_user != user_id:
        return False
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_LOCK_NAME)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]])
        return True
    except: return False

def release_lock(creds, user_id):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_LOCK_NAME)
        val = wks.cell(2, 2).value
        if val == user_id:
            wks.update("A2:C2", [["FALSE", "", ""]])
    except: pass

# --- LOGIN ---
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True
            st.session_state['current_user_id'] = AUTHORIZED_USERS[key]
            return True
    if st.session_state['logged_in']: return True
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.header("🛡️ Đăng nhập hệ thống")
        pwd = st.text_input("Nhập mật khẩu:", type="password")
        if st.button("Đăng Nhập", use_container_width=True):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True
                st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]
                log_user_action(get_creds(), AUTHORIZED_USERS[pwd], "Đăng nhập", "OK")
                st.rerun()
            else: st.error("Sai mật khẩu!")
    return False

# --- 3. QUẢN LÝ NOTE ---
def load_notes_data(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_NOTE_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_NOTE_NAME, rows=100, cols=5)
            wks.append_row([NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT])
            return pd.DataFrame(columns=[NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT])
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df.empty or NOTE_COL_ID not in df.columns: return pd.DataFrame(columns=[NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT])
        return df.dropna(how='all')
    except: return pd.DataFrame(columns=[NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT])

def save_notes_data(df_notes, creds, user_id):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_NOTE_NAME)
        if not df_notes.empty:
            for idx, row in df_notes.iterrows():
                if not row[NOTE_COL_ID] or str(row[NOTE_COL_ID]) == 'nan' or str(row[NOTE_COL_ID]) == '':
                    df_notes.at[idx, NOTE_COL_ID] = str(uuid.uuid4())[:8]
        cols = [NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT]
        for c in cols:
            if c not in df_notes.columns: df_notes[c] = ""
        df_notes = df_notes[cols]
        wks.clear(); wks.update([df_notes.columns.tolist()] + df_notes.astype(str).values.tolist())
        log_user_action(creds, user_id, "Cập nhật Note (Popup)", "Thành công")
        return True
    except Exception as e: st.error(f"Lỗi: {e}"); return False

# --- 4. CORE ETL (ĐÃ CẬP NHẬT 3 CỘT CHUẨN) ---
def fetch_data_v2(row_config, creds):
    link_src = str(row_config.get(COL_SRC_LINK, '')).strip()
    source_label = str(row_config.get(COL_SRC_SHEET, '')).strip()
    month_val = str(row_config.get(COL_MONTH, ''))
    data_range_str = str(row_config.get(COL_DATA_RANGE, 'Lấy hết')).strip()
    filter_query = str(row_config.get(COL_FILTER, '')).strip()
    include_header = str(row_config.get(COL_HEADER, 'TRUE')).strip().upper() == 'TRUE'

    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    
    df = None
    try:
        sh_source = get_sh_with_retry(creds, sheet_id)
        if source_label:
            try: wks_source = sh_source.worksheet(source_label)
            except: return None, sheet_id, f"❌ Không tìm thấy sheet: '{source_label}'"
        else: wks_source = sh_source.sheet1
            
        data = wks_source.get_all_values()
        if data and len(data) > 0:
            if include_header:
                headers = data[0]; rows = data[1:]
                df = pd.DataFrame(rows, columns=headers)
            else:
                df = pd.DataFrame(data)
            
            # Lọc vùng dữ liệu
            if data_range_str != "Lấy hết" and ":" in data_range_str:
                try:
                    start_col_str, end_col_str = data_range_str.split(":")
                    start_idx = col_name_to_index(start_col_str.strip())
                    end_idx = col_name_to_index(end_col_str.strip())
                    if start_idx >= 0:
                        end_idx = min(end_idx, len(df.columns) - 1)
                        df = df.iloc[:, start_idx : end_idx + 1]
                except: pass

            # Lọc điều kiện
            if filter_query and filter_query.lower() not in ['nan', '']:
                try: df = df.query(filter_query)
                except Exception as e: return None, sheet_id, f"⚠️ Lỗi lọc: {e}"

            df = df.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
            status_msg = "Thành công"
        else:
            status_msg = "Sheet trắng tinh"
            df = pd.DataFrame()
    except Exception as e: return None, sheet_id, f"Lỗi tải: {str(e)}"

    if df is not None:
        # [CHUẨN HÓA] Thêm 3 cột theo yêu cầu: Link file nguồn, Sheet nguồn, Tháng
        df['Link file nguồn'] = link_src
        df['Sheet nguồn'] = source_label
        df['Tháng'] = month_val
        return df, sheet_id, status_msg
    return None, sheet_id, "Không lấy được dữ liệu"

def write_smart_v2(tasks_list, target_link, target_sheet_name, creds, write_mode="APPEND"):
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi"
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        # Gộp tất cả data lại
        final_df_list = []
        for df, src_link in tasks_list:
            final_df_list.append(df)
        
        if not final_df_list: return True, "Không có data mới"
        combined_df = pd.concat(final_df_list, ignore_index=True)
        
        # --- [SAFE WRITE LOGIC] ---
        # 1. Tính toán vùng cần xóa (Chỉ xóa cột A đến cột cuối cùng của Data mới)
        num_cols = len(combined_df.columns)
        if num_cols == 0: return True, "Data rỗng"
        
        last_col_char = gspread.utils.rowcol_to_a1(1, num_cols).replace("1", "") # VD: Z
        
        # 2. Xóa dữ liệu cũ (Chỉ trong phạm vi cột A -> last_col_char)
        # Để lại hàng 1 (Header) nếu muốn append, hoặc xóa hết nếu table
        # Ở đây ta dùng chiến thuật: Xóa hết vùng data cũ trong phạm vi cột, sau đó ghi đè.
        
        if write_mode == "TABLE":
            # Chế độ TABLE: Xóa từ A2 đến hết, giữ Header dòng 1
            # Nhưng để an toàn cho ct bên cạnh, ta chỉ clear vùng A2:Z
            try: wks.batch_clear([f"A2:{last_col_char}"]) 
            except: pass
            
            # Ghi đè từ A2 (Bỏ header vì header đã có hoặc giữ nguyên)
            # Nếu muốn ghi cả header mới, thì clear A1. Nhưng thường TABLE giữ header cũ.
            # Để an toàn nhất: Ghi đè toàn bộ từ A1 (nhưng cẩn thận CT)
            # Theo yêu cầu: "từ AA trở đi giữ nguyên".
            
            # Cách tốt nhất: Clear A:Z. Ghi đè A:Z.
            # Lưu ý: wks.clear() xóa cả sheet -> SAI.
            
            # Thực hiện:
            # B1. Lấy hết data cũ để biết dòng cuối
            # B2. Clear A2:{Col_Z}{Max_Row}
            # B3. Ghi từ A2
            
            # Đơn giản hóa: Clear range lớn ước lượng, vd A2:Z5000
            try: wks.batch_clear([f"A2:{last_col_char}10000"]) 
            except: pass
            
            # Ghi data (Không header) vào A2
            set_with_dataframe(wks, combined_df, row=2, col=1, include_index=False, include_column_header=False)
            return True, f"Đã làm mới Table ({len(combined_df)} dòng)"

        else: # APPEND (Mặc định)
            # APPEND cũng phải thông minh: Xóa dòng cũ của Link Nguồn này, rồi Append xuống cuối
            # Nhưng để đơn giản và an toàn theo yêu cầu "như code gốc":
            # Ta dùng append_rows. Nhưng append_rows ghi xuống dòng trống cuối cùng.
            
            # Logic "Xóa dòng cũ của Link này" hơi phức tạp nếu không load hết về.
            # Nếu user chấp nhận Append nối đuôi:
            
            # Ở đây tôi sẽ dùng set_with_dataframe ghi đè từ dòng cuối hiện có + 1
            # Nhưng cần đảm bảo không ghi sang cột AA.
            
            # Lấy dòng cuối hiện tại của cột A
            # (Giả sử cột A luôn có dữ liệu)
            col_a = wks.col_values(1)
            next_row = len(col_a) + 1
            
            set_with_dataframe(wks, combined_df, row=next_row, col=1, include_index=False, include_column_header=False)
            return True, f"Append thành công (+{len(combined_df)} dòng)"

    except Exception as e: return False, f"Lỗi Ghi: {str(e)}"

# --- SYSTEM LOGS ---
def write_detailed_log(creds, log_data_list):
    if not log_data_list: return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=15)
            wks.append_row(["Thời gian", "Vùng lấy", "Tháng", "User", "Link Nguồn", "Link Đích", "Sheet Đích", "Sheet Nguồn", "Kết Quả", "Số Dòng", "Range", "Block"])
        wks.append_rows(log_data_list)
    except: pass

def load_sys_schedule(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_SYS_CONFIG)
        except: 
            wks = sh.add_worksheet(SHEET_SYS_CONFIG, rows=20, cols=5)
            wks.append_row([COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if COL_BLOCK_NAME not in df.columns: return pd.DataFrame(columns=[COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
        return df.dropna(how='all')
    except: return pd.DataFrame(columns=[COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])

def save_sys_schedule(df_schedule, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_SYS_CONFIG)
    wks.clear()
    wks.update([df_schedule.columns.tolist()] + df_schedule.fillna('').values.tolist())

# --- PIPELINE (LỌC TRẠNG THÁI) ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi"
    try:
        get_sh_with_retry(creds, sheet_id)
        return True, "OK"
    except Exception as e: return False, f"Lỗi: {e}"

def check_permissions_strict(rows_to_run, creds):
    errs = []
    checked = {} 
    for row in rows_to_run:
        for col_type in [COL_SRC_LINK, COL_TGT_LINK]:
            link = str(row.get(col_type, '')).strip()
            if "docs.google.com" in link:
                if link not in checked: checked[link] = verify_access_fast(link, creds)
                if not checked[link][0]: errs.append(f"❌ Lỗi quyền ({col_type}): {checked[link][1]} -> {link}")
    return (len(errs) == 0), errs

def process_pipeline_mixed(rows_to_run, user_id, block_name_run, status_container=None):
    creds = get_creds()
    if not acquire_lock(creds, user_id):
        return False, f"HỆ THỐNG ĐANG BẬN! Vui lòng thử lại sau.", 0
    
    log_user_action(creds, user_id, f"Chạy Job: {block_name_run}", "Đang chạy...")
    try:
        if status_container: status_container.write("🔄 Đang phân nhóm dữ liệu...")
        grouped_tasks = defaultdict(list)
        
        # [QUAN TRỌNG] Chỉ xử lý dòng "Chưa chốt & đang cập nhật"
        # Mặc dù UI đã lọc, ta lọc lại lần nữa ở đây cho chắc chắn
        valid_rows = [r for r in rows_to_run if str(r.get(COL_STATUS, '')).strip() == "Chưa chốt & đang cập nhật"]
        
        if not valid_rows:
             return True, {}, 0 # Không có gì để chạy

        for row in valid_rows:
            t_link = str(row.get(COL_TGT_LINK, '')).strip()
            t_sheet = str(row.get(COL_TGT_SHEET, '')).strip()
            mode = str(row.get(COL_MODE, 'APPEND')).strip().upper()
            grouped_tasks[(t_link, t_sheet, mode)].append(row)
        
        global_results_map = {} 
        all_success = True; log_entries = []
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")
        total_rows_all = 0
        
        for idx, ((target_link, target_sheet, write_mode), group_rows) in enumerate(grouped_tasks.items()):
            if status_container: status_container.write(f"⏳ Xử lý nhóm {idx+1}/{len(grouped_tasks)}: ...{target_link[-10:]}")
            tasks_list = []
            for row in group_rows:
                s_link = row.get(COL_SRC_LINK, '')
                df, sid, msg = fetch_data_v2(row, creds)
                if df is not None:
                    tasks_list.append((df, s_link)); total_rows_all += len(df)
                    if status_container: status_container.write(f"   + Lấy {len(df)} dòng: {row.get(COL_SRC_SHEET)}")
                else:
                    global_results_map[s_link] = ("Lỗi tải", "")
                    log_entries.append([time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH), user_id, s_link, target_link, target_sheet, row.get(COL_SRC_SHEET), "Lỗi tải", "0", "", block_name_run])
            
            success_update, msg_update = False, "No Data"
            if tasks_list:
                success_update, msg_update = write_smart_v2(tasks_list, target_link, target_sheet, creds, write_mode)
                if not success_update: all_success = False
            
            status_str = "Thành công" if success_update else f"Lỗi: {msg_update}"
            for row in group_rows:
                s_link = str(row.get(COL_SRC_LINK, '')).strip()
                global_results_map[s_link] = (status_str, msg_update)
                cnt = 0
                for d, l in tasks_list:
                    if l == s_link: cnt = len(d)
                log_entries.append([time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH), user_id, s_link, target_link, target_sheet, row.get(COL_SRC_SHEET), status_str, str(cnt), "", block_name_run])
        
        write_detailed_log(creds, log_entries)
        log_user_action(creds, user_id, f"Hoàn tất Job: {block_name_run}", f"Tổng {total_rows_all} dòng")
        return all_success, global_results_map, total_rows_all
    finally:
        release_lock(creds, user_id)

# --- 7. QUẢN LÝ CONFIG (SAFE MODE) ---

@st.cache_data
def load_full_config(_creds):
    sh = get_sh_with_retry(_creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    if df.empty:
        return pd.DataFrame(columns=[COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE])
    df = df.dropna(how='all')
    required_cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE]
    for c in required_cols:
        if c not in df.columns: df[c] = ""
    
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    df[COL_MODE] = df[COL_MODE].replace('', 'APPEND').fillna('APPEND')
    df[COL_HEADER] = df[COL_HEADER].replace('', 'TRUE').fillna('TRUE')
    
    df = df[required_cols]
    return df

def delete_block_direct(block_name_to_delete, creds, user_id):
    if not acquire_lock(creds, user_id):
        st.error("Hệ thống đang bận, vui lòng thử lại!"); return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        if COL_BLOCK_NAME not in df_server.columns: return
        df_new = df_server[df_server[COL_BLOCK_NAME] != block_name_to_delete]
        
        cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE]
        for c in cols:
            if c not in df_new.columns: df_new[c] = ""
        wks.clear(); wks.update([cols] + df_new[cols].values.tolist())
        log_user_action(creds, user_id, f"Xóa khối: {block_name_to_delete}", "Thành công")
    finally:
        release_lock(creds, user_id)

def save_block_config_to_sheet(df_current_ui, current_block_name, creds, user_id):
    if not acquire_lock(creds, user_id):
        st.error("Hệ thống đang bận (có người khác đang lưu), vui lòng đợi giây lát!"); return

    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        
        if df_server.empty and len(df_current_ui) > 0:
            st.error("⚠️ Lỗi đọc dữ liệu Server. Đã chặn lưu đè để bảo vệ dữ liệu."); return

        if COL_BLOCK_NAME not in df_server.columns: df_server[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
        
        df_server_old_block = df_server[df_server[COL_BLOCK_NAME] == current_block_name].copy().reset_index(drop=True)
        df_other = df_server[df_server[COL_BLOCK_NAME] != current_block_name]
        
        df_save = df_current_ui.copy().reset_index(drop=True)
        for c in ['STT', COL_COPY_FLAG]: 
            if c in df_save.columns: df_save = df_save.drop(columns=[c])
        df_save[COL_BLOCK_NAME] = current_block_name
        
        # --- LOG CHI TIẾT ---
        detail_log = detect_changes_detailed(df_server_old_block, df_save)
        log_user_action(creds, user_id, f"Sửa cấu hình: {current_block_name}", detail_log)
        
        df_final = pd.concat([df_other, df_save], ignore_index=True).astype(str).replace(['nan', 'None'], '')
        cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE]
        for c in cols:
            if c not in df_final.columns: df_final[c] = ""
        wks.clear(); wks.update([cols] + df_final[cols].values.tolist())
        st.toast(f"✅ Đã lưu cấu hình: {current_block_name}!", icon="💾")
    finally:
        release_lock(creds, user_id)

def rename_block_action(old_name, new_name, creds, user_id):
    if not acquire_lock(creds, user_id): return False
    try:
        if not new_name or new_name == old_name: return False
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if COL_BLOCK_NAME in df.columns:
            df.loc[df[COL_BLOCK_NAME] == old_name, COL_BLOCK_NAME] = new_name
            wks.clear(); wks.update([df.columns.tolist()] + df.fillna('').values.tolist())
        log_user_action(creds, user_id, f"Đổi tên: {old_name} -> {new_name}", "Thành công")
        return True
    finally:
        release_lock(creds, user_id)

def save_full_direct(df_full, creds, user_id):
    if not acquire_lock(creds, user_id): return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE]
        df_full = df_full.astype(str).replace(['nan', 'None'], '')
        for c in cols:
             if c not in df_full.columns: df_full[c] = ""
        wks.clear(); wks.update([cols] + df_full[cols].values.tolist())
        log_user_action(creds, user_id, "Lưu toàn bộ hệ thống", "Thành công")
    finally:
        release_lock(creds, user_id)

# --- 8. POPUP QUẢN LÝ NOTE ---
@st.dialog("📝 Quản lý Note (Note_Tung_Khoi)", width="large")
def show_note_popup(creds, all_blocks, user_id):
    st.caption("Quản lý danh sách ghi chú cho từng khối công việc.")
    if 'df_notes_temp' not in st.session_state: st.session_state['df_notes_temp'] = load_notes_data(creds)
    df_notes = st.session_state['df_notes_temp']
    edited_notes = st.data_editor(
        df_notes, num_rows="dynamic", use_container_width=True,
        column_config={
            NOTE_COL_ID: st.column_config.TextColumn("ID (Auto)", disabled=True, width="small"),
            NOTE_COL_BLOCK: st.column_config.SelectboxColumn("Tên Khối", options=all_blocks, required=True, width="medium"),
            NOTE_COL_CONTENT: st.column_config.TextColumn("Nội dung Note", width="large")
        },
        key="note_editor_popup"
    )
    if st.button("💾 Lưu Ghi Chú", type="primary"):
        if save_notes_data(edited_notes, creds, user_id):
            st.success("Đã lưu ghi chú thành công!")
            st.session_state['df_notes_temp'] = edited_notes
            time.sleep(1); st.rerun()

# --- 9. UI CHÍNH ---
@st.dialog("📘 TÀI LIỆU", width="large")
def show_guide():
    st.markdown(f"""
    **Email Bot:** `{BOT_EMAIL_DISPLAY}`
    ### Hướng Dẫn (V22 - Standard):
    1. **Nguyên lý chuẩn:** Tự động thêm 3 cột Link, Sheet, Tháng khi chạy.
    2. **An toàn:** Chỉ ghi đè A-Z, không đụng vào AA+.
    3. **Lọc:** Chỉ chạy dòng "Chưa chốt".
    """)

def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    creds = get_creds()
    
    c1, c2 = st.columns([3, 1])
    with c1: st.title("🛡️ Kinkin Manager (V22 - Standard)"); st.caption(f"User: {user_id}")
    with c2: 
        with st.popover("Tiện ích"):
            st.code(BOT_EMAIL_DISPLAY)
            st_copy_to_clipboard(BOT_EMAIL_DISPLAY, "📋 Copy Email Bot")

    # --- SIDEBAR ---
    with st.sidebar:
        if 'df_full_config' not in st.session_state:
             st.session_state['df_full_config'] = load_full_config(creds)
        
        if st.button("🔄 Tải lại dữ liệu"):
            st.cache_data.clear()
            st.session_state['df_full_config'] = load_full_config(creds)
            st.rerun()

        df_config = st.session_state['df_full_config']
        blocks = df_config[COL_BLOCK_NAME].unique().tolist() if not df_config.empty else [DEFAULT_BLOCK_NAME]
        
        if 'target_block_display' not in st.session_state: st.session_state['target_block_display'] = blocks[0]
        if st.session_state['target_block_display'] not in blocks: st.session_state['target_block_display'] = blocks[0]
            
        def on_block_change(): st.session_state['target_block_display'] = st.session_state.sb_selected_block
        sel_block = st.selectbox("Chọn Khối:", blocks, index=blocks.index(st.session_state['target_block_display']), key="sb_selected_block", on_change=on_block_change)
        
        c_copy_blk, c_blank = st.columns([2, 1])
        if st.button("©️ Sao Chép Khối"):
             new_block_name = f"{sel_block}_bản_sao"
             if new_block_name in blocks:
                 st.toast(f"Tên {new_block_name} đã tồn tại!", icon="⚠️")
             else:
                 block_data = df_config[df_config[COL_BLOCK_NAME] == sel_block].copy()
                 block_data[COL_BLOCK_NAME] = new_block_name
                 st.session_state['df_full_config'] = pd.concat([df_config, block_data], ignore_index=True)
                 save_block_config_to_sheet(block_data, new_block_name, creds, user_id)
                 st.session_state['target_block_display'] = new_block_name
                 st.toast(f"Đã tạo: {new_block_name}", icon="✅")
                 time.sleep(0.5); st.rerun()

        with st.expander("⚙️ Quản lý Khối"):
            new_b = st.text_input("Tên khối mới:")
            if st.button("➕ Tạo Mới"):
                row = {c: "" for c in df_config.columns}
                row[COL_BLOCK_NAME] = new_b; row[COL_STATUS] = "Chưa chốt & đang cập nhật"
                st.session_state['df_full_config'] = pd.concat([df_config, pd.DataFrame([row])], ignore_index=True)
                st.session_state['target_block_display'] = new_b
                st.rerun()
            
            rename_val = st.text_input("Đổi tên khối thành:", value=sel_block)
            if st.button("✏️ Đổi Tên") and rename_val != sel_block:
                if rename_block_action(sel_block, rename_val, creds, user_id):
                    st.cache_data.clear(); del st.session_state['df_full_config']
                    st.session_state['target_block_display'] = rename_val
                    st.rerun()
            
            if st.button("🗑️ Xóa Khối Này", type="primary"):
                if len(blocks) <= 1 and blocks[0] == DEFAULT_BLOCK_NAME:
                    st.warning("Không xóa được khối mặc định!")
                else:
                    delete_block_direct(sel_block, creds, user_id)
                    st.cache_data.clear(); del st.session_state['df_full_config']
                    if 'target_block_display' in st.session_state: del st.session_state['target_block_display']
                    time.sleep(1); st.rerun()
        
        st.divider()
        if st.button("📘 Hướng Dẫn"): show_guide()
        
        if st.button("📝 Note_Tung_Khoi"):
            show_note_popup(creds, blocks, user_id)

    # --- EDITOR ---
    st.subheader(f"Cấu hình: {sel_block}")
    
    current_block_df = st.session_state['df_full_config'][
        st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_block
    ].copy().reset_index(drop=True)
    
    if COL_COPY_FLAG not in current_block_df.columns: current_block_df.insert(0, COL_COPY_FLAG, False)
    else: current_block_df[COL_COPY_FLAG] = False
    
    if 'STT' not in current_block_df.columns: current_block_df.insert(1, 'STT', range(1, len(current_block_df)+1))
    else: current_block_df['STT'] = range(1, len(current_block_df)+1)
    
    edited_df = st.data_editor(
        current_block_df,
        column_order=[
            COL_COPY_FLAG, "STT", COL_STATUS, 
            COL_DATA_RANGE, COL_MONTH, 
            COL_SRC_LINK, COL_SRC_SHEET, 
            COL_TGT_LINK, COL_TGT_SHEET, 
            COL_FILTER, COL_HEADER, 
            COL_RESULT, COL_LOG_ROW
        ],
        column_config={
            COL_COPY_FLAG: st.column_config.CheckboxColumn("Copy", width="small", default=False),
            "STT": st.column_config.NumberColumn("STT", width="small", disabled=True),
            COL_STATUS: st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            COL_DATA_RANGE: st.column_config.TextColumn("Vùng lấy", width="small", help="VD: A:E hoặc để trống"),
            COL_MONTH: st.column_config.TextColumn("Tháng", width="small"),
            COL_SRC_LINK: st.column_config.LinkColumn("Link Nguồn", display_text="Open", width="medium"), 
            COL_SRC_SHEET: st.column_config.TextColumn("Sheet Nguồn", width="medium"),
            COL_TGT_LINK: st.column_config.LinkColumn("Link Đích", display_text="Open", width="medium"),
            COL_TGT_SHEET: st.column_config.TextColumn("Sheet Đích", width="medium"),
            COL_FILTER: st.column_config.TextColumn("Dieu_kien_loc", width="medium", help="VD: Cot_A > 100"),
            COL_HEADER: st.column_config.CheckboxColumn("Lay_header", default=True),
            COL_RESULT: st.column_config.TextColumn("Kết quả", disabled=True),
            COL_LOG_ROW: st.column_config.TextColumn("Dòng dữ liệu", disabled=True),
            COL_BLOCK_NAME: None, COL_MODE: None, COL_NOTE: None
        },
        use_container_width=True, num_rows="dynamic", key=f"editor_v22"
    )

    # --- LOGIC UPDATE ---
    has_changes = False
    if edited_df[COL_COPY_FLAG].any():
        new_rows = []
        for index, row in edited_df.iterrows():
            row_clean = row.copy(); row_clean[COL_COPY_FLAG] = False
            new_rows.append(row_clean)
            if row[COL_COPY_FLAG]: 
                row_copy = row.copy(); row_copy[COL_COPY_FLAG] = False
                new_rows.append(row_copy)
        edited_df = pd.DataFrame(new_rows)
        has_changes = True

    df_to_merge = edited_df.copy()
    if 'STT' in df_to_merge.columns: df_to_merge = df_to_merge.drop(columns=['STT'])
    if COL_COPY_FLAG in df_to_merge.columns: df_to_merge = df_to_merge.drop(columns=[COL_COPY_FLAG])
    
    df_full = st.session_state['df_full_config']
    df_other = df_full[df_full[COL_BLOCK_NAME] != sel_block]
    st.session_state['df_full_config'] = pd.concat([df_other, df_to_merge], ignore_index=True)
    
    if has_changes: st.rerun()

    # --- BUTTONS ---
    st.divider()
    c_run, c_all, c_scan, c_save = st.columns([2, 2, 1, 1])
    
    with c_run:
        # [MODIFIED] Filter only "Chưa chốt & đang cập nhật" for running
        if st.button(f"▶️ CHẠY KHỐI: {sel_block}", type="primary"):
            # Lọc ngay tại đây để đếm đúng số dòng sẽ chạy
            all_rows = edited_df.to_dict('records')
            rows_to_run = [r for r in all_rows if str(r.get(COL_STATUS, '')).strip() == "Chưa chốt & đang cập nhật"]
            
            if not rows_to_run: 
                st.warning("Không có dòng nào ở trạng thái 'Chưa chốt & đang cập nhật' để chạy."); st.stop()
                
            with st.status("Đang chạy...", expanded=True) as status:
                ok, res_map, total = process_pipeline_mixed(rows_to_run, user_id, sel_block, status)
                for i, r in edited_df.iterrows():
                    lnk = str(r.get(COL_SRC_LINK, '')).strip()
                    if lnk in res_map:
                        edited_df.at[i, COL_RESULT] = res_map[lnk][0]
                save_block_config_to_sheet(edited_df, sel_block, creds, user_id)
                status.update(label=f"Xong! {total} dòng.", state="complete")
                time.sleep(1); st.rerun()

    with c_all:
        if st.button("🚀 CHẠY TẤT CẢ"):
            with st.status("Đang chạy toàn hệ thống...", expanded=True) as status:
                full_df = load_full_config(creds)
                all_blks = full_df[COL_BLOCK_NAME].unique()
                total_all = 0
                for blk in all_blks:
                    status.write(f"Đang chạy khối: **{blk}**")
                    # Lọc chặt chẽ ngay từ đầu vào
                    mask = (full_df[COL_BLOCK_NAME] == blk) & (full_df[COL_STATUS] == "Chưa chốt & đang cập nhật")
                    rows = full_df[mask].to_dict('records')
                    if rows:
                        _, res_map, cnt = process_pipeline_mixed(rows, f"{user_id} (All)", blk, None)
                        total_all += cnt
                save_full_direct(full_df, creds, user_id)
                status.update(label=f"Hoàn tất! Tổng {total_all} dòng.", state="complete")
                st.rerun()

    with c_scan:
        if st.button("🔍 Quét"): st.toast("Tính năng quét đang cập nhật!")

    with c_save:
        if st.button("💾 Lưu Cấu Hình"):
            save_block_config_to_sheet(edited_df, sel_block, creds, user_id)
            st.cache_data.clear()
            st.session_state['df_full_config'] = load_full_config(creds)
            st.rerun()

    # --- [MỚI] LOG USER ACTIVITY ---
    st.divider()
    st.subheader("📜 Nhật ký hành vi hệ thống")
    if st.button("🔄 Tải lại Log Hành Vi"): st.cache_data.clear()
    
    df_activity = fetch_activity_logs(creds, limit=20)
    if not df_activity.empty:
        st.dataframe(df_activity, use_container_width=True, hide_index=True)
    else:
        st.info("Chưa có nhật ký hoạt động nào.")

if __name__ == "__main__":
    main_ui()
