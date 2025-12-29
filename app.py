import streamlit as st
import pandas as pd
import time
import gspread
import json
import re
import pytz
import uuid
import numpy as np
import gc
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from datetime import datetime
from google.oauth2 import service_account
from collections import defaultdict
from st_copy_to_clipboard import st_copy_to_clipboard

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Kinkin Manager (V30 - Live Log)", layout="wide", page_icon="⚡")

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

# Cột Hệ Thống
SYS_COL_LINK = "Link file nguồn"
SYS_COL_SHEET = "Sheet nguồn"
SYS_COL_MONTH = "Tháng"

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
def detect_changes_detailed(df_old, df_new):
    if len(df_old) > 1000 or len(df_new) > 1000: return f"Thay đổi lớn ({len(df_new)} dòng)"
    return "Cập nhật cấu hình"

# --- 4. HỆ THỐNG LOCK ---
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
    if is_locked and locking_user != user_id: return False
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
        if val == user_id: wks.update("A2:C2", [["FALSE", "", ""]])
    except: pass

# --- LOGIN & NOTE ---
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
        st.header("🛡️ Đăng nhập")
        pwd = st.text_input("Mật khẩu:", type="password")
        if st.button("Đăng Nhập", use_container_width=True):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]; st.rerun()
            else: st.error("Sai mật khẩu")
    return False

def load_notes_data(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_NOTE_NAME)
        except: wks = sh.add_worksheet(SHEET_NOTE_NAME, rows=100, cols=5); wks.append_row([NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT])
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df.empty: return pd.DataFrame(columns=[NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT])
        return df.dropna(how='all')
    except: return pd.DataFrame()

def save_notes_data(df_notes, creds, user_id):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_NOTE_NAME)
        if not df_notes.empty:
            for idx, row in df_notes.iterrows():
                if not row[NOTE_COL_ID]: df_notes.at[idx, NOTE_COL_ID] = str(uuid.uuid4())[:8]
        set_with_dataframe(wks, df_notes, row=1, col=1)
        return True
    except: return False

# --- 4. CORE ETL ---
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
            
            if data_range_str != "Lấy hết" and ":" in data_range_str:
                try:
                    start_col_str, end_col_str = data_range_str.split(":")
                    start_idx = col_name_to_index(start_col_str.strip())
                    end_idx = col_name_to_index(end_col_str.strip())
                    if start_idx >= 0:
                        end_idx = min(end_idx, len(df.columns) - 1)
                        df = df.iloc[:, start_idx : end_idx + 1]
                except: pass

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
        df[SYS_COL_LINK] = link_src
        df[SYS_COL_SHEET] = source_label
        df[SYS_COL_MONTH] = month_val
        return df, sheet_id, status_msg
    return None, sheet_id, "Không lấy được dữ liệu"

def get_row_ranges_to_delete(wks, keys_to_delete):
    """Tìm các dòng cần xóa mà không cần load toàn bộ data vào DF"""
    all_values = wks.get_all_values()
    if not all_values: return []
    
    headers = all_values[0]
    try:
        idx_link = headers.index(SYS_COL_LINK)
        idx_sheet = headers.index(SYS_COL_SHEET)
        idx_month = headers.index(SYS_COL_MONTH)
    except ValueError:
        return [] 
        
    rows_to_delete = []
    # Duyệt qua từng dòng để tìm key khớp
    for i, row in enumerate(all_values[1:], start=2): # Data bắt đầu từ dòng 2
        l = row[idx_link] if len(row) > idx_link else ""
        s = row[idx_sheet] if len(row) > idx_sheet else ""
        m = row[idx_month] if len(row) > idx_month else ""
        if (l, s, m) in keys_to_delete:
            rows_to_delete.append(i)
    return rows_to_delete

def batch_delete_rows(sh, sheet_id, row_indices, log_container=None):
    """Xóa dòng theo lô để tối ưu API"""
    if not row_indices: return
    row_indices.sort(reverse=True)
    
    ranges = []
    if len(row_indices) > 0:
        start = row_indices[0]; end = start
        for r in row_indices[1:]:
            if r == start - 1: start = r
            else:
                ranges.append((start, end))
                start = r; end = r
        ranges.append((start, end))
    
    requests = []
    for start, end in ranges:
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": start - 1,
                    "endIndex": end
                }
            }
        })
    
    batch_size = 100
    total_reqs = len(requests)
    for i in range(0, total_reqs, batch_size):
        if log_container: log_container.write(f"✂️ Đang xóa batch {i//batch_size + 1}/{total_reqs//batch_size + 1}...")
        sh.batch_update({'requests': requests[i:i+batch_size]})
        time.sleep(1)

def write_smart_v2_surgical(tasks_list, target_link, target_sheet_name, creds, log_container):
    # [V30] LIVE LOGGING INCLUDED
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi", {}
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        
        log_container.write(f"📂 Đang mở file đích: ...{target_link[-10:]} | Sheet: {real_sheet_name}")
        try: wks = sh.worksheet(real_sheet_name)
        except: 
            wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
            log_container.write("✨ Đã tạo Sheet mới.")
        
        # 1. Gom dữ liệu MỚI
        df_new_all = pd.DataFrame()
        log_container.write("📦 Đang gom dữ liệu...")
        for df, src_link in tasks_list:
            df_new_all = pd.concat([df_new_all, df], ignore_index=True)
            
        if df_new_all.empty: return True, "Không có data mới", {}

        # 2. Tạo Key cần xóa
        keys_to_delete = set(zip(df_new_all[SYS_COL_LINK], df_new_all[SYS_COL_SHEET], df_new_all[SYS_COL_MONTH]))
        
        # 3. Tìm dòng cần xóa
        log_container.write("🔍 Đang quét tìm dữ liệu cũ cần xóa...")
        rows_to_del = get_row_ranges_to_delete(wks, keys_to_delete)
        log_container.write(f"🛑 Tìm thấy {len(rows_to_del)} dòng cũ cần loại bỏ.")
        
        # 4. Thực hiện XÓA
        if rows_to_del:
            batch_delete_rows(sh, wks.id, rows_to_del, log_container)
            log_container.write("✅ Đã xóa xong dữ liệu cũ.")
            
        # 5. Thực hiện NỐI (Append)
        log_container.write(f"🚀 Đang ghi {len(df_new_all)} dòng mới xuống cuối Sheet...")
        
        if wks.row_count == 0 or not wks.get_values("A1:A1"):
             set_with_dataframe(wks, df_new_all, row=1, col=1)
             start_row_new = 2
        else:
             existing_data = wks.get_all_values()
             start_row_new = len(existing_data) + 1
             
             chunk_size = 5000
             new_vals = df_new_all.fillna('').values.tolist()
             total_chunks = len(new_vals) // chunk_size + 1
             
             for i in range(0, len(new_vals), chunk_size):
                 log_container.write(f"⏳ Đang ghi gói {i//chunk_size + 1}/{total_chunks}...")
                 wks.append_rows(new_vals[i:i+chunk_size], value_input_option='USER_ENTERED')
                 time.sleep(1)

        # 6. Tính Range trả về Config
        range_map = {}
        current_pointer = start_row_new
        for df, src_link in tasks_list:
            count = len(df)
            end_pointer = current_pointer + count - 1
            range_map[(src_link, df[SYS_COL_SHEET].iloc[0])] = f"{current_pointer} - {end_pointer}"
            current_pointer += count

        log_container.write("🎉 Hoàn tất quy trình ghi!")
        return True, f"Đã cập nhật (Xóa {len(rows_to_del)}, Thêm {len(df_new_all)})", range_map

    except Exception as e: 
        log_container.error(f"❌ Lỗi Ghi: {str(e)}")
        return False, f"Lỗi Ghi: {str(e)}", {}

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

# --- PIPELINE ---
def process_pipeline_mixed(rows_to_run, user_id, block_name_run, status_container):
    creds = get_creds()
    if not acquire_lock(creds, user_id): return False, f"HỆ THỐNG ĐANG BẬN! Vui lòng thử lại sau.", 0
    
    log_user_action(creds, user_id, f"Chạy Job: {block_name_run}", "Đang chạy...")
    try:
        # Nhóm các task lại theo File Đích để xử lý 1 thể
        grouped_tasks = defaultdict(list)
        valid_rows = [r for r in rows_to_run if str(r.get(COL_STATUS, '')).strip() == "Chưa chốt & đang cập nhật"]
        
        if not valid_rows: return True, {}, 0 

        for row in valid_rows:
            t_link = str(row.get(COL_TGT_LINK, '')).strip()
            t_sheet = str(row.get(COL_TGT_SHEET, '')).strip()
            grouped_tasks[(t_link, t_sheet)].append(row)
        
        global_results_map = {} 
        all_success = True; log_entries = []
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")
        total_rows_all = 0
        
        # Bắt đầu chạy từng nhóm (Từng file đích)
        for idx, ((target_link, target_sheet), group_rows) in enumerate(grouped_tasks.items()):
            
            with status_container.expander(f"🔄 Đang xử lý File Đích {idx+1}: ...{target_link[-10:]}", expanded=True):
                st.write(f"🎯 Mục tiêu: Sheet '{target_sheet}'")
                
                # Bước 1: Tải dữ liệu từ các nguồn
                tasks_list = []
                for i, row in enumerate(group_rows):
                    s_link = row.get(COL_SRC_LINK, '')
                    s_label = row.get(COL_SRC_SHEET, 'Sheet1')
                    st.write(f"⬇️ [{i+1}/{len(group_rows)}] Đang tải nguồn: ...{s_link[-10:]} ({s_label})")
                    
                    df, sid, msg = fetch_data_v2(row, creds)
                    
                    if df is not None:
                        st.write(f"   ✅ Lấy được {len(df)} dòng.")
                        tasks_list.append((df, s_link)); total_rows_all += len(df)
                    else:
                        st.error(f"   ❌ Lỗi tải: {msg}")
                        global_results_map[s_link] = ("Lỗi tải", "")
                        log_entries.append([time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH), user_id, s_link, target_link, target_sheet, row.get(COL_SRC_SHEET), "Lỗi tải", "0", "", block_name_run])
                    
                    del df; gc.collect()

                # Bước 2: Ghi vào đích (Surgical Update)
                if tasks_list:
                    st.info("⚡ Bắt đầu quy trình cập nhật thông minh...")
                    # Truyền st (container) vào hàm write để nó in log ra màn hình
                    success_update, msg_update, range_map = write_smart_v2_surgical(tasks_list, target_link, target_sheet, creds, st)
                    
                    if not success_update: 
                        all_success = False
                        st.error(f"❌ Ghi thất bại: {msg_update}")
                    else:
                        st.success(f"✅ {msg_update}")

                    del tasks_list; gc.collect()

                    # Bước 3: Cập nhật kết quả
                    status_str = "Thành công" if success_update else f"Lỗi: {msg_update}"
                    for row in group_rows:
                        s_link = str(row.get(COL_SRC_LINK, '')).strip()
                        if s_link not in global_results_map: # Nếu chưa bị lỗi tải
                            s_sheet = str(row.get(COL_SRC_SHEET, '')).strip()
                            calc_range = range_map.get((s_link, s_sheet), "")
                            global_results_map[s_link] = (status_str, calc_range)
                            log_entries.append([time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH), user_id, s_link, target_link, target_sheet, row.get(COL_SRC_SHEET), status_str, "", calc_range, block_name_run])
        
        write_detailed_log(creds, log_entries)
        log_user_action(creds, user_id, f"Hoàn tất Job: {block_name_run}", f"Tổng {total_rows_all} dòng")
        return all_success, global_results_map, total_rows_all
    finally:
        release_lock(creds, user_id)

# --- 7. QUẢN LÝ CONFIG ---
@st.cache_data
def load_full_config(_creds):
    sh = get_sh_with_retry(_creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
    if df.empty: return pd.DataFrame(columns=[COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE])
    
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    df[COL_MODE] = df[COL_MODE].replace('', 'APPEND').fillna('APPEND')
    df[COL_HEADER] = df[COL_HEADER].replace('', 'TRUE').fillna('TRUE')
    if 'STT' in df.columns: df = df.drop(columns=['STT'])
    return df

def save_block_config_to_sheet(df_current_ui, current_block_name, creds, user_id):
    if not acquire_lock(creds, user_id): st.error("Hệ thống đang bận!"); return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        if COL_BLOCK_NAME not in df_server.columns: df_server[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
        df_other = df_server[df_server[COL_BLOCK_NAME] != current_block_name]
        df_save = df_current_ui.copy(); df_save[COL_BLOCK_NAME] = current_block_name
        if 'STT' in df_save.columns: df_save = df_save.drop(columns=['STT'])
        if COL_COPY_FLAG in df_save.columns: df_save = df_save.drop(columns=[COL_COPY_FLAG])
        
        detail_log = detect_changes_detailed(df_server[df_server[COL_BLOCK_NAME] == current_block_name], df_save)
        log_user_action(creds, user_id, f"Sửa cấu hình: {current_block_name}", detail_log)
        
        df_final = pd.concat([df_other, df_save], ignore_index=True).astype(str).replace(['nan', 'None'], '')
        wks.clear(); set_with_dataframe(wks, df_final, row=1, col=1)
        st.toast(f"✅ Đã lưu: {current_block_name}", icon="💾")
    finally: release_lock(creds, user_id)

def rename_block_action(old_name, new_name, creds, user_id):
    if not acquire_lock(creds, user_id): return False
    try:
        if not new_name or new_name == old_name: return False
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if COL_BLOCK_NAME in df.columns:
            df.loc[df[COL_BLOCK_NAME] == old_name, COL_BLOCK_NAME] = new_name
            wks.clear(); set_with_dataframe(wks, df, row=1, col=1)
        log_user_action(creds, user_id, f"Đổi tên: {old_name} -> {new_name}", "Thành công")
        return True
    finally: release_lock(creds, user_id)

def delete_block_direct(block_name_to_delete, creds, user_id):
    if not acquire_lock(creds, user_id): st.error("Hệ thống bận!"); return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        if COL_BLOCK_NAME in df_server.columns:
            df_new = df_server[df_server[COL_BLOCK_NAME] != block_name_to_delete]
            wks.clear(); set_with_dataframe(wks, df_new, row=1, col=1)
        log_user_action(creds, user_id, f"Xóa khối: {block_name_to_delete}", "Thành công")
    finally: release_lock(creds, user_id)

def save_full_direct(df_full, creds, user_id):
    if not acquire_lock(creds, user_id): return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_full = df_full.astype(str).replace(['nan', 'None'], '')
        wks.clear(); set_with_dataframe(wks, df_full, row=1, col=1)
        log_user_action(creds, user_id, "Lưu toàn bộ hệ thống", "Thành công")
    finally: release_lock(creds, user_id)

# --- UI ---
def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']; creds = get_creds()
    c1, c2 = st.columns([3, 1])
    with c1: st.title("⚡ Kinkin (V30 - Live Log)"); st.caption(f"User: {user_id}")
    with c2: 
        with st.popover("Tiện ích"): st.code(BOT_EMAIL_DISPLAY); st_copy_to_clipboard(BOT_EMAIL_DISPLAY, "Copy Email")

    with st.sidebar:
        if 'df_full_config' not in st.session_state: st.session_state['df_full_config'] = load_full_config(creds)
        if st.button("🔄 Tải lại dữ liệu"): st.cache_data.clear(); st.session_state['df_full_config'] = load_full_config(creds); st.rerun()
        df_config = st.session_state['df_full_config']
        blocks = df_config[COL_BLOCK_NAME].unique().tolist() if not df_config.empty else [DEFAULT_BLOCK_NAME]
        if 'target_block_display' not in st.session_state: st.session_state['target_block_display'] = blocks[0]
        def on_block_change(): st.session_state['target_block_display'] = st.session_state.sb_selected_block
        sel_block = st.selectbox("Chọn Khối:", blocks, index=blocks.index(st.session_state['target_block_display']) if st.session_state['target_block_display'] in blocks else 0, key="sb_selected_block", on_change=on_block_change)
        
        if st.button("©️ Sao Chép Khối"):
             new_b = f"{sel_block}_copy"
             bd = df_config[df_config[COL_BLOCK_NAME] == sel_block].copy(); bd[COL_BLOCK_NAME] = new_b
             st.session_state['df_full_config'] = pd.concat([df_config, bd], ignore_index=True)
             save_block_config_to_sheet(bd, new_b, creds, user_id)
             st.session_state['target_block_display'] = new_b; st.rerun()

        with st.expander("⚙️ Quản lý Khối"):
            new_b = st.text_input("Tên khối mới:")
            if st.button("➕ Tạo Mới"):
                row = {c: "" for c in df_config.columns}; row[COL_BLOCK_NAME] = new_b; row[COL_STATUS] = "Chưa chốt & đang cập nhật"
                st.session_state['df_full_config'] = pd.concat([df_config, pd.DataFrame([row])], ignore_index=True)
                st.session_state['target_block_display'] = new_b; st.rerun()
            
            rn_val = st.text_input("Đổi tên thành:", value=sel_block)
            if st.button("✏️ Đổi Tên") and rn_val != sel_block:
                if rename_block_action(sel_block, rn_val, creds, user_id):
                    st.cache_data.clear(); del st.session_state['df_full_config']; st.session_state['target_block_display'] = rn_val; st.rerun()
            if st.button("🗑️ Xóa Khối", type="primary"):
                delete_block_direct(sel_block, creds, user_id)
                st.cache_data.clear(); del st.session_state['df_full_config']; del st.session_state['target_block_display']; st.rerun()
        
        st.divider(); 
        if st.button("📘 Hướng Dẫn"): show_guide()
        if st.button("📝 Note_Tung_Khoi"): show_note_popup(creds, blocks, user_id)

    st.subheader(f"Cấu hình: {sel_block}")
    current_block_df = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_block].copy().reset_index(drop=True)
    if COL_COPY_FLAG not in current_block_df.columns: current_block_df.insert(0, COL_COPY_FLAG, False)
    if 'STT' not in current_block_df.columns: current_block_df.insert(1, 'STT', range(1, len(current_block_df)+1))
    
    edited_df = st.data_editor(
        current_block_df,
        column_order=[COL_COPY_FLAG, "STT", COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_SRC_SHEET, COL_TGT_LINK, COL_TGT_SHEET, COL_FILTER, COL_HEADER, COL_RESULT, COL_LOG_ROW],
        column_config={
            COL_COPY_FLAG: st.column_config.CheckboxColumn("Copy", width="small", default=False),
            "STT": st.column_config.NumberColumn("STT", width="small", disabled=True),
            COL_STATUS: st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            COL_DATA_RANGE: st.column_config.TextColumn("Vùng lấy", width="small", default="Lấy hết"),
            COL_MONTH: st.column_config.TextColumn("Tháng", width="small"),
            COL_SRC_LINK: st.column_config.LinkColumn("Link Nguồn", width="medium"), 
            COL_TGT_LINK: st.column_config.LinkColumn("Link Đích", width="medium"),
            COL_FILTER: st.column_config.TextColumn("Dieu_kien_loc", width="medium"),
            COL_HEADER: st.column_config.CheckboxColumn("Lay_header", default=True),
            COL_RESULT: st.column_config.TextColumn("Kết quả", disabled=True),
            COL_LOG_ROW: st.column_config.TextColumn("Dòng dữ liệu", disabled=True),
            COL_BLOCK_NAME: None, COL_MODE: None, COL_NOTE: None
        },
        use_container_width=True, num_rows="dynamic", key="editor_v30"
    )

    if edited_df[COL_COPY_FLAG].any():
        new_rows = []
        for index, row in edited_df.iterrows():
            row_clean = row.copy(); row_clean[COL_COPY_FLAG] = False; new_rows.append(row_clean)
            if row[COL_COPY_FLAG]: row_copy = row.copy(); row_copy[COL_COPY_FLAG] = False; new_rows.append(row_copy)
        st.session_state['df_full_config'] = pd.concat([st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] != sel_block], pd.DataFrame(new_rows)], ignore_index=True)
        st.rerun()

    st.divider()
    c_run, c_all, c_scan, c_save = st.columns([2, 2, 1, 1])
    
    with c_run:
        if st.button(f"▶️ CHẠY KHỐI: {sel_block}", type="primary"):
            all_rows = edited_df.to_dict('records')
            rows_to_run = [r for r in all_rows if str(r.get(COL_STATUS, '')).strip() == "Chưa chốt & đang cập nhật"]
            if not rows_to_run: st.warning("Không có dòng nào 'Chưa chốt'."); st.stop()
            
            # [LOG CONTAINER]
            log_container = st.status("🚀 Đang khởi động tiến trình...", expanded=True)
            ok, res_map, total = process_pipeline_mixed(rows_to_run, user_id, sel_block, log_container)
            
            # Update UI
            for i, r in edited_df.iterrows():
                lnk = str(r.get(COL_SRC_LINK, '')).strip()
                if lnk in res_map:
                    edited_df.at[i, COL_RESULT] = res_map[lnk][0]
                    if res_map[lnk][1]: edited_df.at[i, COL_LOG_ROW] = res_map[lnk][1]
            save_block_config_to_sheet(edited_df, sel_block, creds, user_id)
            
            log_container.update(label=f"✅ Hoàn tất! Tổng {total} dòng.", state="complete", expanded=False)
            time.sleep(1); st.rerun()

    with c_all:
        if st.button("🚀 CHẠY TẤT CẢ"):
            with st.status("Đang chạy toàn hệ thống...", expanded=True) as status:
                save_block_config_to_sheet(edited_df, sel_block, creds, user_id)
                full_df = load_full_config(creds)
                all_blks = full_df[COL_BLOCK_NAME].unique()
                total_all = 0
                for blk in all_blks:
                    status.write(f"Đang chạy khối: **{blk}**")
                    mask = (full_df[COL_BLOCK_NAME] == blk) & (full_df[COL_STATUS] == "Chưa chốt & đang cập nhật")
                    rows = full_df[mask].to_dict('records')
                    if rows:
                        _, res_map, cnt = process_pipeline_mixed(rows, f"{user_id} (All)", blk, status)
                        total_all += cnt; gc.collect()
                save_full_direct(full_df, creds, user_id)
                status.update(label=f"Hoàn tất! Tổng {total_all} dòng.", state="complete"); st.rerun()

    with c_scan:
        if st.button("🔍 Quét"): 
            st.session_state['df_full_config'] = pd.concat([st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] != sel_block], edited_df], ignore_index=True)
            st.toast("Đã quét!")

    with c_save:
        if st.button("💾 Lưu Cấu Hình"):
            save_block_config_to_sheet(edited_df, sel_block, creds, user_id); st.cache_data.clear(); st.session_state['df_full_config'] = load_full_config(creds); st.rerun()

    st.divider(); st.subheader("📜 Nhật ký"); 
    if st.button("🔄 Tải lại Log"): st.cache_data.clear()
    df_act = fetch_activity_logs(creds, limit=20)
    if not df_act.empty: st.dataframe(df_act, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main_ui()
