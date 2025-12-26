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
st.set_page_config(page_title="Kinkin Manager (V17 - Deep Log)", layout="wide", page_icon="🛡️")

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

# --- [LOG HÀNH VI] ---
def log_user_action(creds, user_id, action, status=""):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_ACTIVITY_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_ACTIVITY_NAME, rows=1000, cols=4)
            wks.append_row(["Thời gian", "Người dùng", "Hành vi", "Trạng thái"])
        
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

# --- [V17 - HÀM SOI CHI TIẾT THAY ĐỔI] ---
def detect_changes_detailed(df_old, df_new):
    """So sánh từng ô dữ liệu để báo cáo chi tiết"""
    changes = []
    
    # 1. So sánh số lượng dòng
    len_old = len(df_old)
    len_new = len(df_new)
    
    # Các cột cần theo dõi kỹ
    monitor_cols = {
        COL_SRC_LINK: "LinkNguon",
        COL_TGT_LINK: "LinkDich",
        COL_SRC_SHEET: "SheetNguon",
        COL_TGT_SHEET: "SheetDich",
        COL_DATA_RANGE: "VungLay",
        COL_FILTER: "BoLoc",
        COL_MODE: "CheDo",
        COL_STATUS: "TrangThai"
    }
    
    # 2. Quét các dòng tồn tại song song (theo thứ tự từ trên xuống)
    min_len = min(len_old, len_new)
    for i in range(min_len):
        row_old = df_old.iloc[i]
        row_new = df_new.iloc[i]
        
        diffs = []
        for col, col_short in monitor_cols.items():
            val_old = str(row_old.get(col, '')).strip().replace('nan', '')
            val_new = str(row_new.get(col, '')).strip().replace('nan', '')
            
            if val_old != val_new:
                # Nếu chuỗi quá dài (link), cắt bớt để log gọn
                if len(val_old) > 20: val_old = val_old[:10] + "..."
                if len(val_new) > 20: val_new = val_new[:10] + "..."
                diffs.append(f"{col_short}: '{val_old}' -> '{val_new}'")
        
        if diffs:
            changes.append(f"Dòng {i+1} sửa: [{', '.join(diffs)}]")

    # 3. Báo cáo thêm/xóa dòng
    if len_new > len_old:
        added_count = len_new - len_old
        changes.append(f"Thêm {added_count} dòng mới ở cuối")
    elif len_new < len_old:
        deleted_count = len_old - len_new
        changes.append(f"Xóa {deleted_count} dòng cuối (Từ dòng {len_new+1})")
            
    if not changes: return "Lưu (Không có thay đổi nội dung)"
    return " | ".join(changes)

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
        df['__Link_Source__'] = link_src; df['__Thang__'] = month_val
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
        
        if write_mode == "TABLE":
            if not tasks_list: return True, "Không có data"
            combined_df = pd.concat([t[0] for t in tasks_list], ignore_index=True)
            cols_to_drop = [c for c in ['__Link_Source__', '__Thang__'] if c in combined_df.columns]
            combined_df = combined_df.drop(columns=cols_to_drop)

            if combined_df.empty or len(combined_df.columns) == 0: return True, "Data rỗng (Check bộ lọc)"
            num_cols = len(combined_df.columns)
            last_col_char = gspread.utils.rowcol_to_a1(1, max(1, num_cols)).replace("1", "")
            try: wks.batch_clear([f"A2:{last_col_char}"])
            except: pass
            set_with_dataframe(wks, combined_df, row=2, col=1, include_index=False, include_column_header=False)
            return True, f"Đã làm mới Table ({len(combined_df)} dòng)"
        else:
            links_to_remove = [t[1] for t in tasks_list if t[1] and len(str(t[1])) > 5]
            existing_headers = []
            try: existing_headers = wks.row_values(1)
            except: pass
            
            col_link_name = "Link file nguồn"
            if existing_headers and links_to_remove and col_link_name in existing_headers:
                try: 
                    link_col_idx = existing_headers.index(col_link_name) + 1
                    col_values = wks.col_values(link_col_idx)
                    rows_to_delete = []
                    for i, val in enumerate(col_values):
                        if i > 0 and str(val).strip() in links_to_remove: rows_to_delete.append(i + 1)
                    if rows_to_delete:
                        rows_to_delete.sort()
                        ranges = []; start = rows_to_delete[0]; end = start
                        for r in rows_to_delete[1:]:
                            if r == end + 1: end = r
                            else: ranges.append((start, end)); start = r; end = r
                        ranges.append((start, end))
                        delete_reqs = []
                        for start, end in reversed(ranges):
                            delete_reqs.append({"deleteDimension": {"range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
                        if delete_reqs: sh.batch_update({'requests': delete_reqs})
                except: pass

            final_df_list = []
            for df, src_link in tasks_list:
                df = df.rename(columns={'__Link_Source__': col_link_name, '__Thang__': 'Tháng'})
                final_df_list.append(df)
            
            if not final_df_list: return True, "Không có data mới"
            combined_df = pd.concat(final_df_list, ignore_index=True)
            
            if not existing_headers:
                set_with_dataframe(wks, combined_df, row=1, col=1)
                return True, f"Tạo mới ({len(combined_df)} dòng)"
            else:
                all_cols = existing_headers + [c for c in combined_df.columns if c not in existing_headers]
                if len(all_cols) > len(existing_headers): wks.update("A1", [all_cols])
                combined_df = combined_df.reindex(columns=all_cols, fill_value="")
                wks.append_rows(combined_df.values.tolist())
                return True, f"Append thành công (+{len(combined_df)} dòng)"
    except Exception as e: return False, f"Lỗi Ghi: {str(e)}"

# --- 5. HỆ THỐNG LOCK & LOG CHẠY JOB ---
def get_system_lock(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
            wks.update([["is_locked", "user", "time_start"], ["FALSE", "", ""]])
            return False, "", ""
        val = wks.cell(2, 1).value
        if val == "TRUE":
            time_str = wks.cell(2, 3).value
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 1800: return False, "", ""
            except: pass
            return True, wks.cell(2, 2).value, time_str
        return False, "", ""
    except: return False, "", ""

def set_system_lock(creds, user_id, lock=True):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_LOCK_NAME)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]] if lock else [["FALSE", "", ""]])
    except: pass

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

# --- 6. PIPELINE ---
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
    is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id and "Auto" not in user_id:
        return False, f"HỆ THỐNG ĐANG BẬN! {locking_user} đang chạy.", 0
    set_system_lock(creds, user_id, lock=True)
    log_user_action(creds, user_id, f"Chạy Job: {block_name_run}", "Đang chạy...")
    try:
        if status_container: status_container.write("🔄 Đang phân nhóm dữ liệu...")
        grouped_tasks = defaultdict(list)
        for row in rows_to_run:
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
        set_system_lock(creds, user_id, lock=False)

# --- 7. QUẢN LÝ CONFIG ---

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

def save_block_config_to_sheet(df_current_ui, current_block_name, creds, user_id):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
    if COL_BLOCK_NAME not in df_server.columns: df_server[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
    
    # Lấy data cũ của block để so sánh
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

def rename_block_action(old_name, new_name, creds, user_id):
    if not new_name or new_name == old_name: return False
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    if COL_BLOCK_NAME in df.columns:
        df.loc[df[COL_BLOCK_NAME] == old_name, COL_BLOCK_NAME] = new_name
        wks.clear(); wks.update([df.columns.tolist()] + df.fillna('').values.tolist())
    log_user_action(creds, user_id, f"Đổi tên: {old_name} -> {new_name}", "Thành công")
    return True

def save_full_direct(df_full, creds, user_id):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE]
    df_full = df_full.astype(str).replace(['nan', 'None'], '')
    for c in cols:
         if c not in df_full.columns: df_full[c] = ""
    wks.clear(); wks.update([cols] + df_full[cols].values.tolist())
    log_user_action(creds, user_id, "Lưu toàn bộ hệ thống", "Thành công")

# --- 8. POPUP QUẢN LÝ NOTE ---
@st.dialog("📝 Quản lý Note (Note_Tung_Khoi)", width="large")
def show_note_popup(creds, all_blocks, user_id):
    st.caption("Quản lý danh sách ghi chú cho từng khối công việc.")
    
    if 'df_notes_temp' not in st.session_state:
        st.session_state['df_notes_temp'] = load_notes_data(creds)
    
    df_notes = st.session_state['df_notes_temp']
    
    edited_notes = st.data_editor(
        df_notes,
        num_rows="dynamic",
        use_container_width=True,
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
            time.sleep(1)
            st.rerun()

# --- 9. UI CHÍNH ---
@st.dialog("📘 TÀI LIỆU", width="large")
def show_guide():
    st.markdown(f"""
    **Email Bot:** `{BOT_EMAIL_DISPLAY}`
    ### Hướng Dẫn (V17):
    1. **Log chi tiết:** Hệ thống sẽ ghi lại cụ thể bạn sửa gì (VD: Đổi Link A -> Link B).
    2. **Note:** Quản lý ghi
