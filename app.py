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
from datetime import datetime, timedelta
from google.oauth2 import service_account
from collections import defaultdict
from st_copy_to_clipboard import st_copy_to_clipboard

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Kinkin Manager (V37 - Scheduler)", layout="wide", page_icon="⏰")

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
SHEET_NOTE_NAME = "database_ghi_chu"

# Cột Config Cơ Bản
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

# Cột Tính Năng & Scheduler [MỚI]
COL_FILTER = "Dieu_Kien_Loc"      
COL_HEADER = "Lay_Header"         
COL_MODE = "Che_Do_Ghi"
COL_FREQ_MIN = "Tan_suat_Phut"    # [NEW] Task 2: Chạy theo phút
COL_LAST_RUN = "Lan_chay_cuoi"    # [NEW] Ghi lại thời gian chạy
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

def smart_filter_fix(query_str):
    if not query_str: return ""
    q = query_str.strip()
    q = re.sub(r'(?<![<>!=])=(?![=])', '==', q)
    operators = ["==", "!=", ">=", "<=", ">", "<"]
    selected_op = None
    for op in operators:
        if op in q:
            selected_op = op
            break
    if selected_op:
        parts = q.split(selected_op, 1)
        left = parts[0].strip()
        right = parts[1].strip()
        if " " in left and not left.startswith("`") and not left.startswith("'") and not left.startswith('"'):
            left = f"`{left}`"
        return f"{left} {selected_op} {right}"
    return q

# --- 3. HỆ THỐNG LOG & LOCK ---
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

# --- 5. TÍNH NĂNG QUÉT QUYỀN (RESTORED) ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link không hợp lệ"
    try:
        sh = get_sh_with_retry(creds, sheet_id)
        return True, f"OK ({sh.title})"
    except Exception as e:
        err_msg = str(e)
        if "403" in err_msg: return False, "⛔ 403: Không có quyền (Share cho bot chưa?)"
        if "404" in err_msg: return False, "❌ 404: Không tìm thấy file"
        return False, f"⚠️ Lỗi khác: {err_msg[:50]}..."

def scan_permissions_all(df_rows, creds, log_container):
    results = {}
    unique_links = set()
    for row in df_rows:
        unique_links.add(str(row.get(COL_SRC_LINK, '')).strip())
        unique_links.add(str(row.get(COL_TGT_LINK, '')).strip())
    
    unique_links.discard('')
    
    total = len(unique_links)
    log_container.write(f"🔍 Bắt đầu quét {total} liên kết...")
    
    progress_bar = log_container.progress(0)
    for i, link in enumerate(unique_links):
        ok, msg = verify_access_fast(link, creds)
        results[link] = (ok, msg)
        progress_bar.progress((i + 1) / total)
        if not ok:
            log_container.error(f"Link lỗi: {link}\n-> {msg}")
    
    log_container.success("Đã quét xong!")
    return results

# --- 6. LOGIC SCHEDULER (TASK 2) ---
def should_run_task(row):
    """
    Logic ưu tiên:
    1. Nếu Tan_suat_Phut > 0 -> Chạy theo chu kỳ phút (Bỏ qua ngày/tháng).
    2. Nếu Tan_suat_Phut = 0 -> (Có thể mở rộng chạy theo ngày/giờ ở đây).
    """
    try:
        freq_min = int(str(row.get(COL_FREQ_MIN, '0')).strip() or 0)
    except: freq_min = 0
    
    if freq_min > 0:
        last_run_str = str(row.get(COL_LAST_RUN, '')).strip()
        if not last_run_str: return True # Chưa chạy bao giờ -> Chạy ngay
        
        try:
            last_run = datetime.strptime(last_run_str, "%d/%m/%Y %H:%M:%S")
            tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
            # Lưu ý: last_run lưu string nên cần xử lý timezone nếu cần. 
            # Ở đây giả sử lưu theo giờ VN
            now = datetime.now(tz_vn).replace(tzinfo=None) # Strip tz để so sánh đơn giản
            diff_min = (now - last_run).total_seconds() / 60
            
            if diff_min >= freq_min:
                return True
            else:
                return False # Chưa đến giờ
        except:
            return True # Lỗi format ngày -> Chạy lại cho chắc
    
    return True # Mặc định chạy nếu không hẹn giờ (khi bấm nút Chạy)

# --- 7. CORE ETL (STRICT SYNC & CLEAN) ---
def fetch_data_v3(row_config, creds, target_headers=None):
    link_src = str(row_config.get(COL_SRC_LINK, '')).strip()
    source_label = str(row_config.get(COL_SRC_SHEET, '')).strip()
    month_val = str(row_config.get(COL_MONTH, ''))
    data_range_str = str(row_config.get(COL_DATA_RANGE, 'Lấy hết')).strip()
    raw_filter = str(row_config.get(COL_FILTER, '')).strip()
    filter_query = smart_filter_fix(raw_filter)
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
                if target_headers:
                    num_src_cols = len(df.columns); num_tgt_cols = len(target_headers)
                    min_cols = min(num_src_cols, num_tgt_cols)
                    rename_map = {i: target_headers[i] for i in range(min_cols)}
                    df = df.rename(columns=rename_map)
                    if num_src_cols > num_tgt_cols: df = df.iloc[:, :num_tgt_cols]

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
                except Exception as e1: return None, sheet_id, f"⚠️ Query Error: {e1}"

            df = df.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
            status_msg = "Thành công"
        else:
            status_msg = "Sheet trắng tinh"
            df = pd.DataFrame()
    except Exception as e: return None, sheet_id, f"Lỗi tải: {str(e)}"

    if df is not None:
        df[SYS_COL_LINK] = link_src.strip()
        df[SYS_COL_SHEET] = source_label.strip()
        df[SYS_COL_MONTH] = month_val.strip()
        return df, sheet_id, status_msg
    return None, sheet_id, "Không lấy được dữ liệu"

def get_rows_to_delete_dynamic(wks, keys_to_delete, log_container):
    all_values = wks.get_all_values()
    if not all_values: return []
    headers = all_values[0]
    try:
        idx_link = headers.index(SYS_COL_LINK); idx_sheet = headers.index(SYS_COL_SHEET); idx_month = headers.index(SYS_COL_MONTH)
    except ValueError:
        if log_container: log_container.warning(f"⚠️ Thiếu cột hệ thống. Bỏ qua bước xóa.")
        return [] 
    rows_to_delete = []
    for i, row in enumerate(all_values[1:], start=2): 
        l = row[idx_link].strip() if len(row) > idx_link else ""
        s = row[idx_sheet].strip() if len(row) > idx_sheet else ""
        m = row[idx_month].strip() if len(row) > idx_month else ""
        if (l, s, m) in keys_to_delete: rows_to_delete.append(i)
    return rows_to_delete

def batch_delete_rows(sh, sheet_id, row_indices, log_container=None):
    if not row_indices: return
    row_indices.sort(reverse=True) 
    ranges = []
    if len(row_indices) > 0:
        start = row_indices[0]; end = start
        for r in row_indices[1:]:
            if r == start - 1: start = r
            else: ranges.append((start, end)); start = r; end = r
        ranges.append((start, end))
    requests = []
    for start, end in ranges:
        requests.append({"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
    batch_size = 100
    for i in range(0, len(requests), batch_size):
        if log_container: log_container.write(f"✂️ Xóa batch {i//batch_size + 1}...")
        sh.batch_update({'requests': requests[i:i+batch_size]}); time.sleep(1)

def write_strict_sync(tasks_list, target_link, target_sheet_name, creds, log_container):
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link lỗi", {}
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        log_container.write(f"📂 Đích: ...{target_link[-10:]} | {real_sheet_name}")
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        df_new_all = pd.DataFrame()
        for df, src_link in tasks_list: df_new_all = pd.concat([df_new_all, df], ignore_index=True)
        if df_new_all.empty: return True, "No Data", {}

        existing_headers = wks.row_values(1)
        if not existing_headers:
            final_headers = df_new_all.columns.tolist()
            wks.update(range_name="A1", values=[final_headers])
            existing_headers = final_headers
        else:
            updated = existing_headers.copy(); added = False
            for col in [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH]:
                if col not in updated: updated.append(col); added = True
            if added: wks.update(range_name="A1", values=[updated]); existing_headers = updated

        df_aligned = pd.DataFrame()
        for col in existing_headers:
            df_aligned[col] = df_new_all[col] if col in df_new_all.columns else ""
        
        keys = set()
        for idx, row in df_new_all.iterrows():
            keys.add((str(row[SYS_COL_LINK]).strip(), str(row[SYS_COL_SHEET]).strip(), str(row[SYS_COL_MONTH]).strip()))
        
        log_container.write("🔍 Quét dòng cũ...")
        rows_to_del = get_rows_to_delete_dynamic(wks, keys, log_container)
        if rows_to_del:
            log_container.write(f"✂️ Xóa {len(rows_to_del)} dòng...")
            batch_delete_rows(sh, wks.id, rows_to_del, log_container)
            log_container.write("✅ Đã xóa.")
        
        log_container.write(f"🚀 Ghi {len(df_aligned)} dòng...")
        # Lấy lại dòng cuối chính xác sau khi xóa
        next_row = len(wks.get_all_values()) + 1
        
        chunk_size = 5000
        new_vals = df_aligned.fillna('').values.tolist()
        for i in range(0, len(new_vals), chunk_size):
            wks.append_rows(new_vals[i:i+chunk_size], value_input_option='USER_ENTERED'); time.sleep(1)

        range_map = {}; curr = next_row
        for df, src_link in tasks_list:
            count = len(df); end = curr + count - 1
            range_map[(src_link, df[SYS_COL_SHEET].iloc[0])] = f"{curr} - {end}"
            curr += count
        return True, f"Cập nhật {len(df_aligned)} dòng", range_map
    except Exception as e: return False, f"Lỗi: {str(e)}", {}

# --- PIPELINE ---
def process_pipeline_mixed(rows_to_run, user_id, block_name_run, status_container, check_freq=False):
    creds = get_creds()
    if not acquire_lock(creds, user_id): return False, f"Hệ thống bận", 0
    log_user_action(creds, user_id, f"Chạy: {block_name_run}", "Running")
    try:
        # Lọc các dòng cần chạy (Chưa chốt + Thỏa mãn điều kiện thời gian)
        valid_rows_to_process = []
        for r in rows_to_run:
            status_ok = str(r.get(COL_STATUS, '')).strip() == "Chưa chốt & đang cập nhật"
            time_ok = True
            if check_freq: # Nếu bật chế độ kiểm tra tần suất
                time_ok = should_run_task(r)
            
            if status_ok and time_ok:
                valid_rows_to_process.append(r)

        if not valid_rows_to_process: return True, {}, 0

        grouped = defaultdict(list)
        for r in valid_rows_to_process:
            grouped[(str(r.get(COL_TGT_LINK, '')).strip(), str(r.get(COL_TGT_SHEET, '')).strip())].append(r)
        
        res_map = {}; all_ok = True; total_rows = 0; log_ents = []
        tz = pytz.timezone('Asia/Ho_Chi_Minh'); now = datetime.now(tz).strftime("%d/%m/%Y %H:%M:%S")

        for idx, ((t_link, t_sheet), group_rows) in enumerate(grouped.items()):
            with status_container.expander(f"Processing File {idx+1}...", expanded=True):
                target_headers = []
                try:
                    tid = extract_id(t_link)
                    if tid:
                        sh_t = get_sh_with_retry(creds, tid)
                        try: wks_t = sh_t.worksheet(t_sheet)
                        except: wks_t = None
                        if wks_t: target_headers = wks_t.row_values(1)
                except: pass

                tasks = []
                for i, r in enumerate(group_rows):
                    lnk = r.get(COL_SRC_LINK, ''); lbl = r.get(COL_SRC_SHEET, '')
                    st.write(f"⬇️ Load {i+1}: {lnk[-10:]}")
                    df, sid, msg = fetch_data_v3(r, creds, target_headers)
                    if df is not None: tasks.append((df, lnk)); total_rows += len(df)
                    else: st.error(f"Err: {msg}"); res_map[lnk] = ("Lỗi tải", "", now)
                    del df; gc.collect()

                if tasks:
                    ok, msg, r_map = write_strict_sync(tasks, t_link, t_sheet, creds, st)
                    if not ok: st.error(msg); all_ok = False
                    else: st.success(msg)
                    del tasks; gc.collect()
                    
                    for r in group_rows:
                        l = str(r.get(COL_SRC_LINK, '')).strip()
                        if l not in res_map:
                            s = str(r.get(COL_SRC_SHEET, '')).strip()
                            calc = r_map.get((l, s), "")
                            # Trả về thêm timestamp để update Lan_chay_cuoi
                            res_map[l] = ("Thành công" if ok else "Lỗi", calc, now)
                            log_ents.append([now, r.get(COL_DATA_RANGE), r.get(COL_MONTH), user_id, l, t_link, t_sheet, r.get(COL_SRC_SHEET), "OK", "", calc, block_name_run])

        write_detailed_log(creds, log_ents)
        return all_ok, res_map, total_rows
    finally: release_lock(creds, user_id)

# --- CONFIG & UI ---
@st.cache_data
def load_full_config(_creds):
    sh = get_sh_with_retry(_creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
    if df.empty: return pd.DataFrame(columns=[COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE, COL_FREQ_MIN, COL_LAST_RUN])
    
    # Fill defaults
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    df[COL_MODE] = df[COL_MODE].replace('', 'APPEND').fillna('APPEND')
    df[COL_HEADER] = df[COL_HEADER].replace('', 'TRUE').fillna('TRUE')
    if COL_FREQ_MIN not in df.columns: df[COL_FREQ_MIN] = "0"
    if COL_LAST_RUN not in df.columns: df[COL_LAST_RUN] = ""
    
    if 'STT' in df.columns: df = df.drop(columns=['STT'])
    return df

def save_block_config_to_sheet(df_ui, blk_name, creds, uid):
    if not acquire_lock(creds, uid): st.error("Busy!"); return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_svr = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        if COL_BLOCK_NAME not in df_svr.columns: df_svr[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
        df_oth = df_svr[df_svr[COL_BLOCK_NAME] != blk_name]
        df_save = df_ui.copy(); df_save[COL_BLOCK_NAME] = blk_name
        
        # Clean temp cols
        for c in ['STT', COL_COPY_FLAG]: 
            if c in df_save.columns: df_save = df_save.drop(columns=[c])
            
        # Ensure all columns exist
        final_cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE, COL_FREQ_MIN, COL_LAST_RUN]
        for c in final_cols:
            if c not in df_save.columns: df_save[c] = ""
            if c not in df_oth.columns: df_oth[c] = ""
            
        df_fin = pd.concat([df_oth, df_save], ignore_index=True).astype(str).replace(['nan', 'None'], '')
        
        # Reorder for safety
        df_fin = df_fin[final_cols + [c for c in df_fin.columns if c not in final_cols]]
        
        wks.clear(); set_with_dataframe(wks, df_fin, row=1, col=1)
        st.toast("Saved!", icon="💾")
    finally: release_lock(creds, uid)

def login_ui():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS: st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[key]; return True
    if st.session_state['logged_in']: return True
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        pwd = st.text_input("Mật khẩu:", type="password")
        if st.button("Đăng Nhập"):
            if pwd in AUTHORIZED_USERS: st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]; st.rerun()
            else: st.error("Sai")
    return False

# --- MAIN ---
def main_ui():
    if not login_ui(): return
    uid = st.session_state['current_user_id']; creds = get_creds()
    st.title("⏰ Kinkin (V37 - Full)"); st.caption(f"User: {uid}")

    with st.sidebar:
        if 'df_full_config' not in st.session_state: st.session_state['df_full_config'] = load_full_config(creds)
        if st.button("🔄 Reload"): st.cache_data.clear(); st.session_state['df_full_config'] = load_full_config(creds); st.rerun()
        df_cfg = st.session_state['df_full_config']
        blks = df_cfg[COL_BLOCK_NAME].unique().tolist() if not df_cfg.empty else [DEFAULT_BLOCK_NAME]
        sel_blk = st.selectbox("Block:", blks)
        
        # [NEW] Nút Quét Quyền
        if st.button("🔍 Quét Quyền & Kết Nối"):
            with st.status("Đang kiểm tra...", expanded=True) as s:
                scan_permissions_all(df_cfg.to_dict('records'), creds, s)

    st.subheader(f"Config: {sel_blk}")
    curr_df = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_blk].copy().reset_index(drop=True)
    if COL_COPY_FLAG not in curr_df.columns: curr_df.insert(0, COL_COPY_FLAG, False)
    if 'STT' not in curr_df.columns: curr_df.insert(1, 'STT', range(1, len(curr_df)+1))
    
    # Hiển thị thêm cột Tần suất & Lần chạy cuối
    edt_df = st.data_editor(
        curr_df,
        column_order=[COL_COPY_FLAG, "STT", COL_STATUS, COL_FREQ_MIN, COL_LAST_RUN, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_SRC_SHEET, COL_TGT_LINK, COL_TGT_SHEET, COL_FILTER, COL_HEADER, COL_RESULT, COL_LOG_ROW],
        column_config={
            COL_FREQ_MIN: st.column_config.NumberColumn("Phút (Auto)", help="0 = Thủ công. >0 = Auto chạy sau X phút", default=0),
            COL_LAST_RUN: st.column_config.TextColumn("Chạy cuối", disabled=True),
            COL_COPY_FLAG: st.column_config.CheckboxColumn("Copy", width="small", default=False),
            "STT": st.column_config.NumberColumn("STT", width="small", disabled=True),
            COL_STATUS: st.column_config.SelectboxColumn("Status", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            COL_DATA_RANGE: st.column_config.TextColumn("Range", width="small", default="Lấy hết"),
            COL_MONTH: st.column_config.TextColumn("Month", width="small"),
            COL_SRC_LINK: st.column_config.LinkColumn("Src Link", width="medium"), 
            COL_TGT_LINK: st.column_config.LinkColumn("Tgt Link", width="medium"),
            COL_FILTER: st.column_config.TextColumn("Filter", width="medium"),
            COL_HEADER: st.column_config.CheckboxColumn("Header?", default=True),
            COL_RESULT: st.column_config.TextColumn("Result", disabled=True),
            COL_LOG_ROW: st.column_config.TextColumn("Log Row", disabled=True),
            COL_BLOCK_NAME: None, COL_MODE: None 
        },
        use_container_width=True, num_rows="dynamic", key="edt_v37"
    )

    if edt_df[COL_COPY_FLAG].any():
        nw = []
        for i, r in edt_df.iterrows():
            rc = r.copy(); rc[COL_COPY_FLAG] = False; nw.append(rc)
            if r[COL_COPY_FLAG]: cp = r.copy(); cp[COL_COPY_FLAG] = False; nw.append(cp)
        st.session_state['df_full_config'] = pd.concat([st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] != sel_blk], pd.DataFrame(nw)], ignore_index=True)
        st.rerun()

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("▶️ RUN BLOCK (Manual)", type="primary"):
            # Chạy thủ công: Bỏ qua check tần suất (check_freq=False)
            rows = [r for r in edt_df.to_dict('records')]
            st_cont = st.status("Running...", expanded=True)
            ok, res, tot = process_pipeline_mixed(rows, uid, sel_blk, st_cont, check_freq=False)
            
            # Cập nhật kết quả vào bảng
            for i, r in edt_df.iterrows():
                l = str(r.get(COL_SRC_LINK,'')).strip()
                if l in res: 
                    edt_df.at[i, COL_RESULT] = res[l][0]
                    edt_df.at[i, COL_LOG_ROW] = res[l][1]
                    edt_df.at[i, COL_LAST_RUN] = res[l][2] # Cập nhật thời gian chạy
            save_block_config_to_sheet(edt_df, sel_blk, creds, uid)
            st_cont.update(label="Done!", state="complete", expanded=False); time.sleep(1); st.rerun()
            
    with c2:
        if st.button("⏳ RUN AUTO (Check Frequency)"):
            # Chạy tự động: Kiểm tra tần suất (check_freq=True)
            rows = [r for r in edt_df.to_dict('records')]
            st_cont = st.status("Checking Schedule...", expanded=True)
            ok, res, tot = process_pipeline_mixed(rows, uid, sel_blk, st_cont, check_freq=True)
            
            if tot > 0: # Chỉ save nếu có chạy gì đó
                for i, r in edt_df.iterrows():
                    l = str(r.get(COL_SRC_LINK,'')).strip()
                    if l in res: 
                        edt_df.at[i, COL_RESULT] = res[l][0]
                        edt_df.at[i, COL_LOG_ROW] = res[l][1]
                        edt_df.at[i, COL_LAST_RUN] = res[l][2]
                save_block_config_to_sheet(edt_df, sel_blk, creds, uid)
                st_cont.update(label=f"Auto Run Executed ({tot} rows)!", state="complete")
                time.sleep(1); st.rerun()
            else:
                st_cont.update(label="Nothing to run (Schedule not met)", state="complete")

    with c3:
        if st.button("💾 Save Config Only"): save_block_config_to_sheet(edt_df, sel_blk, creds, uid); st.rerun()

if __name__ == "__main__":
    main_ui()
