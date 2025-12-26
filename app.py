import streamlit as st
import pandas as pd
import requests
import time
import random
import gspread
import json
import re
import threading
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from datetime import datetime
from google.oauth2 import service_account
import google.auth.transport.requests
import pytz
from collections import defaultdict
from st_copy_to_clipboard import st_copy_to_clipboard

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Kinkin Data Manager (V4 - Copy Mode)", layout="wide", page_icon="🚀")

AUTHORIZED_USERS = {
    "admin2025": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

# Tên Sheet Hệ Thống
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"

# Định nghĩa Cột
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
# Cột Mới
COL_FILTER = "Dieu_Kien_Loc"      
COL_HEADER = "Lay_Header"         
COL_MODE = "Che_Do_Ghi"           
COL_NOTE = "Ghi_Chu_User"
COL_COPY_ACTION = "Copy_Hanh_Dong" # Cột ảo để bắt sự kiện copy dòng

DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- 2. HÀM HỖ TRỢ CƠ BẢN ---
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

def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[key]; return True
    if st.session_state['logged_in']: return True
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.header("🔒 Đăng nhập hệ thống")
        pwd = st.text_input("Nhập mật khẩu:", type="password")
        if st.button("Đăng Nhập", use_container_width=True):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]; st.rerun()
            else: st.error("Sai mật khẩu!")
    return False

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

# --- 3. CORE LOGIC (GIỮ NGUYÊN) ---

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
                headers = data[0]
                rows = data[1:]
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
    except Exception as e:
        return None, sheet_id, f"Lỗi tải: {str(e)}"

    if df is not None:
        df['__Link_Source__'] = link_src
        df['__Thang__'] = month_val
        return df, sheet_id, status_msg
    return None, sheet_id, "Không lấy được dữ liệu"

def scan_realtime_row_ranges(target_link, target_sheet_name, creds):
    results = {}
    try:
        target_id = extract_id(target_link)
        if not target_id: return {}
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: return {}
        
        all_data = wks.get_all_values()
        if not all_data: return {}
        
        headers = all_data[0]
        try: link_col_idx = headers.index("Link file nguồn")
        except ValueError: return {} 
        
        temp_map = {} 
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) > link_col_idx:
                link_val = row[link_col_idx]
                if link_val:
                    if link_val not in temp_map: temp_map[link_val] = [i, i]
                    else: temp_map[link_val][1] = i 
        
        for link, (start, end) in temp_map.items():
            results[link] = f"{start} - {end}"
    except: return {}
    return results

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

            if combined_df.empty or len(combined_df.columns) == 0:
                return True, "Data rỗng (Check lại bộ lọc)"

            num_cols = len(combined_df.columns)
            last_col_char = gspread.utils.rowcol_to_a1(1, max(1, num_cols)).replace("1", "")
            try: wks.batch_clear([f"A2:{last_col_char}"])
            except: pass
            
            set_with_dataframe(wks, combined_df, row=2, col=1, include_index=False, include_column_header=False)
            return True, f"Đã làm mới Table ({len(combined_df)} dòng)"
        else:
            links_to_remove = [t[1] for t in tasks_list if t[1]]
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

# --- 4. HỆ THỐNG LOCK & LOG & SCHEDULE ---
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

def fetch_recent_logs(creds, limit=50):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_LOG_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df.empty: return pd.DataFrame()
        return df.tail(limit).iloc[::-1]
    except: return pd.DataFrame()

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

# --- 5. PIPELINE XỬ LÝ ---
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
    try:
        if status_container: status_container.write("🔄 Đang phân nhóm dữ liệu...")
        grouped_tasks = defaultdict(list)
        for row in rows_to_run:
            t_link = str(row.get(COL_TGT_LINK, '')).strip()
            t_sheet = str(row.get(COL_TGT_SHEET, '')).strip()
            mode = str(row.get(COL_MODE, 'APPEND')).strip().upper()
            grouped_tasks[(t_link, t_sheet, mode)].append(row)

        global_results_map = {} 
        all_success = True
        log_entries = []
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
                    tasks_list.append((df, s_link))
                    total_rows_all += len(df)
                    if status_container: status_container.write(f"   + Lấy {len(df)} dòng: {row.get(COL_SRC_SHEET)}")
                else:
                    global_results_map[s_link] = ("Lỗi tải", "")
                    log_entries.append([time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH), user_id, s_link, target_link, target_sheet, row.get(COL_SRC_SHEET), "Lỗi tải", "0", "", block_name_run])

            success_update, msg_update = False, "No Data"
            if tasks_list:
                success_update, msg_update = write_smart_v2(tasks_list, target_link, target_sheet, creds, write_mode)
                if not success_update: all_success = False
            
            realtime_ranges = scan_realtime_row_ranges(target_link, target_sheet, creds)
            
            status_str = "Thành công" if success_update else f"Lỗi: {msg_update}"
            for row in group_rows:
                s_link = str(row.get(COL_SRC_LINK, '')).strip()
                final_range = realtime_ranges.get(s_link, "")
                global_results_map[s_link] = (status_str, final_range if final_range else msg_update)
                
                cnt = 0
                for d, l in tasks_list:
                    if l == s_link: cnt = len(d)
                log_entries.append([time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH), user_id, s_link, target_link, target_sheet, row.get(COL_SRC_SHEET), status_str, str(cnt), final_range, block_name_run])

        write_detailed_log(creds, log_entries)
        return all_success, global_results_map, total_rows_all

    finally:
        set_system_lock(creds, user_id, lock=False)

# --- 6. QUẢN LÝ CONFIG (GỐC) ---
def load_full_config(creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df = df.dropna(how='all')
    
    required_cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE, COL_NOTE]
    for c in required_cols:
        if c not in df.columns: df[c] = ""
    
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    df[COL_MODE] = df[COL_MODE].replace('', 'APPEND').fillna('APPEND')
    df[COL_HEADER] = df[COL_HEADER].replace('', 'TRUE').fillna('TRUE')
    
    if 'STT' in df.columns: df = df.drop(columns=['STT'])
    return df

def save_block_config(df_current_ui, current_block_name, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    
    df_full = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
    if COL_BLOCK_NAME not in df_full.columns: df_full[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
    
    df_other = df_full[df_full[COL_BLOCK_NAME] != current_block_name]
    
    df_save = df_current_ui.copy()
    if 'STT' in df_save.columns: df_save = df_save.drop(columns=['STT'])
    # Bỏ cột check copy khi lưu
    if COL_COPY_ACTION in df_save.columns: df_save = df_save.drop(columns=[COL_COPY_ACTION])
    
    df_save[COL_BLOCK_NAME] = current_block_name
    
    df_final = pd.concat([df_other, df_save], ignore_index=True).astype(str).replace(['nan', 'None'], '')
    
    cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE, COL_NOTE]
    for c in cols:
        if c not in df_final.columns: df_final[c] = ""
    
    wks.clear()
    wks.update([cols] + df_final[cols].values.tolist())
    st.toast("✅ Đã lưu!", icon="💾")

def rename_block_action(old_name, new_name, creds):
    if not new_name or new_name == old_name: return False
    
    # 1. Update Sheet Config
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    if COL_BLOCK_NAME in df.columns:
        df.loc[df[COL_BLOCK_NAME] == old_name, COL_BLOCK_NAME] = new_name
        wks.clear(); wks.update([df.columns.tolist()] + df.fillna('').values.tolist())
    
    # 2. Update Sheet Schedule
    try:
        wks_sch = sh.worksheet(SHEET_SYS_CONFIG)
        df_sch = get_as_dataframe(wks_sch, evaluate_formulas=True, dtype=str)
        if COL_BLOCK_NAME in df_sch.columns:
            df_sch.loc[df_sch[COL_BLOCK_NAME] == old_name, COL_BLOCK_NAME] = new_name
            wks_sch.clear(); wks_sch.update([df_sch.columns.tolist()] + df_sch.fillna('').values.tolist())
    except: pass
    
    return True

def save_full_direct(df_full, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    cols = [COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_MODE, COL_NOTE]
    df_full = df_full.astype(str).replace(['nan', 'None'], '')
    for c in cols:
         if c not in df_full.columns: df_full[c] = ""
    wks.clear()
    wks.update([cols] + df_full[cols].values.tolist())

# --- 7. UI CHÍNH ---
@st.dialog("📘 TÀI LIỆU", width="large")
def show_guide():
    st.markdown(f"""
    **Email Bot:** `{BOT_EMAIL_DISPLAY}`
    ### Tính năng Mới:
    1. **Sao chép Khối:** Bấm nút "©️ Sao Chép Khối" ở Sidebar.
    2. **Sao chép Dòng:** Tích vào ô "☑️" đầu dòng để nhân đôi dòng đó.
    3. **Đổi tên khối:** Vào Sidebar > Sửa tên khối.
    """)

def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    creds = get_creds()
    
    # Header
    c1, c2 = st.columns([3, 1])
    with c1: st.title("🚀 Kinkin Data Manager (V4)"); st.caption(f"User: {user_id}")
    with c2: 
        with st.popover("Tiện ích"):
            st.code(BOT_EMAIL_DISPLAY)
            st_copy_to_clipboard(BOT_EMAIL_DISPLAY, "📋 Copy Email Bot")

    # --- SIDEBAR: QUẢN LÝ KHỐI ---
    with st.sidebar:
        if 'df_full_config' not in st.session_state:
            with st.spinner("Load config..."): st.session_state['df_full_config'] = load_full_config(creds)
        
        blocks = st.session_state['df_full_config'][COL_BLOCK_NAME].unique().tolist()
        if not blocks: blocks = [DEFAULT_BLOCK_NAME]
        
        # Chọn Khối & Copy Khối
        sel_block = st.selectbox("Chọn Khối:", blocks)
        
        c_copy_blk, c_name_copy = st.columns([1, 2])
        if st.button("©️ Sao Chép Khối"):
             # Logic Copy Khối
             new_block_name = f"{sel_block}_bản_sao"
             if new_block_name in blocks:
                 st.toast(f"Tên {new_block_name} đã tồn tại!", icon="⚠️")
             else:
                 # Lọc lấy data của block cũ
                 block_data = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_block].copy()
                 block_data[COL_BLOCK_NAME] = new_block_name
                 # Nối vào Dataframe chính
                 st.session_state['df_full_config'] = pd.concat([st.session_state['df_full_config'], block_data], ignore_index=True)
                 st.toast(f"Đã tạo: {new_block_name}")
                 time.sleep(0.5); st.rerun()

        with st.expander("⚙️ Quản lý Khối (Thêm/Sửa/Xóa)"):
            # Thêm
            new_b = st.text_input("Tên khối mới:")
            if st.button("➕ Tạo Mới"):
                row = {c: "" for c in st.session_state['df_full_config'].columns}
                row[COL_BLOCK_NAME] = new_b; row[COL_STATUS] = "Chưa chốt & đang cập nhật"
                st.session_state['df_full_config'] = pd.concat([st.session_state['df_full_config'], pd.DataFrame([row])], ignore_index=True)
                st.rerun()
                
            # Sửa tên
            rename_val = st.text_input("Đổi tên khối thành:", value=sel_block)
            if st.button("✏️ Đổi Tên") and rename_val != sel_block:
                if rename_block_action(sel_block, rename_val, creds):
                    st.toast(f"Đã đổi {sel_block} -> {rename_val}")
                    del st.session_state['df_full_config']
                    time.sleep(1); st.rerun()
                    
            # Xóa
            if st.button("🗑️ Xóa Khối"):
                st.session_state['df_full_config'] = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] != sel_block]
                save_block_config(pd.DataFrame(), sel_block, creds)
                st.rerun()
        
        st.divider()
        if st.button("📘 Hướng Dẫn"): show_guide()

    # --- MAIN EDITOR ---
    st.subheader(f"Cấu hình: {sel_block}")
    
    # Lấy data của block hiện tại để hiển thị
    # Sử dụng Session State để giữ data giữa các lần rerun (tránh reset mất nút check)
    if 'current_block_data' not in st.session_state or st.session_state.get('last_sel_block') != sel_block:
        df_show = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_block].copy().reset_index(drop=True)
        # Thêm cột Checkbox Copy (Mặc định False)
        df_show[COL_COPY_ACTION] = False
        st.session_state['current_block_data'] = df_show
        st.session_state['last_sel_block'] = sel_block

    df_display = st.session_state['current_block_data']
    
    # Thêm cột STT hiển thị
    if 'STT' not in df_display.columns: df_display.insert(0, 'STT', range(1, len(df_display)+1))
    else: df_display['STT'] = range(1, len(df_display)+1)
    
    # Cấu hình hiển thị Editor
    edited_df = st.data_editor(
        df_display,
        column_order=[COL_COPY_ACTION, "STT", COL_STATUS, COL_MODE, COL_SRC_LINK, COL_SRC_SHEET, COL_TGT_LINK, COL_TGT_SHEET, COL_FILTER, COL_HEADER, COL_RESULT, COL_LOG_ROW, COL_NOTE],
        column_config={
            COL_COPY_ACTION: st.column_config.CheckboxColumn("©️", width="small", help="Tích để nhân bản dòng này", default=False),
            "STT": st.column_config.NumberColumn(width="small", disabled=True),
            COL_STATUS: st.column_config.SelectboxColumn(options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            COL_MODE: st.column_config.SelectboxColumn(options=["APPEND", "TABLE"], help="APPEND: Nối đuôi | TABLE: Xóa cũ ghi mới"),
            COL_SRC_LINK: st.column_config.LinkColumn("Link Nguồn", display_text="Mở Link", width="medium"), 
            COL_TGT_LINK: st.column_config.LinkColumn("Link Đích", display_text="Mở Link", width="medium"),
            COL_FILTER: st.column_config.TextColumn(help="VD: Cot_A > 100"),
            COL_HEADER: st.column_config.CheckboxColumn(default=True),
            COL_RESULT: st.column_config.TextColumn(disabled=True),
            COL_LOG_ROW: st.column_config.TextColumn(disabled=True),
            COL_BLOCK_NAME: None
        },
        use_container_width=True, num_rows="dynamic", key=f"edit_{sel_block}"
    )

    # --- XỬ LÝ SỰ KIỆN COPY DÒNG ---
    # Kiểm tra xem có dòng nào được tích vào cột Copy không
    if edited_df[COL_COPY_ACTION].any():
        new_rows = []
        for index, row in edited_df.iterrows():
            # Thêm dòng hiện tại
            new_rows.append(row)
            # Nếu dòng này được tích copy
            if row[COL_COPY_ACTION]:
                # Tạo bản sao
                row_copy = row.copy()
                row_copy[COL_COPY_ACTION] = False # Reset nút check của dòng mới
                new_rows.append(row_copy)
                
                # Reset nút check của dòng gốc (để không copy mãi)
                new_rows[-2][COL_COPY_ACTION] = False 
        
        # Cập nhật lại DataFrame hiển thị
        st.session_state['current_block_data'] = pd.DataFrame(new_rows).reset_index(drop=True)
        st.rerun() # Load lại trang ngay để hiện dòng mới

    # Cập nhật lại vào df_full_config khi user chỉnh sửa nội dung khác
    # Lưu ý: Chỉ update khi không phải sự kiện copy (vì copy đã rerun rồi)
    if not edited_df[COL_COPY_ACTION].any():
        # Update session state block data để đồng bộ
        st.session_state['current_block_data'] = edited_df
        
        # Merge ngược lại vào df_full_config (để nút Lưu hoạt động đúng)
        df_full = st.session_state['df_full_config']
        # Xóa data cũ của block này trong df_full
        df_full = df_full[df_full[COL_BLOCK_NAME] != sel_block]
        
        # Chuẩn bị data mới để nối
        df_to_merge = edited_df.copy()
        if 'STT' in df_to_merge.columns: df_to_merge = df_to_merge.drop(columns=['STT'])
        if COL_COPY_ACTION in df_to_merge.columns: df_to_merge = df_to_merge.drop(columns=[COL_COPY_ACTION])
        df_to_merge[COL_BLOCK_NAME] = sel_block
        
        # Nối lại
        st.session_state['df_full_config'] = pd.concat([df_full, df_to_merge], ignore_index=True)


    # --- SCHEDULE SECTION ---
    st.divider()
    st.markdown(f"**⏰ Cài Đặt Hẹn Giờ (Block: {sel_block})**")
    if 'df_sys_schedule' not in st.session_state: st.session_state['df_sys_schedule'] = load_sys_schedule(creds)
    df_sch = st.session_state['df_sys_schedule']
    row_sch = df_sch[df_sch[COL_BLOCK_NAME] == sel_block]
    cur_h, cur_f = 8, "Hàng ngày"
    if not row_sch.empty:
        try: cur_h = int(row_sch.iloc[0]['Run_Hour']); cur_f = str(row_sch.iloc[0]['Run_Freq'])
        except: pass
        
    c1, c2, c3 = st.columns(3)
    with c1: new_freq = st.selectbox("Tần suất:", ["Hàng ngày", "Hàng tuần", "Hàng tháng"], index=["Hàng ngày", "Hàng tuần", "Hàng tháng"].index(cur_f) if cur_f in ["Hàng ngày", "Hàng tuần", "Hàng tháng"] else 0)
    with c2: new_hour = st.slider("Giờ chạy (VN):", 0, 23, value=cur_h)
    with c3: 
        st.write("")
        if st.button("Lưu Hẹn Giờ"):
            new_r = {COL_BLOCK_NAME: sel_block, "Run_Hour": str(new_hour), "Run_Freq": new_freq}
            df_sch = df_sch[df_sch[COL_BLOCK_NAME] != sel_block]
            df_sch = pd.concat([df_sch, pd.DataFrame([new_r])], ignore_index=True)
            save_sys_schedule(df_sch, creds)
            st.session_state['df_sys_schedule'] = df_sch
            st.toast("Đã lưu lịch!")

    # --- BUTTONS ---
    st.divider()
    c_run, c_all, c_scan, c_save = st.columns([2, 2, 1, 1])
    
    with c_run:
        if st.button(f"▶️ CHẠY KHỐI: {sel_block}", type="primary"):
            rows = edited_df[edited_df[COL_STATUS] == "Chưa chốt & đang cập nhật"].to_dict('records')
            rows = [r for r in rows if len(str(r.get(COL_SRC_LINK, ''))) > 5]
            if not rows: st.warning("Không có việc cần chạy."); st.stop()
            
            with st.status("Đang chạy...", expanded=True) as status:
                ok, errs = check_permissions_strict(rows, creds)
                if not ok: st.error("Lỗi quyền!"); st.write(errs); st.stop()
                
                start = time.time()
                ok, res_map, total = process_pipeline_mixed(rows, user_id, sel_block, status)
                
                for i, r in edited_df.iterrows():
                    lnk = str(r.get(COL_SRC_LINK, '')).strip()
                    if lnk in res_map:
                        edited_df.at[i, COL_RESULT] = res_map[lnk][0]
                        edited_df.at[i, COL_LOG_ROW] = res_map[lnk][1]
                
                save_block_config(edited_df, sel_block, creds)
                status.update(label=f"Xong! {total} dòng trong {time.time()-start:.1f}s", state="complete")
                time.sleep(1); st.rerun()

    with c_all:
        if st.button("🚀 CHẠY TẤT CẢ"):
            with st.status("Đang chạy toàn hệ thống...", expanded=True) as status:
                full_df = load_full_config(creds)
                all_blks = full_df[COL_BLOCK_NAME].unique()
                total_all = 0
                for blk in all_blks:
                    status.write(f"Đang chạy khối: **{blk}**")
                    mask = (full_df[COL_BLOCK_NAME] == blk) & (full_df[COL_STATUS] == "Chưa chốt & đang cập nhật")
                    rows = full_df[mask].to_dict('records')
                    if rows:
                        _, res_map, cnt = process_pipeline_mixed(rows, f"{user_id} (All)", blk, None)
                        total_all += cnt
                        for i, r in full_df[mask].iterrows():
                            lnk = str(r[COL_SRC_LINK]).strip()
                            if lnk in res_map:
                                full_df.at[i, COL_RESULT] = res_map[lnk][0]
                                full_df.at[i, COL_LOG_ROW] = res_map[lnk][1]
                save_full_direct(full_df, creds)
                st.session_state['df_full_config'] = full_df
                status.update(label=f"Hoàn tất! Tổng {total_all} dòng.", state="complete")
                st.rerun()

    with c_scan:
        if st.button("🔍 Quét"):
            ok, errs = check_permissions_strict(edited_df.to_dict('records'), creds)
            if ok: st.toast("OK!")
            else: 
                with st.expander("Lỗi"): st.write(errs)

    with c_save:
        if st.button("💾 Lưu"):
            save_block_config(edited_df, sel_block, creds)
            del st.session_state['df_full_config']
            st.rerun()

    # --- KHU VỰC LOG USER ---
    st.divider()
    with st.expander("📜 Nhật ký hoạt động gần đây (Real-time)", expanded=False):
        if st.button("🔄 Tải lại Log"):
            st.cache_data.clear() 
            
        df_log = fetch_recent_logs(creds, limit=20)
        if not df_log.empty:
            st.dataframe(df_log, use_container_width=True, hide_index=True)
        else:
            st.info("Chưa có log nào.")

if __name__ == "__main__":
    main_ui()
