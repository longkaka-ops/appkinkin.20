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
from st_copy_to_clipboard import st_copy_to_clipboard  # Thư viện Copy nút bấm

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Kinkin Data Manager (Pro)", layout="wide", page_icon="🚀")

# 🔐 DANH SÁCH USER (Demo)
AUTHORIZED_USERS = {
    "admin": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

# EMAIL BOT (Để user copy share quyền)
BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

# Tên các Sheet Hệ Thống
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"

# --- ĐỊNH NGHĨA CỘT (MAPPING) ---
# Các cột cấu hình trong Google Sheet
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

# Các cột MỚI (Advanced Features)
COL_FILTER = "Dieu_Kien_Loc"      # Task 3: Filter query (VD: Cot_A > 100)
COL_HEADER = "Lay_Header"         # Task 10: TRUE/FALSE
COL_MODE = "Che_Do_Ghi"           # Task 11: APPEND hoặc TABLE
COL_NOTE = "Ghi_Chu_User"         # Task 16: Note nghiệp vụ

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- 2. HÀM HỖ TRỢ & AUTH ---
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
    
    # Auto login qua URL param
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True
            st.session_state['current_user_id'] = AUTHORIZED_USERS[key]
            return True
            
    if st.session_state['logged_in']: return True
    
    # Form đăng nhập
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.header("🔒 Đăng nhập hệ thống Kinkin")
        pwd = st.text_input("Mật khẩu truy cập:", type="password")
        if st.button("Đăng Nhập", use_container_width=True):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True
                st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]
                st.rerun()
            else: st.error("Mật khẩu không đúng!")
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

# --- 3. CORE LOGIC (V2 - FIXED) ---

def fetch_data_v2(row_config, creds):
    """
    Hàm lấy dữ liệu nâng cao:
    - Hỗ trợ cắt cột (Range)
    - Hỗ trợ Lọc (Filter Query) - Task 3
    - Hỗ trợ Bỏ Header - Task 10
    """
    link_src = str(row_config.get(COL_SRC_LINK, '')).strip()
    source_label = str(row_config.get(COL_SRC_SHEET, '')).strip()
    month_val = str(row_config.get(COL_MONTH, ''))
    data_range_str = str(row_config.get(COL_DATA_RANGE, 'Lấy hết')).strip()
    
    # New Configs
    filter_query = str(row_config.get(COL_FILTER, '')).strip()
    include_header = str(row_config.get(COL_HEADER, 'TRUE')).strip().upper() == 'TRUE'

    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    
    df = None
    status_msg = ""
    
    try:
        sh_source = get_sh_with_retry(creds, sheet_id)
        if source_label:
            try: wks_source = sh_source.worksheet(source_label)
            except: return None, sheet_id, f"❌ Không thấy sheet: '{source_label}'"
        else: wks_source = sh_source.sheet1
            
        # Lấy toàn bộ data (Tối ưu cho < 200k dòng)
        data = wks_source.get_all_values()
        
        if data and len(data) > 0:
            # Xử lý Header / Data
            if include_header:
                headers = data[0]
                rows = data[1:]
                df = pd.DataFrame(rows, columns=headers)
            else:
                # Nếu không lấy header, coi dòng đầu là dữ liệu luôn
                df = pd.DataFrame(data)

            # 1. Cắt Vùng (Range)
            if data_range_str != "Lấy hết" and ":" in data_range_str:
                try:
                    start_col_str, end_col_str = data_range_str.split(":")
                    start_idx = col_name_to_index(start_col_str.strip())
                    end_idx = col_name_to_index(end_col_str.strip())
                    if start_idx >= 0:
                        end_idx = min(end_idx, len(df.columns) - 1)
                        df = df.iloc[:, start_idx : end_idx + 1]
                except: pass

            # 2. Lọc Dữ Liệu (Task 3)
            if filter_query and filter_query.lower() not in ['nan', '']:
                try:
                    # VD: `Cot_A == 'HN'`
                    original_rows = len(df)
                    df = df.query(filter_query)
                except Exception as e:
                    return None, sheet_id, f"⚠️ Lỗi cú pháp lọc: {e}"

            # Clean data
            df = df.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
            status_msg = "Thành công"
        else:
            status_msg = "Sheet trắng tinh"
            df = pd.DataFrame()

    except Exception as e:
        return None, sheet_id, f"Lỗi tải: {str(e)}"

    if df is not None:
        # Gắn Meta Data chuẩn bị cho bước ghi
        df['__Link_Source__'] = link_src # Cột tạm để định danh
        df['__Thang__'] = month_val
        return df, sheet_id, status_msg
    return None, sheet_id, "Không lấy được data"


def write_smart_v2(tasks_list, target_link, target_sheet_name, creds, write_mode="APPEND"):
    """
    Hàm ghi dữ liệu thông minh (Task 11 & 12) - Đã Fix lỗi IncorrectCellLabel
    """
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi"
        sh = get_sh_with_retry(creds, target_id)
        
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)

        # --- MODE 1: TABLE (Ghi đè bảo toàn công thức) ---
        if write_mode == "TABLE":
            if not tasks_list: return True, "Không có data"
            
            # Gộp tất cả DF
            combined_df = pd.concat([t[0] for t in tasks_list], ignore_index=True)
            
            # Xóa cột tạm hệ thống
            cols_to_drop = [c for c in ['__Link_Source__', '__Thang__'] if c in combined_df.columns]
            combined_df = combined_df.drop(columns=cols_to_drop)

            # --- FIX: Kiểm tra kỹ cột trước khi tính toán ---
            if combined_df.empty or len(combined_df.columns) == 0:
                return True, "Data sau khi lọc bị rỗng (Không có cột hiển thị)"

            # Tìm vùng cần xóa (Từ A2 -> Cột cuối cùng của Data)
            num_cols = len(combined_df.columns)
            
            # Tính chữ cái của cột cuối cùng.
            # FIX: Đảm bảo num_cols > 0 để tránh lỗi IncorrectCellLabel
            last_col_char = gspread.utils.rowcol_to_a1(1, max(1, num_cols)).replace("1", "")
            
            # Xóa data cũ (Batch clear nhanh hơn loop)
            # Lưu ý: Xóa từ dòng 2 để giữ Header
            try:
                wks.batch_clear([f"A2:{last_col_char}"])
            except Exception as e:
                print(f"Warning Clear: {e}")
            
            # Ghi data mới vào từ A2
            set_with_dataframe(wks, combined_df, row=2, col=1, include_index=False, include_column_header=False)
            
            return True, f"Đã làm mới Table ({len(combined_df)} dòng). Mode: TABLE"

        # --- MODE 2: APPEND (Gom nhiều nguồn - Logic cũ) ---
        else:
            links_to_remove = [t[1] for t in tasks_list if t[1]]
            
            # 1. Đọc Header hiện tại để tìm cột 'Link file nguồn'
            existing_headers = []
            try: existing_headers = wks.row_values(1)
            except: pass
            
            col_link_name = "Link file nguồn" # Tên cột hệ thống tự sinh
            
            # Xóa dòng cũ
            if existing_headers and links_to_remove and col_link_name in existing_headers:
                try: 
                    link_col_idx = existing_headers.index(col_link_name) + 1
                    col_values = wks.col_values(link_col_idx)
                    rows_to_delete = []
                    for i, val in enumerate(col_values):
                        if i > 0 and str(val).strip() in links_to_remove: 
                            rows_to_delete.append(i + 1)
                    
                    if rows_to_delete:
                        rows_to_delete.sort()
                        ranges = []
                        start = rows_to_delete[0]; end = start
                        for r in rows_to_delete[1:]:
                            if r == end + 1: end = r
                            else: ranges.append((start, end)); start = r; end = r
                        ranges.append((start, end))
                        
                        delete_reqs = []
                        for start, end in reversed(ranges):
                            delete_reqs.append({
                                "deleteDimension": {
                                    "range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}
                                }
                            })
                        if delete_reqs:
                            sh.batch_update({'requests': delete_reqs})
                except: pass

            # Chuẩn bị data mới
            final_df_list = []
            for df, src_link in tasks_list:
                df = df.rename(columns={'__Link_Source__': col_link_name, '__Thang__': 'Tháng'})
                final_df_list.append(df)
            
            if not final_df_list: return True, "Không có data mới"
            
            combined_df = pd.concat(final_df_list, ignore_index=True)
            
            # Xử lý Header cho file đích (Nếu chưa có thì tạo)
            if not existing_headers:
                set_with_dataframe(wks, combined_df, row=1, col=1)
                return True, f"Tạo mới & Ghi {len(combined_df)} dòng"
            else:
                all_cols = existing_headers + [c for c in combined_df.columns if c not in existing_headers]
                if len(all_cols) > len(existing_headers):
                    wks.update("A1", [all_cols])
                
                combined_df = combined_df.reindex(columns=all_cols, fill_value="")
                wks.append_rows(combined_df.values.tolist())
                return True, f"Cập nhật (+{len(combined_df)} dòng). Mode: APPEND"

    except Exception as e: return False, f"Lỗi Ghi: {str(e)}"

# --- 4. HỆ THỐNG KHÓA & LOG ---
def get_system_lock(creds):
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
                # Auto unlock sau 30 phút
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 1800: return False, "", ""
            except: pass
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def set_system_lock(creds, user_id, lock=True):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
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
            wks.append_row([
                "Thời gian", "Vùng lấy", "Tháng", "User", 
                "Link Nguồn", "Link Đích", "Sheet Đích", "Sheet Nguồn", 
                "Kết Quả", "Số Dòng", "Range", "Block"
            ])
        wks.append_rows(log_data_list)
    except Exception as e: print(f"Lỗi log: {e}")

# --- 5. PIPELINE & PERMISSIONS ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi/Sai định dạng"
    try:
        get_sh_with_retry(creds, sheet_id)
        return True, "OK"
    except Exception as e: return False, f"Lỗi: {e}"

def check_permissions_strict(rows_to_run, creds):
    errs = []
    checked_links = {} 
    for row in rows_to_run:
        # Check Nguồn
        link_src = str(row.get(COL_SRC_LINK, '')).strip()
        if "docs.google.com" in link_src:
            if link_src not in checked_links: checked_links[link_src] = verify_access_fast(link_src, creds)
            is_ok, msg = checked_links[link_src]
            if not is_ok: errs.append(f"❌ Nguồn (Không đọc được): {msg} -> {link_src}")

        # Check Đích
        link_tgt = str(row.get(COL_TGT_LINK, '')).strip()
        if "docs.google.com" in link_tgt:
            if link_tgt not in checked_links: checked_links[link_tgt] = verify_access_fast(link_tgt, creds)
            is_ok, msg = checked_links[link_tgt]
            if not is_ok: errs.append(f"❌ Đích (Không ghi được): {msg} -> {link_tgt}")
    
    return (len(errs) == 0), errs

def process_pipeline_ui(rows_to_run, user_id, block_name_run, status_container):
    creds = get_creds()
    is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id:
        return False, f"HỆ THỐNG ĐANG BẬN! {locking_user} đang chạy từ {lock_time}.", 0
    
    set_system_lock(creds, user_id, lock=True)
    try:
        status_container.write("🔄 Đang phân nhóm Tasks...")
        grouped_tasks = defaultdict(list)
        total_fetched_rows = 0
        
        # Gom nhóm theo File Đích + Sheet Đích để xử lý batch
        for row in rows_to_run:
            t_link = str(row.get(COL_TGT_LINK, '')).strip()
            t_sheet = str(row.get(COL_TGT_SHEET, '')).strip() or "Tong_Hop_Data"
            mode = str(row.get(COL_MODE, 'APPEND')).strip().upper()
            grouped_tasks[(t_link, t_sheet, mode)].append(row)

        global_results_map = {} 
        all_success = True
        log_entries = []
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")

        total_groups = len(grouped_tasks)
        current_group = 0

        for (target_link, target_sheet, write_mode), group_rows in grouped_tasks.items():
            current_group += 1
            status_container.write(f"⏳ [{current_group}/{total_groups}] Đang xử lý đích: ...{target_link[-15:]} (Sheet: {target_sheet}) | Mode: {write_mode}")
            
            tasks_list = []
            # 1. FETCH DATA
            for row in group_rows:
                s_link = str(row.get(COL_SRC_LINK, '')).strip()
                df, sid, status = fetch_data_v2(row, creds)
                
                if df is not None:
                    tasks_list.append((df, s_link))
                    total_fetched_rows += len(df)
                    status_container.write(f"   - ✅ Lấy {len(df)} dòng từ nguồn: {row.get(COL_SRC_SHEET)}")
                else:
                    status_container.warning(f"   - ⚠️ Lỗi nguồn: {s_link}")
                    global_results_map[s_link] = ("Lỗi tải/Quyền", "")
                    log_entries.append([
                        time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH), 
                        user_id, s_link, target_link, target_sheet,
                        row.get(COL_SRC_SHEET), "Lỗi tải", "0", "", block_name_run
                    ])

            # 2. WRITE DATA
            msg_update = ""
            success_update = True
            if tasks_list:
                status_container.write(f"   - 💾 Đang ghi xuống đích...")
                success_update, msg_update = write_smart_v2(tasks_list, target_link, target_sheet, creds, write_mode)
                if not success_update: all_success = False
            else:
                success_update = False
                msg_update = "Không có data nguồn hợp lệ"

            # 3. UPDATE RESULT MAP
            status_str = "Thành công" if success_update else f"Lỗi: {msg_update}"
            
            for row in group_rows:
                s_link = str(row.get(COL_SRC_LINK, '')).strip()
                row_count = 0
                for d, l in tasks_list:
                    if l == s_link: row_count = len(d)
                
                log_entries.append([
                    time_now, row.get(COL_DATA_RANGE), row.get(COL_MONTH),
                    user_id, s_link, target_link, target_sheet,
                    row.get(COL_SRC_SHEET), 
                    status_str, str(row_count), msg_update, block_name_run
                ])
                global_results_map[s_link] = (status_str, msg_update)
        
        status_container.write("📝 Đang ghi Log hệ thống...")
        write_detailed_log(creds, log_entries)
        return all_success, global_results_map, total_fetched_rows
    finally:
        set_system_lock(creds, user_id, lock=False)

# --- 6. QUẢN LÝ CONFIG (LOAD/SAVE) ---
def load_full_config(creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df = df.dropna(how='all')
    
    required_cols = [
        COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, 
        COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, 
        COL_RESULT, COL_LOG_ROW, 
        COL_FILTER, COL_HEADER, COL_MODE, COL_NOTE
    ]
    
    for c in required_cols:
        if c not in df.columns: df[c] = ""
    
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', 'Default_Block').fillna('Default_Block')
    df[COL_HEADER] = df[COL_HEADER].replace('', 'TRUE').fillna('TRUE')
    df[COL_MODE] = df[COL_MODE].replace('', 'APPEND').fillna('APPEND')
    
    if 'STT' in df.columns: df = df.drop(columns=['STT'])
    return df

def save_block_config(df_current_ui, current_block_name, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    
    df_full_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df_full_server = df_full_server.dropna(how='all')
    
    if COL_BLOCK_NAME not in df_full_server.columns: df_full_server[COL_BLOCK_NAME] = 'Default_Block'
    
    df_other_blocks = df_full_server[df_full_server[COL_BLOCK_NAME] != current_block_name]
    
    df_to_save = df_current_ui.copy()
    if 'STT' in df_to_save.columns: df_to_save = df_to_save.drop(columns=['STT'])
    df_to_save[COL_BLOCK_NAME] = current_block_name 
    
    df_final = pd.concat([df_other_blocks, df_to_save], ignore_index=True)
    df_final = df_final.astype(str).replace(['nan', 'None', '<NA>'], '')
    
    required_cols = [
        COL_BLOCK_NAME, COL_STATUS, COL_DATA_RANGE, COL_MONTH, 
        COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, 
        COL_RESULT, COL_LOG_ROW, 
        COL_FILTER, COL_HEADER, COL_MODE, COL_NOTE
    ]
    for c in required_cols:
        if c not in df_final.columns: df_final[c] = ""
        
    df_final = df_final[required_cols]
    
    wks.clear()
    wks.update([df_final.columns.tolist()] + df_final.values.tolist())
    st.toast(f"✅ Đã lưu cấu hình khối: {current_block_name}!", icon="💾")

# --- 7. GIAO DIỆN CHÍNH (UI) ---
@st.dialog("📘 TÀI LIỆU HƯỚNG DẪN (V2)", width="large")
def show_guide_popup():
    st.markdown(f"""
    ### 1. Quy Trình Cấp Quyền (Bắt buộc)
    * **Bước 1:** Copy email Bot: `{BOT_EMAIL_DISPLAY}`
    * **Bước 2:** Share quyền **Viewer** cho file Nguồn.
    * **Bước 3:** Share quyền **Editor** cho file Đích.

    ### 2. Các Tính Năng Mới (Advanced)
    | Tính Năng | Cột Config | Hướng Dẫn |
    | :--- | :--- | :--- |
    | **Lọc Dữ Liệu** | `{COL_FILTER}` | Nhập điều kiện lọc. VD: `Cot_A == 'HaNoi'` hoặc `Doanh_So > 1000`. |
    | **Chế Độ Ghi** | `{COL_MODE}` | `APPEND`: Thêm dòng mới vào cuối (Gom file). <br> `TABLE`: Xóa dữ liệu cũ, ghi mới hoàn toàn (Bảo toàn công thức bên cạnh). |
    | **Bỏ Header** | `{COL_HEADER}` | Bỏ chọn nếu file nguồn không có tiêu đề hoặc muốn tự đặt tiêu đề. |

    ### 3. Lưu Ý
    * Với chế độ **TABLE**, hệ thống sẽ xóa dữ liệu từ dòng 2 trở đi trong Sheet đích. Hãy cẩn thận!
    """)

def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    creds = get_creds()
    
    # Header & Tiện ích nhanh
    c_head_1, c_head_2 = st.columns([3, 1])
    with c_head_1:
        st.title(f"🚀 Kinkin Data Manager")
        st.caption(f"User: {user_id} | System Ready")
    with c_head_2:
        with st.popover("🛠️ Tiện ích nhanh"):
            st.write("Email Bot System:")
            st.code(BOT_EMAIL_DISPLAY)
            st_copy_to_clipboard(BOT_EMAIL_DISPLAY, "📋 Copy Email Bot", "Đã copy!")
    
    st.divider()

    # --- SIDEBAR: QUẢN LÝ KHỐI ---
    with st.sidebar:
        st.header("📦 Quản Lý Khối")
        if 'df_full_config' not in st.session_state:
            with st.spinner("Đang tải dữ liệu cấu hình..."): 
                st.session_state['df_full_config'] = load_full_config(creds)
            
        unique_blocks = st.session_state['df_full_config'][COL_BLOCK_NAME].unique().tolist()
        if not unique_blocks: unique_blocks = ["Default_Block"]
        
        selected_block = st.selectbox("Chọn Khối làm việc:", unique_blocks, key="sb_block_select")
        
        with st.expander("Thao tác Khối (Thêm/Xóa)"):
            new_block_input = st.text_input("Tên khối mới:")
            if st.button("➕ Thêm Khối"):
                if new_block_input and new_block_input not in unique_blocks:
                    new_row = {c: "" for c in st.session_state['df_full_config'].columns}
                    new_row[COL_BLOCK_NAME] = new_block_input
                    new_row[COL_STATUS] = 'Chưa chốt & đang cập nhật'
                    new_row[COL_MODE] = 'APPEND'
                    new_row[COL_HEADER] = 'TRUE'
                    
                    st.session_state['df_full_config'] = pd.concat([
                        st.session_state['df_full_config'], pd.DataFrame([new_row])
                    ], ignore_index=True)
                    st.rerun()
            
            if st.button("🗑️ Xóa Khối Này", type="primary"):
                if len(unique_blocks) > 1:
                    st.session_state['df_full_config'] = st.session_state['df_full_config'][
                        st.session_state['df_full_config'][COL_BLOCK_NAME] != selected_block
                    ]
                    empty_df = pd.DataFrame(columns=st.session_state['df_full_config'].columns)
                    save_block_config(empty_df, selected_block, creds)
                    st.rerun()

        st.divider()
        if st.button("📘 Hướng Dẫn Sử Dụng"):
            show_guide_popup()

    # --- MAIN: DATA EDITOR ---
    st.subheader(f"⚡ Cấu hình chi tiết: {selected_block}")
    
    df_display = st.session_state['df_full_config'][
        st.session_state['df_full_config'][COL_BLOCK_NAME] == selected_block
    ].copy().reset_index(drop=True)
    
    df_display.insert(0, 'STT', range(1, len(df_display) + 1))
    
    column_config = {
        "STT": st.column_config.NumberColumn("STT", width="small", disabled=True),
        COL_STATUS: st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True, width="medium"),
        COL_SRC_LINK: st.column_config.TextColumn("Link Nguồn", width="large", help="Link Google Sheet chứa dữ liệu"),
        COL_TGT_LINK: st.column_config.TextColumn("Link Đích", width="large", help="Link Google Sheet nhận dữ liệu"),
        COL_SRC_SHEET: st.column_config.TextColumn("Sheet Nguồn", width="medium"),
        COL_TGT_SHEET: st.column_config.TextColumn("Sheet Đích", width="medium"),
        COL_MODE: st.column_config.SelectboxColumn("Chế Độ Ghi", options=["APPEND", "TABLE"], width="medium", help="APPEND: Nối thêm | TABLE: Xóa cũ ghi mới (Bảo toàn công thức)"),
        COL_FILTER: st.column_config.TextColumn("Bộ Lọc (Query)", width="medium", help="VD: Cot_A == 'HN'"),
        COL_HEADER: st.column_config.CheckboxColumn("Lấy Header?", default=True),
        COL_RESULT: st.column_config.TextColumn("Kết quả chạy", disabled=True),
        COL_LOG_ROW: st.column_config.TextColumn("Log dòng", disabled=True),
        COL_NOTE: st.column_config.TextColumn("Ghi chú", width="large"),
        COL_BLOCK_NAME: None
    }
    
    col_order = [
        "STT", COL_STATUS, COL_MODE, 
        COL_SRC_LINK, COL_SRC_SHEET, 
        COL_TGT_LINK, COL_TGT_SHEET, 
        COL_FILTER, COL_HEADER, 
        COL_RESULT, COL_LOG_ROW, COL_NOTE
    ]

    edited_df = st.data_editor(
        df_display,
        column_order=col_order,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        key=f"editor_{selected_block}",
        height=400
    )

    # --- ACTION BUTTONS ---
    st.divider()
    c_run, c_save, c_check = st.columns([2, 1, 1])
    
    with c_run:
        if st.button(f"▶️ CHẠY KHỐI: {selected_block}", type="primary", use_container_width=True):
            rows_run = edited_df[edited_df[COL_STATUS] == "Chưa chốt & đang cập nhật"].to_dict('records')
            rows_run = [r for r in rows_run if len(str(r.get(COL_SRC_LINK, ''))) > 5]
            
            if not rows_run:
                st.warning("⚠️ Không có dòng nào ở trạng thái 'Chưa chốt' để chạy.")
            else:
                with st.status(f"🚀 Đang khởi động xử lý {len(rows_run)} tasks...", expanded=True) as status:
                    status.write("🔐 Đang kiểm tra quyền truy cập...")
                    ok_check, err_list = check_permissions_strict(rows_run, creds)
                    
                    if not ok_check:
                        status.update(label="❌ Lỗi Quyền!", state="error")
                        st.error("Thiếu quyền truy cập các file sau:")
                        for e in err_list: st.error(e)
                    else:
                        status.write("✅ Quyền OK. Bắt đầu Pipeline...")
                        start_t = time.time()
                        
                        all_ok, results_map, total_rows = process_pipeline_ui(rows_run, user_id, selected_block, status)
                        
                        elapsed = time.time() - start_t
                        
                        for idx, row in edited_df.iterrows():
                            s_link = str(row.get(COL_SRC_LINK, '')).strip()
                            if s_link in results_map:
                                msg, log_info = results_map[s_link]
                                if row[COL_STATUS] == "Chưa chốt & đang cập nhật":
                                    edited_df.at[idx, COL_RESULT] = msg
                                edited_df.at[idx, COL_LOG_ROW] = log_info

                        status.write("💾 Đang lưu kết quả...")
                        save_block_config(edited_df, selected_block, creds)
                        
                        status.update(label=f"🏁 Hoàn tất! ({total_rows} dòng / {elapsed:.1f}s)", state="complete", expanded=False)
                        st.success("Đã chạy xong quy trình.")
                        time.sleep(1)
                        st.rerun()

    with c_save:
        if st.button("💾 Lưu Cấu Hình", use_container_width=True):
            save_block_config(edited_df, selected_block, creds)
            del st.session_state['df_full_config']
            st.rerun()

    with c_check:
        if st.button("🔍 Quét Lỗi", use_container_width=True):
            rows_check = edited_df.to_dict('records')
            ok, errs = check_permissions_strict(rows_check, creds)
            if ok: st.toast("✅ Tất cả link đều ổn!", icon="check")
            else: 
                with st.expander("Chi tiết lỗi", expanded=True):
                    for e in errs: st.error(e)

if __name__ == "__main__":
    main_ui()
