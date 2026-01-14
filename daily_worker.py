import pandas as pd
import gspread
import re
import os
import json
import base64
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# --- CẤU HÌNH ---
SPREADSHEET_ID = '15Q7_YzBYMjCceBB5-yi51noA0d03oqRIcd-icDvCdqI'

# Lấy từ biến môi trường (Github) hoặc hardcode (Local)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://wpzigasfuizrabqqzxln.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_secret_tPw7wEcEku1sVGVITE2X7A_MNtKlCww")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyChr_rRRYlsH9_wfY8JB1UJ30fPDMBtp0c") 

# --- MÚI GIỜ HÀ NỘI (GMT+7) ---
def get_hanoi_time():
    tz_vn = timezone(timedelta(hours=7))
    return datetime.now(tz_vn)

# --- AUTHENTICATION ---
def get_gspread_client():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    if os.environ.get("TOKEN_JSON_BASE64"):
        try:
            token_json_str = base64.b64decode(os.environ.get("TOKEN_JSON_BASE64")).decode('utf-8')
            token_info = json.loads(token_json_str)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        except Exception as e:
            raise Exception(f"❌ Lỗi decode token base64: {e}")
            
    elif os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        raise Exception("❌ Không tìm thấy Token đăng nhập!")

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return gspread.authorize(creds)

# Init Clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
gc = get_gspread_client()

# --- HELPER ---
def extract_video_id(url):
    """Trích xuất Video ID từ link Youtube"""
    if not isinstance(url, str): return None
    match = re.search(r'(?:v=|/|embed/|youtu\.be/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

# --- TASK 1: SYNC TỪ SHEET PROGRESS -> SUPABASE (MAP ID STRATEGY) ---
def sync_progress_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Progress -> DB)...")
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PROGRESS')
        records = ws.get_all_records()
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Progress: {e}")
        return

    # Map ID Youtube -> Link URL đang tồn tại trong DB
    db_id_to_url_map = {} 
    existing_urls_set = set() 

    try:
        db_urls = supabase.table('videos').select('video_url').execute().data
        for item in db_urls:
            u = item['video_url']
            existing_urls_set.add(u)
            
            vid_id = extract_video_id(u)
            if vid_id:
                db_id_to_url_map[vid_id] = u
                
        print(f"ℹ️ Đã load {len(db_urls)} videos từ DB. Mapping được {len(db_id_to_url_map)} Youtube IDs.")
    except Exception as e:
        print(f"⚠️ Không load được danh sách URL cũ: {e}")

    count_new = 0
    kols_map = {} 

    for row_idx, row in enumerate(records):
        kol_name = str(row.get('Name', '')).strip()
        if not kol_name: continue 
        
        # --- 1. XỬ LÝ KOL ---
        if kol_name not in kols_map:
            kol_data = {
                'name': kol_name,
                'email': row.get('Email', ''),
                'country': row.get('Location', ''),
                'subscriber_count': str(row.get('Subscriber/Follower', ''))
            }
            try:
                res = supabase.table('kols').upsert(kol_data, on_conflict='name').execute()
                if res.data:
                    kols_map[kol_name] = res.data[0]['id']
                else:
                    data = supabase.table('kols').select('id').eq('name', kol_name).execute().data
                    if data: kols_map[kol_name] = data[0]['id']
            except Exception as e:
                print(f"⚠️ Lỗi xử lý KOL {kol_name}: {e}")
                continue
        
        kol_id = kols_map.get(kol_name)
        if not kol_id: continue

        # --- 2. XỬ LÝ VIDEO ---
        raw_report_link_cell = str(row.get('Report Link', ''))
        found_links = re.findall(r'(https?://[^\s,]+)', raw_report_link_cell)
        
        agreement = row.get('Signed Agreement', '')
        package = str(row.get('Total Package', ''))
        try:
            raw_count = row.get('No. Of Content', 0)
            content_count = int(str(raw_count).replace(',', '').strip()) if raw_count else 0
        except: content_count = 0

        for raw_link in found_links:
            vid_id = extract_video_id(raw_link)
            final_url_to_upsert = raw_link 

            if vid_id:
                # Youtube: Check duplicate ID
                if vid_id in db_id_to_url_map:
                    final_url_to_upsert = db_id_to_url_map[vid_id]
                else:
                    final_url_to_upsert = f"https://www.youtube.com/watch?v={vid_id}"
            else:
                # Non-Youtube
                final_url_to_upsert = raw_link

            video_data = {
                'kol_id': kol_id,
                'video_url': final_url_to_upsert, 
                'agreement_link': agreement,
                'total_package': package,
                'content_count': content_count,
                'status': 'Active'
            }
            
            try:
                supabase.table('videos').upsert(video_data, on_conflict='video_url').execute()
                count_new += 1
            except Exception as e:
                print(f"⚠️ Lỗi insert video {final_url_to_upsert}: {e}")

    print(f"✅ Đã đồng bộ metadata (xử lý {count_new} link video).")

# --- TASK 2: TRACK VIEW (FAIL-SAFE MODE - NO GROWTH COLUMN) ---
def track_youtube_views():
    print("\n>>> TASK 2: Tracking Views (Fail-Safe Mode)...")
    
    # 1. Lấy toàn bộ video Active
    try:
        videos = supabase.table('videos').select('*').eq('status', 'Active').execute().data
    except Exception as e:
        print(f"❌ Lỗi đọc Supabase: {e}")
        return
    
    youtube_videos = []
    other_videos = [] 

    # 2. Phân loại
    for v in videos:
        vid = extract_video_id(v['video_url'])
        if vid:
            v['yt_id'] = vid
            youtube_videos.append(v)
        else:
            other_videos.append(v)
    
    print(f"🔍 Total Scan: {len(videos)} videos ({len(youtube_videos)} Youtube | {len(other_videos)} Others)")

    now_vn = get_hanoi_time()
    today_str = now_vn.strftime('%Y-%m-%d') 
    
    updated_count = 0
    filled_count = 0

    # --- PHẦN A: XỬ LÝ NON-YOUTUBE (AUTO FILL VIEW CŨ) ---
    non_yt_metrics = []
    for ov in other_videos:
        last_known_view = ov.get('current_views', 0) or 0
        non_yt_metrics.append({
            'video_id': ov['id'],
            'view_count': last_known_view,
            'recorded_at': today_str 
        })
        filled_count += 1
    
    if non_yt_metrics:
        try:
            print(f"⚡ Đang Auto-fill {len(non_yt_metrics)} video Non-Youtube...")
            supabase.table('video_metrics').upsert(non_yt_metrics, on_conflict='video_id,recorded_at').execute()
        except Exception as e:
            print(f"❌ Lỗi Insert Non-Youtube: {e}")

    # --- PHẦN B: XỬ LÝ YOUTUBE (FAIL-SAFE LOGIC) ---
    chunk_size = 50
    for i in range(0, len(youtube_videos), chunk_size):
        chunk = youtube_videos[i:i+chunk_size]
        ids_to_send = [v['yt_id'] for v in chunk]
        ids_string = ",".join(ids_to_send)
        
        metrics_insert = []
        returned_ids_set = set() 

        # B.1: CỐ GẮNG GỌI API
        try:
            url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={ids_string}&key={YOUTUBE_API_KEY}"
            res = requests.get(url).json()
            
            returned_items = res.get('items', [])
            
            # Nếu API trả về data, xử lý bình thường
            for item in returned_items:
                yt_id = item['id']
                returned_ids_set.add(yt_id) 
                
                stats = item['statistics']
                snippet = item['snippet']
                
                try: view_count = int(stats.get('viewCount', 0))
                except: view_count = 0
                
                title = snippet.get('title', '')
                published_at = snippet.get('publishedAt', '').split('T')[0]

                db_vid = next((v for v in chunk if v['yt_id'] == yt_id), None)
                
                if db_vid:
                    # Add vào list insert Metrics
                    metrics_insert.append({
                        'video_id': db_vid['id'],
                        'view_count': view_count,
                        'recorded_at': today_str 
                    })

                    # FIX: CHỈ UPDATE VIEW HIỆN TẠI, KHÔNG TÍNH GROWTH, KHÔNG GHI LAST_7_DAYS
                    final_title = title if title else (db_vid.get('title') or db_vid.get('video_url'))
                    supabase.table('videos').update({
                        'title': final_title,
                        'released_date': published_at,
                        'current_views': view_count
                        # Bỏ dòng 'last_7_days_views': growth -> Hết lỗi PGRST204
                    }).eq('id', db_vid['id']).execute()

        except Exception as e:
            print(f"⚠️ API Chunk Error (sẽ chuyển sang auto-fill): {e}")

        # B.2: ĐIỀN CHỖ TRỐNG (AUTO FILL MISSING / API ERROR)
        for original_vid in chunk:
            if original_vid['yt_id'] not in returned_ids_set:
                last_known_view = original_vid.get('current_views', 0) or 0
                
                print(f"⚠️ Auto-fill view cũ cho {original_vid['yt_id']}: {last_known_view}")
                
                metrics_insert.append({
                    'video_id': original_vid['id'],
                    'view_count': last_known_view,
                    'recorded_at': today_str 
                })
                filled_count += 1

        # B.3: BATCH UPSERT
        if metrics_insert:
            try:
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)
            except Exception as e:
                print(f"❌ Lỗi CRITICAL khi Upsert Metrics vào Supabase: {e}")

    print(f"✅ DONE: {updated_count} rows updated (Bao gồm cả Live và Filled).")

# --- TASK 3: BUILD DASHBOARD (CPM FIX) ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD (Raw Data & History)...")
    
    try:
        gc = get_gspread_client()
    except Exception as e:
        print(f"❌ Lỗi Auth Google Sheet (gc): {e}")
        return

    # 1. Query Data Video & KOL
    try:
        print("   - Đang lấy data từ Supabase...")
        res = supabase.table('videos').select('*, kols(name, country, subscriber_count)').order('released_date', desc=True).execute()
        data = res.data
        print(f"   - Tìm thấy {len(data)} videos.")
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    # 2. Query Data History (View 7 ngày trước) - QUERY TRỰC TIẾP TỪ VIDEO_METRICS
    try:
        date_7_ago = (get_hanoi_time() - timedelta(days=7)).strftime('%Y-%m-%d')
        metrics_res = supabase.table('video_metrics')\
            .select('video_id, view_count')\
            .eq('recorded_at', date_7_ago)\
            .execute()
        
        history_map = {item['video_id']: item['view_count'] for item in metrics_res.data}
    except Exception as e:
        print(f"⚠️ Warning: Không lấy được history ({e}) -> Sẽ mặc định view cũ = 0")
        history_map = {}

    headers = [
        'Video Title', 'KOL Name', 'Country', 'Released', 
        'Total Views', 'View 7 Days Ago', 'Growth (7 Days)', 'CPM ($)',
        'Agreement', 'Package', 'Content Count'
    ]
    rows = []
    
    for item in data:
        raw_title = item.get('title')
        video_url = item.get('video_url', '')
        video_id = item.get('id')
        
        display_title = raw_title if raw_title and str(raw_title).strip() != "" else video_url
        display_title = str(display_title).replace('"', '""')

        title_cell = f'=HYPERLINK("{video_url}", "{display_title}")'
        
        agreement_link = item.get('agreement_link', '')
        agreement_cell = f'=HYPERLINK("{agreement_link}", "View Contract")' if agreement_link else "-"

        kol_info = item.get('kols', {}) or {}
        kol_name = kol_info.get('name', 'Unknown')
        country = kol_info.get('country', '')

        # --- DATA VIEW ---
        current_views = item.get('current_views', 0)
        old_views = history_map.get(video_id, 0)
        growth = current_views - old_views
        
        # --- DATA CPM INPUTS ---
        raw_package = str(item.get('total_package', '0'))
        clean_package_str = re.sub(r'[^\d.]', '', raw_package) 
        try:
            total_package = float(clean_package_str) if clean_package_str else 0
        except:
            total_package = 0
        
        try:
            content_count = int(item.get('content_count', 0))
        except: content_count = 1
        
        # --- CPM CALCULATION ---
        cpm = 0
        denominator = current_views * content_count
        
        if denominator > 0:
            cpm = (total_package * 1000) / denominator

        row = [
            title_cell, kol_name, country, item.get('released_date'),
            current_views, old_views, growth, cpm,
            agreement_cell, item.get('total_package'), content_count
        ]
        rows.append(row)

    # 4. Ghi vào Sheet
    try:
        print("   - Đang ghi vào Google Sheet...")
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet('KOL DASHBOARD')
            ws.clear()
        except:
            print("   - Sheet 'KOL DASHBOARD' chưa có, đang tạo mới...")
            ws = sh.add_worksheet(title='KOL DASHBOARD', rows=1000, cols=20)

        ws.update(range_name='A1', values=[headers])
        ws.format('A1:K1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}})

        if rows:
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
            ws.format(f'E2:G{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            ws.format(f'H2:H{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0.00'}})
            ws.format(f'K2:K{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '0'}})
            
            requests = [
                {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(rows)+1, "startColumnIndex": 6, "endColumnIndex": 7}], "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}}}}}, "index": 0}},
                {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(rows)+1, "startColumnIndex": 6, "endColumnIndex": 7}], "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 1, "green": 0, "blue": 0}}}}}, "index": 1}}
            ]
            sh.batch_update({"requests": requests})
            ws.set_basic_filter(f'A1:K{len(rows)+1}') 

        print("✅ DONE! Vào Sheet check đi tml.")
    except Exception as e:
        print(f"❌ Chết đoạn ghi Sheet: {e}")

if __name__ == "__main__":
    try:
        sync_progress_to_db()
        track_youtube_views()
        build_dashboard()
        print("\n🚀 ALL TASKS COMPLETED!")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
