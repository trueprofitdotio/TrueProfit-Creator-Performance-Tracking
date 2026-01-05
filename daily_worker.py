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
    # Tạo timezone GMT+7
    tz_vn = timezone(timedelta(hours=7))
    return datetime.now(tz_vn)

# --- AUTHENTICATION ---
def get_gspread_client():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    # 1. Ưu tiên lấy từ Github Secret (Base64)
    if os.environ.get("TOKEN_JSON_BASE64"):
        print("🔑 Đang dùng Token từ Github Secret...")
        try:
            token_json_str = base64.b64decode(os.environ.get("TOKEN_JSON_BASE64")).decode('utf-8')
            token_info = json.loads(token_json_str)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        except Exception as e:
            raise Exception(f"❌ Lỗi decode token base64: {e}")
            
    # 2. Nếu không có thì tìm file local
    elif os.path.exists('token.json'):
        print("🔑 Đang dùng Token từ file Local...")
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        raise Exception("❌ Không tìm thấy Token đăng nhập (token.json hoặc ENV Var)!")

    # Auto refresh token nếu hết hạn
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return gspread.authorize(creds)

# Init Clients
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    gc = get_gspread_client()
except Exception as e:
    print(f"❌ Lỗi khởi tạo Client: {e}")
    exit(1)

# --- HELPER ---
def extract_video_id(url):
    """
    Trích xuất Video ID từ mọi thể loại link Youtube
    """
    if not isinstance(url, str): return None
    match = re.search(r'(?:v=|/|embed/|youtu\.be/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

# --- TASK 1: SYNC TỪ SHEET PROGRESS -> SUPABASE (FIX DUPLICATE LOGIC) ---
def sync_progress_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Progress -> DB) - SMART MATCHING...")
    
    # BƯỚC 1: LẤY TOÀN BỘ DATA CŨ TỪ DB RA ĐỂ SO KHỚP
    # Mục đích: Biết được video nào đã tồn tại (kể cả link bẩn) để không tạo mới
    try:
        print("   - Đang load cache video từ Supabase...")
        # Lấy id và video_url để đối chiếu
        all_videos_db = supabase.table('videos').select('id, video_url').execute().data
        
        # Tạo Dictionary map: { 'VIDEO_ID_11_CHARS': {'id': 'uuid-...', 'original_url': '...'} }
        db_cache = {}
        for v in all_videos_db:
            v_id = extract_video_id(v['video_url'])
            if v_id:
                db_cache[v_id] = {
                    'id': v['id'],
                    'original_url': v['video_url'] # Lưu lại link gốc (dù bẩn hay sạch)
                }
    except Exception as e:
        print(f"❌ Lỗi load cache Supabase: {e}")
        return

    # Load Sheet
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PROGRESS')
        records = ws.get_all_records()
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Progress: {e}")
        return

    count_processed = 0
    kols_map = {} 

    for row_idx, row in enumerate(records):
        kol_name = str(row.get('Name', '')).strip()
        if not kol_name: continue 
        
        # --- XỬ LÝ KOL (Giữ nguyên) ---
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
                # print(f"⚠️ Lỗi xử lý KOL {kol_name}: {e}")
                continue
        
        kol_id = kols_map.get(kol_name)
        if not kol_id: continue

        # --- XỬ LÝ VIDEO (LOGIC MỚI FIX DUP) ---
        raw_report_link_cell = str(row.get('Report Link', ''))
        found_links = re.findall(r'(https?://[^\s,]+)', raw_report_link_cell)
        
        agreement = row.get('Signed Agreement', '')
        package = str(row.get('Total Package', ''))
        try:
            raw_count = row.get('No. Of Content', 0)
            content_count = int(str(raw_count).replace(',', '').strip()) if raw_count else 0
        except: content_count = 0

        for raw_link in found_links:
            # 1. Bóc tách ID từ link trên Sheet
            vid_id = extract_video_id(raw_link)
            
            if vid_id:
                # 2. CHECK TRONG DATABASE CŨ
                existing_info = db_cache.get(vid_id)
                
                if existing_info:
                    # TRƯỜNG HỢP 1: Video đã có trong DB (kể cả link bẩn)
                    # -> Dùng lại UUID cũ và URL cũ để update metadata
                    payload = {
                        'id': existing_info['id'],            # QUAN TRỌNG: Key để update đúng dòng cũ
                        'video_url': existing_info['original_url'], # QUAN TRỌNG: Giữ nguyên link cũ của mày
                        'kol_id': kol_id,
                        'agreement_link': agreement,
                        'total_package': package,
                        'content_count': content_count,
                        'status': 'Active'
                    }
                else:
                    # TRƯỜNG HỢP 2: Video hoàn toàn mới
                    # -> Tạo link sạch và insert mới
                    clean_url = f"https://www.youtube.com/watch?v={vid_id}"
                    payload = {
                        'video_url': clean_url,
                        'kol_id': kol_id,
                        'agreement_link': agreement,
                        'total_package': package,
                        'content_count': content_count,
                        'status': 'Active'
                    }
                
                try:
                    # Upsert thông minh (Update nếu có ID, Insert nếu chưa)
                    supabase.table('videos').upsert(payload, on_conflict='video_url').execute()
                    count_processed += 1
                except Exception as e:
                    print(f"⚠️ Lỗi upsert video {vid_id}: {e}")
            else:
                pass

    print(f"✅ Đã đồng bộ metadata (xử lý {count_processed} video, khớp ID thông minh).")

# --- TASK 2: TRACK VIEW (YOUTUBE API -> DB) ---
def track_youtube_views():
    print("\n>>> TASK 2: Tracking Youtube Views...")
    
    # Lấy list video Active
    try:
        videos = supabase.table('videos').select('*').eq('status', 'Active').execute().data
    except Exception as e:
        print(f"❌ Lỗi đọc Supabase: {e}")
        return
    
    valid_videos = []
    for v in videos:
        vid = extract_video_id(v['video_url'])
        if vid:
            v['yt_id'] = vid
            valid_videos.append(v)
    
    print(f"🔍 Scan {len(valid_videos)} videos...")

    chunk_size = 50
    now_vn = get_hanoi_time()
    today_str = now_vn.strftime('%Y-%m-%d') 
    
    updated_count = 0

    for i in range(0, len(valid_videos), chunk_size):
        chunk = valid_videos[i:i+chunk_size]
        ids = ",".join([v['yt_id'] for v in chunk])
        
        try:
            url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={ids}&key={YOUTUBE_API_KEY}"
            res = requests.get(url).json()
            
            metrics_insert = []
            
            for item in res.get('items', []):
                yt_id = item['id']
                stats = item['statistics']
                snippet = item['snippet']
                
                view_count = int(stats.get('viewCount', 0))
                title = snippet.get('title', '')
                published_at = snippet.get('publishedAt', '').split('T')[0]

                db_vid = next((v for v in chunk if v['yt_id'] == yt_id), None)
                if db_vid:
                    # 1. Chuẩn bị data Metrics (Lịch sử hôm nay)
                    metrics_insert.append({
                        'video_id': db_vid['id'],
                        'view_count': view_count,
                        'recorded_at': today_str 
                    })

                    # 2. Update Metadata vào bảng Videos
                    # UPDATE: Đã bỏ logic tính growth và update cột last_7_days_views ở đây
                    final_title = title if title else (db_vid.get('title') or db_vid.get('video_url'))

                    supabase.table('videos').update({
                        'title': final_title,
                        'released_date': published_at,
                        'current_views': view_count
                    }).eq('id', db_vid['id']).execute()
            
            if metrics_insert:
                # Upsert metrics
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)

        except Exception as e:
            print(f"❌ Lỗi batch Youtube API: {e}")

    print(f"✅ Đã update view cho {updated_count} videos (Ngày recorded: {today_str}).")

# --- TASK 3: BUILD DASHBOARD (DB -> SHEET FRONTEND) ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD (Raw Data + History Column)...")
    
    # 1. Query Data Video & KOL
    try:
        res = supabase.table('videos').select('*, kols(name, country, subscriber_count)').order('released_date', desc=True).execute()
        data = res.data
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    # 2. Query Data History (View 7 ngày trước) - Source of Truth từ DB
    history_map = {}
    try:
        date_7_ago = (get_hanoi_time() - timedelta(days=7)).strftime('%Y-%m-%d')
        print(f"📅 Đang lấy dữ liệu view từ Supabase ngày: {date_7_ago}")

        metrics_res = supabase.table('video_metrics')\
            .select('video_id, view_count')\
            .eq('recorded_at', date_7_ago)\
            .execute()
        
        # Map: Video ID -> View 7 ngày trước
        history_map = {item['video_id']: item['view_count'] for item in metrics_res.data}
    except Exception as e:
        print(f"⚠️ Warning: Không lấy được history ({e}) -> Coi như view cũ = 0")

    # 3. Build Rows
    # Cấu trúc cột mới: 
    # [Total Views] | [View (Last 7 Days)] | [Growth]
    headers = [
        'Video Title', 'KOL Name', 'Country', 'Released', 
        'Total Views', 'View (Last 7 Days)', 'Growth', 
        'Agreement', 'Package', 'Status'
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

        # --- LOGIC TÍNH TOÁN MỚI ---
        current_views = item.get('current_views', 0) or 0
        
        # Lấy view cũ từ history_map (DB)
        # Nếu video mới chưa có lịch sử 7 ngày trước -> old_views = 0
        old_views = history_map.get(video_id, 0) 
        
        # Tính Growth = Total - Last 7 Days
        growth_value = current_views - old_views

        row = [
            title_cell,
            kol_name,
            country,
            item.get('released_date'),
            current_views,  # Cột E: Total View
            old_views,      # Cột F: View 7 ngày trước (Mới)
            growth_value,   # Cột G: Growth (Số thuần túy)
            agreement_cell,
            item.get('total_package'),
            item.get('status')
        ]
        rows.append(row)

    # 4. Ghi vào Sheet
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet('KOL DASHBOARD')
            ws.clear()
        except:
            ws = sh.add_worksheet(title='KOL DASHBOARD', rows=1000, cols=20)

        # Update Header
        ws.update(range_name='A1', values=[headers])
        ws.format('A1:J1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}})

        if rows:
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
            
            # Format số có dấu phẩy cho cả 3 cột E, F, G
            ws.format(f'E2:G{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            
            # Set filter
            ws.set_basic_filter(f'A1:J{len(rows)+1}') 
            
        print("✅ Dashboard built successfully! (No Icons, New Columns)")
    except Exception as e:
        print(f"❌ Lỗi ghi Google Sheet: {e}")

# --- MAIN ---
if __name__ == "__main__":
    try:
        sync_progress_to_db()
        track_youtube_views()
        build_dashboard()
        print("\n🚀 ALL TASKS COMPLETED!")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")

