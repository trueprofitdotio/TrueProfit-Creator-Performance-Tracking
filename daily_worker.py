import pandas as pd
import gspread
import re
import os
import json
import base64
import requests
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# --- CẤU HÌNH ---
SPREADSHEET_ID = '15Q7_YzBYMjCceBB5-yi51noA0d03oqRIcd-icDvCdqI'

# Supabase Config
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://wpzigasfuizrabqqzxln.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_secret_tPw7wEcEku1sVGVITE2X7A_MNtKlCww")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyChr_rRRYlsH9_wfY8JB1UJ30fPDMBtp0c")

# --- AUTHENTICATION & SETUP ---
def get_hanoi_time():
    tz_vn = timezone(timedelta(hours=7))
    return datetime.now(tz_vn)

def get_gspread_client():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    if os.environ.get("TOKEN_JSON_BASE64"):
        print("🔑 Đang dùng Token từ Github Secret...")
        try:
            token_json_str = base64.b64decode(os.environ.get("TOKEN_JSON_BASE64")).decode('utf-8')
            token_info = json.loads(token_json_str)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        except Exception as e:
            raise Exception(f"❌ Lỗi decode token base64: {e}")
    elif os.path.exists('token.json'):
        print("🔑 Đang dùng Token từ file Local...")
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        raise Exception("❌ Không tìm thấy Token đăng nhập!")

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return gspread.authorize(creds)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
gc = get_gspread_client()

# --- HELPERS ---
def extract_video_id(url):
    if not isinstance(url, str): return None
    # Regex bắt ID youtube chuẩn (cả short, embed, watch?v=)
    match = re.search(r'(?:v=|\/|youtu\.be\/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

# --- TASK 1: SYNC TỪ SHEET PERFORMANCE -> SUPABASE ---
def sync_performance_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Performance -> DB)...")
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PERFORMANCE - TP')
        
        # --- THAY ĐỔI QUAN TRỌNG: Đọc trực tiếp Columns A đến D ---
        # Thay vì get_all_records, ta đọc mảng giá trị để tránh lỗi header
        # Lấy từ dòng 2 đến hết (A2:D)
        raw_rows = ws.get('A2:D') 
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Performance: {e}")
        return

    print(f"📊 Đã đọc {len(raw_rows)} dòng từ Sheet. Bắt đầu xử lý...")
    
    count_new = 0
    kol_cache = {} # Cache để giảm request DB

    for index, row in enumerate(raw_rows):
        # Đảm bảo row có đủ ít nhất 1 cột (Link)
        if not row: continue
        
        # Parse theo vị trí cột (Index 0 = A, 1 = B, 2 = C, 3 = D)
        # Sử dụng try-except để tránh lỗi index out of range nếu dòng thiếu dữ liệu
        try:
            video_link = row[0].strip() if len(row) > 0 else ""
            video_title = row[1].strip() if len(row) > 1 else ""
            release_date = row[2].strip() if len(row) > 2 else ""
            kol_name = row[3].strip() if len(row) > 3 else ""
        except:
            continue

        # Validate cơ bản
        if not video_link or "youtube" not in video_link.lower() and "youtu.be" not in video_link.lower():
            continue
        if not kol_name:
            continue

        # 1. Xử lý KOL (Map Name -> ID)
        kol_id = kol_cache.get(kol_name)
        if not kol_id:
            try:
                # Upsert KOL để lấy ID
                res = supabase.table('kols').upsert({'name': kol_name}, on_conflict='name').select().execute()
                if res.data:
                    kol_id = res.data[0]['id']
                else:
                    # Fallback tìm ID cũ
                    exist = supabase.table('kols').select('id').eq('name', kol_name).execute()
                    if exist.data: kol_id = exist.data[0]['id']
                
                if kol_id: kol_cache[kol_name] = kol_id
            except Exception as e:
                print(f"⚠️ Lỗi KOL {kol_name}: {e}")
                continue
        
        if not kol_id: continue

        # 2. Upsert Video
        # Clean date format
        if len(release_date) < 8: release_date = None
        
        video_data = {
            'kol_id': kol_id,
            'video_url': video_link,
            'title': video_title,
            'released_date': release_date,
            'status': 'Active'
        }

        try:
            # Upsert video
            supabase.table('videos').upsert(video_data, on_conflict='video_url').execute()
            count_new += 1
        except Exception as e:
            print(f"⚠️ Lỗi sync video {video_link}: {e}")

    print(f"✅ Đã xử lý xong {count_new} video.")

# --- TASK 2: TRACK VIEW (YOUTUBE API -> DB) ---
def track_youtube_views():
    print("\n>>> TASK 2: Tracking Youtube Views...")
    
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
    
    print(f"🔍 Scan {len(valid_videos)} videos trên hệ thống...")

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
            api_items = {item['id']: item for item in res.get('items', [])}

            for db_vid in chunk:
                item = api_items.get(db_vid['yt_id'])
                if not item: continue 

                stats = item['statistics']
                snippet = item['snippet']
                
                view_count = int(stats.get('viewCount', 0))
                title = snippet.get('title', '')
                published_at = snippet.get('publishedAt', '').split('T')[0] # Lấy ngày release chuẩn từ Youtube

                # --- LOGIC TÍNH TOÁN GROWTH ---
                date_7_ago = (now_vn - timedelta(days=7)).strftime('%Y-%m-%d')
                
                # Query metrics cũ (7 ngày trước)
                hist = supabase.table('video_metrics').select('view_count')\
                    .eq('video_id', db_vid['id'])\
                    .eq('recorded_at', date_7_ago)\
                    .execute()
                
                # Nếu không có data 7 ngày trước -> Growth = view hiện tại (coi như mới tăng)
                # Hoặc Growth = 0 tuỳ logic (ở đây để view hiện tại trừ 0)
                view_7_days_ago = hist.data[0]['view_count'] if hist.data else 0
                
                # Nếu video mới release < 7 ngày thì view_7_days_ago có thể chưa có, growth chính là view hiện tại
                growth = view_count - view_7_days_ago

                # 1. Lưu Metrics History (quan trọng để tính growth cho tương lai)
                metrics_insert.append({
                    'video_id': db_vid['id'],
                    'view_count': view_count,
                    'recorded_at': today_str 
                })

                # 2. Update Metadata
                update_payload = {
                    'current_views': view_count,
                    'last_7_days_views': growth
                }
                # Chỉ update title/date nếu DB đang thiếu hoặc API trả về chuẩn hơn
                if title: update_payload['title'] = title
                if published_at: update_payload['released_date'] = published_at

                supabase.table('videos').update(update_payload).eq('id', db_vid['id']).execute()
            
            if metrics_insert:
                # Upsert metrics history
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)

        except Exception as e:
            print(f"❌ Lỗi batch Youtube API: {e}")

    print(f"✅ Đã update view cho {updated_count} videos.")

# --- TASK 3: BUILD DASHBOARD (DB -> SHEET FRONTEND) ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD...")
    
    try:
        # Lấy dữ liệu đã sort theo ngày release giảm dần
        res = supabase.table('videos').select('*, kols(name, country)').order('released_date', desc=True).execute()
        data = res.data
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    # Header chuẩn
    headers = ['Video Title', 'KOL Name', 'Released Date', 'Current Views', 'View Last Week', 'Growth (7 Days)', 'Status']
    rows = []
    
    for item in data:
        video_url = item.get('video_url', '')
        raw_title = item.get('title')
        # Fallback title nếu rỗng
        display_title = raw_title if raw_title else video_url
        display_title = str(display_title).replace('"', '""') # Escape quote cho Excel formula
        
        # Hyperlink
        title_cell = f'=HYPERLINK("{video_url}", "{display_title}")'

        kol_info = item.get('kols', {}) or {}
        kol_name = kol_info.get('name', 'Unknown')

        current_views = item.get('current_views', 0) or 0
        growth_7_days = item.get('last_7_days_views', 0) or 0
        
        # --- LOGIC VIEW LAST WEEK ---
        # View Last Week = Current - Growth
        # Ví dụ: Nay 1000, Tăng 200 -> Tuần trước là 800
        view_last_week = current_views - growth_7_days
        if view_last_week < 0: view_last_week = 0 # Safety

        row = [
            title_cell,
            kol_name,
            item.get('released_date'),
            current_views,
            view_last_week,
            growth_7_days, # Chỉ hiện số, không icon
            item.get('status')
        ]
        rows.append(row)

    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet('KOL DASHBOARD')
            ws.clear()
        except:
            ws = sh.add_worksheet(title='KOL DASHBOARD', rows=1000, cols=20)

        # Format Header
        ws.update(range_name='A1', values=[headers])
        ws.format('A1:G1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.85, 'green': 0.85, 'blue': 0.85}})

        if rows:
            # Ghi data
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
            
            # Format cột số (D, E, F) dạng #,##0
            ws.format(f'D2:F{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            
            # Auto resize & Filter
            ws.columns_auto_resize(0, 6)
            ws.set_basic_filter(f'A1:G{len(rows)+1}') 
            
        print("✅ Dashboard built successfully!")
    except Exception as e:
        print(f"❌ Lỗi ghi Google Sheet: {e}")

# --- MAIN ---
if __name__ == "__main__":
    try:
        sync_performance_to_db()   # Task 1
        track_youtube_views()      # Task 2
        build_dashboard()          # Task 3
        print("\n🚀 ALL TASKS COMPLETED!")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
