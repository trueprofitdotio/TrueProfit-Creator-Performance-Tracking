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

# Init Clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
gc = get_gspread_client()

# --- HELPERS ---
def extract_video_id(url):
    if not isinstance(url, str): return None
    # Regex bắt ID youtube (hỗ trợ cả link thường, link short, link embed)
    match = re.search(r'(?:v=|\/|youtu\.be\/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

# --- TASK 1: SYNC TỪ SHEET PERFORMANCE -> SUPABASE ---
def sync_performance_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Performance -> DB)...")
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        # UPDATE: Đọc sheet mới KOL PERFORMANCE - TP
        ws = sh.worksheet('KOL PERFORMANCE - TP')
        records = ws.get_all_records() # Đọc toàn bộ row có header
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Performance: {e}")
        return

    count_new = 0
    
    # Cache KOL ID để tránh gọi DB quá nhiều lần
    kol_cache = {}

    for row in records:
        # 1. Parse Data theo cột trong Screenshot
        video_link = str(row.get('Link', '')).strip() # Cột Link (A)
        video_title = str(row.get('Title', '')).strip() # Cột Title (B)
        release_date = str(row.get('Date', '')).strip() # Cột Date (C)
        kol_name = str(row.get('Name', '')).strip() # Cột Name (D)
        
        # Nếu dòng không có Link hoặc tên KOL -> Skip
        if not video_link or not kol_name: 
            continue
            
        video_id_yt = extract_video_id(video_link)
        if not video_id_yt:
            continue

        # 2. Xử lý KOL (Upsert & Get ID)
        kol_id = kol_cache.get(kol_name)
        
        if not kol_id:
            # Nếu chưa có trong cache thì upsert vào DB
            try:
                # Upsert name, trả về ID
                res = supabase.table('kols').upsert({'name': kol_name}, on_conflict='name').select().execute()
                if res.data:
                    kol_id = res.data[0]['id']
                    kol_cache[kol_name] = kol_id
                else:
                    # Fallback tìm ID cũ
                    exist = supabase.table('kols').select('id').eq('name', kol_name).execute()
                    if exist.data:
                        kol_id = exist.data[0]['id']
                        kol_cache[kol_name] = kol_id
            except Exception as e:
                print(f"⚠️ Lỗi xử lý KOL {kol_name}: {e}")
                continue
        
        if not kol_id: continue

        # 3. Upsert Video
        # Chuẩn hóa format date nếu cần (Sheet thường là YYYY-MM-DD sẵn rồi)
        try:
            # Check format date sơ bộ, nếu rỗng để None
            if len(release_date) < 8: release_date = None 
        except:
            release_date = None

        video_data = {
            'kol_id': kol_id,
            'video_url': video_link,
            'title': video_title,
            'released_date': release_date,
            'status': 'Active'
        }

        try:
            # Upsert video dựa trên URL, update lại title/date nếu trên sheet có thay đổi
            supabase.table('videos').upsert(video_data, on_conflict='video_url').execute()
            count_new += 1
        except Exception as e:
            print(f"⚠️ Lỗi sync video {video_link}: {e}")

    print(f"✅ Đã đồng bộ metadata (đã xử lý {count_new} dòng).")

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
            api_items = {item['id']: item for item in res.get('items', [])}

            for db_vid in chunk:
                item = api_items.get(db_vid['yt_id'])
                if not item: continue 

                stats = item['statistics']
                snippet = item['snippet']
                
                view_count = int(stats.get('viewCount', 0))
                # Ưu tiên lấy title từ API vì nó chuẩn nhất
                title = snippet.get('title', '')
                
                # Logic Growth: So với 7 ngày trước
                date_7_ago = (now_vn - timedelta(days=7)).strftime('%Y-%m-%d')
                
                # Lấy số view cũ từ bảng metrics
                hist = supabase.table('video_metrics').select('view_count')\
                    .eq('video_id', db_vid['id'])\
                    .eq('recorded_at', date_7_ago)\
                    .execute()
                
                view_7_days_ago = hist.data[0]['view_count'] if hist.data else view_count
                growth = view_count - view_7_days_ago

                # Insert Metrics History (Quan trọng để tính Growth sau này)
                metrics_insert.append({
                    'video_id': db_vid['id'],
                    'view_count': view_count,
                    'recorded_at': today_str 
                })

                # Update Metadata Video
                update_payload = {
                    'current_views': view_count,
                    'last_7_days_views': growth 
                    # last_7_days_views lưu số lượng view TĂNG THÊM trong 7 ngày
                }
                
                # Nếu title trên DB đang rỗng hoặc API trả về title mới -> update
                if title: 
                    update_payload['title'] = title
                
                supabase.table('videos').update(update_payload).eq('id', db_vid['id']).execute()
            
            if metrics_insert:
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)

        except Exception as e:
            print(f"❌ Lỗi batch Youtube API: {e}")

    print(f"✅ Đã update view cho {updated_count} videos.")

# --- TASK 3: BUILD DASHBOARD (DB -> SHEET FRONTEND) ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD...")
    
    try:
        # Join bảng videos và kols
        res = supabase.table('videos').select('*, kols(name, country)').order('released_date', desc=True).execute()
        data = res.data
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    # UPDATE HEADER: Thêm cột View Last Week
    headers = ['Video Title', 'KOL Name', 'Released Date', 'Current Views', 'View Last Week', 'Growth (7 Days)', 'Status']
    rows = []
    
    for item in data:
        # Title & Link
        video_url = item.get('video_url', '')
        raw_title = item.get('title')
        display_title = raw_title if raw_title else video_url
        display_title = str(display_title).replace('"', '""') 
        title_cell = f'=HYPERLINK("{video_url}", "{display_title}")'

        # KOL Info
        kol_info = item.get('kols', {}) or {}
        kol_name = kol_info.get('name', 'Unknown')

        # Metrics
        current_views = item.get('current_views', 0)
        growth_7_days = item.get('last_7_days_views', 0)
        
        # LOGIC MỚI: Tính View Last Week
        # Vì Growth = Current - LastWeek => LastWeek = Current - Growth
        view_last_week = current_views - growth_7_days
        if view_last_week < 0: view_last_week = 0 # Safety check

        # LOGIC MỚI: Bỏ icon, chỉ để số, format sau
        growth_display = growth_7_days 

        row = [
            title_cell,
            kol_name,
            item.get('released_date'),
            current_views,
            view_last_week, # Cột mới
            growth_display,
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

        # Write Header
        ws.update(range_name='A1', values=[headers])
        ws.format('A1:G1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.85, 'green': 0.85, 'blue': 0.85}})

        if rows:
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
            
            # Format Numbers: Cột D, E, F là số (View, Last Week, Growth)
            ws.format(f'D2:F{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            
            # Auto resize
            ws.columns_auto_resize(0, 6)
            ws.set_basic_filter(f'A1:G{len(rows)+1}') 
            
        print("✅ Dashboard built successfully!")
    except Exception as e:
        print(f"❌ Lỗi ghi Google Sheet: {e}")

# --- MAIN ---
if __name__ == "__main__":
    try:
        sync_performance_to_db()
        track_youtube_views()
        build_dashboard()
        print("\n🚀 ALL TASKS COMPLETED!")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
