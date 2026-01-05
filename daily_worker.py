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
    # Regex bắt ID youtube chuẩn
    match = re.search(r'(?:v=|\/|youtu\.be\/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

# --- TASK 1: SYNC TỪ SHEET PERFORMANCE -> SUPABASE ---
def sync_performance_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Performance -> DB)...")
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PERFORMANCE - TP')
        
        # [UPDATE] Đọc từ A2 đến E (Lấy thêm cột Content Count)
        raw_rows = ws.get('A2:E') 
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Performance: {e}")
        return

    print(f"📊 Đã đọc {len(raw_rows)} dòng từ Sheet. Bắt đầu xử lý...")
    
    count_new = 0
    kol_cache = {} 

    for index, row in enumerate(raw_rows):
        if not row: continue
        
        # [FIX QUAN TRỌNG] Tự động bù cột thiếu nếu dòng bị ngắn
        # Đảm bảo row luôn có đủ 5 phần tử (0-4) để tránh lỗi IndexError
        if len(row) < 5:
            row = row + [''] * (5 - len(row))

        # Parse Data (Map đúng cột theo yêu cầu)
        # Col A (0): Link
        # Col B (1): Title
        # Col C (2): Date
        # Col D (3): Name
        # Col E (4): Content Count
        
        video_link = row[0].strip()
        video_title = row[1].strip()
        release_date = row[2].strip()
        kol_name = row[3].strip()
        raw_content_count = row[4]

        # Validate bắt buộc
        if not video_link: continue # Không có link -> bỏ qua
        
        # Check link youtube hợp lệ
        if "youtube" not in video_link.lower() and "youtu.be" not in video_link.lower():
            continue
            
        # Nếu không có tên KOL -> bỏ qua
        if not kol_name:
            print(f"⚠️ Dòng {index+2}: Có link nhưng thiếu tên KOL -> Skip.")
            continue

        # 1. Xử lý KOL (Map Name -> ID)
        kol_id = kol_cache.get(kol_name)
        if not kol_id:
            try:
                # Upsert KOL lấy ID
                res = supabase.table('kols').upsert({'name': kol_name}, on_conflict='name').select().execute()
                if res.data:
                    kol_id = res.data[0]['id']
                else:
                    # Fallback tìm ID cũ
                    exist = supabase.table('kols').select('id').eq('name', kol_name).execute()
                    if exist.data: kol_id = exist.data[0]['id']
                
                if kol_id: kol_cache[kol_name] = kol_id
            except Exception as e:
                print(f"⚠️ Lỗi xử lý KOL {kol_name}: {e}")
                continue
        
        if not kol_id: continue

        # 2. Xử lý Data Video
        # Clean date
        if len(release_date) < 8: release_date = None
        
        # Clean content count (chuyển về int)
        try:
            content_count = int(str(raw_content_count).replace(',', '').strip())
        except:
            content_count = 0

        video_data = {
            'kol_id': kol_id,
            'video_url': video_link,
            'title': video_title,
            'released_date': release_date,
            'content_count': content_count, # [NEW] Map cột này
            'status': 'Active'
        }

        try:
            # Upsert video vào Supabase
            # Dùng video_url làm key để update nếu đã tồn tại
            supabase.table('videos').upsert(video_data, on_conflict='video_url').execute()
            count_new += 1
        except Exception as e:
            print(f"⚠️ Lỗi sync video {video_link}: {e}")

    print(f"✅ Đã xử lý xong. (Scan {count_new} videos).")

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
                published_at = snippet.get('publishedAt', '').split('T')[0]

                # --- LOGIC TÍNH GROWTH ---
                date_7_ago = (now_vn - timedelta(days=7)).strftime('%Y-%m-%d')
                
                # Lấy view 7 ngày trước
                hist = supabase.table('video_metrics').select('view_count')\
                    .eq('video_id', db_vid['id'])\
                    .eq('recorded_at', date_7_ago)\
                    .execute()
                
                view_7_days_ago = hist.data[0]['view_count'] if hist.data else 0
                
                # Nếu video mới tinh (<7 ngày) hoặc chưa có lịch sử, coi growth chính là view hiện tại (hoặc 0 tuỳ logic)
                # Logic tốt nhất: Growth = Current - LastWeek. Nếu LastWeek = 0 (do mới add), Growth = Current.
                if not hist.data:
                     # Nếu chưa có data quá khứ, ta tạm tính growth = 0 để tránh số liệu nhảy vọt bất thường
                     # Hoặc để growth = view_count tuỳ bạn. Ở đây tôi để logic an toàn:
                     growth = view_count if view_count < 1000 else 0 # (Hack nhẹ: nếu view nhỏ coi như mới tăng, lớn quá thì coi như mới add vào tool)
                     # UPDATE: Đơn giản nhất là cứ lấy hieu so
                     growth = view_count - view_7_days_ago 
                else:
                    growth = view_count - view_7_days_ago

                # Metrics History
                metrics_insert.append({
                    'video_id': db_vid['id'],
                    'view_count': view_count,
                    'recorded_at': today_str 
                })

                # Update Metadata
                update_payload = {
                    'current_views': view_count,
                    'last_7_days_views': growth
                }
                if title: update_payload['title'] = title
                if published_at: update_payload['released_date'] = published_at

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
        res = supabase.table('videos').select('*, kols(name, country)').order('released_date', desc=True).execute()
        data = res.data
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    # Header
    headers = ['Video Title', 'KOL Name', 'Released Date', 'Content Count', 'Current Views', 'View Last Week', 'Growth (7 Days)', 'Status']
    rows = []
    
    for item in data:
        video_url = item.get('video_url', '')
        raw_title = item.get('title')
        display_title = raw_title if raw_title else video_url
        display_title = str(display_title).replace('"', '""') 
        
        title_cell = f'=HYPERLINK("{video_url}", "{display_title}")'

        kol_info = item.get('kols', {}) or {}
        kol_name = kol_info.get('name', 'Unknown')

        current_views = item.get('current_views', 0) or 0
        growth_7_days = item.get('last_7_days_views', 0) or 0
        content_count = item.get('content_count', 0) # [NEW]

        # Logic View Last Week
        view_last_week = current_views - growth_7_days
        if view_last_week < 0: view_last_week = 0

        row = [
            title_cell,
            kol_name,
            item.get('released_date'),
            content_count,
            current_views,
            view_last_week,
            growth_7_days,
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

        # Update Header & Data
        ws.update(range_name='A1', values=[headers])
        ws.format('A1:H1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.85, 'green': 0.85, 'blue': 0.85}})

        if rows:
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
            # Format số cho cột E, F, G
            ws.format(f'E2:G{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            ws.columns_auto_resize(0, 7)
            ws.set_basic_filter(f'A1:H{len(rows)+1}') 
            
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
