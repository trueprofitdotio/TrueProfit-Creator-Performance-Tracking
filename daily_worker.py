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
    # Regex tối ưu hơn để bắt ID youtube (cả dạng ngắn youtu.be và dạng dài)
    match = re.search(r'(?:v=|\/|youtu\.be\/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

def extract_urls(text):
    """
    Hàm helper mới: Tách tất cả link từ một chuỗi hỗn độn
    (xử lý dấu phẩy, xuống dòng, khoảng trắng)
    """
    if not text: return []
    # Regex bắt các chuỗi bắt đầu bằng http/https và kết thúc trước khoảng trắng hoặc dấu phẩy
    return re.findall(r'(https?://[^\s,;"\'<>]+)', str(text))

# --- TASK 1: SYNC TỪ SHEET PROGRESS -> SUPABASE ---
def sync_progress_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Progress -> DB)...")
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PROGRESS')
        records = ws.get_all_records()
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Progress: {e}")
        return

    count_new = 0

    for row in records:
        kol_name = str(row.get('Name', '')).strip()
        
        # --- FIX 1: Dùng hàm extract_urls thay vì split('\n') ---
        raw_links = row.get('Report Link', '')
        report_links = extract_urls(raw_links)
        
        email = row.get('Email', '')
        country = row.get('Location', '')
        sub_count = str(row.get('Subscriber/Follower', ''))
        agreement = row.get('Signed Agreement', '')
        package = str(row.get('Total Package', ''))
        
        try:
            raw_count = row.get('No. Of Content', 0)
            content_count = int(str(raw_count).replace(',', '').strip()) if raw_count else 0
        except:
            content_count = 0

        if not kol_name: continue

        # 1. Upsert KOL
        # --- FIX 2: Thêm .select() để đảm bảo luôn trả về ID ---
        kol_data = {
            'name': kol_name,
            'email': email,
            'country': country,
            'subscriber_count': sub_count
        }
        
        try:
            # .select() là quan trọng để lấy data trả về ngay lập tức
            kol_res = supabase.table('kols').upsert(kol_data, on_conflict='name').select().execute()
            
            if kol_res.data:
                kol_id = kol_res.data[0]['id']
            else:
                # Fallback phòng hờ (nhưng hiếm khi vào đây nếu có .select())
                kol_id = supabase.table('kols').select('id').eq('name', kol_name).execute().data[0]['id']
        except Exception as e:
            print(f"⚠️ Lỗi xử lý KOL {kol_name}: {e}")
            continue

        # 2. Upsert Videos
        for link in report_links: # Giờ report_links là list sạch sẽ từ regex
            clean_link = link.strip()
            
            # Chỉ lấy link youtube valid mới sync
            if not extract_video_id(clean_link): 
                continue
            
            video_data = {
                'kol_id': kol_id, # Link đúng với ID của KOL vừa upsert
                'video_url': clean_link,
                'agreement_link': agreement,
                'total_package': package,
                'content_count': content_count, 
                'status': 'Active'
            }
            try:
                # Upsert video based on URL
                supabase.table('videos').upsert(video_data, on_conflict='video_url').execute()
                count_new += 1
            except Exception as e:
                print(f"⚠️ Lỗi sync video {clean_link}: {e}") 

    print(f"✅ Đã đồng bộ metadata (tìm thấy {count_new} link Youtube valid).")

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
            
            # Map response API
            api_items = {item['id']: item for item in res.get('items', [])}

            for db_vid in chunk:
                item = api_items.get(db_vid['yt_id'])
                if not item: continue # Video có thể bị xóa hoặc private

                stats = item['statistics']
                snippet = item['snippet']
                
                view_count = int(stats.get('viewCount', 0))
                title = snippet.get('title', '')
                published_at = snippet.get('publishedAt', '').split('T')[0]

                # 1. Chuẩn bị data Metrics
                metrics_insert.append({
                    'video_id': db_vid['id'],
                    'view_count': view_count,
                    'recorded_at': today_str 
                })

                # 2. Tính Growth (So với 7 ngày trước)
                date_7_ago = (now_vn - timedelta(days=7)).strftime('%Y-%m-%d')
                
                # Query lịch sử view cũ
                hist = supabase.table('video_metrics').select('view_count')\
                    .eq('video_id', db_vid['id'])\
                    .eq('recorded_at', date_7_ago)\
                    .execute()
                
                view_7_days_ago = hist.data[0]['view_count'] if hist.data else view_count
                growth = view_count - view_7_days_ago

                # 3. Update Cache & Metadata
                final_title = title if title else (db_vid.get('title') or db_vid.get('video_url'))

                supabase.table('videos').update({
                    'title': final_title,
                    'released_date': published_at,
                    'current_views': view_count,
                    'last_7_days_views': growth
                }).eq('id', db_vid['id']).execute()
            
            if metrics_insert:
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)

        except Exception as e:
            print(f"❌ Lỗi batch Youtube API: {e}")

    print(f"✅ Đã update view cho {updated_count} videos (Ngày recorded: {today_str}).")

# --- TASK 3: BUILD DASHBOARD (DB -> SHEET FRONTEND) ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD...")
    
    try:
        # Lấy thêm content_count để hiển thị nếu cần
        res = supabase.table('videos').select('*, kols(name, country, subscriber_count)').order('released_date', desc=True).execute()
        data = res.data
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    headers = ['Video Title', 'KOL Name', 'Country', 'Released', 'Total Views', 'Growth (7 Days)', 'Agreement', 'Package', 'Status']
    rows = []
    
    for item in data:
        raw_title = item.get('title')
        video_url = item.get('video_url', '')
        
        display_title = raw_title if raw_title and str(raw_title).strip() != "" else video_url
        display_title = str(display_title).replace('"', '""') 

        title_cell = f'=HYPERLINK("{video_url}", "{display_title}")'

        agreement_link = item.get('agreement_link', '')
        agreement_cell = f'=HYPERLINK("{agreement_link}", "View Contract")' if agreement_link else "-"

        kol_info = item.get('kols', {}) or {}
        kol_name = kol_info.get('name', 'Unknown')
        country = kol_info.get('country', '')

        views = item.get('current_views', 0)
        growth = item.get('last_7_days_views', 0)
        
        growth_display = f"{growth:,}" 
        if growth > 0: growth_display = "🟢 +" + growth_display
        elif growth == 0: growth_display = "⚪ " + growth_display
        else: growth_display = "🔴 " + growth_display

        row = [
            title_cell,
            kol_name,
            country,
            item.get('released_date'),
            views,
            growth_display,
            agreement_cell,
            item.get('total_package'),
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

        ws.update(range_name='A1', values=[headers])
        ws.format('A1:I1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}})

        if rows:
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
            ws.format(f'E2:E{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            ws.set_basic_filter(f'A1:I{len(rows)+1}') 
            
            # Auto resize column (tuỳ chọn)
            ws.columns_auto_resize(0, 8)
            
        print("✅ Dashboard built successfully!")
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
