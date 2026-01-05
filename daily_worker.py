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
SHEET_NAME_SOURCE = 'KOL PROGRESS'
SHEET_NAME_DASHBOARD = 'KOL DASHBOARD'

# Env Variables
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://wpzigasfuizrabqqzxln.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_secret_tPw7wEcEku1sVGVITE2X7A_MNtKlCww")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyChr_rRRYlsH9_wfY8JB1UJ30fPDMBtp0c")

# --- AUTH SETUP ---
def get_hanoi_time():
    tz_vn = timezone(timedelta(hours=7))
    return datetime.now(tz_vn)

def get_gspread_client():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    if os.environ.get("TOKEN_JSON_BASE64"):
        print("🔑 Auth: Using Github Secret Token...")
        try:
            token_json = base64.b64decode(os.environ.get("TOKEN_JSON_BASE64")).decode('utf-8')
            creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
        except Exception as e:
            raise Exception(f"❌ Token Error: {e}")
    elif os.path.exists('token.json'):
        print("🔑 Auth: Using Local token.json...")
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        raise Exception("❌ No auth token found!")

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return gspread.authorize(creds)

# Init Clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
gc = get_gspread_client()

# --- HELPERS ---
def extract_video_id(url):
    """Lấy YouTube ID chuẩn từ mọi định dạng link"""
    if not isinstance(url, str): return None
    match = re.search(r'(?:v=|\/|youtu\.be\/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

def extract_all_links(text_blob):
    """Tách tất cả link từ cell, handle xuống dòng, dấu phẩy..."""
    if not text_blob: return []
    # Regex bắt link http/https kết thúc trước khoảng trắng hoặc dấu câu
    return re.findall(r'(https?://[^\s,;"\']+)', str(text_blob))

def get_youtube_details(video_id):
    """Gọi API lấy Title và Date"""
    if not video_id: return None, None
    try:
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet&id={video_id}&key={YOUTUBE_API_KEY}"
        res = requests.get(url).json()
        if 'items' in res and len(res['items']) > 0:
            snippet = res['items'][0]['snippet']
            title = snippet.get('title', '')
            published_at = snippet.get('publishedAt', '').split('T')[0] # Lấy YYYY-MM-DD
            return title, published_at
    except Exception as e:
        print(f"⚠️ YouTube API Error ({video_id}): {e}")
    return None, None

# --- TASK 1: SYNC NEW VIDEOS (PROGRESS -> SUPABASE) ---
def sync_progress_to_db():
    print("\n>>> TASK 1: Scanning for NEW videos in 'KOL PROGRESS'...")
    
    # 1. Lấy danh sách video hiện có trên DB để so sánh (tránh query lặp)
    try:
        existing_res = supabase.table('videos').select('video_url').execute()
        existing_urls = {item['video_url'] for item in existing_res.data}
        print(f"📚 Database hiện có: {len(existing_urls)} videos.")
    except Exception as e:
        print(f"❌ Error fetching existing videos: {e}")
        return

    # 2. Đọc Google Sheet
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(SHEET_NAME_SOURCE)
        rows = ws.get_all_values() # Lấy toàn bộ data dạng mảng
        # Headers: No(0), Email(1), Name(2), Channel(3), Loc(4), Sub(5), Agreement(6), Package(7), ..., Report Link(11)
    except Exception as e:
        print(f"❌ Error reading sheet: {e}")
        return

    new_videos_count = 0
    kol_cache = {} # Cache Name -> ID

    # Bỏ qua dòng header (index 0)
    for i, row in enumerate(rows[1:], start=2):
        # Safety check độ dài row
        if len(row) < 12: continue

        kol_name = row[2].strip() # Cột C
        raw_links = row[11].strip() # Cột L (Report Link)
        
        # Nếu không có tên KOL hoặc không có link -> Skip
        if not kol_name or not raw_links: continue

        # Tách link (xử lý cell có nhiều link)
        links = extract_all_links(raw_links)
        
        for link in links:
            clean_link = link.strip()
            
            # --- CHECK: Nếu link đã có trong DB -> BỎ QUA ---
            if clean_link in existing_urls:
                continue

            print(f"⚡ Phát hiện video mới: {clean_link}")
            
            # --- START PROCESS NEW VIDEO ---
            vid_id_yt = extract_video_id(clean_link)
            if not vid_id_yt: continue

            # A. Xử lý KOL (Upsert & Get ID)
            kol_id = kol_cache.get(kol_name)
            if not kol_id:
                # Map thông tin KOL từ row hiện tại
                kol_data = {
                    'name': kol_name,
                    'email': row[1].strip(),           # Cột B
                    'country': row[4].strip(),         # Cột E
                    'subscriber_count': row[5].strip() # Cột F
                }
                try:
                    res = supabase.table('kols').upsert(kol_data, on_conflict='name').select().execute()
                    if res.data:
                        kol_id = res.data[0]['id']
                    else:
                        # Fallback select nếu upsert không trả data
                        res = supabase.table('kols').select('id').eq('name', kol_name).execute()
                        kol_id = res.data[0]['id']
                    kol_cache[kol_name] = kol_id
                except Exception as e:
                    print(f"⚠️ Lỗi KOL {kol_name}: {e}")
                    continue

            # B. Gọi API lấy thông tin Video (Title, Date)
            yt_title, yt_date = get_youtube_details(vid_id_yt)
            
            # Nếu API fail, dùng tạm tên file/ngày hiện tại (để sửa sau)
            final_title = yt_title if yt_title else f"Video {vid_id_yt}"
            final_date = yt_date if yt_date else get_hanoi_time().strftime('%Y-%m-%d')

            # C. Insert Video vào DB
            video_data = {
                'kol_id': kol_id,
                'video_url': clean_link,
                'title': final_title,
                'released_date': final_date,
                'agreement_link': row[6].strip(), # Cột G
                'total_package': row[7].strip(),  # Cột H
                'status': 'Active'
            }
            
            try:
                supabase.table('videos').upsert(video_data, on_conflict='video_url').execute()
                existing_urls.add(clean_link) # Add vào cache local để không add trùng trong cùng 1 lần chạy
                new_videos_count += 1
                print(f"✅ Đã thêm video: {final_title}")
            except Exception as e:
                print(f"❌ Lỗi insert video: {e}")

    print(f"\n📊 TỔNG KẾT TASK 1: Đã sync thành công {new_videos_count} video mới.")

# --- TASK 2: UPDATE VIEW & DASHBOARD (DB -> SHEET DASHBOARD) ---
def update_metrics_and_dashboard():
    print("\n>>> TASK 2: Updating Views & Dashboard...")
    
    # 1. Lấy tất cả video Active từ DB
    try:
        # Join bảng kols để lấy tên hiển thị dashboard
        videos = supabase.table('videos').select('*, kols(name, country)').eq('status', 'Active').order('released_date', desc=True).execute().data
    except Exception as e:
        print(f"❌ Lỗi đọc Supabase: {e}")
        return

    # Lọc video có ID youtube hợp lệ
    valid_videos = [v for v in videos if extract_video_id(v['video_url'])]
    print(f"🔍 Đang check view cho {len(valid_videos)} videos...")

    # 2. Batch Request Youtube API (50 id/lần)
    chunk_size = 50
    now_str = get_hanoi_time().strftime('%Y-%m-%d')
    date_7_ago = (get_hanoi_time() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    rows_for_dashboard = []
    
    for i in range(0, len(valid_videos), chunk_size):
        chunk = valid_videos[i:i+chunk_size]
        ids_str = ",".join([extract_video_id(v['video_url']) for v in chunk])
        
        try:
            url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id={ids_str}&key={YOUTUBE_API_KEY}"
            res = requests.get(url).json()
            api_map = {item['id']: item for item in res.get('items', [])}
            
            metrics_upsert = []

            for vid in chunk:
                vid_id_yt = extract_video_id(vid['video_url'])
                yt_data = api_map.get(vid_id_yt)
                
                # Default values (nếu video bị xóa/private)
                current_view = vid.get('current_views', 0)
                display_title = vid.get('title', vid['video_url'])
                
                if yt_data:
                    current_view = int(yt_data['statistics'].get('viewCount', 0))
                    # Tiện thể update luôn title nếu DB đang sai/cũ
                    api_title = yt_data['snippet'].get('title')
                    if api_title: display_title = api_title
                
                # --- Tính Growth ---
                # Lấy view count của 7 ngày trước từ bảng history (video_metrics)
                # Query này nằm trong loop nên hơi chậm, nhưng chính xác. 
                # Có thể tối ưu sau bằng batch query metrics.
                try:
                    hist = supabase.table('video_metrics').select('view_count')\
                        .eq('video_id', vid['id']).eq('recorded_at', date_7_ago).execute()
                    view_last_week = hist.data[0]['view_count'] if hist.data else current_view
                except:
                    view_last_week = current_view
                
                # Nếu video mới < 7 ngày, last week coi như = 0 hoặc logic tuỳ ý
                # Logic: View Last Week là số view tại thời điểm 7 ngày trước.
                # Growth = Current - Last Week
                growth = current_view - view_last_week
                
                # --- Prepare Data Sync ---
                metrics_upsert.append({
                    'video_id': vid['id'],
                    'view_count': current_view,
                    'recorded_at': now_str
                })
                
                # Update lại Main Table
                supabase.table('videos').update({
                    'current_views': current_view,
                    'last_7_days_views': growth,
                    'title': display_title
                }).eq('id', vid['id']).execute()

                # --- Prepare Dashboard Row ---
                # Link Formula
                title_cell = f'=HYPERLINK("{vid["video_url"]}", "{str(display_title).replace("\"", "\"\"")}")'
                
                # Agreement Formula
                agree_link = vid.get('agreement_link', '')
                agree_cell = f'=HYPERLINK("{agree_link}", "View Contract")' if agree_link else "-"

                kol_name = vid['kols']['name'] if vid.get('kols') else 'Unknown'

                row = [
                    title_cell,         # A: Video Title
                    kol_name,           # B: KOL Name
                    vid['released_date'], # C: Released Date
                    vid.get('content_count', 0), # D (Optional)
                    current_view,       # E: Current Views
                    view_last_week,     # F: View Last Week
                    growth,             # G: Growth
                    agree_cell,         # H: Agreement
                    vid.get('total_package'), # I: Package
                    vid.get('status')   # J: Status
                ]
                rows_for_dashboard.append(row)
            
            # Batch upsert metrics history
            if metrics_upsert:
                supabase.table('video_metrics').upsert(metrics_upsert, on_conflict='video_id,recorded_at').execute()
                
        except Exception as e:
            print(f"❌ API Batch Error: {e}")

    # 3. Ghi ra Sheet Dashboard
    if rows_for_dashboard:
        try:
            sh = gc.open_by_key(SPREADSHEET_ID)
            try:
                ws = sh.worksheet(SHEET_NAME_DASHBOARD)
                ws.clear()
            except:
                ws = sh.add_worksheet(title=SHEET_NAME_DASHBOARD, rows=1000, cols=20)
            
            headers = ['Video Title', 'KOL Name', 'Released Date', 'Content #', 'Current Views', 'View Last Week', 'Growth (7d)', 'Agreement', 'Package', 'Status']
            
            ws.update(range_name='A1', values=[headers])
            ws.format('A1:J1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}})
            
            ws.update(range_name='A2', values=rows_for_dashboard, value_input_option='USER_ENTERED')
            
            # Format Numbers (View columns E, F, G)
            ws.format(f'E2:G{len(rows_for_dashboard)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            ws.columns_auto_resize(0, 9)
            ws.set_basic_filter(f'A1:J{len(rows_for_dashboard)+1}')
            
            print("✅ Dashboard updated successfully!")
        except Exception as e:
            print(f"❌ Error writing to Sheet: {e}")

# --- MAIN ---
if __name__ == "__main__":
    try:
        sync_progress_to_db()
        update_metrics_and_dashboard()
        print("\n🚀 ALL PROCESS COMPLETED!")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
