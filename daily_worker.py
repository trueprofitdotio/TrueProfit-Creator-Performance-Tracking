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
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyChr_rRRYlsH9_wfY8JB1UJ30fPDMBtp0c") # Thay key của mày vào

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
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
gc = get_gspread_client()

# --- HELPERS ---
# --- CẬP NHẬT HELPER MẠNH MẼ HƠN ---
def extract_video_id(url):
    """
    Trích xuất Video ID từ mọi thể loại link Youtube (ngắn, dài, embed, dính tham số...)
    """
    if not isinstance(url, str): return None
    # Regex bắt ID 11 ký tự, chấp nhận cả dấu gạch ngang (-) và gạch dưới (_)
    # Bắt các dạng: youtube.com/watch?v=ID, youtu.be/ID, youtube.com/embed/ID
    match = re.search(r'(?:v=|/|embed/|youtu\.be/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

# --- TASK 1: SYNC TỪ SHEET PROGRESS -> SUPABASE (OPTIMIZED) ---
def sync_progress_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Progress -> DB) - CLEAN VERSION...")
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PROGRESS')
        records = ws.get_all_records()
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Progress: {e}")
        return

    count_new = 0
    kols_map = {} # Cache KOL ID để đỡ gọi DB nhiều lần

    for row_idx, row in enumerate(records):
        kol_name = str(row.get('Name', '')).strip()
        if not kol_name: continue # Bỏ qua dòng trống tên
        
        # --- 1. XỬ LÝ KOL ---
        # Kiểm tra cache trước, nếu chưa có thì Upsert lấy ID
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
                    # Fallback nếu upsert không trả data (hiếm)
                    data = supabase.table('kols').select('id').eq('name', kol_name).execute().data
                    if data: kols_map[kol_name] = data[0]['id']
            except Exception as e:
                print(f"⚠️ Lỗi xử lý KOL {kol_name}: {e}")
                continue
        
        kol_id = kols_map.get(kol_name)
        if not kol_id: continue

        # --- 2. XỬ LÝ VIDEO (LOGIC MỚI) ---
        raw_report_link_cell = str(row.get('Report Link', ''))
        
        # Dùng Regex để tìm TẤT CẢ các link có trong ô (bất chấp Alt+Enter, dấu cách, dấu phẩy)
        # Pattern này bắt chuỗi bắt đầu bằng http/https và kết thúc khi gặp khoảng trắng/xuống dòng
        found_links = re.findall(r'(https?://[^\s,]+)', raw_report_link_cell)
        
        agreement = row.get('Signed Agreement', '')
        package = str(row.get('Total Package', ''))
        try:
            raw_count = row.get('No. Of Content', 0)
            content_count = int(str(raw_count).replace(',', '').strip()) if raw_count else 0
        except: content_count = 0

        for raw_link in found_links:
            # BƯỚC QUAN TRỌNG: Chỉ lấy ID và tạo link sạch
            vid_id = extract_video_id(raw_link)
            
            if vid_id:
                # Tái tạo link chuẩn -> Tránh duplicate do tham số rác (&t=...)
                clean_url = f"https://www.youtube.com/watch?v={vid_id}"
                
                video_data = {
                    'kol_id': kol_id,
                    'video_url': clean_url, # Lưu link sạch vào DB
                    'agreement_link': agreement,
                    'total_package': package,
                    'content_count': content_count,
                    'status': 'Active'
                }
                
                try:
                    # Upsert vào DB
                    supabase.table('videos').upsert(video_data, on_conflict='video_url').execute()
                    count_new += 1
                except Exception as e:
                    print(f"⚠️ Lỗi insert video {vid_id}: {e}")
            else:
                # Link không đúng định dạng Youtube -> Bỏ qua hoặc log nhẹ
                pass

    print(f"✅ Đã đồng bộ metadata (tìm thấy và xử lý {count_new} link video).")
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
    # FIX: Lấy ngày theo giờ Hà Nội (GMT+7)
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

                # Map với DB
                db_vid = next((v for v in chunk if v['yt_id'] == yt_id), None)
                if db_vid:
                    # 1. Chuẩn bị data Metrics (Lịch sử hôm nay)
                    metrics_insert.append({
                        'video_id': db_vid['id'],
                        'view_count': view_count,
                        'recorded_at': today_str 
                        # created_at sẽ tự động lấy giờ server (UTC), ko cần chỉnh
                    })

                    # 2. Tính Growth (So với 7 ngày trước)
                    # Logic: Lấy ngày hiện tại - 7 ngày
                    date_7_ago = (now_vn - timedelta(days=7)).strftime('%Y-%m-%d')
                    
                    hist = supabase.table('video_metrics').select('view_count')\
                        .eq('video_id', db_vid['id'])\
                        .eq('recorded_at', date_7_ago)\
                        .execute()
                    
                    # Nếu tìm thấy view cũ thì trừ, ko thì coi growth = 0 (hoặc bằng view hiện tại nếu là video mới tinh)
                    view_7_days_ago = hist.data[0]['view_count'] if hist.data else view_count
                    growth = view_count - view_7_days_ago

                    # 3. Update Cache & Metadata vào bảng Videos
                    # Logic title: Nếu API trả về rỗng, giữ nguyên cũ, hoặc dùng URL
                    final_title = title if title else (db_vid.get('title') or db_vid.get('video_url'))

                    supabase.table('videos').update({
                        'title': final_title,
                        'released_date': published_at,
                        'current_views': view_count,
                        'last_7_days_views': growth
                    }).eq('id', db_vid['id']).execute()
            
            if metrics_insert:
                # FIX: Upsert dựa trên (video_id, recorded_at)
                # Đảm bảo 1 ngày chỉ có 1 dòng, chạy lại sẽ update view
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)

        except Exception as e:
            print(f"❌ Lỗi batch Youtube API: {e}")

    print(f"✅ Đã update view cho {updated_count} videos (Ngày recorded: {today_str}).")

# --- TASK 3: BUILD DASHBOARD (DB -> SHEET FRONTEND) ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD...")
    
    # Query Data
    try:
        res = supabase.table('videos').select('*, kols(name, country, subscriber_count)').order('released_date', desc=True).execute()
        data = res.data
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    headers = ['Video Title', 'KOL Name', 'Country', 'Released', 'Total Views', 'Growth (7 Days)', 'Agreement', 'Package', 'Status']
    rows = []
    
    for item in data:
        # FIX: Xử lý Title rỗng -> Lấy URL
        raw_title = item.get('title')
        video_url = item.get('video_url', '')
        
        display_title = raw_title if raw_title and str(raw_title).strip() != "" else video_url
        display_title = str(display_title).replace('"', '""') # Escape cho công thức

        # Hyperlink Formula
        title_cell = f'=HYPERLINK("{video_url}", "{display_title}")'

        agreement_link = item.get('agreement_link', '')
        agreement_cell = f'=HYPERLINK("{agreement_link}", "View Contract")' if agreement_link else "-"

        kol_info = item.get('kols', {}) or {}
        kol_name = kol_info.get('name', 'Unknown')
        country = kol_info.get('country', '')

        views = item.get('current_views', 0)
        growth = item.get('last_7_days_views', 0)
        
        # Icon Growth
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

    # Ghi vào Sheet
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
            # Set filter
            ws.set_basic_filter(f'A1:I{len(rows)+1}') 
            
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
