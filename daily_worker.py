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
        raise Exception("❌ Không tìm thấy Token đăng nhập (token.json hoặc ENV Var)!")

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
    if not isinstance(url, str): return None
    match = re.search(r'(?:v=|/|embed/|youtu\.be/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

def parse_currency(value):
    """Clean string currency to float (e.g., '$1,000' -> 1000.0)"""
    try:
        # Giữ lại số và dấu chấm, loại bỏ , $ và chữ
        clean = re.sub(r'[^\d.]', '', str(value).replace(',', ''))
        return float(clean)
    except:
        return 0.0

# --- TASK 1: SYNC TỪ SHEET PROGRESS -> SUPABASE ---
def sync_progress_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Progress -> DB)...")
    
    # 1. Load cache video cũ để check duplicate logic
    try:
        print("   - Đang load cache video từ Supabase...")
        all_videos_db = supabase.table('videos').select('id, video_url').execute().data
        db_cache = {}
        for v in all_videos_db:
            v_id = extract_video_id(v['video_url'])
            if v_id:
                db_cache[v_id] = {'id': v['id'], 'original_url': v['video_url']}
    except Exception as e:
        print(f"❌ Lỗi load cache Supabase: {e}")
        return

    # 2. Load Sheet
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PROGRESS')
        records = ws.get_all_records()
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Progress: {e}")
        return

    count_processed = 0
    kols_map = {} 

    for row in records:
        kol_name = str(row.get('Name', '')).strip()
        if not kol_name: continue 
        
        # Upsert KOL
        if kol_name not in kols_map:
            kol_data = {
                'name': kol_name,
                'email': row.get('Email', ''),
                'country': row.get('Location', ''),
                'subscriber_count': str(row.get('Subscriber/Follower', ''))
            }
            try:
                res = supabase.table('kols').upsert(kol_data, on_conflict='name').execute()
                if res.data: kols_map[kol_name] = res.data[0]['id']
                else:
                    data = supabase.table('kols').select('id').eq('name', kol_name).execute().data
                    if data: kols_map[kol_name] = data[0]['id']
            except: continue
        
        kol_id = kols_map.get(kol_name)
        if not kol_id: continue

        # Upsert Video
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
            if vid_id:
                existing_info = db_cache.get(vid_id)
                # Logic: Nếu video đã có -> Dùng lại ID & URL cũ. Nếu chưa -> Tạo mới.
                if existing_info:
                    payload = {
                        'id': existing_info['id'],            
                        'video_url': existing_info['original_url'], 
                        'kol_id': kol_id,
                        'agreement_link': agreement,
                        'total_package': package,
                        'content_count': content_count,
                        'status': 'Active'
                    }
                else:
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
                    supabase.table('videos').upsert(payload, on_conflict='video_url').execute()
                    count_processed += 1
                except Exception as e:
                    print(f"⚠️ Lỗi upsert video {vid_id}: {e}")

    print(f"✅ Đã đồng bộ metadata (xử lý {count_processed} video).")

# --- TASK 2: TRACK VIEW & CALC CPM (YOUTUBE API -> DB) ---
def track_youtube_views():
    print("\n>>> TASK 2: Tracking Views & Calculating CPM...")
    
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
                    # 1. Prepare Metrics
                    metrics_insert.append({
                        'video_id': db_vid['id'],
                        'view_count': view_count,
                        'recorded_at': today_str 
                    })

                    # 2. CALC CPM & Update Metadata
                    # Công thức: CPM = (Total Package * 1000) / (View * Content Count)
                    package_val = parse_currency(db_vid.get('total_package', 0))
                    content_cnt = db_vid.get('content_count', 1)
                    # Tránh chia cho 0
                    if content_cnt == 0: content_cnt = 1
                    
                    cpm_value = 0.0
                    if view_count > 0:
                        cpm_value = (package_val * 1000) / (view_count * content_cnt)

                    final_title = title if title else (db_vid.get('title') or db_vid.get('video_url'))

                    supabase.table('videos').update({
                        'title': final_title,
                        'released_date': published_at,
                        'current_views': view_count,
                        'current_cpm': cpm_value # Save calculated CPM to DB
                    }).eq('id', db_vid['id']).execute()
            
            if metrics_insert:
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)

        except Exception as e:
            print(f"❌ Lỗi batch Youtube API: {e}")

    print(f"✅ Đã update view & CPM cho {updated_count} videos.")

# --- TASK 3: BUILD DASHBOARD (DB -> SHEET FRONTEND) ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD (Updated Columns)...")
    
    # 1. Query Data
    try:
        res = supabase.table('videos').select('*, kols(name, country, subscriber_count)').order('released_date', desc=True).execute()
        data = res.data
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {e}")
        return

    # 2. Query History for Growth
    history_map = {}
    try:
        date_7_ago = (get_hanoi_time() - timedelta(days=7)).strftime('%Y-%m-%d')
        metrics_res = supabase.table('video_metrics').select('video_id, view_count').eq('recorded_at', date_7_ago).execute()
        history_map = {item['video_id']: item['view_count'] for item in metrics_res.data}
    except: pass

    # 3. Build Rows
    # Structure: [Title, KOL, Country, Released, Total View, View 7 Days, Growth, Current CPM, Agreement, Package, Content Count]
    headers = [
        'Video Title', 'KOL Name', 'Country', 'Released', 
        'Total Views', 'View (Last 7 Days)', 'Growth', 
        'Current CPM', 'Agreement', 'Total Package', 'Content Count'
    ]
    rows = []
    
    for item in data:
        video_url = item.get('video_url', '')
        video_id = item.get('id')
        
        # Title Display
        raw_title = item.get('title')
        display_title = raw_title if raw_title and str(raw_title).strip() != "" else video_url
        display_title = str(display_title).replace('"', '""') 
        title_cell = f'=HYPERLINK("{video_url}", "{display_title}")'
        
        # Agreement Link
        agreement_link = item.get('agreement_link', '')
        agreement_cell = f'=HYPERLINK("{agreement_link}", "View Contract")' if agreement_link else "-"

        # KOL Info
        kol_info = item.get('kols', {}) or {}
        kol_name = kol_info.get('name', 'Unknown')
        country = kol_info.get('country', '')

        # Metrics
        current_views = item.get('current_views', 0) or 0
        old_views = history_map.get(video_id, 0) 
        growth_value = current_views - old_views
        
        # CPM
        cpm = item.get('current_cpm', 0) or 0
        
        # Content Count
        content_count = item.get('content_count', 0)

        row = [
            title_cell,
            kol_name,
            country,
            item.get('released_date'),
            current_views,  # E
            old_views,      # F
            growth_value,   # G
            cpm,            # H (Current CPM)
            agreement_cell, # I
            item.get('total_package'), # J
            content_count   # K (Content Count - Last Column)
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

        ws.update(range_name='A1', values=[headers])
        ws.format('A1:K1', {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER', 'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}})

        if rows:
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
            
            # Format Number (Views): E, F, G
            ws.format(f'E2:G{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
            
            # Format Number (CPM): H (2 decimal places)
            ws.format(f'H2:H{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0.00'}})
            
            # Format Number (Content Count): K
            ws.format(f'K2:K{len(rows)+1}', {'numberFormat': {'type': 'NUMBER', 'pattern': '0'}})

            ws.set_basic_filter(f'A1:K{len(rows)+1}') 
            
        print("✅ Dashboard built successfully! (Added CPM & Content Count, Removed Status)")
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
