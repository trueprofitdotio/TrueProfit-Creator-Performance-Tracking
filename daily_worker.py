import pandas as pd
import gspread
import re
import os
import json
import requests
import yt_dlp
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from google.oauth2.service_account import Credentials

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
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    gcp_secret = os.environ.get("GCP_SERVICE_ACCOUNT")
    
    if not gcp_secret:
        raise ValueError("Lỗi: Thiếu GCP_SERVICE_ACCOUNT trong biến môi trường Github Secrets!")
        
    creds_dict = json.loads(gcp_secret)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

# Init Clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- HELPER ---
def extract_video_id(url):
    """Trích xuất Video ID từ link Youtube"""
    if not isinstance(url, str): return None
    match = re.search(r'(?:v=|/|embed/|youtu\.be/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

def fetch_non_yt_data(url):
    """Dùng yt-dlp cào data các nền tảng khác (TikTok, IG,...)"""
    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'extract_flat': False,
        'no_warnings': True,
        'socket_timeout': 15, # Set timeout 15s để GHA ko bị treo nếu bị block
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get('view_count'), info.get('title')
    except Exception as e:
        print(f"   [!] yt-dlp không cào được {url} (Lỗi: {repr(e)})")
        return None, None

def check_video_availability(url):
    """
    Sử dụng yt-dlp để kiểm tra trạng thái video (Public/Unlisted/Private)
    Dùng extract_info để lấy metadata và verify field 'availability'.
    """
    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'no_warnings': True,
        'socket_timeout': 10,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # extract_info sẽ raise exception nếu video bị xóa hoặc private hoàn toàn
            info = ydl.extract_info(url, download=False)
            
            # Lấy availability field (phổ biến trên YouTube)
            # Nếu không có (TikTok/IG), mà extract thành công thì coi như Public
            availability = info.get('availability', 'public')
            
            if not availability:
                return "Public"
                
            availability = str(availability).lower()
            if availability == 'public':
                return "Public"
            elif availability in ['unlisted', 'private', 'limited', 'needs_auth']:
                return "Unlisted/Private"
            else:
                return "Public"
    except Exception as e:
        # Nếu lỗi (404, private, block) -> Coi như không còn Public
        print(f"   [!] yt-dlp check failed (Unavailable): {url}")
        return "Unlisted/Private"

# --- TASK 1: SYNC TỪ SHEET PROGRESS -> SUPABASE ---
def sync_progress_to_db():
    print("\n>>> TASK 1: Syncing Metadata (Progress -> DB)...")
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet('KOL PROGRESS')
        records = ws.get_all_records()
    except Exception as e:
        print(f"❌ Lỗi đọc sheet Progress: {repr(e)}")
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
        print(f"⚠️ Không load được danh sách URL cũ: {repr(e)}")

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
                print(f"⚠️ Lỗi xử lý KOL {kol_name}: {repr(e)}")
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
                if vid_id in db_id_to_url_map:
                    final_url_to_upsert = db_id_to_url_map[vid_id]
                else:
                    final_url_to_upsert = f"https://www.youtube.com/watch?v={vid_id}"
            else:
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
                print(f"⚠️ Lỗi insert video {final_url_to_upsert}: {repr(e)}")

    print(f"✅ Đã đồng bộ metadata (xử lý {count_new} link video).")

# --- TASK 2: TRACK VIEW ---
def track_youtube_views():
    print("\n>>> TASK 2: Tracking Views...")
    
    try:
        videos = supabase.table('videos').select('*').eq('status', 'Active').execute().data
    except Exception as e:
        print(f"❌ Lỗi đọc Supabase: {repr(e)}")
        return
    
    youtube_videos = []
    other_videos = [] 

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

    # --- PHẦN A: XỬ LÝ NON-YOUTUBE BẰNG YT-DLP ---
    non_yt_metrics = []
    if other_videos:
        print(f"⚡ Đang cào view cho {len(other_videos)} video Non-Youtube (TikTok, IG, ...)")
        
    for ov in other_videos:
        url = ov['video_url']
        last_known_view = ov.get('current_views', 0) or 0
        
        scraped_view, scraped_title = fetch_non_yt_data(url)
        
        # Logic fail-safe: Cào tịt thì xài view cũ
        final_view = scraped_view if scraped_view is not None else last_known_view
        
        if scraped_view is not None:
            # Update DB (videos table)
            final_title = scraped_title if scraped_title else (ov.get('title') or url)
            try:
                supabase.table('videos').update({
                    'title': final_title,
                    'current_views': final_view
                }).eq('id', ov['id']).execute()
            except Exception as e:
                pass
        else:
            print(f"   ⚠️ Fail-safe: Tự động fill view cũ cho {url} -> {final_view} views")

        non_yt_metrics.append({
            'video_id': ov['id'],
            'view_count': final_view,
            'recorded_at': today_str 
        })
    
    if non_yt_metrics:
        try:
            supabase.table('video_metrics').upsert(non_yt_metrics, on_conflict='video_id,recorded_at').execute()
            updated_count += len(non_yt_metrics)
        except Exception as e:
            print(f"❌ Lỗi Insert Non-Youtube Metrics: {repr(e)}")

    # --- PHẦN B: XỬ LÝ YOUTUBE BẰNG API V3 ---
    if youtube_videos:
        print(f"⚡ Đang quét view API cho {len(youtube_videos)} video Youtube...")
        
    chunk_size = 50
    for i in range(0, len(youtube_videos), chunk_size):
        chunk = youtube_videos[i:i+chunk_size]
        ids_to_send = [v['yt_id'] for v in chunk]
        ids_string = ",".join(ids_to_send)
        
        metrics_insert = []
        returned_ids_set = set() 

        try:
            url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={ids_string}&key={YOUTUBE_API_KEY}"
            res = requests.get(url).json()
            returned_items = res.get('items', [])
            
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
                    metrics_insert.append({
                        'video_id': db_vid['id'],
                        'view_count': view_count,
                        'recorded_at': today_str 
                    })
                    final_title = title if title else (db_vid.get('title') or db_vid.get('video_url'))
                    supabase.table('videos').update({
                        'title': final_title,
                        'released_date': published_at,
                        'current_views': view_count
                    }).eq('id', db_vid['id']).execute()

        except Exception as e:
            print(f"⚠️ API Chunk Error: {repr(e)}")

        # Fail-safe Youtube
        for original_vid in chunk:
            if original_vid['yt_id'] not in returned_ids_set:
                last_known_view = original_vid.get('current_views', 0) or 0
                print(f"   ⚠️ Fail-safe Youtube: Tự động fill view cũ cho {original_vid['yt_id']} -> {last_known_view} views")
                metrics_insert.append({
                    'video_id': original_vid['id'],
                    'view_count': last_known_view,
                    'recorded_at': today_str 
                })

        if metrics_insert:
            try:
                supabase.table('video_metrics').upsert(metrics_insert, on_conflict='video_id,recorded_at').execute()
                updated_count += len(metrics_insert)
            except Exception as e:
                print(f"❌ Lỗi CRITICAL khi Upsert Youtube Metrics: {repr(e)}")

    print(f"✅ DONE: {updated_count} records cập nhật (Gồm Live Cào và Fail-safe).")

# --- TASK 2.5: UPDATE VIDEO STATUSES (Stalled, Possibly Unlisted) ---
def update_video_statuses():
    print("\n>>> TASK 2.5: Detecting Stalled/Unlisted Videos...")
    try:
        # Lấy tất cả videos
        videos = supabase.table('videos').select('*').execute().data
        
        # Lấy view 7 ngày trước để tính growth
        date_7_ago = (get_hanoi_time() - timedelta(days=7)).strftime('%Y-%m-%d')
        metrics_res = supabase.table('video_metrics').select('video_id, view_count').eq('recorded_at', date_7_ago).execute()
        history_map = {item['video_id']: item['view_count'] for item in metrics_res.data}
        
        print(f"   - Đã load view cũ cho {len(history_map)} videos.")
    except Exception as e:
        print(f"❌ Lỗi truy vấn data status: {repr(e)}")
        return

    updates_count = 0
    for v in videos:
        vid_id = v['id']
        url = v['video_url']
        current_views = v.get('current_views', 0) or 0
        old_views = history_map.get(vid_id, 0)
        
        # Tính % growth (7 ngày)
        if old_views > 0:
            growth_pct = ((current_views - old_views) / old_views) * 100
        else:
            growth_pct = 100 if current_views > 0 else 0
            
        new_status = "Healthy"
        
        # Logic detect status:
        if growth_pct > 2:
            new_status = "Healthy"
        else:
            # Nếu growth <= 2% -> satisfy stalled condition -> Trigger unlisted check
            print(f"   🔎 Video {vid_id} is stalled (Growth: {growth_pct:.2f}%). Checking unlisted status...")
            
            # Chỉ check unlisted nếu đã bị stall
            availability_status = check_video_availability(url)
            
            if availability_status == "Public":
                new_status = "Stalled"
            else:
                new_status = "Possibly Unlisted"
        
        # Update if changed
        if new_status != v.get('status'):
            try:
                supabase.table('videos').update({'status': new_status}).eq('id', vid_id).execute()
                updates_count += 1
            except Exception as e:
                print(f"⚠️ Lỗi cập nhật status {vid_id}: {repr(e)}")

    print(f"✅ Đã cập nhật trạng thái cho {updates_count} videos.")

# --- TASK 3: BUILD DASHBOARD ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD (Value Update Only)...")
    
    try:
        gc = get_gspread_client()
    except Exception as e:
        print(f"❌ Lỗi Auth Google Sheet: {repr(e)}")
        return

    try:
        print("   - Đang lấy data từ Supabase...")
        res = supabase.table('videos').select('*, kols(name, country, subscriber_count)').order('released_date', desc=True).execute()
        data = res.data
        print(f"   - Tìm thấy {len(data)} videos.")
    except Exception as e:
        print(f"❌ Lỗi query Supabase Dashboard: {repr(e)}")
        return

    try:
        date_7_ago = (get_hanoi_time() - timedelta(days=7)).strftime('%Y-%m-%d')
        metrics_res = supabase.table('video_metrics')\
            .select('video_id, view_count')\
            .eq('recorded_at', date_7_ago)\
            .execute()
        
        history_map = {item['video_id']: item['view_count'] for item in metrics_res.data}
    except Exception as e:
        history_map = {}

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

        current_views = item.get('current_views', 0)
        old_views = history_map.get(video_id, 0)
        growth = current_views - old_views
        
        raw_package = str(item.get('total_package', '0'))
        clean_package_str = re.sub(r'[^\d.]', '', raw_package) 
        try:
            total_package = float(clean_package_str) if clean_package_str else 0
        except:
            total_package = 0
        
        try:
            content_count = int(item.get('content_count', 0))
        except: content_count = 1
        
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

    try:
        print("   - Đang ghi vào Google Sheet...")
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet('KOL DASHBOARD')
        except:
            ws = sh.add_worksheet(title='KOL DASHBOARD', rows=1000, cols=20)

        if rows:
            ws.batch_clear(['A2:K'])
            ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')

        print("✅ DONE! Data updated. Format preserved.")
    except Exception as e:
        print(f"❌ Chết đoạn ghi Sheet: {repr(e)}")

if __name__ == "__main__":
    try:
        sync_progress_to_db()
        track_youtube_views()
        update_video_statuses() # Cập nhật Healthy/Stalled/Unlisted
        build_dashboard()
        print("\n🚀 ALL TASKS COMPLETED!")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {repr(e)}")
