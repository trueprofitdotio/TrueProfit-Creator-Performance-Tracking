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

# YouTube API Key pool - rotate khi bị quota limit (403)
YOUTUBE_API_KEYS = [
    os.environ.get("YOUTUBE_API_KEY",  "AIzaSyChr_rRRYlsH9_wfY8JB1UJ30fPDMBtp0c"),  # key1
    "AIzaSyAHFSLQGngrIVVMw2ERmyuOhCuJLhtM5jc",  # key2
    "AIzaSyDiyxt3nc4qdSx7OtsOIkKCU7S94_uWiUc",  # key3
    "AIzaSyDgftThC9A0310-g0ocCeDd_Pkf8v-zhZM",  # key4
]
_yt_key_index = 0  # con trỏ key hiện tại


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

# Set lưu video_id của những video bị fail-safe (API không trả về khi track views)
failsafe_video_ids: set = set()

def get_yt_api_key() -> str:
    """Trả về API key hiện tại."""
    return YOUTUBE_API_KEYS[_yt_key_index % len(YOUTUBE_API_KEYS)]

def rotate_yt_api_key() -> str:
    """Rotate sang key tiếp theo khi bị quota (403)."""
    global _yt_key_index
    _yt_key_index += 1
    new_key = get_yt_api_key()
    print(f"   🔄 Rotated to YouTube API key #{_yt_key_index % len(YOUTUBE_API_KEYS) + 1}")
    return new_key


# --- HELPER ---
def extract_video_id(url):
    """Trích xuất Video ID từ link Youtube"""
    if not isinstance(url, str): return None
    match = re.search(r'(?:v=|/|embed/|youtu\.be/)([\w-]{11})(?=&|\?|$)', url)
    return match.group(1) if match else None

def extract_canonical_id(url):
    """
    Trích xuất canonical platform-native ID từ URL.
    Format trả về: yt_<id>, tt_<id>, x_<id>, ig_<id>
    """
    if not isinstance(url, str): return None
    
    # YouTube
    yt_id = extract_video_id(url)
    if yt_id: return f"yt_{yt_id}"
    
    # TikTok
    tt_match = re.search(r'tiktok\.com/.*video/(\d+)', url)
    if tt_match: return f"tt_{tt_match.group(1)}"
    
    # X/Twitter
    x_match = re.search(r'(?:twitter\.com|x\.com)/.*/status/(\d+)', url)
    if x_match: return f"x_{x_match.group(1)}"
    
    # Instagram
    ig_match = re.search(r'instagram\.com/(?:reels?|p|reel)/([^/?#&]+)', url)
    if ig_match: return f"ig_{ig_match.group(1)}"
    
    return None

def upsert_metrics_with_comparison(metrics_list):
    """
    Upsert metrics vào Supabase, giữ lại view_count lớn nhất nếu xảy ra xung đột (cùng video, cùng ngày).
    """
    if not metrics_list: return
    
    today_str = get_hanoi_time().strftime('%Y-%m-%d')
    
    try:
        # Lấy danh sách video_id để kiểm tra record hiện có trong ngày
        vids = [m['video_id'] for m in metrics_list]
        res = supabase.table('video_metrics').select('video_id, view_count').eq('recorded_at', today_str).in_('video_id', vids).execute()
        existing_map = {m['video_id']: m['view_count'] for m in res.data}
    except:
        existing_map = {}

    to_upsert = []
    for m in metrics_list:
        vid_id = m['video_id']
        new_views = m['view_count']
        if vid_id in existing_map:
            # Chỉ cập nhật nếu số view mới lớn hơn số view đã lưu
            if new_views > existing_map[vid_id]:
                to_upsert.append(m)
        else:
            to_upsert.append(m)
            
    if to_upsert:
        try:
            supabase.table('video_metrics').upsert(to_upsert, on_conflict='video_id,recorded_at').execute()
        except Exception as e:
            print(f"   [!] Lỗi upsert video_metrics: {repr(e)}")

def fetch_non_yt_data(url):
    """Dùng yt-dlp cào data các nền tảng khác (TikTok, IG,...)"""
    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'extract_flat': False,
        'no_warnings': True,
        'socket_timeout': 15,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get('view_count'), info.get('title')
    except Exception as e:
        print(f"   [!] yt-dlp không cào được {url} (Lỗi: {repr(e)})")
        return None, None

def check_youtube_video_status(video_id: str) -> str:
    """
    Dùng YouTube Data API v3 (videos.list) để kiểm tra privacy status của video.
    """
    tried_keys = 0
    while tried_keys < len(YOUTUBE_API_KEYS):
        api_key = get_yt_api_key()
        try:
            url = (
                f"https://www.googleapis.com/youtube/v3/videos"
                f"?part=status,snippet,statistics"
                f"&id={video_id}"
                f"&key={api_key}"
            )
            res = requests.get(url, timeout=15)

            if res.status_code == 403:
                rotate_yt_api_key()
                tried_keys += 1
                continue

            if res.status_code != 200:
                return "Unknown"

            data = res.json()
            items = data.get('items', [])

            if not items:
                return "UNLISTED/REMOVED"

            privacy = items[0].get('status', {}).get('privacyStatus', '').lower()
            return "HEALTHY" if privacy == 'public' else "UNLISTED/REMOVED"

        except Exception as e:
            return "Unknown"

    return "Unknown"


def check_non_yt_status(url: str) -> str:
    """
    Kiểm tra video TikTok/IG/Twitter bằng yt-dlp.
    """
    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'extract_flat': False,
        'no_warnings': True,
        'socket_timeout': 15,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            view_count = info.get('view_count')
            if view_count is not None and isinstance(view_count, int):
                return "HEALTHY"
            else:
                return "UNLISTED/REMOVED"
    except Exception as e:
        err_str = str(e).lower()
        BLOCKED_KEYWORDS = ["block", "429", "rate limit", "login required", "cookies"]
        if any(kw in err_str for kw in BLOCKED_KEYWORDS):
            return "Blocked"
        return "UNLISTED/REMOVED"


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

    # Load existing videos by new_id to check status
    existing_videos = {} # new_id -> status
    try:
        db_res = supabase.table('videos').select('new_id, status').execute().data
        for item in db_res:
            if item.get('new_id'):
                existing_videos[item['new_id']] = item.get('status')
        print(f"ℹ️ Đã load {len(db_res)} videos từ DB.")
    except Exception as e:
        print(f"⚠️ Không load được danh sách video cũ: {repr(e)}")

    kols_map = {} 
    for row in records:
        kol_name = str(row.get('Name', '')).strip()
        if not kol_name: continue 
        
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

        raw_report_link_cell = str(row.get('Report Link', ''))
        found_links = re.findall(r'(https?://[^\s,]+)', raw_report_link_cell)
        
        agreement = row.get('Signed Agreement', '')
        package = str(row.get('Total Package', ''))
        try:
            raw_count = row.get('No. Of Content', 0)
            content_count = int(str(raw_count).replace(',', '').strip()) if raw_count else 0
        except: content_count = 0

        for raw_link in found_links:
            new_id = extract_canonical_id(raw_link)
            if not new_id: continue

            # Chuẩn hóa URL cho Youtube
            yt_id = extract_video_id(raw_link)
            final_url = f"https://www.youtube.com/watch?v={yt_id}" if yt_id else raw_link

            video_data = {
                'new_id': new_id,
                'kol_id': kol_id,
                'video_url': final_url, 
                'agreement_link': agreement,
                'total_package': package,
                'content_count': content_count
            }
            # Chỉ set status mặc định là HEALTHY nếu chưa tồn tại
            if new_id not in existing_videos:
                video_data['status'] = 'HEALTHY'

            try:
                # Upsert dựa trên new_id thay vì video_url
                supabase.table('videos').upsert(video_data, on_conflict='new_id').execute()
            except Exception as e:
                print(f"   [!] Lỗi upsert video {new_id}: {repr(e)}")

    print("✅ Đã đồng bộ metadata.")


# --- TASK 2: TRACK VIEW ---
def track_youtube_views():
    print("\n>>> TASK 2: Tracking Views...")
    try:
        # Lấy danh sách video đang hoạt động (không phải UNLISTED/REMOVED)
        videos = supabase.table('videos').select('*').neq('status', 'UNLISTED/REMOVED').execute().data
    except Exception as e:
        print(f"❌ Lỗi Supabase: {repr(e)}")
        return
    
    youtube_videos = []
    other_videos = [] 
    for v in videos:
        # Dùng extract_video_id để phân loại Youtube vs nền tảng khác
        yt_id = extract_video_id(v['video_url'])
        if yt_id:
            v['yt_id'] = yt_id
            youtube_videos.append(v)
        else:
            other_videos.append(v)
    
    now_vn = get_hanoi_time()
    today_str = now_vn.strftime('%Y-%m-%d') 
    updated_count = 0

    # Xử lý Non-YouTube
    non_yt_metrics = []
    for ov in other_videos:
        scraped_view, scraped_title = fetch_non_yt_data(ov['video_url'])
        # Dự phòng nếu lỗi scrap: lấy views hiện tại
        final_view = scraped_view if scraped_view is not None else (ov.get('current_views', 0) or 0)
        
        if scraped_view is not None:
            try:
                supabase.table('videos').update({
                    'title': scraped_title or ov.get('video_url'), 
                    'current_views': final_view
                }).eq('id', ov['id']).execute()
            except: pass
        
        non_yt_metrics.append({'video_id': ov['id'], 'view_count': final_view, 'recorded_at': today_str})
    
    if non_yt_metrics:
        upsert_metrics_with_comparison(non_yt_metrics)
        updated_count += len(non_yt_metrics)

    # Xử lý YouTube
    if youtube_videos:
        chunk_size = 50
        for i in range(0, len(youtube_videos), chunk_size):
            chunk = youtube_videos[i:i+chunk_size]
            ids_string = ",".join([v['yt_id'] for v in chunk])
            metrics_insert = []
            returned_ids = set()

            tried_keys = 0
            res_json = None
            while tried_keys < len(YOUTUBE_API_KEYS):
                api_key = get_yt_api_key()
                try:
                    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={ids_string}&key={api_key}"
                    resp = requests.get(url, timeout=15)
                    if resp.status_code == 403:
                        rotate_yt_api_key()
                        tried_keys += 1
                        continue
                    if resp.status_code == 200:
                        res_json = resp.json()
                        break
                    else: break
                except: break

            if res_json:
                for item in res_json.get('items', []):
                    yt_id = item['id']
                    returned_ids.add(yt_id)
                    view_count = int(item['statistics'].get('viewCount', 0))
                    title = item['snippet'].get('title', '')
                    pub_date = item['snippet'].get('publishedAt', '').split('T')[0]
                    
                    db_v = next(v for v in chunk if v['yt_id'] == yt_id)
                    metrics_insert.append({'video_id': db_v['id'], 'view_count': view_count, 'recorded_at': today_str})
                    try:
                        supabase.table('videos').update({
                            'title': title, 
                            'released_date': pub_date, 
                            'current_views': view_count
                        }).eq('id', db_v['id']).execute()
                    except: pass
            
            # Xử lý fail-safe cho các video không trả về kết quả từ API (bị xóa/unlisted)
            for v in chunk:
                if v['yt_id'] not in returned_ids:
                    view = v.get('current_views', 0) or 0
                    metrics_insert.append({'video_id': v['id'], 'view_count': view, 'recorded_at': today_str})
                    failsafe_video_ids.add(v['id'])
            
            if metrics_insert:
                upsert_metrics_with_comparison(metrics_insert)
                updated_count += len(metrics_insert)

    print(f"✅ Đã cập nhật {updated_count} records.")


# --- TASK 2.5: UPDATE VIDEO STATUSES ---
def update_video_statuses():
    print("\n>>> TASK 2.5: Updating Video Statuses...")
    try:
        videos = supabase.table('videos').select('id, video_url, status').execute().data
    except: return

    for v in videos:
        vid_id, url, current = v['id'], v['video_url'], v.get('status', 'HEALTHY')
        yt_id = extract_video_id(url)

        if yt_id:
            if vid_id in failsafe_video_ids: continue
            new_status = check_youtube_video_status(yt_id)
        else:
            new_status = check_non_yt_status(url)
            if new_status == "Blocked": continue

        if new_status != "Unknown" and new_status != current:
            try:
                supabase.table('videos').update({'status': new_status}).eq('id', vid_id).execute()
                print(f"   ✏️ {url[:50]}: {current} -> {new_status}")
            except: pass


# --- TASK 3: BUILD DASHBOARD ---
def build_dashboard():
    print("\n>>> TASK 3: Building KOL DASHBOARD...")
    try:
        gc = get_gspread_client()
        data = supabase.table('videos').select('*, kols(name, country, subscriber_count)').order('released_date', desc=True).execute().data
        date_7_ago = (get_hanoi_time() - timedelta(days=7)).strftime('%Y-%m-%d')
        hist_res = supabase.table('video_metrics').select('video_id, view_count').eq('recorded_at', date_7_ago).execute().data
        history_map = {item['video_id']: item['view_count'] for item in hist_res}
    except: return

    rows = []
    # Deduplicate by new_id to prevent any potential UI/logic duplicates in the sheet
    seen_ids = set()
    deduped_data = []
    for item in data:
        nid = item.get('new_id')
        if nid:
            if nid not in seen_ids:
                seen_ids.add(nid)
                deduped_data.append(item)
        else:
            # If for some reason new_id is missing, use ID as fallback for safety
            deduped_data.append(item)
    
    data = deduped_data

    for item in data:
        video_url = item.get('video_url', '')
        title = str(item.get('title') or video_url).replace('"', '""')
        title_cell = f'=HYPERLINK("{video_url}", "{title}")'
        kol = item.get('kols', {}) or {}
        
        current_v = item.get('current_views', 0)
        old_v = history_map.get(item['id'], 0)
        growth = current_v - old_v
        
        package_str = re.sub(r'[^\d.]', '', str(item.get('total_package', '0')))
        pkg = float(package_str) if package_str else 0
        count = int(item.get('content_count', 1) or 1)
        cpm = (pkg * 1000) / (current_v * count) if (current_v * count) > 0 else 0

        rows.append([
            title_cell, kol.get('name', 'Unknown'), kol.get('country', ''), item.get('released_date'),
            current_v, old_v, growth, cpm,
            f'=HYPERLINK("{item.get("agreement_link","")}", "View Contract")' if item.get("agreement_link") else "-",
            item.get('total_package'), count
        ])

    try:
        ws = gc.open_by_key(SPREADSHEET_ID).worksheet('KOL DASHBOARD')
        ws.batch_clear(['A2:K'])
        ws.update(range_name='A2', values=rows, value_input_option='USER_ENTERED')
        print("✅ Dashboard updated.")
    except: pass


if __name__ == "__main__":
    try:
        sync_progress_to_db()
        track_youtube_views()
        update_video_statuses()
        build_dashboard()
        print("\n🚀 ALL TASKS COMPLETED!")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {repr(e)}")
