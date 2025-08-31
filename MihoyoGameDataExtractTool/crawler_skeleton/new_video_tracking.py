# new_video_tracking.py
# 位置：ExtractTool/MihoyoGameDataExtractTool/crawler_skeleton/new_video_tracking.py
# 功能：讀 JSON 策略 + Glossary Excel，依 Channel_Locales 的 channel_id 省額度抓近 N 天影片
#      以 videos.list 取得 views/likes/comments 與 publishedAt，寫入 YouTube_Results.xlsx
# 需求：pip install pandas openpyxl python-dateutil requests

from __future__ import annotations
import os, re, json, argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from dateutil import tz, relativedelta

# ---------- 專案根路徑 ----------
def find_repo_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(6):
        if (cur / "Common_LocalizationGlossary.xlsx").exists():
            return cur
        cur = cur.parent
    return start.resolve()

HERE = Path(__file__).resolve()
REPO = find_repo_root(HERE)
GLOSSARY_XLSX = REPO / "Common_LocalizationGlossary.xlsx"

# ---------- 各遊戲路徑 ----------
GAME_DIR = {
    "gsi": REPO / "GSI" / "media_popularity",
    "hsr": REPO / "HSR" / "media_popularity",
    "zzz": REPO / "ZZZ" / "media_popularity",
}

def game_paths(game: str) -> Tuple[Path, Path]:
    base = GAME_DIR[game]
    results_xlsx = base / "YouTube_Results.xlsx"
    policy_json = base / "policies.json"
    if not policy_json.exists():
        policy_json = REPO / "policies.json"
    return results_xlsx, policy_json

# ---------- 讀策略 ----------
def load_policy(policy_json: Path, name="new_video_sampling_plan") -> dict:
    data = json.loads(policy_json.read_text(encoding="utf-8"))
    for p in data["policies"]:
        if p["name"] == name:
            return p
    raise KeyError(f"Policy '{name}' not found in {policy_json}")

# （下兩個目前只用在你之後要做「依策略抽樣」時）
def expand_active_times(publish_dt: datetime, policy: dict, horizon_days=365):
    out = []
    for ph in policy["active_phases"]:
        s, e, step = ph["start_min"], ph["end_min"], ph["interval_min"]
        for m in range(s, e+1, step):
            t = publish_dt + timedelta(minutes=m)
            if t <= publish_dt + timedelta(days=horizon_days):
                out.append((t, ph["label"], m))
    post = policy.get("post_active_recurring")
    if post:
        start_m = post["start_min"]
        t = publish_dt + timedelta(minutes=start_m)
        while t <= publish_dt + timedelta(days=horizon_days):
            out.append((t, "post-30d / 1w", int((t - publish_dt).total_seconds()//60)))
            t += timedelta(days=7 * post.get("interval", 1))
    return sorted(out, key=lambda x: x[0])

def expand_archived_times(archived_dt: datetime, months=24):
    out = []
    for i in range(1, months+1):
        t = archived_dt + relativedelta.relativedelta(months=+i)
        out.append((t, f"archived / monthly +{i}", None))
    return out

# ---------- 讀 Glossary ----------
def read_channel_locales(glossary_xlsx: Path, game: str) -> pd.DataFrame:
    """
    需要欄位（寬鬆對應）：game/game_code, lang, channel_id（優先）, web_link/channel_url（備用）
    只取 YouTube（若表內含 bilibili 之類會被自動略過）
    """
    df = pd.read_excel(glossary_xlsx, sheet_name="Channel_Locales")
    df.columns = [str(c).strip().lower() for c in df.columns]

    # 嘗試抓 platform；若無則用網址來判斷是不是 YouTube
    platform_col = "platform" if "platform" in df.columns else None

    # 欄名標準化
    colmap = {
        "game":"game", "game_code":"game",
        "lang":"lang", "language":"lang",
        "channel_id":"channel_id",
        "web_link":"channel_url", "channelurl":"channel_url", "channel_url":"channel_url", "url":"channel_url"
    }
    for c in list(df.columns):
        key = c if c in colmap else c.replace("_","")
        if key in colmap:
            df.rename(columns={c: colmap[key]}, inplace=True)

    # 平台過濾
    if platform_col and platform_col in df.columns:
        youtube_mask = df[platform_col].astype(str).str.lower().str.contains("yt|you?tube")
        df = df[youtube_mask]
    else:
        # 用網址判斷是否為 YouTube
        url_mask = df.get("channel_url","").astype(str).str.contains("youtube.com|youtu.be", case=False, na=False)
        df = df[url_mask | df["channel_id"].notna()]

    df = df[df["game"].str.lower()==game.lower()].copy()
    return df[["game","lang","channel_id","channel_url"]]

def read_video_lexicon(glossary_xlsx: Path, game: str):
    """
    VideoType_Lexicon：game / (lang) / video_type / pattern
    """
    df = pd.read_excel(glossary_xlsx, sheet_name="VideoType_Lexicon")
    df.columns = [str(c).strip().lower() for c in df.columns]
    colmap = {
        "game":"game", "game_code":"game",
        "lang":"lang",
        "videotype":"video_type", "video_type":"video_type", "content_type":"video_type",
        "pattern":"pattern", "regex":"pattern", "title_pattern":"pattern"
    }
    for c in list(df.columns):
        key = c if c in colmap else c.replace("_","")
        if key in colmap:
            df.rename(columns={c: colmap[key]}, inplace=True)
    df = df[df["game"].str.lower()==game.lower()].dropna(subset=["video_type","pattern"])

    rules = []
    for _,row in df.iterrows():
        pat = str(row["pattern"])
        try:
            rx = re.compile(pat, re.I)
        except re.error:
            rx = re.compile(re.escape(pat), re.I)
        rules.append((rx, str(row["video_type"]), row.get("lang")))
    return rules

def classify_title(title: str, rules, lang: Optional[str]=None, default="other") -> str:
    for rx, vtype, rlang in rules:
        if rlang and lang and rlang.lower()!=lang.lower():
            continue
        if rx.search(title or ""):
            return vtype
    return default

# ---------- YouTube API（省額度路線） ----------
def yt_get_uploads_playlist_id(channel_id: str, api_key: str) -> Optional[str]:
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"key": api_key, "part": "contentDetails", "id": channel_id}
    r = requests.get(url, params=params, timeout=15)
    if not r.ok: return None
    items = r.json().get("items", [])
    if not items: return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def yt_list_recent_from_uploads(uploads_playlist_id: str, published_after_utc: datetime, api_key: str, max_pages=3) -> List[Dict]:
    """
    從 uploads 播放清單抓影片（每頁 50 筆；1 配額/頁），直到 publishedAfter 以前就停止。
    回傳：[{video_id, title, published_at}, ...]
    """
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {
        "key": api_key,
        "part": "snippet,contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": 50
    }
    out, pages = [], 0
    cutoff = published_after_utc.replace(tzinfo=timezone.utc)
    while True:
        r = requests.get(url, params=params, timeout=20)
        if not r.ok: break
        data = r.json()
        for it in data.get("items", []):
            vid = it["contentDetails"]["videoId"]
            sn = it["snippet"]
            pub = sn.get("publishedAt") or it["contentDetails"].get("videoPublishedAt")
            if not pub:  # 防呆
                continue
            pub_dt = datetime.fromisoformat(pub.replace("Z","+00:00"))
            if pub_dt < cutoff:
                # uploads 清單是新到舊排序；一旦小於 cutoff 就可以整批停止
                return out
            out.append({"video_id": vid, "title": sn.get("title",""), "published_at": pub})
        pages += 1
        if "nextPageToken" not in data or pages >= max_pages:
            break
        params["pageToken"] = data["nextPageToken"]
    return out

def yt_get_video_stats(video_ids: List[str], api_key: str) -> Dict[str, Dict]:
    """
    videos.list（1 配額/次；一次最多 50 支）
    回傳 dict：vid -> {publishedAt, viewCount, likeCount, commentCount, etag}
    """
    out = {}
    url = "https://www.googleapis.com/youtube/v3/videos"
    for i in range(0, len(video_ids), 50):
        ids = ",".join(video_ids[i:i+50])
        params = {"key": api_key, "part": "snippet,statistics", "id": ids, "hl":"en"}
        r = requests.get(url, params=params, timeout=20)
        if not r.ok: continue
        for it in r.json().get("items", []):
            vid = it["id"]
            sn = it.get("snippet", {})
            st = it.get("statistics", {})
            out[vid] = {
                "publishedAt": sn.get("publishedAt"),
                "views": int(st.get("viewCount", 0)) if "viewCount" in st else None,
                "likes": int(st.get("likeCount", 0)) if "likeCount" in st else None,
                "comments": int(st.get("commentCount", 0)) if "commentCount" in st else None,
                "etag": it.get("etag","")
            }
    return out

# ---------- Excel 資料庫 ----------
def ensure_results_book(path: Path):
    if not path.exists():
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            pd.DataFrame(columns=["video_id","channel_id","title","published_at","status","policy","archived_at","last_checked_at","notes"]).to_excel(w, sheet_name="Videos", index=False)
            pd.DataFrame(columns=["log_id","video_id","checked_at_utc","stage_label","offset_min","views","likes","comments","favoriteCount","watch_time_sec","api_etag","api_cost","source","error_code","error_message"]).to_excel(w, sheet_name="FetchLog", index=False)
            pd.DataFrame(columns=["video_id","latest_checked_at_utc","views","likes","comments"]).to_excel(w, sheet_name="Latest", index=False)
            pd.DataFrame(columns=["ts_utc","video_id","context","error_code","error_message","raw_payload_excerpt"]).to_excel(w, sheet_name="Errors", index=False)

def upsert_video(videos_path: Path, row: Dict):
    df = pd.read_excel(videos_path, sheet_name="Videos")
    key = row["video_id"]
    mask = (df["video_id"]==key)
    if mask.any():
        for k,v in row.items():
            df.loc[mask, k] = v
    else:
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    with pd.ExcelWriter(videos_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name="Videos", index=False)

def append_fetch_log(videos_path: Path, row: Dict):
    df = pd.read_excel(videos_path, sheet_name="FetchLog")
    row["log_id"] = (df["log_id"].max() if not df.empty else 0) + 1
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    with pd.ExcelWriter(videos_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name="FetchLog", index=False)

# ---------- 主流程 ----------
def run(game: str, days: int, policy_name: str, api_key: str):
    if not api_key:
        raise SystemExit("缺少 API key：請用 --api-key 或環境變數 YOUTUBE_API_KEY 提供。")

    results_xlsx, policy_json = game_paths(game)
    ensure_results_book(results_xlsx)
    policy = load_policy(policy_json, name=policy_name)
    rules  = read_video_lexicon(GLOSSARY_XLSX, game)
    ch_df  = read_channel_locales(GLOSSARY_XLSX, game)

    tz_tpe = tz.gettz("Asia/Taipei")
    now_utc = datetime.now(timezone.utc)
    published_after_utc = now_utc - timedelta(days=days)

    for _, row in ch_df.iterrows():
        lang = str(row.get("lang",""))
        channel_id = str(row.get("channel_id") or "").strip()
        channel_url = str(row.get("channel_url") or "").strip()

        if not channel_id:
            # 安全起見，如無 channel_id 就跳過（也可在這裡加 search/resolve）
            append_fetch_log(results_xlsx, {
                "video_id":"", "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds")+"Z",
                "stage_label":"bootstrap", "offset_min":None,
                "views":None,"likes":None,"comments":None,"favoriteCount":None,"watch_time_sec":None,
                "api_etag":"", "api_cost":0, "source":"resolve_channel", "error_code":"NO_CHANNEL_ID",
                "error_message":f"No channel_id for {channel_url}"
            })
            continue

        uploads = yt_get_uploads_playlist_id(channel_id, api_key)
        if not uploads:
            append_fetch_log(results_xlsx, {
                "video_id":"", "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds")+"Z",
                "stage_label":"bootstrap", "offset_min":None,
                "views":None,"likes":None,"comments":None,"favoriteCount":None,"watch_time_sec":None,
                "api_etag":"", "api_cost":1, "source":"channels.list", "error_code":"NO_UPLOADS",
                "error_message":f"Cannot get uploads playlist for {channel_id}"
            })
            continue

        # 近 N 天影片（title/published_at 來自 playlistItems）
        items = yt_list_recent_from_uploads(uploads, published_after_utc, api_key)
        if not items:
            continue

        vid_ids = [x["video_id"] for x in items]
        stats   = yt_get_video_stats(vid_ids, api_key)

        # 寫入 Excel
        for x in items:
            vid   = x["video_id"]
            title = x["title"]
            # 優先用 videos.list 的 publishedAt（較完整），否則用 playlistItems 的
            pub   = stats.get(vid,{}).get("publishedAt") or x["published_at"]
            vtype = classify_title(title, rules, lang=lang, default="other")

            upsert_video(results_xlsx, {
                "video_id": vid,
                "channel_id": channel_id,
                "title": title,
                "published_at": pub,
                "status": "Active",
                "policy": policy_name,
                "archived_at": "",
                "last_checked_at": "",
                "notes": f"lang={lang}; type={vtype}"
            })

            st = stats.get(vid, {})
            append_fetch_log(results_xlsx, {
                "video_id": vid,
                "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds")+"Z",
                "stage_label":"adhoc",
                "offset_min": None,
                "views": st.get("views"),
                "likes": st.get("likes"),
                "comments": st.get("comments"),
                "favoriteCount": None,
                "watch_time_sec": None,
                "api_etag": st.get("etag",""),
                "api_cost": 2,  # channels.list(1) + playlistItems.list(1/page) + videos.list(1) => 平均估 2
                "source": "yt:uploads+videos",
                "error_code": "",
                "error_message": ""
            })

    print(f"[OK] {game}: wrote stats → {results_xlsx}")

# ---------- CLI ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--game", choices=["gsi","hsr","zzz"], required=True)
    ap.add_argument("--days", type=int, default=2, help="lookback days from now (UTC)")
    ap.add_argument("--policy", default="new_video_sampling_plan")
    ap.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY",""))
    args = ap.parse_args()
    run(args.game, args.days, args.policy, args.api_key)
