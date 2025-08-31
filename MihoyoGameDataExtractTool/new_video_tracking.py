# -*- coding: utf-8 -*-

# new_video_tracking.py
# 位置：ExtractTool/MihoyoGameDataExtractTool/crawler_skeleton/new_video_tracking.py
# 功能：讀 JSON 策略 + Glossary Excel，依 Channel_Locales 的 channel_id 省額度抓近 N 天影片
#      以 videos.list 取得 views/likes/comments 與 publishedAt，寫入 YouTube_Results.xlsx
# 需求：pip install pandas openpyxl python-dateutil requests

from __future__ import annotations

import argparse
import json
import math
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from datetime import datetime, timedelta, timezone
from dateutil import tz

import re as _re
import pandas as pd
import requests
from dateutil import tz, relativedelta
from typing import List, Dict
import warnings

from urllib.parse import urlparse
from functools import lru_cache

SHORTS_TIME_LIMIT_S = 60      # 嚴格「小於 60 秒」
SHORTS_URL_TIMEOUT = 6        # /shorts/ 檢測逾時（秒）

# --- optional: OpenCC for s2t fallback ---
try:
    from opencc import OpenCC  # pip install opencc-python-reimplemented
    _CC_S2T = OpenCC('s2t.json')
except Exception:
    _CC_S2T = None

def _to_zh_hant_fallback(s: str) -> str:
    if not s:
        return s
    if _CC_S2T:
        try:
            return _CC_S2T.convert(s)
        except Exception:
            return s
    return s

def _dur_to_seconds(iso):
    m = _re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", str(iso))
    if not m: return None
    h, m_, s = (int(x) if x else 0 for x in m.groups())
    return h*3600 + m_*60 + s

@lru_cache(maxsize=4096)
def detect_shorts_url(video_id: str) -> bool:
    bases = [f"https://www.youtube.com/shorts/{video_id}",
             f"https://m.youtube.com/shorts/{video_id}"]
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ja,en-US;q=0.9,zh-TW;q=0.8"}
    for base in bases:
        try:
            r = requests.head(base, allow_redirects=True, timeout=SHORTS_URL_TIMEOUT, headers=headers)
            if "/shorts/" in (urlparse(r.url).path or ""): return True
        except Exception:
            pass
        try:
            r = requests.get(base, allow_redirects=True, timeout=SHORTS_URL_TIMEOUT,
                             headers={**headers, "Range": "bytes=0-0"})
            if "/shorts/" in (urlparse(r.url).path or ""): return True
        except Exception:
            pass
    return False

def find_repo_root(start: Path) -> Path:
    """Walk up until 'Common_LocalizationGlossary.xlsx' is found; use that as repo root."""
    for q in [start, *start.parents]:
        if (q / "Common_LocalizationGlossary.xlsx").exists():
            return q
    return start.parent  # fallback

HERE = Path(__file__).resolve()
REPO = find_repo_root(HERE)
GLOSSARY_XLSX = REPO / "Common_LocalizationGlossary.xlsx"

def yt_list_recent_from_uploads(uploads_playlist_id: str,
                                published_after_utc: datetime,
                                api_key: str,
                                max_pages: int = 3) -> List[Dict]:
    """
    從 channel 的 uploads 播放清單抓近 N 天影片（新→舊），直到 published_after_utc 以前即停止。
    回傳：[{video_id, title, published_at}, ...]
    """
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {
        "key": api_key,
        "part": "snippet,contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": 50,
    }
    out, pages = [], 0
    cutoff = published_after_utc.replace(tzinfo=timezone.utc)

    while True:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        for it in data.get("items", []):
            vid = it["contentDetails"]["videoId"]
            sn  = it.get("snippet", {}) or {}
            pub = sn.get("publishedAt") or it["contentDetails"].get("videoPublishedAt")
            if not pub:
                continue
            pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            if pub_dt < cutoff:
                return out
            out.append({
                "video_id": vid,
                "title": sn.get("title", ""),
                "published_at": pub
            })

        pages += 1
        if pages >= max_pages or "nextPageToken" not in data:
            break
        params["pageToken"] = data["nextPageToken"]

    return out

def ensure_results_path(game: str) -> Path:
    """<REPO>/<GAME>/media_popularity/YouTube_Results.xlsx (auto-create folders)."""
    d = REPO / game.upper() / "media_popularity"
    d.mkdir(parents=True, exist_ok=True)
    return d / "YouTube_Results.xlsx"

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
TZ_TPE = tz.gettz("Asia/Taipei")

VIDEOS_NEW = [
    "type",
    "video_id","channel_id","title",
    "published_at_utc","published_at_tpe",
    "status","archived_at","notes",
    "api_etag","last_checked_at_utc",
]

FETCHLOG_NEW = [
    "log_id","video_id","checked_at_tpe",
    "stage_label","offset_min","scheduled_time_tpe",
    "views","likes","comments","api_cost",
    "error_code","error_message",
]

# ---- lang -> hl 映射（依 Channel_Locales.lang 決定 YouTube API 的 hl）----
def _lang_to_hl(lang: str) -> str:
    l = (lang or "").strip().lower()
    if l in ("zh-hant","zh_tw","zh-tw","zh"):  # 你的表多半用 zh-Hant
        return "zh-Hant"
    if l in ("ja","ja-jp"):
        return "ja"
    if l.startswith("en"):
        return "en"
    # 其他語言就用上傳預設，等同不特別指定
    return "en"

# --- optional: OpenCC for s2t fallback（只在 hl=zh-Hant 且沒有本地化時啟動） ---
try:
    from opencc import OpenCC  # pip install opencc-python-reimplemented
    _CC_S2T = OpenCC('s2t.json')

except Exception:
    _CC_S2T = None

def _to_zh_hant_fallback(s: str) -> str:
    if not s: return s
    if _CC_S2T:
        try:
            return _CC_S2T.convert(s)
        except Exception:
            return s
    return s

def tune_to_minutes(s: str) -> int:
    s = str(s).strip().lower()
    if s.endswith("m"): return int(float(s[:-1]))
    if s.endswith("h"): return int(float(s[:-1]) * 60)
    if s.endswith("d"): return int(float(s[:-1]) * 1440)
    if s.endswith("w"): return int(float(s[:-1]) * 10080)
    raise ValueError(f"Unsupported round_step: {s}")

def level_round_for_time(publish_dt_utc: datetime, ref_dt_utc: datetime, policy: dict):
    """
    根據 policy['levels'] 回傳這次抓取應落在哪個 (level, round)。
    回傳 dict: {level, round, offset_min, scheduled_time_utc, label}
    - round 起始為 1（第一個時間點）
    - level 的 'end_min' 允許為 None（表示無上限，例如 >30d）
    """
    if "levels" not in policy:
        return None

    offset_min = int((ref_dt_utc - publish_dt_utc).total_seconds() // 60)
    if offset_min < 0:
        return None

    for lv in policy["levels"]:
        L = int(lv["level"])
        start = int(lv["start_min"])
        end = lv.get("end_min")
        end = int(end) if end is not None else None
        step = tune_to_minutes(lv["round_step"])

        in_range = (offset_min >= start) and (end is None or offset_min <= end)
        if not in_range:
            continue

        after = max(0, offset_min - start)
        rnd = max(1, math.ceil(after / step))      # 第幾個 round（1-based）
        planned_offset = start + rnd * step         # 發佈後的目標分鐘
        sched_time_utc = publish_dt_utc + timedelta(minutes=planned_offset)
        return {
            "level": L,
            "round": rnd,
            "offset_min": planned_offset,
            "scheduled_time_utc": sched_time_utc,
            "label": f"level{L},round{rnd}"
        }
    return None

def load_api_key_strict(filename: str = "youtube_api.json") -> str:
    """只從與此檔同層的 youtube_api.json 讀取金鑰，讀不到就直接結束。"""
    cfg_path = Path(__file__).resolve().parent / filename
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
        key = data["youtube"]["api_key"].strip()
        if not key:
            raise ValueError("empty api_key")
        return key
    except FileNotFoundError:
        raise SystemExit(f"找不到 {cfg_path}，請把 youtube_api.json 放在與 new_video_tracking.py 同層。")
    except Exception as e:
        raise SystemExit(f"讀取 {cfg_path} 失敗：{e}")

def game_paths(game: str) -> tuple[Path, Path]:
    results_xlsx = ensure_results_path(game)
    # 先找遊戲資料夾內的 policies.json，找不到就退回 repo 根目錄
    candidate = results_xlsx.parent / "policies.json"
    policy_json = candidate if candidate.exists() else (REPO / "policies.json")
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
    """回傳 [(scheduled_dt, label, offset_min)]；支援 levels + round_step / tune；end_min 可為 None。"""
    out = []

    levels = policy.get("levels", [])
    for lv in levels:
        label = lv.get("label", f"level{lv.get('level','')}")
        start = int(lv.get("start_min", 0))
        end_val = lv.get("end_min")  # 允許 None
        step_str = lv.get("round_step") or lv.get("tune")  # 相容兩種欄名
        if not step_str:
            continue
        step = tune_to_minutes(step_str)

        # 無上限時，用 horizon 作為邊界
        last_min = int(end_val) if end_val is not None else horizon_days * 24 * 60

        m = start + step  # round1 就是 start+step
        while m <= last_min:
            t = publish_dt + timedelta(minutes=m)
            if t > publish_dt + timedelta(days=horizon_days):
                break
            out.append((t, f"{label} / {step_str}", m))
            m += step

    # 30 天後的循環（相容 recurring_active / post_active_recurring）
    rec = policy.get("recurring_active") or policy.get("post_active_recurring")
    if rec:
        step_str = rec.get("round_step") or rec.get("tune")
        if step_str:
            step = tune_to_minutes(step_str)
            start_m = int(rec.get("start_min", 43200))
            t = publish_dt + timedelta(minutes=start_m)
            while t <= publish_dt + timedelta(days=horizon_days):
                m = int((t - publish_dt).total_seconds() // 60)
                out.append((t, f"post-30d / {step_str}", m))
                t += timedelta(minutes=step)

    return sorted(out, key=lambda x: x[0])

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

def _split_keywords(s: str) -> list[str]:
    """Split keywords by | , ; ／ newline/tab; trim quotes/spaces."""
    if not s:
        return []
    raw = str(s)
    for sep in ["\n", "\t", "／"]:
        raw = raw.replace(sep, "|")
    import re as _re
    parts = _re.split(r"[|,;]", raw)
    return [p.strip().strip('"').strip("'") for p in parts if p.strip().strip('"').strip("'")]

def read_video_lexicon(glossary_xlsx: Path, game: str):
    """
    VideoType_Lexicon columns (your sheet):
      game_code | content_type | lang | platform | regex_title | keywords | ...
    Rules: prefer regex_title; else build OR-regex from keywords. Optional lang filter; platform=yt only.
    Returns: [(compiled_regex, content_type, lang_or_None), ...]
    """
    try:
        df = pd.read_excel(glossary_xlsx, sheet_name="VideoType_Lexicon")
    except ValueError:
        print("[WARN] sheet 'VideoType_Lexicon' not found; typing=other.")
        return []

    df.columns = [str(c).strip().lower() for c in df.columns]

    # must-have columns
    if not {"game_code", "content_type"} <= set(df.columns):
        print("[WARN] VideoType_Lexicon missing 'game_code' or 'content_type'; typing=other.")
        return []

    # filter by game
    df = df[df["game_code"].astype(str).str.lower() == game.lower()].copy()

    # platform filter (keep yt/youtube; blank means no filter)
    if "platform" in df.columns:
        m = df["platform"].astype(str).str.lower().isin(["yt", "youtube", "you tube"])
        df = df[m | df["platform"].isna() | (df["platform"].astype(str).str.strip() == "")]

    rules = []
    for _, r in df.iterrows():
        vtype = (str(r["content_type"]).strip()
                 if pd.notna(r.get("content_type")) else None)
        if not vtype:
            continue

        pattern = None
        if "regex_title" in df.columns and pd.notna(r.get("regex_title")) and str(r["regex_title"]).strip():
            pattern = str(r["regex_title"]).strip()
        elif "keywords" in df.columns and pd.notna(r.get("keywords")) and str(r["keywords"]).strip():
            kws = _split_keywords(r["keywords"])
            if kws:
                pattern = "(" + "|".join(re.escape(k) for k in kws) + ")"

        if not pattern:
            continue

        try:
            rx = re.compile(pattern, re.I)
        except re.error:
            rx = re.compile(re.escape(pattern), re.I)

        lang_val = None
        if "lang" in df.columns and pd.notna(r.get("lang")):
            lv = str(r["lang"]).strip()
            lang_val = lv if lv else None

        rules.append((rx, vtype, lang_val))

    print(f"[INFO] Loaded {len(rules)} typing rules from VideoType_Lexicon.")
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

def build_notes(lang: str, content_type: str, anomalies: Optional[List[str]] = None) -> str:
    items: List[str] = []
    if (lang or "").lower() not in ("zh-hant", "zh_tw", "zh-tw"):
        items.append(f"lang={lang}")
    if content_type and content_type != "other":
        items.append(f"content_type={content_type}")
    if anomalies:
        items.extend(a for a in anomalies if a)
    return "; ".join(items)

def yt_get_video_stats(video_ids: List[str], api_key: str, hl: Optional[str] = None) -> Dict[str, Dict]:
    out = {}
    url = "https://www.googleapis.com/youtube/v3/videos"
    for i in range(0, len(video_ids), 50):
        ids = ",".join(video_ids[i:i+50])
        params = {
            "key": api_key,
            "part": "snippet,statistics,localizations,contentDetails",
            "id": ids,
        }
        if hl:
            params["hl"] = hl

        r = requests.get(url, params=params, timeout=20)
        if not r.ok:
            continue
        data = r.json()
        for it in data.get("items", []):
            vid  = it["id"]
            sn   = it.get("snippet", {}) or {}
            st   = it.get("statistics", {}) or {}
            locs = it.get("localizations", {}) or {}
            cd   = it.get("contentDetails", {}) or {}

            # 取時長（秒）
            dur_iso = cd.get("duration")
            dur_s   = _dur_to_seconds(dur_iso)

            # 嚴格判斷是否真的有 zh-Hant 本地化
            title_raw   = sn.get("title", "")  # 上傳者原始標題
            want_hant   = (hl or "").lower() == "zh-hant"
            has_hant_loc = any((k or "").lower()=="zh-hant" for k in locs.keys()) if want_hant else False

            # 選擇標題來源 + 必要時做簡→繁
            title_source = "raw"
            if want_hant and has_hant_loc:
                title_best = (locs.get("zh-Hant") or locs.get("zh-hant") or {}).get("title") or title_raw
                title_source = "loc.zh-Hant"
            else:
                title_best = title_raw
                if want_hant:
                    title_best = _to_zh_hant_fallback(title_best)
                    title_source = "s2t_fallback"

            out[vid] = {
                "title": title_best,
                "title_original": title_raw,
                "publishedAt": sn.get("publishedAt"),
                "views": int(st.get("viewCount", 0)) if "viewCount" in st else None,
                "likes": int(st.get("likeCount", 0)) if "likeCount" in st else None,
                "comments": int(st.get("commentCount", 0)) if "commentCount" in st else None,
                "etag": it.get("etag",""),
                "has_localization": has_hant_loc,
                "title_source": title_source,
                "duration_s": dur_s,
            }
    return out

# ---------- Excel 資料庫 ----------
def ensure_results_book(path: Path):
    import pandas as pd
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            pd.DataFrame(columns=VIDEOS_NEW).to_excel(w, sheet_name="Videos", index=False)
            pd.DataFrame(columns=FETCHLOG_NEW).to_excel(w, sheet_name="FetchLog", index=False)
            pd.DataFrame(columns=["video_id","latest_checked_at_tpe","views","likes","comments"]
            ).to_excel(w, sheet_name="Latest", index=False)
            pd.DataFrame(columns=["ts_utc","video_id","context","error_code","error_message","raw_payload_excerpt"]
            ).to_excel(w, sheet_name="Errors", index=False)

def upsert_video(results_xlsx: Path, row: dict):
    vdf = pd.read_excel(results_xlsx, sheet_name="Videos")
    for c in VIDEOS_NEW:
        if c not in vdf.columns: vdf[c] = ""  # 保險補欄
    key = (vdf["video_id"] == row["video_id"])
    if key.any():
        idx = vdf[key].index[-1]
        for k,v in row.items():
            if k in vdf.columns:
                vdf.at[idx, k] = v
    else:
        vdf = pd.concat([vdf, pd.DataFrame([row])], ignore_index=True)
    with pd.ExcelWriter(results_xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        vdf.to_excel(w, sheet_name="Videos", index=False)

def append_fetch_log(results_xlsx: Path, row: dict):
    fdf = pd.read_excel(results_xlsx, sheet_name="FetchLog")
    # 只保留我們定義的欄；舊檔若有多餘欄（source/title_source/title_zh_hant_ok），這裡會自動丟掉
    fdf = fdf[[c for c in fdf.columns if str(c) in FETCHLOG_NEW]] if len(fdf.columns) else fdf
    for c in FETCHLOG_NEW:
        if c not in fdf.columns:
            fdf[c] = None if c in ["views","likes","comments","offset_min"] else ""

    # 只留下允許的鍵
    clean = {k: row.get(k, "") for k in FETCHLOG_NEW if k != "log_id"}
    clean["log_id"] = int(fdf["log_id"].max()+1) if "log_id" in fdf.columns and pd.notna(fdf["log_id"]).any() else 1

    fdf = pd.concat([fdf, pd.DataFrame([clean])], ignore_index=True)
    with pd.ExcelWriter(results_xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        fdf.to_excel(w, sheet_name="FetchLog", index=False)

def _to_utc(s: str) -> datetime:
    return datetime.fromisoformat(str(s).replace("Z","+00:00")).astimezone(timezone.utc)

def migrate_results_book(path: Path):
    """
    把舊的 YouTube_Results.xlsx 轉成新結構：
      Videos:  video_id, channel_id, title, published_at_utc, published_at_tpe,
               status, archived_at, notes, api_etag, last_checked_at_utc
      FetchLog: log_id, video_id, checked_at_tpe, stage_label, offset_min, scheduled_time_tpe,
                views, likes, comments, api_cost, source, error_code, error_message
    可重複執行（冪等）。
    """
    import pandas as pd

    if not path.exists():
        print(f"[MIGRATE] {path} not found, skip.")
        return

    print(f"[MIGRATE] start → {path}")

    vdf = pd.read_excel(path, sheet_name="Videos")
    fdf = pd.read_excel(path, sheet_name="FetchLog")

    lc_v = {str(c).lower(): c for c in vdf.columns}
    lc_f = {str(c).lower(): c for c in fdf.columns}

    def vget(name): return vdf[lc_v[name]] if name in lc_v else None
    def fget(name): return fdf[lc_f[name]] if name in lc_f else None

    # --- Videos 部分 ---
    # 1) 發佈時間（UTC 一定要有）
    pub_utc = None
    for cand in ["published_at_utc", "published_at", "published"]:
        if cand in lc_v:
            pub_utc = vget(cand).astype(str)
            break
    if pub_utc is None:
        raise SystemExit("Videos 缺少 published_at（UTC）欄。")

    # 2) 發佈的東八（沒有就把 UTC 轉 TPE）
    if "published_at_tpe" in lc_v:
        pub_tpe = vget("published_at_tpe").astype(str)
    else:
        pub_tpe = pub_utc.apply(lambda s: _to_utc(s).astimezone(TZ_TPE).isoformat(timespec="seconds"))

    # 3) 先建立新 Videos 骨架（先放空字串，等會兒 merge 再填）
    vout = pd.DataFrame({
        "video_id": vget("video_id") if "video_id" in lc_v else "",
        "channel_id": vget("channel_id") if "channel_id" in lc_v else "",
        "title": vget("title") if "title" in lc_v else "",
        "published_at_utc": pub_utc,
        "published_at_tpe": pub_tpe,
        "status": vget("status") if "status" in lc_v else "Active",
        "archived_at": vget("archived_at") if "archived_at" in lc_v else "",
        "notes": vget("notes") if "notes" in lc_v else "",
        "api_etag": "",                 # 先給空欄
        "last_checked_at_utc": "",      # 先給空欄
    })

    # 4) 從 FetchLog 帶 api_etag（最後一筆）
    if "api_etag" in lc_f:
        etag_last = (fdf.dropna(subset=[lc_f["api_etag"]])
                        .sort_values(by=[lc_f.get("video_id","video_id"), lc_f.get("log_id","log_id")])
                        .groupby(lc_f.get("video_id","video_id"))[lc_f["api_etag"]]
                        .last()
                        .rename("api_etag"))
        vout = vout.merge(etag_last, left_on="video_id", right_index=True, how="left", suffixes=("", "_last"))
        # 左邊空則用右邊的值
        if "api_etag_last" in vout.columns:
            mask = (vout["api_etag"].astype(str) == "") | vout["api_etag"].isna()
            vout.loc[mask, "api_etag"] = vout.loc[mask, "api_etag_last"].fillna("")
            vout.drop(columns=["api_etag_last"], inplace=True)

    # 5) 從 FetchLog 帶最後一次 UTC 抓取時間 → 寫到 Videos.last_checked_at_utc
    if "last_checked_at_utc" not in vout.columns:
        vout["last_checked_at_utc"] = ""
    if "checked_at_utc" in lc_f:
        last_chk = (fdf.dropna(subset=[lc_f["checked_at_utc"]])
                       .groupby(lc_f.get("video_id","video_id"))[lc_f["checked_at_utc"]]
                       .max()
                       .rename("last_checked_at_utc"))
        # 併入，右表欄名加 _last 後綴
        vout = vout.merge(last_chk, left_on="video_id", right_index=True, how="left", suffixes=("", "_last"))
        if "last_checked_at_utc_last" in vout.columns:
            mask = (vout["last_checked_at_utc"].astype(str) == "") | vout["last_checked_at_utc"].isna()
            vout.loc[mask, "last_checked_at_utc"] = vout.loc[mask, "last_checked_at_utc_last"].fillna("")
            vout.drop(columns=["last_checked_at_utc_last"], inplace=True)

    vout = vout[[
        "video_id","channel_id","title",
        "published_at_utc","published_at_tpe",
        "status","archived_at","notes",
        "api_etag","last_checked_at_utc"
    ]]

    # --- FetchLog 部分 ---
    # 只保留東八時間；若只有 UTC 就轉成 TPE
    if "checked_at_tpe" in lc_f:
        chk_tpe = fget("checked_at_tpe").astype(str)
    elif "checked_at_utc" in lc_f:
        chk_tpe = fget("checked_at_utc").astype(str).apply(lambda s: _to_utc(s).astimezone(TZ_TPE).isoformat(timespec="seconds"))
    else:
        chk_tpe = ""

    # 重新計算排程的東八（若有 offset_min）
    if "offset_min" in lc_f:
        pub_map = dict(zip(vout["video_id"], vout["published_at_utc"]))
        def _sched(row):
            vid = row.get(lc_f.get("video_id","video_id"))
            off = row.get(lc_f["offset_min"])
            if pd.isna(off) or vid not in pub_map:
                return ""
            dt = _to_utc(pub_map[vid]) + timedelta(minutes=int(off))
            return dt.astimezone(TZ_TPE).isoformat(timespec="seconds")
        scheduled_tpe = fdf.apply(_sched, axis=1)
    else:
        scheduled_tpe = ""

    fout = pd.DataFrame({
        "log_id": fget("log_id") if "log_id" in lc_f else range(1, len(fdf)+1),
        "video_id": fget("video_id") if "video_id" in lc_f else "",
        "checked_at_tpe": chk_tpe,
        "stage_label": fget("stage_label") if "stage_label" in lc_f else "",
        "offset_min": fget("offset_min") if "offset_min" in lc_f else None,
        "scheduled_time_tpe": scheduled_tpe,
        "views": fget("views") if "views" in lc_f else None,
        "likes": fget("likes") if "likes" in lc_f else None,
        "comments": fget("comments") if "comments" in lc_f else None,
        "api_cost": fget("api_cost") if "api_cost" in lc_f else 0,
        "error_code": fget("error_code") if "error_code" in lc_f else "",
        "error_message": fget("error_message") if "error_message" in lc_f else "",
    })[[
        "log_id","video_id","checked_at_tpe","stage_label","offset_min","scheduled_time_tpe",
        "views","likes","comments","api_cost","error_code","error_message"
    ]]

    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        vout.to_excel(w, sheet_name="Videos", index=False)
        fout.to_excel(w, sheet_name="FetchLog", index=False)

    print("[MIGRATE] done.")

# ---------- 主流程 ----------
def run(game: str, days: int, policy_name: str, api_key: str, hl: str = "zh-Hant"):
    if not api_key:
        raise SystemExit("缺少 API key：請用 --api-key 或環境變數 YOUTUBE_API_KEY 提供。")

    results_xlsx, policy_json = game_paths(game)
    ensure_results_book(results_xlsx)
    policy = load_policy(policy_json, name=policy_name)
    rules  = read_video_lexicon(GLOSSARY_XLSX, game)
    ch_df  = read_channel_locales(GLOSSARY_XLSX, game)
    print(f"[INFO] {len(ch_df)} YouTube channels for {game}, lookback={days}d")

    
    now_utc = datetime.now(timezone.utc)
    published_after_utc = now_utc - timedelta(days=days)
    tz_tpe = tz.gettz("Asia/Taipei")
    now_tpe = now_utc.astimezone(tz_tpe)


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
        print(f"[{channel_id}] {len(items)} videos within last {days}d")
        if not items:
            continue

        vid_ids = [x["video_id"] for x in items]

        # 不要覆蓋 CLI --hl；若 CLI 沒給，才用 Channel_Locales.lang 推測
        channel_hl = _lang_to_hl(lang)
        eff_hl = hl or channel_hl

        stats = yt_get_video_stats(vid_ids, api_key, hl=eff_hl)

        # 寫入 Excel
        for x in items:
            vid   = x["video_id"]
            st    = stats.get(vid, {})
            title = st.get("title") or x["title"]
            pub_iso = st.get("publishedAt") or x["published_at"]

            # Shorts：片長 < 60 且 /shorts/ URL 成立
            dur_s = st.get("duration_s")
            is_short_by_time = (dur_s is not None and dur_s < SHORTS_TIME_LIMIT_S)
            is_short_by_url  = detect_shorts_url(vid)
            video_type = "shorts" if (is_short_by_time and is_short_by_url) else "video"

            # 只有一般影片才跑分類
            content_type = classify_title(title, rules, lang=lang, default="other") if video_type=="video" else "other"

            pub_dt_utc = datetime.fromisoformat(pub_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
            pub_dt_tpe = pub_dt_utc.astimezone(TZ_TPE)

            match = level_round_for_time(pub_dt_utc, now_utc, policy)
            stage_label = match["label"] if match else "adhoc"
            planned_offset_min = match["offset_min"] if match else None
            scheduled_tpe = (pub_dt_tpe + timedelta(minutes=int(planned_offset_min))).isoformat(timespec="seconds") if planned_offset_min is not None else ""

            # 執行時提示（是否有 zh-Hant）
            src = st.get("title_source","raw")
            if (hl or "").lower()=="zh-hant":
                mark = "✔︎" if src in ("loc.zh-Hant","s2t_fallback") else "✘"
                print(f"[{vid}] zh-Hant={mark} via {src} (lang={lang})")

            notes = build_notes(lang, content_type)

            upsert_video(results_xlsx, {
                "type": video_type,                      # ← A欄：video|shorts
                "video_id": vid,
                "channel_id": channel_id,
                "title": title,
                "published_at_utc": pub_dt_utc.isoformat(timespec="seconds").replace("+00:00","Z"),
                "published_at_tpe": pub_dt_tpe.isoformat(timespec="seconds"),
                "status": "Active",
                "archived_at": "",
                "notes": notes,                          # ← 精簡 notes（預設空）
                "api_etag": st.get("etag",""),
                "last_checked_at_utc": now_utc.isoformat(timespec="seconds").replace("+00:00","Z"),
            })

            append_fetch_log(results_xlsx, {
                "video_id": vid,
                "checked_at_tpe": now_tpe.isoformat(timespec="seconds"),
                "stage_label": stage_label,
                "offset_min": planned_offset_min,
                "scheduled_time_tpe": scheduled_tpe,
                "views": st.get("views"),
                "likes": st.get("likes"),
                "comments": st.get("comments"),
                "api_cost": 2,
                # 不再寫 source/title_source/title_zh_hant_ok
                "error_code": "",
                "error_message": "",
            })

    print(f"[OK] {game}: wrote stats → {results_xlsx}")

# ---------- CLI ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--game", choices=["gsi","hsr","zzz"], required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--policy", default="new_video_sampling_plan_v3")
    ap.add_argument("--migrate-only", action="store_true",
                    help="migrate YouTube_Results.xlsx schema then exit")
    ap.add_argument("--hl", default="zh-Hant", help="preferred localization language for titles (e.g., zh-Hant, en, ja)")
    args = ap.parse_args()

    results_xlsx = ensure_results_path(args.game)

    if args.migrate_only:
        ensure_results_book(results_xlsx)
        migrate_results_book(results_xlsx)
        print(f"[MIGRATE] done → {results_xlsx}")
        raise SystemExit(0)

    api_key = load_api_key_strict()
    run(args.game, args.days, args.policy, api_key, hl=args.hl)
