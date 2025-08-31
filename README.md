# Mihoyo Extract Tool
一個可擴充的爬蟲骨幹（skeleton）。特色：
- **依遊戲載入任務清單**（原神 gsi、崩鐵 hsr、絕區零 zzz），之後可擴充更多遊戲。
- **統一 Task 註冊與執行介面**（`Task`、`@task` 裝飾器）。
- **互動式 CLI**：選遊戲 → 看任務 → 選擇要執行的動作（可一次選多個，以逗號分隔）。
- **無第三方相依**，純標準庫；之後可自行加上 `requests`、`playwright` 等。
- **清楚的擴充點**：在 `crawler_skeleton/games/*.py` 新增任務即可。
## 快速開始

#主要任務
- **1.Youtube影片資料
- **2.BiliBili影片資料
- **3.各遊戲角色基本資料
- **4.各角色UP資料
- **5.各遊戲在淘寶的商品清單、價格、銷量
#並輸出到 Excel

```bash
# 方式 A：直接執行
python main.py

# 方式 B：以模組方式執行
python -m crawler_skeleton

## 目錄結構

```
crawler_skeleton/
├─ main.py                  # 入口點（支援 python -m crawler_skeleton）
├─ __init__.py
├─ cli.py                   # CLI 互動選單
├─ core.py                  # Task/Registry/裝飾器/載入與執行邏輯
├─ utils.py                 # 日誌、輸出資料夾等共用工具
└─ games/
   ├─ __init__.py
   ├─ gsi.py                # 原神（示範任務）
   ├─ hsr.py                # 崩壞：星穹鐵道（示範任務）
   └─ zzz.py                # 絕區零（示範任務）
```

