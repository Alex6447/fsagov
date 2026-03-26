import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))

from config import TIMEOUT, RETRY_MAX, DELAY_BETWEEN_REQUESTS, BEARER_TOKEN
from src.utils.db_tools import Database
from src.utils.log_tools import setup_logging

setup_logging()


@st.cache_resource
def _init_db_once():
    db = Database()
    db.init_db()


_init_db_once()

CONFIG_FILE = Path("config.json")
DEFAULT_CONFIG = {
    "timeout": TIMEOUT,
    "retry_max": RETRY_MAX,
    "delay": DELAY_BETWEEN_REQUESTS,
    "fgis_token": BEARER_TOKEN,
}
UI_CONFIG_FILE = Path("ui_config.json")
RUN_STATE_FILE = Path("logs/run_state.json")
DEFAULT_UI_CONFIG = {
    "primary_color": "#10B981",
    "progress_color": "#34D399",
    "text_color": "#F9FAFB",
    "background_color": "#0A0E1A",
}

session_state = st.session_state
if "running" not in session_state:
    session_state.running = False
if "log_messages" not in session_state:
    session_state.log_messages = []
if "progress" not in session_state:
    session_state.progress = 0
if "process" not in session_state:
    session_state.process = None
if "run_started_at" not in session_state:
    session_state.run_started_at = None
if "run_command" not in session_state:
    session_state.run_command = ""


def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return {**DEFAULT_CONFIG, **json.load(f)}
    return DEFAULT_CONFIG.copy()


def save_config(config: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def load_ui_config() -> dict:
    if UI_CONFIG_FILE.exists():
        with open(UI_CONFIG_FILE, "r", encoding="utf-8") as f:
            return {**DEFAULT_UI_CONFIG, **json.load(f)}
    return DEFAULT_UI_CONFIG.copy()


def save_ui_config(config: dict):
    with open(UI_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def _read_run_state() -> dict | None:
    if not RUN_STATE_FILE.exists():
        return None
    try:
        with open(RUN_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_run_state(state: dict):
    RUN_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(RUN_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _clear_run_state():
    if RUN_STATE_FILE.exists():
        RUN_STATE_FILE.unlink()


def _is_pid_running(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _sync_runtime_state():
    state = _read_run_state()
    if not state:
        session_state.running = False
        return

    pid = int(state.get("pid") or 0)
    if _is_pid_running(pid):
        session_state.running = True
        session_state.run_started_at = state.get("started_at")
        session_state.run_command = state.get("command", "")
    else:
        _clear_run_state()
        session_state.running = False
        session_state.run_started_at = None


def _get_active_run_info() -> dict | None:
    state = _read_run_state()
    if not state:
        return None

    pid = int(state.get("pid") or 0)
    if not _is_pid_running(pid):
        return None

    return {
        "pid": pid,
        "command": state.get("command", ""),
        "started_at": state.get("started_at"),
    }


def _svg(path: str, size: int = 16) -> str:
    """Inline Lucide-style SVG icon."""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" '
        f'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" '
        f'stroke-linecap="round" stroke-linejoin="round" '
        f'style="vertical-align:-2px;margin-right:6px;">{path}</svg>'
    )


ICON = {
    "home": _svg(
        '<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>'
        '<polyline points="9 22 9 12 15 12 15 22"/>'
    ),
    "chart": _svg(
        '<line x1="18" y1="20" x2="18" y2="10"/>'
        '<line x1="12" y1="20" x2="12" y2="4"/>'
        '<line x1="6" y1="20" x2="6" y2="14"/>'
    ),
    "sliders": _svg(
        '<line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/>'
        '<line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/>'
        '<line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/>'
        '<line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/>'
        '<line x1="17" y1="16" x2="23" y2="16"/>'
    ),
    "book": _svg(
        '<path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/>'
        '<path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/>'
    ),
    "key": _svg(
        '<path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 '
        '5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/>'
    ),
    "terminal": _svg(
        '<polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/>'
    ),
    "download": _svg(
        '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
        '<polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>'
    ),
    "upload": _svg(
        '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
        '<polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>'
    ),
    "refresh": _svg(
        '<polyline points="23 4 23 10 17 10"/>'
        '<path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>'
    ),
    "stop": _svg('<rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>'),
    "save": _svg(
        '<path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/>'
        '<polyline points="17 21 17 13 7 13 7 21"/>'
        '<polyline points="7 3 7 8 15 8"/>'
    ),
    "undo": _svg(
        '<polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-3.36"/>'
    ),
    "activity": _svg('<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>'),
    "database": _svg(
        '<ellipse cx="12" cy="5" rx="9" ry="3"/>'
        '<path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>'
        '<path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>'
    ),
}


def apply_ui_theme(ui_config: dict):
    primary = ui_config.get("primary_color", "#6366F1")
    progress = ui_config.get("progress_color", "#8B5CF6")
    text = ui_config.get("text_color", "#F9FAFB")
    bg = ui_config.get("background_color", "#0A0E1A")

    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        /* ── Global ── */
        html, body, [class*="css"] {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }}
        .stApp {{
            background-color: {bg};
            color: {text};
        }}

        /* ── Sidebar / main block ── */
        .block-container {{
            padding-top: 2rem;
            padding-bottom: 2rem;
        }}

        /* ── Title ── */
        h1 {{
            font-size: 1.75rem !important;
            font-weight: 700 !important;
            letter-spacing: -0.02em;
            color: {primary} !important;
            margin-bottom: 0.25rem !important;
        }}
        h2, h3 {{
            font-weight: 600 !important;
            color: {text} !important;
        }}

        /* ── Tabs ── */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 4px;
            background: rgba(255,255,255,0.04);
            border-radius: 12px;
            padding: 4px;
            border: 1px solid rgba(255,255,255,0.06);
        }}
        .stTabs [data-baseweb="tab"] {{
            border-radius: 8px;
            padding: 8px 20px;
            font-size: 14px;
            font-weight: 500;
            color: #9CA3AF;
            transition: all 0.2s ease;
            border: none !important;
            background: transparent !important;
        }}
        .stTabs [aria-selected="true"] {{
            background: rgba(16,185,129,0.15) !important;
            color: {primary} !important;
            border: 1px solid rgba(16,185,129,0.35) !important;
            box-shadow: 0 2px 12px rgba(16,185,129,0.15);
        }}
        .stTabs [data-baseweb="tab-highlight"] {{
            display: none;
        }}

        /* ── Buttons (все в одном стиле) ── */
        .stButton > button,
        .stDownloadButton > button {{
            background: rgba(16,185,129,0.12);
            color: {primary};
            border: 1px solid rgba(16,185,129,0.35);
            border-radius: 10px;
            padding: 10px 20px;
            font-size: 14px;
            font-weight: 600;
            width: 100%;
            transition: all 0.2s ease;
            letter-spacing: 0.01em;
        }}
        .stButton > button:hover,
        .stDownloadButton > button:hover {{
            background: rgba(16,185,129,0.22);
            border-color: rgba(16,185,129,0.6);
            color: {progress};
            transform: translateY(-1px);
            box-shadow: 0 4px 16px rgba(16,185,129,0.2);
        }}
        .stButton > button:active,
        .stDownloadButton > button:active {{
            transform: translateY(0);
        }}
        .stButton > button:disabled {{
            background: rgba(255,255,255,0.04) !important;
            color: #374151 !important;
            border-color: rgba(255,255,255,0.06) !important;
            box-shadow: none;
            transform: none;
        }}

        /* ── Metric cards ── */
        [data-testid="metric-container"] {{
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.07);
            border-radius: 14px;
            padding: 18px 20px !important;
            backdrop-filter: blur(10px);
            transition: border-color 0.2s ease;
        }}
        [data-testid="metric-container"]:hover {{
            border-color: rgba(16,185,129,0.3);
        }}
        [data-testid="metric-container"] label {{
            font-size: 12px !important;
            font-weight: 500 !important;
            color: #6B7280 !important;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        [data-testid="metric-container"] [data-testid="stMetricValue"] {{
            font-size: 2rem !important;
            font-weight: 700 !important;
            color: {text} !important;
            letter-spacing: -0.02em;
        }}

        /* ── Run progress (custom single bar) ── */
        .run-progress-label {{
            color: {text};
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 8px;
            line-height: 1.2;
        }}
        .run-progress-track {{
            height: 12px;
            border-radius: 999px;
            background: rgba(255,255,255,0.10);
            overflow: hidden;
            margin-bottom: 12px;
        }}
        .run-progress-fill {{
            height: 100%;
            border-radius: 999px;
            background: linear-gradient(90deg, {primary}, {progress});
        }}
        .run-active-card {{
            background: rgba(16,185,129,0.12);
            color: {primary};
            border: 1px solid rgba(16,185,129,0.35);
            border-radius: 10px;
            padding: 10px 12px;
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 8px;
        }}
        .run-inactive-card {{
            background: rgba(239,68,68,0.12);
            color: #EF4444;
            border: 1px solid rgba(239,68,68,0.35);
            border-radius: 10px;
            padding: 10px 12px;
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 8px;
        }}

        /* ── Select boxes ── */
        .stSelectbox [data-baseweb="select"] > div {{
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 10px;
            color: {text};
        }}
        .stSelectbox [data-baseweb="select"] > div:hover {{
            border-color: rgba(16,185,129,0.5);
        }}

        /* ── Number inputs ── */
        .stNumberInput > div > div > input {{
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 10px;
            color: {text};
        }}
        .stNumberInput > div > div > input:focus {{
            border-color: rgba(16,185,129,0.6);
            box-shadow: 0 0 0 2px rgba(16,185,129,0.15);
        }}

        /* ── Text inputs ── */
        .stTextInput > div > div > input {{
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 10px;
            color: {text};
        }}
        .stTextInput > div > div > input:focus {{
            border-color: rgba(16,185,129,0.6);
            box-shadow: 0 0 0 2px rgba(16,185,129,0.15);
        }}

        /* ── Text area (logs) ── */
        .stTextArea > div > div > textarea {{
            background: rgba(0,0,0,0.3);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px;
            font-family: 'JetBrains Mono', 'Fira Code', monospace;
            font-size: 12px;
            color: #A3E635;
            line-height: 1.6;
        }}

        /* ── Alerts / warnings ── */
        .stWarning {{
            background: rgba(245,158,11,0.1);
            border: 1px solid rgba(245,158,11,0.3);
            border-radius: 10px;
            color: #FCD34D;
        }}
        .stSuccess {{
            background: rgba(16,185,129,0.1);
            border: 1px solid rgba(16,185,129,0.3);
            border-radius: 10px;
        }}
        .stInfo {{
            background: rgba(99,102,241,0.1);
            border: 1px solid rgba(99,102,241,0.25);
            border-radius: 10px;
        }}

        /* ── Divider ── */
        hr {{
            border-color: rgba(255,255,255,0.07) !important;
            margin: 1.5rem 0 !important;
        }}

        /* ── Expander ── */
        .streamlit-expanderHeader {{
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 10px;
            font-weight: 500;
        }}
        .streamlit-expanderContent {{
            border: 1px solid rgba(255,255,255,0.06);
            border-top: none;
            border-radius: 0 0 10px 10px;
            background: rgba(255,255,255,0.02);
        }}

        /* ── Dataframe / chart ── */
        [data-testid="stVegaLiteChart"] {{
            border-radius: 14px;
            overflow: hidden;
        }}

        /* ── Caption / small text ── */
        .stCaption {{
            color: #6B7280 !important;
            font-size: 12px !important;
        }}

        /* ── Subheader styling ── */
        .stSubheader {{
            color: {text} !important;
        }}

        /* ── Scrollbar ── */
        ::-webkit-scrollbar {{
            width: 6px;
            height: 6px;
        }}
        ::-webkit-scrollbar-track {{
            background: transparent;
        }}
        ::-webkit-scrollbar-thumb {{
            background: rgba(255,255,255,0.1);
            border-radius: 99px;
        }}
        ::-webkit-scrollbar-thumb:hover {{
            background: rgba(16,185,129,0.4);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_db_stats() -> dict:
    db = Database()
    stats = {
        "total": db.get_count(),
        "districts": [],
        "statuses": {},
        "last_update": None,
    }

    db.connect()
    cursor = db.conn.cursor()

    # Все округа из справочника (всегда 8)
    cursor.execute("SELECT id, name, total_source FROM nsi_districts ORDER BY name")
    all_districts = [
        {"id": row["id"], "name": row["name"], "total_source": row["total_source"]}
        for row in cursor.fetchall()
    ]

    # Скачанные записи по округу
    cursor.execute(
        "SELECT federal_district, COUNT(*) as cnt FROM showcases GROUP BY federal_district"
    )
    downloaded_by_district = {}
    for row in cursor.fetchall():
        name = row[0]
        if name:
            downloaded_by_district[name.strip()] = row[1]

    for d in all_districts:
        stats["districts"].append(
            {
                "name": d["name"],
                "downloaded": downloaded_by_district.get(d["name"], 0),
                "total_source": d["total_source"],
            }
        )

    cursor.execute("SELECT name_status, COUNT(*) FROM showcases GROUP BY name_status")
    for row in cursor.fetchall():
        stats["statuses"][row[0] or "Unknown"] = row[1]

    cursor.execute("SELECT MAX(run_at) FROM run_metrics")
    row = cursor.fetchone()
    if row and row[0]:
        stats["last_update"] = row[0]

    db.close()
    return stats


def fetch_district_totals():
    """Запрашивает у API общее количество записей по каждому округу и сохраняет в БД."""
    from src.utils.api_tools import RosreestrAPIClient

    db = Database()
    districts = db.get_districts()

    client = RosreestrAPIClient()

    # Проверяем токен ДО цикла запросов
    if not client.session_mgr.is_valid():
        db.close()
        raise RuntimeError(
            "Bearer-токен истёк. Обновите fgis_token в разделе Настройки."
        )

    # Короткие задержки для web-контекста (не замораживать UI)
    client.delay_after_error = 5
    client.retry_max = 2

    for district in districts:
        regions = db.get_regions(district["id"])
        region_master_ids = [r["masterId"] for r in regions if r.get("masterId")]
        if not region_master_ids:
            continue
        filters = {"idAddressSubject": region_master_ids}
        total = client.get_total(filters=filters)
        if total is None:
            raise RuntimeError("API вернул ошибку. Проверьте fgis_token в Настройках.")
        db.update_district_total(district["id"], total)

    db.close()


def get_districts_and_regions() -> tuple:
    db = Database()
    districts = db.get_districts()

    district_map = {}
    for d in districts:
        regions = db.get_regions(d["id"])
        district_map[d["name"]] = [r["name"] for r in regions]

    db.close()
    return districts, district_map


def _build_run_command(script_name: str, *args) -> list[str]:
    clean_args = [arg for arg in args if arg]
    return ["uv", "run", "python", "-u", script_name, *clean_args]


def _get_live_status() -> str:
    log_messages = _read_log_tail(limit=30)
    for line in reversed(log_messages):
        text = line.strip()
        if text:
            return text

    return "Процесс запущен, ожидаем появление записей в логе..."


def _count_region_records(region_name: str) -> int | None:
    if not region_name:
        return None

    db = Database()
    try:
        db.connect()
        cursor = db.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM showcases WHERE region = ?", (region_name,)
        )
        row = cursor.fetchone()
        return int(row[0] if row else 0)
    except Exception:
        return None
    finally:
        db.close()


def _get_district_progress(
    district_name: str,
) -> tuple[int | None, int | None, float | None]:
    if not district_name or district_name == "—":
        return None, None, None

    db = Database()
    try:
        db.connect()
        cursor = db.conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) FROM showcases WHERE federal_district = ?",
            (district_name,),
        )
        downloaded_row = cursor.fetchone()
        downloaded = int(downloaded_row[0] if downloaded_row else 0)

        cursor.execute(
            "SELECT total_source FROM nsi_districts WHERE name = ?", (district_name,)
        )
        total_row = cursor.fetchone()
        total_source = (
            int(total_row[0] or 0) if total_row and total_row[0] is not None else 0
        )

        if total_source > 0:
            ratio = max(0.0, min(1.0, downloaded / total_source))
        else:
            ratio = None

        return downloaded, total_source, ratio
    except Exception:
        return None, None, None
    finally:
        db.close()


def _region_to_district_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    districts, district_map = get_districts_and_regions()
    for district in districts:
        district_name = district.get("name", "")
        for region_name in district_map.get(district_name, []):
            mapping[region_name] = district_name
    return mapping


def _get_live_parse_snapshot() -> tuple[str, float]:
    lines = _read_log_tail(limit=1200)
    if not lines:
        return "— - — - всего записей: — - записано: —", 0.02

    region_name = None
    total_records = None

    pattern_check = re.compile(r"=== CHECK: region=(.+?) -> pages=(\d+) ===")
    pattern_update_total = re.compile(r"Region '(.+?)': total=(\d+)")
    pattern_response_total = re.compile(
        r"RESPONSE: total=(\d+), items=(\d+), pages=(\d+)"
    )

    for line in reversed(lines):
        if region_name is None:
            m = pattern_check.search(line)
            if m:
                region_name = m.group(1).strip()

        if region_name is None:
            m = pattern_update_total.search(line)
            if m:
                region_name = m.group(1).strip()
                total_records = int(m.group(2))

        if total_records is None:
            m = pattern_response_total.search(line)
            if m:
                total_records = int(m.group(1))

        if region_name and total_records is not None:
            break

    district_name = "—"
    if region_name:
        district_name = _region_to_district_map().get(region_name, "—")

    inserted = _count_region_records(region_name) if region_name else None

    total_text = (
        f"{total_records:,}".replace(",", " ") if total_records is not None else "—"
    )
    inserted_text = f"{inserted:,}".replace(",", " ") if inserted is not None else "—"
    region_text = region_name or "—"

    _, _, district_ratio = _get_district_progress(district_name)
    progress = district_ratio if district_ratio is not None else 0.02

    text = (
        f"{district_name} - {region_text} - всего записей: {total_text} - "
        f"записано: {inserted_text}"
    )
    return text, progress


def _read_log_tail(limit: int = 200) -> list[str]:
    log_path = Path("logs/log.log")
    if not log_path.exists():
        return []

    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return lines[-limit:]
    except Exception:
        return []


def _render_readme(readme_path: Path):
    """Render README with support for local markdown images."""
    try:
        content = readme_path.read_text(encoding="utf-8")
    except Exception as e:
        st.error(f"Не удалось прочитать README.md: {e}")
        return

    image_pattern = re.compile(r"!\[(.*?)\]\((.*?)\)")
    markdown_buffer: list[str] = []

    for line in content.splitlines():
        match = image_pattern.search(line.strip())
        if not match:
            markdown_buffer.append(line)
            continue

        if markdown_buffer:
            st.markdown("\n".join(markdown_buffer))
            markdown_buffer = []

        alt_text = match.group(1).strip() or None
        image_ref = match.group(2).strip().split()[0]

        if image_ref.startswith(("http://", "https://")):
            st.markdown(line)
            continue

        local_image = (readme_path.parent / image_ref).resolve()
        if local_image.exists():
            st.image(str(local_image), caption=alt_text, use_container_width=True)
        else:
            st.warning(f"Изображение не найдено: {image_ref}")

    if markdown_buffer:
        st.markdown("\n".join(markdown_buffer))


def run_script(script_name: str, *args):
    active_state = _read_run_state()
    if active_state and _is_pid_running(int(active_state.get("pid") or 0)):
        pid = active_state.get("pid")
        return False, f"Уже выполняется процесс (PID {pid}). Сначала остановите его."

    cwd = os.getcwd()
    cmd = _build_run_command(script_name, *args)
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        shell=False,
        creationflags=creationflags,
    )

    started_at = time.time()
    command_text = " ".join(cmd)
    _write_run_state(
        {
            "pid": proc.pid,
            "script": script_name,
            "command": command_text,
            "started_at": started_at,
        }
    )

    session_state.running = True
    session_state.run_started_at = started_at
    session_state.run_command = command_text
    session_state.log_messages = [f"Запущено: {session_state.run_command}\n"]
    session_state.progress = 0
    session_state.process = proc
    return True, f"Запущено (PID {proc.pid})"


def stop_script():
    state = _read_run_state()
    if not state:
        session_state.running = False
        session_state.process = None
        session_state.run_started_at = None
        return "Активный процесс не найден"

    pid = int(state.get("pid") or 0)
    if pid and _is_pid_running(pid):
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            shell=False,
        )

    _clear_run_state()
    session_state.log_messages.append(f"Остановка: процесс PID {pid} остановлен.\n")
    session_state.running = False
    session_state.process = None
    session_state.run_started_at = None
    return f"Процесс PID {pid} остановлен"


def main():
    _sync_runtime_state()

    ui_config = load_ui_config()
    apply_ui_theme(ui_config)

    st.set_page_config(
        page_title="Росреестр Парсер",
        page_icon="◈",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(
        f"<h1>{ICON['home']}Росреестр Парсер</h1>"
        "<p style='color:#6B7280;font-size:14px;margin-top:-8px;margin-bottom:24px;'>"
        "Мониторинг реестра аккредитованных лиц · ФСА Росреестр"
        "</p>",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3, tab4 = st.tabs(["  Дашборд", "  Настройки", "  README", "  Логи"])

    with tab1:
        col1, col2 = st.columns([2, 1])

        with col1:
            stats = get_db_stats()

            st.markdown("### Статистика базы данных")

            m1, m2, m3 = st.columns(3)
            m1.metric("Всего записей", f"{stats['total']:,}".replace(",", " "))

            downloaded_total = sum(
                int(d.get("downloaded") or 0) for d in stats["districts"]
            )
            source_total = sum(
                max(int(d.get("total_source") or 0), int(d.get("downloaded") or 0))
                for d in stats["districts"]
            )
            fill_percent = (
                (downloaded_total / source_total * 100) if source_total > 0 else 0.0
            )
            m2.metric("Заполнение БД", f"{fill_percent:.1f}%")

            active_count = stats["statuses"].get("Действует", 0) + stats[
                "statuses"
            ].get("Действующий", 0)
            m3.metric("Действующих", f"{active_count:,}".replace(",", " "))

            if stats["last_update"]:
                st.caption(f"Последнее обновление данных: {stats['last_update']}")

            if stats["districts"]:
                import pandas as pd
                import altair as alt

                df = pd.DataFrame(stats["districts"])
                df["short_name"] = df["name"].str.replace(
                    " федеральный округ", "", regex=False
                )
                # ensure total_source >= downloaded
                df["total_source"] = df["total_source"].fillna(0).astype(int)
                df["total_source"] = df[["total_source", "downloaded"]].max(axis=1)
                df = df.sort_values("downloaded", ascending=True).reset_index(drop=True)

                has_totals = (df["total_source"] > 0).any()
                sort_order = df["short_name"].tolist()

                y_enc = alt.Y(
                    "short_name:N",
                    sort=sort_order,
                    title=None,
                    axis=alt.Axis(labelLimit=220, labelColor="#9CA3AF"),
                )
                x_axis = alt.Axis(format="d", labelColor="#9CA3AF", gridColor="#1F2937")

                st.markdown("#### Распределение по округам")

                # Слой 1: полный контурный бар (total_source) — только рамка, без заливки
                outline_bar = (
                    alt.Chart(df)
                    .mark_bar(
                        filled=False,
                        stroke="#10B981",
                        strokeWidth=1,
                        strokeOpacity=0.45,
                        cornerRadiusTopRight=3,
                        cornerRadiusBottomRight=3,
                    )
                    .encode(
                        x=alt.X("total_source:Q", title=None, axis=x_axis),
                        y=y_enc,
                        tooltip=[
                            alt.Tooltip("name:N", title="Округ"),
                            alt.Tooltip(
                                "total_source:Q", title="Всего на сайте", format="d"
                            ),
                            alt.Tooltip("downloaded:Q", title="Скачано", format="d"),
                        ],
                    )
                )

                # Слой 2: заливочный бар (downloaded) — поверх
                filled_bar = (
                    alt.Chart(df)
                    .mark_bar(
                        color="#10B981",
                        opacity=0.85,
                        cornerRadiusTopRight=3,
                        cornerRadiusBottomRight=3,
                    )
                    .encode(
                        x=alt.X("downloaded:Q", title=None, axis=x_axis),
                        y=y_enc,
                        tooltip=[
                            alt.Tooltip("name:N", title="Округ"),
                            alt.Tooltip("downloaded:Q", title="Скачано", format="d"),
                            alt.Tooltip(
                                "total_source:Q", title="Всего на сайте", format="d"
                            ),
                        ],
                    )
                )

                chart = (
                    alt.layer(outline_bar, filled_bar)
                    .properties(height=alt.Step(30))
                    .configure_view(
                        strokeWidth=0,
                        fill="transparent",
                    )
                    .configure_axis(
                        domain=False,
                        ticks=False,
                    )
                )

                st.altair_chart(chart, use_container_width=True)

                if not has_totals:
                    st.caption(
                        "Данные источника не загружены — нажмите «⟳ Обновить данные источника»"
                    )

            if session_state.running:
                elapsed = 0
                if session_state.run_started_at:
                    elapsed = int(time.time() - session_state.run_started_at)
                elapsed_label = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
                snapshot_text, snapshot_progress = _get_live_parse_snapshot()
                fill_percent = max(2, min(100, int(snapshot_progress * 100)))

                st.markdown(
                    (
                        f"<div class='run-progress-label'>Выполняется парсинг... {elapsed_label}</div>"
                        f"<div class='run-progress-track'><div class='run-progress-fill' style='width:{fill_percent}%;'></div></div>"
                    ),
                    unsafe_allow_html=True,
                )
                st.caption(snapshot_text)

        with col2:
            active_run = _get_active_run_info()
            if active_run:
                st.markdown(
                    f"<div class='run-active-card'>Активный запуск: PID {active_run['pid']}</div>",
                    unsafe_allow_html=True,
                )
                attach_col, stop_col = st.columns(2)
                if attach_col.button(
                    "Подцепиться",
                    use_container_width=True,
                    key="attach_run_btn",
                ):
                    session_state.running = True
                    session_state.run_started_at = active_run.get("started_at")
                    session_state.run_command = active_run.get("command", "")
                    session_state.log_messages = [
                        f"Подцеплено к процессу PID {active_run['pid']}\n"
                    ]
                    st.rerun()

                if stop_col.button(
                    "Остановить PID",
                    use_container_width=True,
                    key="stop_run_from_status_btn",
                ):
                    stop_script()
                    st.rerun()
            else:
                st.markdown(
                    "<div class='run-inactive-card'>Нет активного фонового процесса</div>",
                    unsafe_allow_html=True,
                )

            if not session_state.running:
                st.markdown(
                    "<p style='font-size:11px;color:#6B7280;margin:4px 0 8px;'>Полная загрузка всех данных</p>",
                    unsafe_allow_html=True,
                )

                if st.button(
                    "⇩ Полная загрузка", use_container_width=True, key="full_parse_btn"
                ):
                    cfg = load_config()
                    token = cfg.get("fgis_token", "")
                    started, message = run_script(
                        "main.py", f"--token={token}" if token else ""
                    )
                    if started:
                        st.rerun()
                    else:
                        st.warning(message)

            if st.button(
                "⟳  Обновить данные источника",
                disabled=session_state.running,
                use_container_width=True,
                key="refresh_source_btn",
            ):
                with st.spinner("Запрашиваю данные источника..."):
                    try:
                        fetch_district_totals()
                        st.success("Данные источника обновлены!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Ошибка: {e}")

            st.markdown(
                "<hr style='border-color:rgba(255,255,255,0.06);margin:12px 0;'>",
                unsafe_allow_html=True,
            )

            st.markdown(
                f"<p style='font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6B7280;"
                f"text-transform:uppercase;margin-bottom:12px;'>{ICON['download']}Экспорт в XLSX</p>",
                unsafe_allow_html=True,
            )

            district_names = ["Все"] + [d["name"] for d in stats["districts"]]
            selected_district = st.selectbox(
                "Федеральный округ", district_names, key="district_select"
            )

            selected_region = st.selectbox("Регион", ["Все"], key="region_select")

            district_arg = None if selected_district == "Все" else selected_district
            region_arg = None if selected_region == "Все" else selected_region

            db = Database()
            records = db.get_all_records()

            if district_arg:
                records = [
                    r for r in records if r.get("federal_district") == district_arg
                ]
            if region_arg:
                records = [r for r in records if r.get("region") == region_arg]

            db.close()

            if records:
                from src.utils.xlsx_tools import XLSXExporter
                from io import BytesIO

                exporter = XLSXExporter()
                wb = exporter._create_workbook(records)

                output = BytesIO()
                wb.save(output)
                output.seek(0)

                st.download_button(
                    "↓  Скачать XLSX",
                    data=output.getvalue(),
                    file_name=f"rosreestr_{district_arg or 'all'}_{region_arg or 'all'}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                st.caption(
                    f"{len(records):,} записей готово к экспорту".replace(",", " ")
                )
            else:
                st.markdown(
                    "<p style='font-size:13px;color:#6B7280;'>Нет данных для экспорта</p>",
                    unsafe_allow_html=True,
                )

            st.markdown(
                "<hr style='border-color:rgba(255,255,255,0.06);margin:12px 0;'>",
                unsafe_allow_html=True,
            )

            st.markdown(
                "<p style='font-size:11px;color:#6B7280;margin:4px 0 8px;'>Только новые записи</p>",
                unsafe_allow_html=True,
            )

            if st.button(
                "⤴ Закачать обновления",
                disabled=session_state.running,
                use_container_width=True,
                key="update_btn",
            ):
                cfg = load_config()
                token = cfg.get("fgis_token", "")
                started, message = run_script(
                    "main_update.py", f"--token={token}" if token else ""
                )
                if started:
                    st.rerun()
                else:
                    st.warning(message)

    # Logs tab
    with tab4:
        st.markdown("### Логи")

        refresh_col, hint_col = st.columns([1, 2])
        with refresh_col:
            st.button("Обновить логи", use_container_width=True, key="refresh_logs_btn")
        with hint_col:
            if session_state.running:
                st.caption(
                    "Парсинг выполняется. Обновляйте логи кнопкой, чтобы не блокировать вкладки."
                )

        # Show logs
        log_messages = _read_log_tail(limit=300)
        if not log_messages:
            log_messages = session_state.log_messages[-200:]

        log_text = "".join(log_messages)
        st.text_area("Журнал", log_text, height=500, key="log_area")

        # Stop button
        if session_state.running:
            if st.button("✕ Остановить", use_container_width=True, key="stop_btn"):
                stop_script()
                st.rerun()

    with tab2:
        st.markdown(
            f"<p style='font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6B7280;"
            f"text-transform:uppercase;margin-bottom:16px;'>{ICON['sliders']}Параметры парсинга</p>",
            unsafe_allow_html=True,
        )

        config = load_config()

        timeout_val = st.number_input(
            "Таймаут запроса (сек)",
            min_value=5,
            max_value=300,
            value=config.get("timeout", TIMEOUT),
        )
        delay_val = st.number_input(
            "Задержка между запросами (сек)",
            min_value=0,
            max_value=60,
            value=config.get("delay", DELAY_BETWEEN_REQUESTS),
        )
        retry_val = st.number_input(
            "Максимум повторных попыток",
            min_value=0,
            max_value=10,
            value=config.get("retry_max", RETRY_MAX),
        )

        st.markdown(
            "<hr style='border-color:rgba(255,255,255,0.06);margin:20px 0;'>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<p style='font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6B7280;"
            f"text-transform:uppercase;margin-bottom:16px;'>{ICON['key']}Токен авторизации</p>",
            unsafe_allow_html=True,
        )

        with st.expander("Как получить токен (инструкция)"):
            st.markdown("""
            **Инструкция:**
            1. Откройте браузер и зайдите на https://pub.fsa.gov.ru/ral
            2. Нажмите **F12** (или правой кнопкой → Inspect)
            3. Перейдите на вкладку **Application** (или "Приложение")
            4. В левом меню раскройте **Local Storage** → выберите https://pub.fsa.gov.ru
            5. Найдите ключ **fgis_token** и скопируйте его значение
            6. Вставьте значение в поле ниже и нажмите "Сохранить"
            
            **Примечание:** Токен нужно получить один раз, он сохранится и будет использоваться при следующих запусках.
            """)
        fgis_token_val = st.text_input(
            "fgis_token",
            value=config.get("fgis_token", ""),
            type="password",
            help="Вставьте токен из Local Storage браузера",
        )

        col_save, col_reset = st.columns(2)

        if col_save.button("✓  Сохранить", key="save_config_btn"):
            config["timeout"] = timeout_val
            config["delay"] = delay_val
            config["retry_max"] = retry_val
            config["fgis_token"] = fgis_token_val
            save_config(config)
            st.success("Сохранено!")

        if col_reset.button("↺  Сбросить", key="reset_config_btn"):
            save_config(DEFAULT_CONFIG)
            st.success("Сброшено к значениям по умолчанию!")
            st.rerun()

        st.markdown(
            "<hr style='border-color:rgba(255,255,255,0.06);margin:20px 0;'>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<p style='font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6B7280;"
            f"text-transform:uppercase;margin-bottom:16px;'>{ICON['activity']}Настройки темы</p>",
            unsafe_allow_html=True,
        )

        ui_config = load_ui_config()

        primary_color = st.color_picker(
            "Основной цвет кнопок",
            ui_config.get("primary_color", "#32CD32"),
        )
        progress_color = st.color_picker(
            "Цвет progress bar",
            ui_config.get("progress_color", "#00FF00"),
        )
        text_color = st.color_picker(
            "Цвет текста",
            ui_config.get("text_color", "#FFFFFF"),
        )
        bg_color = st.color_picker(
            "Цвет фона",
            ui_config.get("background_color", "#0E1117"),
        )

        col_ui_save, col_ui_reset = st.columns(2)

        if col_ui_save.button("✓  Сохранить", key="save_ui_btn"):
            ui_config["primary_color"] = primary_color
            ui_config["progress_color"] = progress_color
            ui_config["text_color"] = text_color
            ui_config["background_color"] = bg_color
            save_ui_config(ui_config)
            st.success("Сохранено!")
            st.rerun()

        if col_ui_reset.button("↺  Сбросить", key="reset_ui_btn"):
            save_ui_config(DEFAULT_UI_CONFIG)
            st.success("Сброшено!")
            st.rerun()

    with tab3:
        readme_path = Path("README.md")
        if readme_path.exists():
            _render_readme(readme_path)
        else:
            st.warning("README.md не найден в корне проекта")

    if session_state.running:
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()
