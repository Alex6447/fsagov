import json
import os
import subprocess
import sys
import threading
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


def _svg(path: str, size: int = 16) -> str:
    """Inline Lucide-style SVG icon."""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" '
        f'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" '
        f'stroke-linecap="round" stroke-linejoin="round" '
        f'style="vertical-align:-2px;margin-right:6px;">{path}</svg>'
    )


ICON = {
    "home": _svg('<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>'
                 '<polyline points="9 22 9 12 15 12 15 22"/>'),
    "chart": _svg('<line x1="18" y1="20" x2="18" y2="10"/>'
                  '<line x1="12" y1="20" x2="12" y2="4"/>'
                  '<line x1="6" y1="20" x2="6" y2="14"/>'),
    "sliders": _svg('<line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/>'
                    '<line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/>'
                    '<line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/>'
                    '<line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/>'
                    '<line x1="17" y1="16" x2="23" y2="16"/>'),
    "book": _svg('<path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/>'
                 '<path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/>'),
    "key": _svg('<path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 '
                '5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/>'),
    "terminal": _svg('<polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/>'),
    "download": _svg('<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
                     '<polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>'),
    "upload": _svg('<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
                   '<polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>'),
    "refresh": _svg('<polyline points="23 4 23 10 17 10"/>'
                    '<path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>'),
    "stop": _svg('<rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>'),
    "save": _svg('<path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/>'
                 '<polyline points="17 21 17 13 7 13 7 21"/>'
                 '<polyline points="7 3 7 8 15 8"/>'),
    "undo": _svg('<polyline points="1 4 1 10 7 10"/>'
                 '<path d="M3.51 15a9 9 0 1 0 .49-3.36"/>'),
    "activity": _svg('<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>'),
    "database": _svg('<ellipse cx="12" cy="5" rx="9" ry="3"/>'
                     '<path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>'
                     '<path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>'),
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

        /* ── Progress bar ── */
        .stProgress > div > div > div > div {{
            background: linear-gradient(90deg, {primary}, {progress});
            border-radius: 99px;
            box-shadow: 0 0 10px rgba(16,185,129,0.4);
        }}
        .stProgress > div > div {{
            background: rgba(16,185,129,0.1);
            border-radius: 99px;
            height: 8px !important;
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
        stats["districts"].append({
            "name": d["name"],
            "downloaded": downloaded_by_district.get(d["name"], 0),
            "total_source": d["total_source"],
        })

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
            raise RuntimeError(
                "API вернул ошибку. Проверьте fgis_token в Настройках."
            )
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


def run_script(script_name: str, *args):
    def target():
        import os

        cwd = os.getcwd()
        # Use uv run
        cmd = f"uv run python {script_name} {' '.join(args)}"
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            shell=True,
        )
        session_state.process = proc

        for line in proc.stdout:
            if not session_state.running:
                break
            session_state.log_messages.append(line)
            if len(session_state.log_messages) > 500:
                session_state.log_messages.pop(0)

        proc.wait()
        session_state.running = False
        session_state.process = None
        session_state.progress = 100

    session_state.running = True
    session_state.log_messages = []
    session_state.progress = 0
    session_state.process = None
    thread = threading.Thread(target=target)
    thread.start()


def stop_script():
    if session_state.process:
        session_state.process.terminate()
    session_state.running = False
    session_state.process = None


def main():
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

    tab1, tab2, tab3 = st.tabs(["  Дашборд", "  Настройки", "  Документация"])

    with tab1:
        col1, col2 = st.columns([2, 1])

        with col1:
            stats = get_db_stats()

            st.markdown("### Статистика базы данных")

            m1, m2, m3 = st.columns(3)
            m1.metric("Всего записей", f"{stats['total']:,}".replace(",", " "))
            m2.metric("Федеральных округов", len(stats["districts"]))
            active_count = stats["statuses"].get("Действующий", 0)
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
                outline_bar = alt.Chart(df).mark_bar(
                    filled=False,
                    stroke="#10B981",
                    strokeWidth=1.5,
                    strokeOpacity=0.45,
                    cornerRadiusTopRight=3,
                    cornerRadiusBottomRight=3,
                ).encode(
                    x=alt.X("total_source:Q", title=None, axis=x_axis),
                    y=y_enc,
                    tooltip=[
                        alt.Tooltip("name:N", title="Округ"),
                        alt.Tooltip("total_source:Q", title="Всего на сайте", format="d"),
                        alt.Tooltip("downloaded:Q", title="Скачано", format="d"),
                    ],
                )

                # Слой 2: заливочный бар (downloaded) — поверх
                filled_bar = alt.Chart(df).mark_bar(
                    color="#10B981",
                    opacity=0.85,
                    cornerRadiusTopRight=3,
                    cornerRadiusBottomRight=3,
                ).encode(
                    x=alt.X("downloaded:Q", title=None, axis=x_axis),
                    y=y_enc,
                    tooltip=[
                        alt.Tooltip("name:N", title="Округ"),
                        alt.Tooltip("downloaded:Q", title="Скачано", format="d"),
                        alt.Tooltip("total_source:Q", title="Всего на сайте", format="d"),
                    ],
                )

                chart = alt.layer(outline_bar, filled_bar).properties(
                    height=alt.Step(30)
                ).configure_view(
                    strokeWidth=0,
                    fill="transparent",
                ).configure_axis(
                    domain=False,
                    ticks=False,
                )

                st.altair_chart(chart, use_container_width=True)

                if not has_totals:
                    st.caption(
                        "Данные источника не загружены — нажмите «⟳ Обновить данные источника»"
                    )

        with col2:
            st.markdown(
                f"<div style='background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.07);"
                f"border-radius:16px;padding:20px 18px;'>"
                f"<p style='font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6B7280;"
                f"text-transform:uppercase;margin-bottom:16px;'>{ICON['activity']}Управление</p>",
                unsafe_allow_html=True,
            )

            if session_state.running:
                st.progress(session_state.progress / 100.0)
                st.markdown(
                    f"<p style='font-size:13px;color:#FCD34D;margin:8px 0;'>"
                    f"{ICON['activity']}Операция выполняется...</p>",
                    unsafe_allow_html=True,
                )
                if st.button("✕  Остановить", use_container_width=True):
                    stop_script()
                    st.rerun()

            if st.button(
                "↻  Полный парсинг",
                disabled=session_state.running,
                use_container_width=True,
            ):
                run_script("main.py")

            st.markdown(
                "<p style='font-size:11px;color:#6B7280;margin:4px 0 8px;'>Полная загрузка всех данных</p>",
                unsafe_allow_html=True,
            )

            if st.button(
                "⟳  Обновить данные источника",
                disabled=session_state.running,
                use_container_width=True,
                help="Запрашивает у API общее количество записей по каждому из 8 округов",
            ):
                with st.spinner("Запрашиваю данные источника..."):
                    try:
                        fetch_district_totals()
                        st.success("Данные источника обновлены!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Ошибка: {e}")

            st.markdown(
                "<p style='font-size:11px;color:#6B7280;margin:4px 0 16px;'>Счётчики записей на сайте</p>",
                unsafe_allow_html=True,
            )

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
                st.caption(f"{len(records):,} записей готово к экспорту".replace(",", " "))
            else:
                st.markdown(
                    "<p style='font-size:13px;color:#6B7280;'>Нет данных для экспорта</p>",
                    unsafe_allow_html=True,
                )

            st.markdown(
                "<hr style='border-color:rgba(255,255,255,0.06);margin:12px 0;'>",
                unsafe_allow_html=True,
            )

            if st.button(
                "↑  Загрузить обновления",
                disabled=session_state.running,
                use_container_width=True,
            ):
                run_script("main_update.py")

            st.markdown(
                "<p style='font-size:11px;color:#6B7280;margin:4px 0 0;'>Только новые записи</p>"
                "</div>",
                unsafe_allow_html=True,
            )

        if session_state.log_messages:
            st.markdown(
                f"<p style='font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6B7280;"
                f"text-transform:uppercase;margin:24px 0 8px;'>{ICON['terminal']}Журнал операций</p>",
                unsafe_allow_html=True,
            )
            log_text = "".join(session_state.log_messages[-100:])
            st.text_area("", log_text, height=260, key="log_area", label_visibility="collapsed")

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

        if col_save.button("✓  Сохранить"):
            config["timeout"] = timeout_val
            config["delay"] = delay_val
            config["retry_max"] = retry_val
            config["fgis_token"] = fgis_token_val
            save_config(config)
            st.success("Сохранено!")

        if col_reset.button("↺  Сбросить"):
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

        if col_ui_save.button("✓  Сохранить"):
            ui_config["primary_color"] = primary_color
            ui_config["progress_color"] = progress_color
            ui_config["text_color"] = text_color
            ui_config["background_color"] = bg_color
            save_ui_config(ui_config)
            st.success("Сохранено!")
            st.rerun()

        if col_ui_reset.button("↺  Сбросить"):
            save_ui_config(DEFAULT_UI_CONFIG)
            st.success("Сброшено!")
            st.rerun()

    with tab3:
        st.markdown("""
        # Руководство пользователя

        ## О проекте

        **Росреестр Парсер** — это приложение для автоматического сбора данных о юридических лицах из реестра аккредитованных лиц (РАЛ) Федеральной службы государственной регистрации, кадастра и картографии (Росреестр).

        ### Возможности:
        - Просмотр статистики по базе данных
        - Полный парсинг всех данных с сайта Росреестра
        - Экспорт данных в Excel (XLSX) с фильтрацией по округу и региону
        - Загрузка только новых записей (обновление базы)
        - Настройка параметров парсинга и внешнего вида

        ---

        ## Как запустить

        ### Вариант 1: Ярлык (рекомендуется)
        Дважды кликните по файлу `run.bat` на рабочем столе. Приложение откроется в браузере.

        ### Вариант 2: Вручную
        ```bash
        uv sync
        uv run streamlit run web_app.py
        ```
        После запуска откройте в браузере: http://localhost:8501

        ---

        ## Описание кнопок и функций

        ### Дашборд

        | Кнопка/Элемент | Описание |
        |----------------|----------|
        | **Статистика БД** | Показывает общее количество записей, разбивку по округам и статусам, дату последнего обновления |
        | **↻ Полный парсинг** | Запускает полную загрузку данных с сайта Росреестра. Собирает все записи по всем регионам России. Может занять несколько часов |
        | **↓ Скачать XLSX** | Экспортирует данные из базы в файл Excel |

        ### Фильтры экспорта

        - **Округ** — выберите федеральный округ (Центральный, Сибирский, Приволжский и т.д.)
        - **Регион** — после выбора округа станет доступен список регионов этого округа
        - Если выбрано "Все" — экспортируются все записи

        ### ↑ Загрузить обновления

        Загружает только новые записи с сайта Росреестра:
        - Сравнивает данные с базой
        - Добавляет только новые записи
        - Не изменяет существующие данные
        - Работает значительно быстрее полного парсинга

        ---

        ## Настройки

        ### Параметры парсинга

        | Параметр | Описание | По умолчанию |
        |----------|----------|--------------|
        | Таймаут запроса | Время ожидания ответа от сервера (сек) | 30 |
        | Задержка между запросами | Пауза между запросами (сек) | 3 |
        | Максимум повторных попыток | Сколько раз повторять запрос при ошибке | 3 |

        ### Настройки UI

        | Параметр | Описание |
        |----------|----------|
        | Основной цвет кнопок | Цвет кнопок в приложении |
        | Цвет progress bar | Цвет индикатора прогресса |
        | Цвет текста | Цвет текста в приложении |
        | Цвет фона | Цвет фона приложения |

        ---

        ## Файлы проекта

        ```
        fsagov/
        ├── main.py           # Основной скрипт парсинга
        ├── main_xlsx.py      # Скрипт экспорта в Excel
        ├── main_update.py    # Скрипт обновления (загрузка новых записей)
        ├── web_app.py        # Web-интерфейс (Streamlit)
        ├── run.bat           # Ярлык для запуска
        ├── config.py         # Конфигурация
        ├── data/             # База данных и экспорты
        └── logs/             # Логи работы
        ```

        ---

        ## Советы

        1. **Первое использование**: Сначала нажмите "Полный парсинг", чтобы загрузить все данные
        2. **Регулярные обновления**: Используйте "Закачать обновления" для добавления новых записей
        3. **Экспорт**: Выберите нужный округ/регион или экспортируйте всё
        4. **Настройки**: Подберите оптимальные тайминги под ваше интернет-соединение
        """)


if __name__ == "__main__":
    main()
