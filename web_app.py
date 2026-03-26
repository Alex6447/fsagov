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

CONFIG_FILE = Path("config.json")
DEFAULT_CONFIG = {
    "timeout": TIMEOUT,
    "retry_max": RETRY_MAX,
    "delay": DELAY_BETWEEN_REQUESTS,
    "fgis_token": BEARER_TOKEN,
}
UI_CONFIG_FILE = Path("ui_config.json")
DEFAULT_UI_CONFIG = {
    "primary_color": "#32CD32",
    "progress_color": "#00FF00",
    "text_color": "#FFFFFF",
    "background_color": "#0E1117",
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


def apply_ui_theme(ui_config: dict):
    st.markdown(
        f"""
        <style>
        .stButton > button {{
            background-color: {ui_config.get("primary_color", "#32CD32")};
            color: white;
            border: none;
            border-radius: 4px;
            padding: 8px 20px;
            font-size: 14px;
            width: 100%;
        }}
        .stButton > button:hover {{
            background-color: {ui_config.get("primary_color", "#32CD32")};
            opacity: 0.8;
        }}
        .stMetric {{
            background-color: #1E1E1E;
            padding: 10px;
            border-radius: 8px;
        }}
        .stMetricLabel {{
            font-size: 12px !important;
            color: #888 !important;
        }}
        .stMetricValue {{
            font-size: 20px !important;
        }}
        .progress-bar {{
            background-color: {ui_config.get("progress_color", "#00FF00")};
            height: 20px;
            border-radius: 5px;
            transition: width 0.3s;
        }}
        .stApp {{
            background-color: {ui_config.get("background_color", "#0E1117")};
        }}
        .text-color {{
            color: {ui_config.get("text_color", "#FFFFFF")};
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

    # Known federal districts
    KNOWN_DISTRICTS = [
        "Центральный федеральный округ",
        "Северо-Западный федеральный округ",
        "Южный федеральный округ",
        "Приволжский федеральный округ",
        "Сибирский федеральный округ",
        "Уральский федеральный округ",
        "Дальневосточный федеральный округ",
        "Северо-Кавказский федеральный округ",
    ]

    cursor.execute(
        "SELECT federal_district, COUNT(*) FROM showcases GROUP BY federal_district"
    )
    district_counts = {}
    for row in cursor.fetchall():
        name = row[0]
        if not name:
            continue
        # Check if it matches a known district exactly
        for known in KNOWN_DISTRICTS:
            if name.strip() == known:
                district_counts[known] = district_counts.get(known, 0) + row[1]
                break

    stats["districts"] = [{"name": k, "count": v} for k, v in district_counts.items()]

    cursor.execute("SELECT name_status, COUNT(*) FROM showcases GROUP BY name_status")
    for row in cursor.fetchall():
        stats["statuses"][row[0] or "Unknown"] = row[1]

    cursor.execute("SELECT MAX(run_at) FROM run_metrics")
    row = cursor.fetchone()
    if row and row[0]:
        stats["last_update"] = row[0]

    db.close()
    return stats


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
        page_icon="🏠",
        layout="wide",
    )

    st.title("🏠 Росреестр Парсер")

    tab1, tab2, tab3 = st.tabs(["📊 Дашборд", "⚙️ Настройки", "📖 README"])

    with tab1:
        col1, col2 = st.columns([2, 1])

        with col1:
            stats = get_db_stats()

            # Header with total count
            st.markdown("### Статистика БД")

            # Top metrics in columns - smaller
            m1, m2 = st.columns(2)
            m1.metric("Записей", stats["total"])
            m2.metric("Округов", len(stats["districts"]))

            if stats["last_update"]:
                st.caption(f"Последнее обновление: {stats['last_update']}")

            # Charts
            if stats["districts"]:
                import pandas as pd

                df = pd.DataFrame(stats["districts"])
                if not df.empty:
                    df = df.sort_values("count", ascending=True)
                    df["short_name"] = df["name"].str.replace(
                        " федеральный округ", "", regex=False
                    )
                    st.markdown("#### По округам")
                    chart_data = df.set_index("short_name")["count"]
                    st.bar_chart(chart_data, horizontal=True)

        with col2:
            st.subheader("Операции")

            if session_state.running:
                st.progress(session_state.progress / 100.0)
                st.warning("⏳ Операция выполняется...")
                if st.button("⏹ Остановить", use_container_width=True):
                    stop_script()
                    st.rerun()

            if st.button(
                "🔄 Полный парсинг",
                disabled=session_state.running,
                use_container_width=True,
            ):
                run_script("main.py")

            st.markdown("---")

            st.write("**Сохранить в XLSX:**")
            district_names = ["Все"] + [d["name"] for d in stats["districts"]]
            selected_district = st.selectbox(
                "Округ", district_names, key="district_select"
            )

            selected_region = st.selectbox("Регион", ["Все"], key="region_select")

            # Prepare data for export
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

                # Export to bytes
                exporter = XLSXExporter()
                wb = exporter._create_workbook(records)

                # Save to bytes
                output = BytesIO()
                wb.save(output)
                output.seek(0)

                st.download_button(
                    "📥 Скачать XLSX",
                    data=output.getvalue(),
                    file_name=f"rosreestr_{district_arg or 'all'}_{region_arg or 'all'}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                st.caption(f"Готов к скачиванию: {len(records)} записей")
            else:
                st.warning("Нет данных для экспорта")

            st.markdown("---")

            if st.button(
                "⬆️ Закачать обновления",
                disabled=session_state.running,
                use_container_width=True,
            ):
                run_script("main_update.py")

        if session_state.log_messages:
            st.markdown("### 📝 Логи")
            log_text = "".join(session_state.log_messages[-100:])
            st.text_area("Лог", log_text, height=300, key="log_area")

    with tab2:
        st.subheader("Параметры парсинга")

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

        st.markdown("---")
        st.subheader("🔑 Токен fgis_token")

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

        if col_save.button("💾 Сохранить"):
            config["timeout"] = timeout_val
            config["delay"] = delay_val
            config["retry_max"] = retry_val
            config["fgis_token"] = fgis_token_val
            save_config(config)
            st.success("Сохранено!")

        if col_reset.button("🔄 Сбросить"):
            save_config(DEFAULT_CONFIG)
            st.success("Сброшено к значениям по умолчанию!")
            st.rerun()

        st.markdown("---")
        st.subheader("Настройки UI")

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

        if col_ui_save.button("💾 Сохранить UI"):
            ui_config["primary_color"] = primary_color
            ui_config["progress_color"] = progress_color
            ui_config["text_color"] = text_color
            ui_config["background_color"] = bg_color
            save_ui_config(ui_config)
            st.success("Сохранено!")
            st.rerun()

        if col_ui_reset.button("🔄 Сбросить UI"):
            save_ui_config(DEFAULT_UI_CONFIG)
            st.success("Сброшено!")
            st.rerun()

    with tab3:
        st.markdown("""
        # 📖 Руководство пользователя

        ## О проекте

        **Росреестр Парсер** — это приложение для автоматического сбора данных о юридических лицах из реестра аккредитованных лиц (РАЛ) Федеральной службы государственной регистрации, кадастра и картографии (Росреестр).

        ### Возможности:
        - 📊 Просмотр статистики по базе данных
        - 🔄 Полный парсинг всех данных с сайта Росреестра
        - 📥 Экспорт данных в Excel (XLSX) с фильтрацией по округу и региону
        - ⬆️ Загрузка только новых записей (обновление базы)
        - ⚙️ Настройка параметров парсинга и внешнего вида

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

        ### 📊 Дашборд

        | Кнопка/Элемент | Описание |
        |----------------|----------|
        | **Статистика БД** | Показывает общее количество записей, разбивку по округам и статусам, дату последнего обновления |
        | **🔄 Полный парсинг** | Запускает полную загрузку данных с сайта Росреестра. Собирает все записи по всем регионам России. Может занять несколько часов |
        | **📥 Экспорт** | Экспортирует данные из базы в файл Excel |

        ### Фильтры экспорта

        - **Округ** — выберите федеральный округ (Центральный, Сибирский, Приволжский и т.д.)
        - **Регион** — после выбора округа станет доступен список регионов этого округа
        - Если выбрано "Все" — экспортируются все записи

        ### ⬆️ Закачать обновления

        Загружает только новые записи с сайта Росреестра:
        - Сравнивает данные с базой
        - Добавляет только новые записи
        - Не изменяет существующие данные
        - Работает значительно быстрее полного парсинга

        ---

        ## ⚙️ Настройки

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

        ## 📁 Файлы проекта

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

        ## 💡 Советы

        1. **Первое использование**: Сначала нажмите "Полный парсинг", чтобы загрузить все данные
        2. **Регулярные обновления**: Используйте "Закачать обновления" для добавления новых записей
        3. **Экспорт**: Выберите нужный округ/регион или экспортируйте всё
        4. **Настройки**: Подберите оптимальные тайминги под ваше интернет-соединение
        """)


if __name__ == "__main__":
    main()
