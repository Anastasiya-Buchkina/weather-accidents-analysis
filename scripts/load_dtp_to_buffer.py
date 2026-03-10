"""
ETL: загрузка сырых данных ДТП (ГИБДД) в public.dtp_buffer (Supabase/Postgres)

Берём 2 города:
- Ставрополь: ParReg=7, district_id=7401
- Кемерово: ParReg=32, district_id=32401

Период: с 2015-01 по текущий месяц включительно

Что пишем в таблицу:
- city, region_name, region_id, district_id, request_year, request_month
- raw_json: СЫРОЙ json (decoded "data" из ответа ГИБДД)
- processed=false (потом будем парсить в нормализованные таблицы)
- error_message: если запрос сломался/вернул пустоту странно

Запуск:
python src/load_dtp_to_buffer.py
"""

import os
import json
import time
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# =========================
# Логи
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# =========================
# Настройки API ГИБДД
# =========================
URL_DTP_CARDS = "http://stat.gibdd.ru/map/getDTPCardData"

FIELD_NAMES = [
    "dat", "time", "coordinates", "infoDtp", "k_ul", "dor", "ndu",
    "k_ts", "ts_info", "pdop", "pog", "osv", "s_pch", "s_pog",
    "n_p", "n_pg", "obst", "sdor", "t_osv", "t_p", "t_s", "v_p", "v_v"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (dtp_weather_project/1.0)",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Пауза между запросами
REQUEST_DELAY_SEC = 0.4

# Ретраи на сетевые глюки
MAX_RETRIES = 4
RETRY_SLEEP_BASE = 2  # 2, 4, 8... сек


# =========================
# Города (пока 2, потом масштабируем списком)
# =========================
CITIES = [
    {
        "city": "Ставрополь",
        "region_name": "Ставропольский край",
        "region_id": "7",     # ParReg
        "district_id": "7401", # reg
    },
    {
        "city": "Кемерово",
        "region_name": "Кемеровская область",
        "region_id": "32",      # ParReg
        "district_id": "32401", # reg
    },
]


# =========================
# Подключение к БД
# =========================
def get_engine():
    base_dir = Path(__file__).resolve().parent.parent  # <project_root>
    load_dotenv(base_dir / ".env")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Нет DATABASE_URL в .env (в корне проекта)")

    return create_engine(db_url, pool_pre_ping=True)


# =========================
# Полезные функции
# =========================
def month_range(start_year: int, start_month: int) -> List[Tuple[int, int]]:
    """Список (year, month) от start до текущего месяца включительно."""
    today = date.today()
    y, m = start_year, start_month
    out = []
    while (y < today.year) or (y == today.year and m <= today.month):
        out.append((y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def request_gibdd_month(
    session: requests.Session,
    region_id: str,
    district_id: str,
    year: int,
    month: int,
    start: int = 1,
    end: int = 10000,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Запрос карточек ДТП за месяц.
    Возвращаем:
      decoded_json (dict) или None
      error_message (str) или None
    """

    payload_inner = {
        "date": [f"MONTHS:{month}.{year}"],
        "ParReg": str(region_id),
        "order": {"type": "1", "fieldName": "dat"},
        "reg": str(district_id),
        "ind": "1",
        "st": str(start),
        "en": str(end),
        "fil": {"isSummary": False},
        "fieldNames": FIELD_NAMES
    }

    # ВАЖНО: API ГИБДД хочет JSON-в-JSON: поле "data" строкой
    request_data = {"data": json.dumps(payload_inner, separators=(",", ":"))}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(URL_DTP_CARDS, json=request_data, headers=HEADERS, timeout=60)

            # если нас "банят" или что-то подобное
            if r.status_code != 200:
                return None, f"HTTP {r.status_code}: {r.text[:200]}"

            outer = r.json()
            data_str = outer.get("data")

            # иногда data приходит пустой строкой
            if not isinstance(data_str, str) or not data_str.strip():
                # считаем это валидным "нет данных" (или странный ответ)
                return {"tab": [], "_note": "outer.data пустой"}, None

            decoded = json.loads(data_str)
            return decoded, None

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # сетевой глюк — ретраим
            if attempt < MAX_RETRIES:
                sleep_s = RETRY_SLEEP_BASE ** attempt
                logger.warning("Сетевой глюк (%s). Ретрай %s/%s через %ss", repr(e), attempt, MAX_RETRIES, sleep_s)
                time.sleep(sleep_s)
                continue
            return None, f"network_error: {repr(e)}"

        except Exception as e:
            return None, f"exception: {repr(e)}"

    return None, "unknown_error"


def exists_buffer_row(engine, city: str, district_id: str, year: int, month: int) -> bool:
    """Чтобы не плодить дубли при повторном запуске."""
    sql = """
    select 1
    from public.dtp_buffer
    where city = :city
      and district_id = :district_id
      and request_year = :y
      and request_month = :m
    limit 1
    """
    with engine.begin() as conn:
        row = conn.execute(
            text(sql),
            {"city": city, "district_id": district_id, "y": year, "m": month}
        ).fetchone()
    return row is not None


def insert_buffer_row(
    engine,
    city: str,
    region_name: str,
    region_id: str,
    district_id: str,
    year: int,
    month: int,
    raw_json: Dict[str, Any],
    error_message: Optional[str],
):
    sql = """
    insert into public.dtp_buffer
    (city, region_name, region_id, district_id, request_year, request_month, raw_json, error_message)
    values
    (:city, :region_name, :region_id, :district_id, :y, :m, cast(:raw_json as jsonb), :error_message)
    """
    params = {
        "city": city,
        "region_name": region_name,
        "region_id": region_id,
        "district_id": district_id,
        "y": year,
        "m": month,
        "raw_json": json.dumps(raw_json, ensure_ascii=False),
        "error_message": error_message,
    }
    with engine.begin() as conn:
        conn.execute(text(sql), params)


# =========================
# Main
# =========================
def main():
    engine = get_engine()

    months = month_range(2015, 1)
    logger.info("Месяцев к загрузке: %s (с 2015-01 по %s-%02d)", len(months), date.today().year, date.today().month)

    with requests.Session() as session:
        total_written = 0
        total_skipped = 0
        total_errors = 0

        for city_cfg in CITIES:
            city = city_cfg["city"]
            region_name = city_cfg["region_name"]
            region_id = city_cfg["region_id"]
            district_id = city_cfg["district_id"]

            logger.info("=== Город: %s (%s) | ParReg=%s district_id=%s ===", city, region_name, region_id, district_id)

            for (y, m) in months:
                # чтобы можно было безопасно перезапускать
                if exists_buffer_row(engine, city, district_id, y, m):
                    total_skipped += 1
                    continue

                decoded, err = request_gibdd_month(session, region_id, district_id, y, m)

                if decoded is None:
                    # даже если упало — всё равно фиксируем, что пытались
                    decoded = {"tab": [], "_note": "request_failed"}
                    total_errors += 1

                # полезно сохранить контекст прямо внутрь raw_json
                decoded["_meta"] = {
                    "city": city,
                    "region_name": region_name,
                    "region_id": region_id,
                    "district_id": district_id,
                    "request_year": y,
                    "request_month": m,
                    "loaded_at": datetime.utcnow().isoformat() + "Z",
                }

                insert_buffer_row(
                    engine=engine,
                    city=city,
                    region_name=region_name,
                    region_id=region_id,
                    district_id=district_id,
                    year=y,
                    month=m,
                    raw_json=decoded,
                    error_message=err,
                )

                # лог по факту (сколько карточек)
                tab = decoded.get("tab", []) or []
                logger.info("%s %s-%02d | карточек: %s%s",
                            city, y, m, len(tab),
                            f" | ERR: {err}" if err else "")

                total_written += 1
                time.sleep(REQUEST_DELAY_SEC)

        logger.info("Готово ✅ Записано строк: %s | Пропущено (дубли): %s | Ошибок: %s",
                    total_written, total_skipped, total_errors)


if __name__ == "__main__":
    main()
