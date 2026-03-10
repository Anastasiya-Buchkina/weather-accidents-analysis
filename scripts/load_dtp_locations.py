"""
ETL: загрузка геоданных и адресной информации ДТП
Источник: public.dtp_buffer.raw_json
Назначение: public.dtp_locations (1 строка = 1 kart_id)

Дополнительно в public.dtp_locations хранятся 3 поля, относящиеся к дорожной среде из dtp_buffer:
- light_condition (из infoDtp.osv)
- local_weather_condition (из infoDtp.s_pog)
- road_surface_condition (из infoDtp.s_pch)

Логика работы:
- Скрипт читает dtp_buffer батчами по buffer_id
- Извлекает данные из raw_json
- Трансформирует поля
- Делает UPSERT в public.dtp_locations
- НЕ выполняет полную очистку таблицы (работает инкрементально)

Запуск:
python src/load_dtp_locations.py
python src/load_dtp_locations.py --batch-size 100
python src/load_dtp_locations.py --cities "Кемерово,Ставрополь"

Параметры:
--batch-size     размер батча (по умолчанию 50)
--cities         фильтр по городам
--conflict-mode  update (по умолчанию) или nothing
"""

from __future__ import annotations

import os
import json
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
import time

# Загружаем .env сразу при старте модуля
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


# ==============================================================
# Константы
# ==============================================================
LIST_DELIMITER = " | "
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


# ==============================================================
# Логирование
# ==============================================================
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("dtp_locations_loader_py")


# ==============================================================
# DB
# ==============================================================
def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL отсутствует в .env")

    # Важно для долгих ETL: предотвращаем "мертвые" коннекты и обрывы на длительных INSERT.
    # keepalives_* актуальны для libpq/psycopg2.
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,  # раз в 30 мин принудительно обновлять соединение
        pool_size=5,
        max_overflow=5,
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )


# ==============================================================
# Вспомогательные функции преобразования
# ==============================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_text_list(value: Any) -> Optional[str]:
    """
    Превращает list[str] → строку через LIST_DELIMITER.
    Если строка — возвращает как есть.
    """
    if value is None:
        return None

    if isinstance(value, list):
        cleaned = [str(v).strip() for v in value if str(v).strip()]
        return LIST_DELIMITER.join(cleaned) if cleaned else None

    s = str(value).strip()
    return s or None


def to_int(value: Any) -> Optional[int]:
    if value in (None, "", "null"):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def to_coord(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        f = float(str(value).replace(",", ".").strip())
        return None if abs(f) < 1e-12 else f
    except Exception:
        return None


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


# ==============================================================
# Бизнес‑логика трансформации
# ==============================================================
def map_road_category_group(category: Optional[str]) -> Optional[str]:
    if not category:
        return None

    c = category.lower()

    if c.startswith("местного значения"):
        return "местная"
    if c.startswith("региональная"):
        return "региональная"
    if c.startswith("федеральная"):
        return "федеральная"
    if category == "Не указано":
        return "не указано"

    return "прочее"


# ==============================================================
# EXTRACT + TRANSFORM
# ==============================================================
def extract_locations(raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    cards = raw_json.get("tab") or []
    if not isinstance(cards, list):
        return []

    loaded_at = utc_now()
    result: List[Dict[str, Any]] = []

    for item in cards:
        if not isinstance(item, dict):
            continue

        kart_id = item.get("KartId")
        if kart_id is None:
            continue

        info = item.get("infoDtp") or {}
        if not isinstance(info, dict):
            info = {}

        row = {
            "kart_id": int(kart_id),
            "road_defects": as_text_list(info.get("ndu")),
            "road_elements": as_text_list(info.get("sdor")),
            "objects_near": as_text_list(info.get("OBJ_DTP")),
            "city": normalize_text(info.get("n_p")),
            "street": normalize_text(info.get("street")),
            "house": normalize_text(info.get("house")),
            "km": to_int(info.get("km")),
            "m": to_int(info.get("m")),
            "road_type": normalize_text(info.get("k_ul")),
            "road_category": normalize_text(info.get("dor_z")),
            "road_category_group": map_road_category_group(info.get("dor_z")),
            "light_condition": normalize_text(info.get("osv")),
            "local_weather_condition": as_text_list(info.get("s_pog")),
            "road_surface_condition": normalize_text(info.get("s_pch")),
            "lat_raw": to_coord(info.get("COORD_W")),
            "lon_raw": to_coord(info.get("COORD_L")),
            "loaded_at": loaded_at,
        }

        result.append(row)

    return result


# ==============================================================
# DB операции
# ==============================================================
def iter_chunks(items: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    """Простой чанкер списков для безопасной записи крупными объёмами."""
    if chunk_size <= 0:
        yield items
        return
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


@dataclass
class UpsertStats:
    written: int
    attempts: int
    retries: int


def upsert_rows(
    engine: Engine,
    rows: List[Dict[str, Any]],
    conflict_mode: str,
    *,
    rows_chunk_size: int = 500,
    max_retries: int = 3,
) -> UpsertStats:
    """UPSERT пачкой словарей.

    Почему чанки важны:
    - `executemany` может быть слишком тяжёлым для сервера при тысячах строк за раз;
    - при сетевых сбоях/перезапуске сервера проще повторить маленький чанк.
    """
    if not rows:
        return UpsertStats(written=0, attempts=0, retries=0)

    base_sql = """
        insert into public.dtp_locations
        (kart_id, road_defects, road_elements, objects_near,
         city, street, house, km, m,
         road_type, road_category, road_category_group,
         light_condition, local_weather_condition, road_surface_condition,
         lat_raw, lon_raw, loaded_at)
        values
        (:kart_id, :road_defects, :road_elements, :objects_near,
         :city, :street, :house, :km, :m,
         :road_type, :road_category, :road_category_group,
         :light_condition, :local_weather_condition, :road_surface_condition,
         :lat_raw, :lon_raw, :loaded_at)
    """

    if conflict_mode == "nothing":
        sql = base_sql + " on conflict (kart_id) do nothing"
    else:
        sql = base_sql + """
            on conflict (kart_id) do update set
                road_defects = excluded.road_defects,
                road_elements = excluded.road_elements,
                objects_near = excluded.objects_near,
                city = excluded.city,
                street = excluded.street,
                house = excluded.house,
                km = excluded.km,
                m = excluded.m,
                road_type = excluded.road_type,
                road_category = excluded.road_category,
                road_category_group = excluded.road_category_group,
                light_condition = excluded.light_condition,
                local_weather_condition = excluded.local_weather_condition,
                road_surface_condition = excluded.road_surface_condition,
                lat_raw = excluded.lat_raw,
                lon_raw = excluded.lon_raw,
                loaded_at = excluded.loaded_at
        """

    written = 0
    attempts = 0
    retries = 0

    for chunk in iter_chunks(rows, rows_chunk_size):
        attempts += 1
        # Ретраи только для конкретного чанка
        for try_no in range(max_retries + 1):
            try:
                with engine.begin() as conn:
                    conn.execute(text(sql), chunk)
                written += len(chunk)
                break
            except OperationalError as e:
                # Типовая причина: сервер/коннект упал на тяжёлом executemany.
                if try_no >= max_retries:
                    raise
                retries += 1
                sleep_s = 2 ** try_no
                logger.warning(
                    "OperationalError на чанке (rows=%s), ретрай %s/%s через %ss: %s",
                    len(chunk),
                    try_no + 1,
                    max_retries,
                    sleep_s,
                    str(e).splitlines()[0],
                )
                time.sleep(sleep_s)

    return UpsertStats(written=written, attempts=attempts, retries=retries)


# ==============================================================
# CLI
# ==============================================================
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--cities", type=str, default="")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--full-refresh", type=str, default="false")
    p.add_argument("--conflict-mode", choices=["update", "nothing"], default="update")
    return p.parse_args()


def str_to_bool(s: str) -> bool:
    return str(s).strip().lower() in {"1", "true", "yes", "y"}


# ==============================================================
# MAIN
# ==============================================================
def main():
    args = parse_args()
    if args.batch_size < 1:
        raise ValueError("--batch-size должен быть >= 1")
    engine = get_engine()

    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    full_refresh = str_to_bool(args.full_refresh)

    logger.info(
        "Старт загрузки public.dtp_locations. Города: %s",
        ", ".join(cities) if cities else "(все)",
    )
    logger.info(
        "FULL_REFRESH=%s | BATCH_SIZE_BUFFERS=%s | CONFLICT_MODE=%s",
        full_refresh,
        args.batch_size,
        args.conflict_mode,
    )

    if full_refresh:
        logger.warning(
            "FULL_REFRESH запрошен, но принудительная очистка отключена. Скрипт работает в инкрементальном режиме."
        )

    # Батчинг по buffer_id (keyset pagination)
    select_sql = """
        select buffer_id, raw_json
        from public.dtp_buffer
        where (:cities_is_empty = true or city = any(:cities))
          and buffer_id > :last_buffer_id
        order by buffer_id
        limit :limit
    """

    batch_no = 0
    last_buffer_id = 0

    attempted_total = 0
    written_total = 0
    null_coords_total = 0
    skipped_bad_json_total = 0
    upsert_attempts_total = 0
    upsert_retries_total = 0

    while True:
        with engine.begin() as conn:
            buf_rows = conn.execute(
                text(select_sql),
                {
                    "cities": cities,
                    "cities_is_empty": len(cities) == 0,
                    "last_buffer_id": last_buffer_id,
                    "limit": int(args.batch_size),
                },
            ).fetchall()

        if not buf_rows:
            break

        batch_no += 1
        batch_min = buf_rows[0][0]
        batch_max = buf_rows[-1][0]

        extracted: List[Dict[str, Any]] = []
        skipped_bad_json = 0

        for buffer_id, raw in buf_rows:
            last_buffer_id = buffer_id

            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    skipped_bad_json += 1
                    continue

            if isinstance(raw, dict):
                extracted.extend(extract_locations(raw))
            else:
                skipped_bad_json += 1

        attempted_rows = len(extracted)
        null_coords = sum(
            1 for r in extracted if r.get("lat_raw") is None or r.get("lon_raw") is None
        )

        stats = upsert_rows(engine, extracted, args.conflict_mode)
        written_rows = stats.written

        attempted_total += attempted_rows
        written_total += written_rows
        null_coords_total += null_coords
        skipped_bad_json_total += skipped_bad_json
        upsert_attempts_total += stats.attempts
        upsert_retries_total += stats.retries

        logger.info(
            "Батч #%s: buffer_id %s..%s (шт=%s) | attempted_rows=%s | written_rows=%s | null_coords=%s | bad_json=%s | upsert_chunks=%s | upsert_retries=%s",
            batch_no,
            batch_min,
            batch_max,
            len(buf_rows),
            attempted_rows,
            written_rows,
            null_coords,
            skipped_bad_json,
            stats.attempts,
            stats.retries,
        )

    logger.info(
        "Готово ✅ attempted_total=%s | written_total=%s | null_coords_total=%s | bad_json_total=%s | upsert_attempts_total=%s | upsert_retries_total=%s",
        attempted_total,
        written_total,
        null_coords_total,
        skipped_bad_json_total,
        upsert_attempts_total,
        upsert_retries_total,
    )


if __name__ == "__main__":
    main()