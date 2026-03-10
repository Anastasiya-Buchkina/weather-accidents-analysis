"""
load_weather_conditions.py

ETL: распарсить public.weather_buffer (RAW JSONB) → public.weather_conditions (почасовые строки).

Что делает
1) Берёт строки из public.weather_buffer (по source и городам).
2) Извлекает из payload.hourly массивы time + погодные массивы.
3) Превращает каждый час в отдельную строку и пишет в public.weather_conditions.
4) Пишет через UPSERT (ON CONFLICT по (source, city_id, observed_at)).

Инкрементальность (умная)
- По умолчанию скрипт НЕ переписывает всё.
- Для каждого города берёт watermark = max(observed_at) в weather_conditions по source.
- Парсит только те часы, которые строго больше watermark (с учётом --lookback-hours, чтобы “доподтянуть хвост”).

Автоматизация/масштабирование
- Поддерживает список городов (по имени) или “все” (из weather_buffer).
- Батчи по weather_buffer (batch-size-buffers) + чанки вставки в weather_conditions (insert-chunk-size).
- Логирование по батчам + метрики качества (плохой JSON / несходящиеся длины массивов / сколько часов пропущено/записано).

Примеры запуска
- Все города, инкрементально:
  python src/parse_weather_buffer_to_conditions.py

- Только 2 города:
  python src/parse_weather_buffer_to_conditions.py --cities "Ставрополь" "Кемерово"

- Перезаписать целиком (осторожно, долго):
  python src/parse_weather_buffer_to_conditions.py --full-refresh true
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ArgumentError

# Загружаем .env сразу при старте модуля
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


# ========================
# Константы
# ========================

BUFFER_TABLE = "public.weather_buffer"
TARGET_TABLE = "public.weather_conditions"
DIM_CITY_TABLE = "public.dim_city"

DEFAULT_SOURCE = "open_meteo_archive"

# Поля в payload.hourly, которые мы ожидаем и пишем в weather_conditions
HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "visibility",
    "wind_speed_10m",
    "wind_gusts_10m",
]

# Маппинг JSON → колонки таблицы weather_conditions
JSON_TO_COL = {
    "temperature_2m": "temperature",
    "relative_humidity_2m": "relative_humidity",
    "precipitation": "precipitation",
    "rain": "rain",
    "snowfall": "snowfall",
    "cloud_cover": "cloud_cover",
    "visibility": "visibility",
    "wind_speed_10m": "wind_speed",
    "wind_gusts_10m": "wind_gusts",
}

# Защита от API rate-limit здесь не нужна — API мы не дергаем, но микропауза полезна для DB
DEFAULT_DB_PAUSE_SEC = 0.0


# ========================
# Логирование
# ========================

def setup_logger(name: str = "weather_conditions_parser") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(h)
    logger.propagate = False
    return logger


# ========================
# Настройки CLI
# ========================

@dataclass(frozen=True)
class Settings:
    cities: Optional[List[str]]          # None => все города из weather_buffer
    source: str
    full_refresh: bool
    lookback_hours: int                 # “хвост” для доподтяжки в инкрементальном режиме
    batch_size_buffers: int             # сколько строк weather_buffer читаем за раз
    insert_chunk_size: int              # сколько почасовых строк апсертим за раз
    db_pause_sec: float                 # микропауза между батчами


def parse_args() -> Settings:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--cities",
        nargs="+",
        default=None,
        help="Список городов (точное совпадение по dim_city.city). Если не задано — берём все города, которые есть в weather_buffer.",
    )
    p.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Источник (weather_buffer.source). По умолчанию: {DEFAULT_SOURCE}",
    )
    p.add_argument(
        "--full-refresh",
        default="false",
        help="true/false. true = очистить weather_conditions и загрузить заново.",
    )
    p.add_argument(
        "--lookback-hours",
        type=int,
        default=48,
        help="Сколько часов назад от watermark перезагружать (инкрементально). Полезно, если есть дозапись/правки хвоста.",
    )
    p.add_argument(
        "--batch-size-buffers",
        type=int,
        default=10,
        help="Сколько строк weather_buffer читать за один батч.",
    )
    p.add_argument(
        "--insert-chunk-size",
        type=int,
        default=5000,
        help="Сколько почасовых строк вставлять (upsert) за один запрос executemany.",
    )
    p.add_argument(
        "--db-pause-sec",
        type=float,
        default=DEFAULT_DB_PAUSE_SEC,
        help="Пауза между батчами по weather_buffer (сек). По умолчанию 0.",
    )

    a = p.parse_args()

    full_refresh = str(a.full_refresh).strip().lower() in ("1", "true", "yes", "y")

    if a.lookback_hours < 0:
        raise ValueError("--lookback-hours должен быть >= 0")
    if a.batch_size_buffers < 1:
        raise ValueError("--batch-size-buffers должен быть >= 1")
    if a.insert_chunk_size < 1:
        raise ValueError("--insert-chunk-size должен быть >= 1")

    return Settings(
        cities=list(a.cities) if a.cities else None,
        source=str(a.source),
        full_refresh=full_refresh,
        lookback_hours=int(a.lookback_hours),
        batch_size_buffers=int(a.batch_size_buffers),
        insert_chunk_size=int(a.insert_chunk_size),
        db_pause_sec=float(a.db_pause_sec),
    )


# ========================
# DB engine
# ========================

def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL не найден в окружении/.env")

    # Общие (безопасные) параметры подключения.
    common_kwargs = dict(
        pool_pre_ping=True,
        pool_recycle=1800,  # защита от обрыва долгих соединений
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )

    # SQLAlchemy + psycopg2 в разных версиях поддерживает разные kwargs для bulk executemany.
    # Поэтому пробуем «ускоренный» режим, а если диалект/версия его не понимает — падаем назад
    # на базовый create_engine.
    candidates = [
        # Самый быстрый и стабильный вариант (если поддерживается вашей версией SQLAlchemy).
        dict(executemany_mode="values_plus_batch", executemany_values_page_size=1000),
        # Чуть менее требовательный вариант (некоторые сборки не знают page_size).
        dict(executemany_mode="values_plus_batch"),
        # Базовый вариант — без специфичных executemany kwargs.
        dict(),
    ]

    last_err: Exception | None = None
    for extra in candidates:
        try:
            return create_engine(db_url, **common_kwargs, **extra)
        except (ArgumentError, TypeError) as e:
            last_err = e
            continue

    # Теоретически не должны сюда попасть, но на всякий случай.
    raise RuntimeError(f"Не удалось создать SQLAlchemy engine: {last_err}")


# ========================
# SQL helpers
# ========================

def fetch_city_ids(engine, city_names: Sequence[str]) -> pd.DataFrame:
    """city -> city_id (и обратно)."""
    sql = f"""
    SELECT city_id, city
    FROM {DIM_CITY_TABLE}
    WHERE city = ANY(:city_names)
    ORDER BY city_id;
    """
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params={"city_names": list(city_names)})


def fetch_city_ids_from_buffer(engine, source: str) -> pd.DataFrame:
    """Все city_id, которые реально есть в weather_buffer по source."""
    sql = f"""
    SELECT DISTINCT city_id, city
    FROM {BUFFER_TABLE}
    WHERE source = :source
    ORDER BY city_id;
    """
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params={"source": source})


def get_watermarks(engine, source: str, city_ids: Sequence[int]) -> Dict[int, datetime]:
    """watermark = max(observed_at) по каждому городу (или epoch, если пусто)."""
    sql = f"""
    SELECT city_id, max(observed_at) AS max_observed_at
    FROM {TARGET_TABLE}
    WHERE source = :source
      AND city_id = ANY(:city_ids)
    GROUP BY city_id;
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params={"source": source, "city_ids": list(city_ids)})

    wm: Dict[int, datetime] = {}
    for _, r in df.iterrows():
        if pd.isna(r["max_observed_at"]):
            continue
        # SQLAlchemy/Pandas обычно уже возвращают tz-aware timestamptz
        wm[int(r["city_id"])] = r["max_observed_at"].to_pydatetime() if hasattr(r["max_observed_at"], "to_pydatetime") else r["max_observed_at"]

    return wm


def truncate_target(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE};"))


def iter_buffer_batches(
    engine,
    source: str,
    city_ids: Sequence[int],
    batch_size: int,
) -> Iterable[pd.DataFrame]:
    """
    Итератор батчей из weather_buffer.

    Важно: сортируем по date_from, чтобы инкрементальность работала предсказуемо.
    """
    offset = 0
    while True:
        sql = f"""
        SELECT buffer_id, source, city_id, city, date_from, date_to, payload, loaded_at, updated_at
        FROM {BUFFER_TABLE}
        WHERE source = :source
          AND city_id = ANY(:city_ids)
        ORDER BY city_id, date_from
        LIMIT :limit OFFSET :offset;
        """
        with engine.begin() as conn:
            df = pd.read_sql(
                text(sql),
                conn,
                params={"source": source, "city_ids": list(city_ids), "limit": batch_size, "offset": offset},
            )
        if df.empty:
            break
        yield df
        offset += len(df)


UPSERT_SQL = f"""
INSERT INTO {TARGET_TABLE} (
    observed_at,
    city_id,
    temperature,
    relative_humidity,
    precipitation,
    rain,
    snowfall,
    cloud_cover,
    visibility,
    wind_speed,
    wind_gusts,
    light_condition,
    local_weather_condition,
    road_surface_condition,
    source,
    updated_at
)
VALUES (
    :observed_at,
    :city_id,
    :temperature,
    :relative_humidity,
    :precipitation,
    :rain,
    :snowfall,
    :cloud_cover,
    :visibility,
    :wind_speed,
    :wind_gusts,
    :light_condition,
    :local_weather_condition,
    :road_surface_condition,
    :source,
    now()
)
ON CONFLICT (source, city_id, observed_at)
DO UPDATE SET
    temperature = EXCLUDED.temperature,
    relative_humidity = EXCLUDED.relative_humidity,
    precipitation = EXCLUDED.precipitation,
    rain = EXCLUDED.rain,
    snowfall = EXCLUDED.snowfall,
    cloud_cover = EXCLUDED.cloud_cover,
    visibility = EXCLUDED.visibility,
    wind_speed = EXCLUDED.wind_speed,
    wind_gusts = EXCLUDED.wind_gusts,
    light_condition = EXCLUDED.light_condition,
    local_weather_condition = EXCLUDED.local_weather_condition,
    road_surface_condition = EXCLUDED.road_surface_condition,
    updated_at = now();
"""


def upsert_many(engine, rows: List[Dict[str, Any]]) -> int:
    """
    Безопасный UPSERT с защитой от слишком больших батчей.
    Делим на внутренние чанки по 1000 строк,
    чтобы не ронять соединение при больших объёмах.
    """
    if not rows:
        return 0

    total = 0
    chunk_size = 1000  # внутренняя защита

    with engine.begin() as conn:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            conn.execute(text(UPSERT_SQL), chunk)
            total += len(chunk)

    return total


# ========================
# Transform (RAW → hourly rows)
# ========================

def _safe_get_hourly(payload: Any) -> Optional[Dict[str, Any]]:
    """
    Пытаемся достать payload["hourly"].

    payload может быть:
    - dict (если драйвер вернул jsonb как python dict)
    - str (если пришло строкой) → пробуем json.loads
    """
    if payload is None:
        return None

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None

    if not isinstance(payload, dict):
        return None

    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return None

    return hourly


def transform_buffer_row_to_hourly(
    buffer_row: Dict[str, Any],
    watermark_utc: datetime,
    lookback_hours: int,
    full_refresh: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Превращает одну строку weather_buffer в список почасовых строк для UPSERT.

    Возвращает:
    - rows: список dict для upsert
    - stats: метрики трансформации
    """
    stats = {
        "bad_json": 0,
        "missing_time": 0,
        "length_mismatch": 0,
        "hours_total": 0,
        "hours_after_watermark": 0,
        "hours_written": 0,
    }

    hourly = _safe_get_hourly(buffer_row.get("payload"))
    if hourly is None:
        stats["bad_json"] = 1
        return [], stats

    times = hourly.get("time")
    if not isinstance(times, list) or len(times) == 0:
        stats["missing_time"] = 1
        return [], stats

    # Мягкая валидация: все нужные поля должны быть списками той же длины (или отсутствовать)
    n = len(times)
    for f in HOURLY_FIELDS:
        arr = hourly.get(f)
        if arr is None:
            continue
        if not isinstance(arr, list) or len(arr) != n:
            stats["length_mismatch"] = 1
            # В случае несходящихся длин лучше пропустить весь row, иначе получим “косые” данные
            return [], stats

    # Парсим время как UTC
    # Open-Meteo присылает iso8601 без tz, но мы запрашивали timezone=UTC => считаем UTC
    dt_series = pd.to_datetime(pd.Series(times), errors="coerce", utc=False)
    # Если нет tz, “прибиваем” UTC
    dt_series = dt_series.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")

    # Watermark с хвостом. При full_refresh фильтрацию по watermark отключаем.
    if full_refresh:
        effective_wm = datetime(1970, 1, 1, tzinfo=timezone.utc)
    else:
        effective_wm = watermark_utc - timedelta(hours=lookback_hours)
        if effective_wm.tzinfo is None:
            effective_wm = effective_wm.replace(tzinfo=timezone.utc)

    stats["hours_total"] = n

    rows: List[Dict[str, Any]] = []

    city_id = int(buffer_row["city_id"])
    source = str(buffer_row["source"])

    # Готовим “колоночные” массивы
    cols: Dict[str, List[Any]] = {}
    for jf in HOURLY_FIELDS:
        cols[jf] = hourly.get(jf, [None] * n)

    for i in range(n):
        observed_at = dt_series.iloc[i]
        if pd.isna(observed_at):
            continue

        # Инкрементальность: пишем только часы после watermark (с хвостом)
        if observed_at <= effective_wm:
            continue

        stats["hours_after_watermark"] += 1

        row = {
            "observed_at": observed_at.to_pydatetime(),  # tz-aware UTC
            "city_id": city_id,
            "source": source,

            # локальные условия (пока не заполняются из weather_buffer,
            # будут заполняться при объединении с dtp_buffer)
            "light_condition": None,
            "local_weather_condition": None,
            "road_surface_condition": None,
        }

        # значения
        for jf, colname in JSON_TO_COL.items():
            val = cols[jf][i] if i < len(cols[jf]) else None
            row[colname] = val

        rows.append(row)

    stats["hours_written"] = len(rows)
    return rows, stats


# ========================
# MAIN
# ========================

def main() -> None:
    logger = setup_logger("weather_conditions_parser_py")
    s = parse_args()
    engine = get_engine()

    logger.info("Старт парсинга %s → %s", BUFFER_TABLE, TARGET_TABLE)
    logger.info("SOURCE=%s | FULL_REFRESH=%s | lookback_hours=%s", s.source, s.full_refresh, s.lookback_hours)
    logger.info("BATCH_SIZE_BUFFERS=%s | INSERT_CHUNK_SIZE=%s", s.batch_size_buffers, s.insert_chunk_size)

    # 1) Города
    if s.cities:
        df_cities = fetch_city_ids(engine, s.cities)
        found_names = set(df_cities["city"].astype(str).tolist())
        missing = sorted(set(s.cities) - found_names)
        if missing:
            logger.warning("Не найдены в dim_city: %s", ", ".join(missing))
    else:
        df_cities = fetch_city_ids_from_buffer(engine, s.source)

    if df_cities.empty:
        logger.warning("Нет городов для обработки (weather_buffer пуст / фильтр cities не нашёл совпадений).")
        return

    city_ids = df_cities["city_id"].astype(int).tolist()
    logger.info("Города к обработке: %s", ", ".join(df_cities["city"].astype(str).tolist()))

    # 2) FULL REFRESH (осторожно)
    if s.full_refresh:
        logger.info("FULL_REFRESH=True: очищаем %s", TARGET_TABLE)
        truncate_target(engine)

    # 3) Watermarks
    watermarks = get_watermarks(engine, s.source, city_ids)
    default_wm = datetime(1970, 1, 1, tzinfo=timezone.utc)

    # 4) Батчи по weather_buffer
    total_buffers = 0
    total_bad_json = 0
    total_len_mismatch = 0
    total_missing_time = 0

    total_hours_total = 0
    total_hours_after_wm = 0
    total_hours_written = 0

    total_upserted = 0

    batch_no = 0
    for df_batch in iter_buffer_batches(engine, s.source, city_ids, s.batch_size_buffers):
        batch_no += 1
        total_buffers += len(df_batch)

        # Накопитель строк для UPSERT (чанкуем, чтобы не держать огромные списки в памяти)
        buffer_rows_to_upsert: List[Dict[str, Any]] = []

        batch_bad_json = 0
        batch_len_mismatch = 0
        batch_missing_time = 0

        batch_hours_total = 0
        batch_hours_after = 0
        batch_hours_written = 0

        # ВАЖНО: чтобы watermark работал правильно, выгодно обновлять его по ходу обработки
        # (на случай если в рамках батча идут годы подряд).
        local_watermarks = dict(watermarks)

        for _, r in df_batch.iterrows():
            br = r.to_dict()
            city_id = int(br["city_id"])
            wm = local_watermarks.get(city_id, default_wm)

            rows, stats = transform_buffer_row_to_hourly(
                buffer_row=br,
                watermark_utc=wm,
                lookback_hours=s.lookback_hours,
                full_refresh=s.full_refresh,
            )

            batch_bad_json += stats["bad_json"]
            batch_len_mismatch += stats["length_mismatch"]
            batch_missing_time += stats["missing_time"]

            batch_hours_total += stats["hours_total"]
            batch_hours_after += stats["hours_after_watermark"]
            batch_hours_written += stats["hours_written"]

            # Обновляем watermark “на лету” до максимального observed_at, чтобы меньше переписывать хвост
            if rows:
                max_observed = max(x["observed_at"] for x in rows if x.get("observed_at") is not None)
                if isinstance(max_observed, datetime):
                    local_watermarks[city_id] = max_observed

            # Добавляем строки в общий буфер и по мере наполнения — upsert чанками
            buffer_rows_to_upsert.extend(rows)

            while len(buffer_rows_to_upsert) >= s.insert_chunk_size:
                chunk = buffer_rows_to_upsert[: s.insert_chunk_size]
                buffer_rows_to_upsert = buffer_rows_to_upsert[s.insert_chunk_size :]

                upserted = upsert_many(engine, chunk)
                total_upserted += upserted

        # Догружаем остатки
        if buffer_rows_to_upsert:
            upserted = upsert_many(engine, buffer_rows_to_upsert)
            total_upserted += upserted

        # Суммарные метрики
        total_bad_json += batch_bad_json
        total_len_mismatch += batch_len_mismatch
        total_missing_time += batch_missing_time

        total_hours_total += batch_hours_total
        total_hours_after_wm += batch_hours_after
        total_hours_written += batch_hours_written

        logger.info(
            "Батч #%s: buffers=%s | hours_total=%s | hours_after_wm=%s | hours_written=%s | bad_json=%s | len_mismatch=%s | missing_time=%s",
            batch_no,
            len(df_batch),
            batch_hours_total,
            batch_hours_after,
            batch_hours_written,
            batch_bad_json,
            batch_len_mismatch,
            batch_missing_time,
        )

        if s.db_pause_sec > 0:
            time.sleep(s.db_pause_sec)

    logger.info(
        "ИТОГ: buffers=%s | attempted_hours=%s | hours_after_wm=%s | upsert_attempted=%s | bad_json=%s | len_mismatch=%s | missing_time=%s",
        total_buffers,
        total_hours_total,
        total_hours_after_wm,
        total_upserted,
        total_bad_json,
        total_len_mismatch,
        total_missing_time,
    )
    logger.info("Готово ✅")


if __name__ == "__main__":
    main()