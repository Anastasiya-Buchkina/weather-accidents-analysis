"""load_weather_buffer.py

Шаг 1/2 ETL: загрузка почасовой погоды Open‑Meteo (Archive API) в public.weather_buffer как RAW JSONB.

Что делает скрипт
1) Берёт список городов из `public.dim_city` (city_id, city, lat, lon).
2) Для каждого города определяет, какие годы уже загружены в `public.weather_buffer`.
3) Загружает данные из Open‑Meteo по годовым диапазонам и пишет 1 запись = 1 год.
4) Использует UPSERT по ключу (source, city_id, date_from, date_to).

Инкрементальность
- «Закрытые» годы (уже есть в БД) — пропускаем.
- Последние N лет (по умолчанию N=1, т.е. текущий год) — перезагружаем каждый запуск,
  чтобы при ежемесячных обновлениях подтягивались новые часы без ручных чисток.

Период по умолчанию
- 2014‑01‑01 .. сегодня

Примеры запуска
- Все города по умолчанию:
  python src/load_weather_buffer.py

- Явно указать города:
  python src/load_weather_buffer.py --cities "Ставрополь" "Кемерово"

- Задать период и зону refresh:
  python src/load_weather_buffer.py --start-date 2014-01-01 --end-date 2026-03-03 --lookback-years 1
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ========================
# Константы и метаданные источника
# ========================

# Open‑Meteo Archive API (исторические почасовые данные)
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Поля, которые запрашиваем из API (hourly)
HOURLY_VARS: List[str] = [
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

# Метки для хранения в буфере
SOURCE_NAME = "open_meteo_archive"
TABLE_NAME = "public.weather_buffer"


# ========================
# Логирование
# ========================

def setup_logger(name: str = "weather_buffer_loader") -> logging.Logger:
    """Создаёт консольный logger с единым форматом.

    Важно: функция идемпотентна — при повторных вызовах не добавляет новые handlers.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ========================
# Настройки
# ========================

@dataclass(frozen=True)
class Settings:
    """Параметры запуска.

    lookback_years:
      Сколько последних лет (включая текущий) обновлять на каждом запуске.
      Для ежемесячного расписания обычно достаточно 1.
    """
    cities: List[str]
    start_date: date
    end_date: date
    lookback_years: int


def parse_args() -> Settings:
    today = date.today()

    p = argparse.ArgumentParser()
    p.add_argument(
        "--cities",
        nargs="+",
        default=["Ставрополь", "Кемерово"],
        help="Список городов из dim_city (точное совпадение по столбцу city).",
    )
    p.add_argument("--start-date", default="2014-01-01", help="Дата начала периода (YYYY-MM-DD).")
    p.add_argument("--end-date", default=today.isoformat(), help="Дата конца периода включительно (YYYY-MM-DD). По умолчанию сегодня.")
    p.add_argument(
        "--lookback-years",
        type=int,
        default=1,
        help="Сколько последних лет перезагружать при каждом запуске (по умолчанию 1 = текущий год)",
    )

    a = p.parse_args()

    start_date = datetime.strptime(a.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(a.end_date, "%Y-%m-%d").date()

    if start_date > end_date:
        raise ValueError("--start-date не может быть позже --end-date")

    lookback_years = int(a.lookback_years)
    if lookback_years < 1:
        raise ValueError("--lookback-years должен быть >= 1")

    return Settings(
        cities=list(a.cities),
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
    )


# ========================
# Подключение
# ========================

def get_engine():
    base_dir = Path(__file__).resolve().parent.parent
    load_dotenv(base_dir / ".env")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL не найден в окружении/.env")

    return create_engine(db_url, pool_pre_ping=True)


# ========================
# Города
# ========================

def fetch_cities(engine, city_names: Sequence[str]) -> pd.DataFrame:
    """Возвращает города из dim_city с координатами.

    Если город указан в аргументах, но отсутствует в dim_city или без lat/lon — он не попадёт в выборку.
    """
    sql = """
    SELECT city_id, city, lat, lon
    FROM public.dim_city
    WHERE city = ANY(:city_names)
      AND lat IS NOT NULL AND lon IS NOT NULL
    ORDER BY city_id;
    """
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params={"city_names": list(city_names)})


# ========================
# Проверка существующих годов
# ========================

def existing_years(engine, city_id: int) -> set[int]:
    """Какие годы уже присутствуют в буфере для данного города и источника."""
    sql = f"""
    SELECT DISTINCT EXTRACT(YEAR FROM date_from)::int AS year
    FROM {TABLE_NAME}
    WHERE city_id = :city_id
      AND source = :source;
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params={"city_id": city_id, "source": SOURCE_NAME})
    return set(df["year"].tolist())


# ========================
# Диапазоны
# ========================

def iter_year_ranges(start: date, end: date) -> Iterable[Tuple[int, date, date]]:
    """Генератор годовых диапазонов внутри [start, end] включительно."""
    y = start.year
    while y <= end.year:
        s = date(y, 1, 1)
        e = date(y, 12, 31)
        if y == start.year:
            s = start
        if y == end.year:
            e = end
        yield y, s, e
        y += 1


# ========================
# API
# ========================

def fetch_open_meteo_hourly(lat: float, lon: float, start: date, end: date) -> Dict[str, Any]:
    """Запрашивает почасовую погоду из Open‑Meteо Archive API.

    Возвращает JSON-ответ API как dict (timezone=UTC).
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
        "timeformat": "iso8601",
    }

    resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


# ========================
# UPSERT
# ========================

# Важно: ON CONFLICT требует UNIQUE/PK в БД по (source, city_id, date_from, date_to).
UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    source,
    city_id,
    city,
    date_from,
    date_to,
    payload,
    loaded_at,
    updated_at
)
VALUES (
    :source,
    :city_id,
    :city,
    :date_from,
    :date_to,
    CAST(:payload AS jsonb),
    now(),
    now()
)
ON CONFLICT (source, city_id, date_from, date_to)
DO UPDATE SET
    payload = EXCLUDED.payload,
    updated_at = now();
"""


def upsert(engine, record: Dict[str, Any]) -> None:
    """Записывает 1 запись в weather_buffer (UPSERT)."""
    record = dict(record)  # не мутируем входной dict
    record["payload"] = json.dumps(record["payload"], ensure_ascii=False)
    with engine.begin() as conn:
        conn.execute(text(UPSERT_SQL), record)


# ========================
# MAIN
# ========================

def main() -> None:
    logger = setup_logger("weather_buffer_loader_py")
    s = parse_args()
    engine = get_engine()

    logger.info("Старт загрузки %s", TABLE_NAME)
    logger.info("Источник: %s", SOURCE_NAME)
    logger.info("Города (запрос): %s", ", ".join(s.cities))
    logger.info("Период: %s .. %s", s.start_date, s.end_date)

    # Последние N лет перезагружаем каждый запуск (решение проблемы текущего года)
    refresh_from_year = s.end_date.year - s.lookback_years + 1
    refresh_years = set(range(refresh_from_year, s.end_date.year + 1))
    logger.info("Ежезапускное обновление лет: %s", ", ".join(map(str, sorted(refresh_years))))

    cities = fetch_cities(engine, s.cities)

    found = set(cities["city"].astype(str).tolist())
    requested = set(s.cities)
    missing = sorted(requested - found)
    if missing:
        logger.warning("Не найдены в dim_city (или нет координат): %s", ", ".join(missing))

    if cities.empty:
        logger.warning("Нет городов для загрузки: проверь список --cities и наличие lat/lon в dim_city")
        return

    total_attempted = 0
    total_loaded = 0
    total_skipped = 0

    for _, c in cities.iterrows():
        city_id = int(c["city_id"])
        city = str(c["city"])
        lat = float(c["lat"])
        lon = float(c["lon"])

        logger.info("===== Город: %s =====", city)

        years_in_db = existing_years(engine, city_id)

        city_attempted = 0
        city_loaded = 0
        city_skipped = 0

        for year, start, end in iter_year_ranges(s.start_date, s.end_date):
            city_attempted += 1
            total_attempted += 1

            # Пропускаем только «закрытые» годы. Текущий/последние N лет обновляем каждый запуск.
            if year in years_in_db and year not in refresh_years:
                logger.info("[%s] Пропуск — уже есть в БД (закрытый год)", year)
                city_skipped += 1
                total_skipped += 1
                continue

            if year in years_in_db and year in refresh_years:
                logger.info("[%s] Обновление — год в зоне refresh (upsert)", year)

            logger.info("[%s] Загрузка %s..%s", year, start, end)

            payload = fetch_open_meteo_hourly(lat, lon, start, end)

            upsert(
                engine,
                {
                    "source": SOURCE_NAME,
                    "city_id": city_id,
                    "city": city,
                    "date_from": start.isoformat(),
                    "date_to": end.isoformat(),
                    "payload": payload,
                },
            )

            city_loaded += 1
            total_loaded += 1

            # Небольшая пауза между запросами к API (защита от rate limit)
            time.sleep(0.5)

        logger.info(
            "ИТОГ по городу %s | attempted=%s | loaded=%s | skipped=%s",
            city,
            city_attempted,
            city_loaded,
            city_skipped,
        )

    logger.info(
        "ОБЩИЙ ИТОГ | attempted=%s | loaded=%s | skipped=%s",
        total_attempted,
        total_loaded,
        total_skipped,
    )
    logger.info("Готово ✅")


if __name__ == "__main__":
    main()