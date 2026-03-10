"""
ETL-скрипт: загрузка городов РФ в базу данных Supabase (PostgreSQL)

Что делает скрипт:
1) Забирает список городов РФ с Википедии (парсинг HTML-таблицы).
2) Чистит данные (убирает сноски [1], население -> int).
3) Сначала заливает "справочник городов" в public.dim_city (UPSERT).
4) Затем геокодит ТОЛЬКО те города, у которых lat/lon ещё пустые.
5) Обновляет координаты в public.dim_city (UPSERT).
"""

# ========================
# Импорты библиотек
# ========================

import os
import re
import requests
import pandas as pd
from pathlib import Path

from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from sqlalchemy import create_engine, text


# ========================
# Константы
# ========================

# Страница Википедии со списком городов РФ
WIKI_URL = "https://ru.wikipedia.org/wiki/Список_городов_России"

# Ограничение отключено
CITY_LIMIT = None

# Как часто печатать прогресс геокодинга
PROGRESS_EVERY = 25

# Минимальная пауза между запросами к Nominatim
GEOCODE_DELAY_SECONDS = 1


# ========================
# Подключение к БД
# ========================

def get_engine():
    """
    Создаёт подключение к Supabase Postgres.

    - Читает DATABASE_URL из .env
    - Возвращает SQLAlchemy engine
    """
    base_dir = Path(__file__).resolve().parent.parent
    load_dotenv(base_dir / ".env")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("В .env не найдена переменная DATABASE_URL")

    # pool_pre_ping=True — проверка доступности соединения 
    # перед выполнением запросов

    return create_engine(db_url, pool_pre_ping=True)


def fetch_cities_without_coords(engine) -> pd.DataFrame:
    """
    Берёт из БД города, у которых нет координат (lat или lon NULL).
    Возвращаем только те поля, которые нужны для геокодинга и UPSERT.
    """
    sql = """
    SELECT city, region, federal_district, population
    FROM public.dim_city
    WHERE lat IS NULL OR lon IS NULL
    ORDER BY city_id;
    """
    with engine.begin() as conn:
        return pd.read_sql(sql, conn)


# ========================
# Вспомогательные функции
# ========================

def clean_population(value):
    """
    Приводит население к числу:
    - убирает сноски [1], пробелы, текст
    - оставляет только цифры
    - возвращает int или None
    """
    if pd.isna(value):
        return None

    s = str(value)
    s = re.sub(r"\[\d+\]", "", s)   # убираем [1], [2] и т.п.
    s = re.sub(r"[^\d]", "", s)     # оставляем только цифры

    return int(s) if s else None


# ========================
# Парсинг Википедии
# ========================

def parse_wiki_cities() -> pd.DataFrame:
    """
    Парсит таблицу городов РФ с Википедии и
    возвращает DataFrame с колонками:
    city, region, federal_district, population
    """
    headers = {"User-Agent": "Mozilla/5.0 (WeatherAccidentsAnalysis)"}
    response = requests.get(WIKI_URL, headers=headers, timeout=30)
    response.raise_for_status()
    html = response.text

    # pandas сам находит все HTML-таблицы на странице
    tables = pd.read_html(html)

    # Берём самую большую таблицу (обычно это и есть список городов)
    df = max(tables, key=len)

    # Приводим названия колонок к нижнему регистру
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Ищем нужные колонки по смыслу
    col_city = next(c for c in df.columns if "город" in c)
    col_region = next(c for c in df.columns if "субъект" in c or "регион" in c)
    col_fd = next(c for c in df.columns if "федераль" in c)
    col_pop = next((c for c in df.columns if "насел" in c), None)

    # Собираем датафрейм
    result = pd.DataFrame({
        "city": df[col_city].astype(str).str.replace(r"\[\d+\]", "", regex=True).str.strip(),
        "region": df[col_region].astype(str).str.strip(),
        "federal_district": df[col_fd].astype(str).str.strip(),
        "population": df[col_pop].apply(clean_population) if col_pop else None,
    })

    # Убираем пустые строки
    result = result[(result["city"] != "") & (result["region"] != "")]

    # На всякий случай убираем дубликаты (по ключу (city, region))
    result = result.drop_duplicates(subset=["city", "region"]).reset_index(drop=True)

    return result


# ========================
# Геокодинг
# ========================

def geocode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет координаты (lat, lon) к городам.

    Используется Nominatim (OpenStreetMap).
    RateLimiter ограничивает запросы (по умолчанию 1 запрос/сек).
    """
    geolocator = Nominatim(user_agent="weather_accidents_analysis")
    geocode_limited = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=GEOCODE_DELAY_SECONDS
    )

    lats, lons, sources = [], [], []

    for i, row in df.iterrows():
        # Чем точнее запрос — тем выше шанс найти нужный город
        query = f"{row['city']}, {row['region']}, Russia"

        try:
            location = geocode_limited(query, timeout=15)
            if location:
                lats.append(float(location.latitude))
                lons.append(float(location.longitude))
                sources.append("nominatim")
            else:
                lats.append(None)
                lons.append(None)
                sources.append("not_found")
        except Exception:
            lats.append(None)
            lons.append(None)
            sources.append("error")

        # Печатаем прогресс, чтобы было видно что он есть
        if (i + 1) % PROGRESS_EVERY == 0:
            print(f"Геокодинг: {i + 1}/{len(df)}")

    df["lat"] = lats
    df["lon"] = lons
    df["coord_source"] = sources

    return df


# ========================
# SQL для UPSERT
# ========================

UPSERT_SQL = """
INSERT INTO public.dim_city
(city, region, federal_district, population, lat, lon, coord_source)
VALUES
(:city, :region, :federal_district, :population, :lat, :lon, :coord_source)
ON CONFLICT (city, region)
DO UPDATE SET
    federal_district = EXCLUDED.federal_district,
    population = EXCLUDED.population,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    coord_source = EXCLUDED.coord_source,
    updated_at = now();
"""


def upsert_dim_city(engine, df: pd.DataFrame):
    """
    Загружает города в таблицу dim_city.

    - Если (city, region) нет → INSERT
    - Если есть → UPDATE
    """
    records = df.to_dict(orient="records")

    # begin() открывает транзакцию и коммитит её автоматически
    with engine.begin() as conn:
        for row in records:
            conn.execute(text(UPSERT_SQL), row)


# ========================
# Главная функция (ETL)
# ========================

def main():
    engine = get_engine()

    print("1) Загружаем города с Википедии...")
    df = parse_wiki_cities()
    print(f"Найдено городов: {len(df)}")

    if CITY_LIMIT:
        df = df.head(CITY_LIMIT)
        print(f"Берём первые {CITY_LIMIT} городов")

    print("2) Записываем города в Supabase ...")
    # Ставим пустые координаты — города попадут в БД как справочник.
    # Координаты на следующем шаге.
    df["lat"] = None
    df["lon"] = None
    df["coord_source"] = "pending"
    upsert_dim_city(engine, df)

    print("3) Берём из БД города без координат и геокодим их...")
    df_missing = fetch_cities_without_coords(engine)
    print(f"Нужно догеокодить: {len(df_missing)}")

    if len(df_missing) == 0:
        print("Готово ✅ Все города уже с координатами")
        return

    df_missing = geocode(df_missing)

    print("4) Обновляем координаты в dim_city...")
    upsert_dim_city(engine, df_missing)

    ok = df_missing["lat"].notna().sum()
    print(f"Готово ✅ Координаты обновлены. Найдено координат: {ok}/{len(df_missing)}")


if __name__ == "__main__":
    main()



