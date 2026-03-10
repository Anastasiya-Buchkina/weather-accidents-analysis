"""
ETL: обогащение public.dim_city кодами ОКАТО/ОКТМО через DaData

Что делает:
1) Подключается к Supabase Postgres (DATABASE_URL из .env)
2) Берёт города, у которых okato или oktmo ещё не заполнены
3) Для каждого города отправляет запрос в DaData (suggest/address)
4) Забирает okato/oktmo (+ fias_id/kladr_id) и обновляет dim_city
5) Сохраняет сырой ответ DaData в колонку dadata_raw (jsonb)
"""

import os
import json
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# =========================
# Настройки
# =========================

DADATA_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address"

# Пауза между запросами (чтобы не превысить лимиты)
REQUEST_DELAY_SEC = 0.25

# Сколько городов обрабатывать за запуск (None = все)
LIMIT = None

# Если True — пишем сырой ответ в dim_city.dadata_raw
SAVE_RAW_TO_DB = True


# =========================
# Логи
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# =========================
# Подключение к БД
# =========================

def get_engine():
    """
    Подключение к Supabase Postgres.
    """
    base_dir = Path(__file__).resolve().parent.parent  # <project_root>
    load_dotenv(base_dir / ".env")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Нет DATABASE_URL в .env (в корне проекта)")

    return create_engine(db_url, pool_pre_ping=True)


def fetch_cities_to_enrich(engine):
    """
    Берём города, которым нужно проставить okato/oktmo.
    """
    sql = """
    select city_id, city, region
    from public.dim_city
    where okato is null or oktmo is null
    order by city_id
    """
    if LIMIT:
        sql += " limit :limit"

    with engine.begin() as conn:
        if LIMIT:
            rows = conn.execute(text(sql), {"limit": LIMIT}).mappings().all()
        else:
            rows = conn.execute(text(sql)).mappings().all()
    return rows


def update_city_codes(
    engine,
    city_id: int,
    okato: Optional[str],
    oktmo: Optional[str],
    fias_id: Optional[str],
    kladr_id: Optional[str],
    raw: Optional[Dict[str, Any]],
):
    """
    Обновляем dim_city для конкретного city_id.
    """
    sql = """
    update public.dim_city
    set
        okato = coalesce(:okato, okato),
        oktmo = coalesce(:oktmo, oktmo),
        dadata_fias_id = coalesce(:fias_id, dadata_fias_id),
        dadata_kladr_id = coalesce(:kladr_id, dadata_kladr_id),
        dadata_raw = case
            when :save_raw then coalesce(cast(:raw as jsonb), dadata_raw)
            else dadata_raw
        end,
        updated_at = now()
    where city_id = :city_id
    """
    params = {
        "city_id": city_id,
        "okato": okato,
        "oktmo": oktmo,
        "fias_id": fias_id,
        "kladr_id": kladr_id,
        "raw": json.dumps(raw, ensure_ascii=False) if raw else None,
        "save_raw": SAVE_RAW_TO_DB,
    }
    with engine.begin() as conn:
        conn.execute(text(sql), params)


# =========================
# DaData client
# =========================

def get_dadata_headers() -> Dict[str, str]:
    """
    Заголовки для DaData: Token + Secret.
    """
    api_key = os.getenv("DADATA_API_KEY")
    secret = os.getenv("DADATA_SECRET_KEY")

    if not api_key or not secret:
        raise RuntimeError("В .env нет DADATA_API_KEY или DADATA_SECRET_KEY")

    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {api_key}",
        "X-Secret": secret,
        "User-Agent": "dtp_weather_project/1.0",
    }


def dadata_lookup_city(city: str, region: str, headers: Dict[str, str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Ищем город в DaData.
    Возвращаем:
      - best_suggestion (dict) или None
      - debug_text (если нужно понять, почему не нашлось)
    """

    # Запрос делаем максимально точным: "город, регион, Россия"
    query = f"{city}, {region}, Россия"

    payload = {
        "query": query,
        "count": 1,
        # ограничим уровень именно "city", чтобы не вернуло улицу/район
        "from_bound": {"value": "city"},
        "to_bound": {"value": "city"},
    }

    try:
        r = requests.post(DADATA_URL, headers=headers, json=payload, timeout=30)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}: {r.text[:200]}"

        data = r.json()
        suggestions = data.get("suggestions", [])
        if not suggestions:
            return None, "suggestions пустые"

        return suggestions[0], None

    except Exception as e:
        return None, f"exception: {repr(e)}"


def extract_codes_from_suggestion(s: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Извлекаем okato/oktmo + идентификаторы.
    """
    d = s.get("data", {}) or {}
    okato = d.get("okato")
    oktmo = d.get("oktmo")
    fias_id = d.get("fias_id")
    kladr_id = d.get("kladr_id")
    return okato, oktmo, fias_id, kladr_id


# =========================
# Main
# =========================

def main():
    engine = get_engine()
    headers = get_dadata_headers()

    cities = fetch_cities_to_enrich(engine)
    logger.info("Городов к обогащению: %s", len(cities))

    ok_cnt = 0
    miss_cnt = 0

    for i, row in enumerate(cities, 1):
        city_id = int(row["city_id"])
        city = row["city"]
        region = row["region"]

        suggestion, debug = dadata_lookup_city(city, region, headers)

        if not suggestion:
            miss_cnt += 1
            logger.warning("[%s/%s] %s (%s) | НЕ НАЙДЕНО: %s", i, len(cities), city, region, debug)
            time.sleep(REQUEST_DELAY_SEC)
            continue

        okato, oktmo, fias_id, kladr_id = extract_codes_from_suggestion(suggestion)

        update_city_codes(
            engine=engine,
            city_id=city_id,
            okato=okato,
            oktmo=oktmo,
            fias_id=fias_id,
            kladr_id=kladr_id,
            raw=suggestion,  # сырой best-match
        )

        ok_cnt += 1
        logger.info(
            "[%s/%s] %s (%s) | okato=%s oktmo=%s",
            i, len(cities), city, region, okato, oktmo
        )

        time.sleep(REQUEST_DELAY_SEC)

    logger.info("Готово ✅ Обновлено: %s | Не найдено: %s", ok_cnt, miss_cnt)


if __name__ == "__main__":
    main()
