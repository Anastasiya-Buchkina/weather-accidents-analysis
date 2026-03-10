"""
ETL: Загрузка карточек ДТП из `public.dtp_buffer` в `public.dtp_main` (Supabase/Postgres).

Назначение
- Делает витрину `dtp_main` (1 карточка ДТП = 1 строка) из буфера `dtp_buffer`.
- Скрипт рассчитан на автоматизацию (cron / GitHub Actions / Airflow / любой scheduler):
  * управление через переменные окружения
  * идемпотентность (дубли по kart_id не вставятся)
  * понятные логи
  * предсказуемое завершение (ошибка = ненулевой exit)

Режимы
- INCREMENTAL (по умолчанию): безопасная догрузка новых буферов.
- FULL_REFRESH=True: очищает `dtp_main` и пересобирает витрину.

Пагинация
- Используем keyset pagination по `buffer_id` (а не OFFSET), чтобы было стабильно при росте данных.

Переменные окружения
- DATABASE_URL                 (обязательно) строка подключения Postgres
- DTP_CITIES                   (опционально) "Кемерово,Ставрополь"
- DTP_BATCH_SIZE               (опционально) размер батча по buffer-строкам
- DTP_ONLY_UNPROCESSED         (опционально) true/false — брать только не processed
- DTP_MARK_PROCESSED           (опционально) true/false — помечать обработанные buffer
- DTP_FULL_REFRESH             (опционально) true/false — полная пересборка
- DTP_DRY_RUN                  (опционально) true/false — без вставки (только логи/подсчёты)
- DTP_RUN_CHECKS               (опционально) true/false — запуск проверок качества после загрузки

Примечание
- Координаты (lat/lon) и условия дороги/освещения НЕ грузим в dtp_main — их лучше держать в dtp_locations.
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load .env early so SETTINGS reads project environment variables correctly
BASE_DIR = Path(__file__).resolve().parent.parent  # <project_root>
load_dotenv(BASE_DIR / ".env")


# =========================
# Config
# =========================

def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError as e:
        raise RuntimeError(f"Env {name} must be int") from e


def _validate_positive(name: str, value: int) -> int:
    if value < 1:
        raise RuntimeError(f"Env {name} must be >= 1")
    return value


@dataclass(frozen=True)
class Settings:
    cities: tuple[str, ...]
    batch_size_buffers: int
    only_unprocessed_buffers: bool
    mark_buffer_processed: bool
    full_refresh: bool
    dry_run: bool
    run_checks: bool


SETTINGS = Settings(
    cities=tuple(filter(None, (c.strip() for c in os.getenv("DTP_CITIES", "Кемерово,Ставрополь").split(",")))),
    batch_size_buffers=_validate_positive("DTP_BATCH_SIZE", _env_int("DTP_BATCH_SIZE", "50")),
    only_unprocessed_buffers=_env_bool("DTP_ONLY_UNPROCESSED", "false"),
    mark_buffer_processed=_env_bool("DTP_MARK_PROCESSED", "false"),
    full_refresh=_env_bool("DTP_FULL_REFRESH", "false"),
    dry_run=_env_bool("DTP_DRY_RUN", "false"),
    run_checks=_env_bool("DTP_RUN_CHECKS", "true"),
)


# =========================
# Logging
# =========================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("dtp_main_loader")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(console)
    return logger


logger = setup_logging()


# =========================
# DB
# =========================

def get_engine() -> Engine:
    """Create SQLAlchemy Engine reading DATABASE_URL from project .env."""

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Нет DATABASE_URL в .env (в корне проекта)")

    return create_engine(db_url, pool_pre_ping=True)


# =========================
# SQL
# =========================

SQL_GET_BUFFER_IDS_KEYSET = """
select b.buffer_id
from public.dtp_buffer b
where b.city = any(:cities)
  and jsonb_typeof(b.raw_json->'tab') = 'array'
  and jsonb_array_length(b.raw_json->'tab') > 0
  and b.buffer_id > :last_buffer_id
  {processed_filter}
order by b.buffer_id
limit :limit
"""

SQL_TRUNCATE_MAIN = """
truncate table public.dtp_main;
"""

SQL_INSERT_MAIN_FROM_BUFFER_IDS = """
with src as (
    select
        b.buffer_id,
        b.city,
        c.city_id,
        jsonb_array_elements(b.raw_json->'tab') as card
    from public.dtp_buffer b
    join public.dim_city c
      on c.city = b.city
    where b.buffer_id = any(:buffer_ids)
),
prepared as (
    select
        buffer_id,
        (card->>'KartId')::bigint as kart_id,
        city_id,

        to_date(card->>'date', 'DD.MM.YYYY') as dtp_date,
        nullif(card->>'Time', '')::time as dtp_time,

        nullif(card->>'DTP_V', '') as dtp_type,
        nullif(card->>'District', '') as district,

        nullif(card->>'POG','')::int as pog,
        nullif(card->>'RAN','')::int as ran,
        nullif(card->>'K_TS','')::int as k_ts,
        nullif(card->>'K_UCH','')::int as k_uch,
        nullif(card->>'rowNum','')::int as row_num
    from src
    where (card ? 'KartId')
)
insert into public.dtp_main (
    buffer_id,
    kart_id,
    city_id,
    dtp_date,
    dtp_time,
    dtp_type,
    district,
    pog,
    ran,
    k_ts,
    k_uch,
    row_num,
    loaded_at
)
select
    buffer_id,
    kart_id,
    city_id,
    dtp_date,
    dtp_time,
    dtp_type,
    district,
    pog,
    ran,
    k_ts,
    k_uch,
    row_num,
    now()
from prepared
on conflict (kart_id) do nothing;
"""

SQL_MARK_BUFFERS_PROCESSED = """
update public.dtp_buffer
set processed = true,
    processed_at = now()
where buffer_id = any(:buffer_ids)
  and processed is distinct from true;
"""

# -------------------------
# Quality checks (optional)
# -------------------------
SQL_CHECK_DUPLICATE_KART_ID = """
select count(*) as duplicate_groups
from (
    select kart_id
    from public.dtp_main
    group by kart_id
    having count(*) > 1
) t;
"""

SQL_CHECK_NULL_KART_ID = """
select count(*) as kart_id_nulls
from public.dtp_main
where kart_id is null;
"""


# =========================
# Helpers
# =========================

def _build_processed_filter(only_unprocessed: bool) -> str:
    if not only_unprocessed:
        return ""
    return "and coalesce(b.processed, false) = false"


def fetch_buffer_ids_keyset(
    conn,
    cities: Sequence[str],
    limit: int,
    last_buffer_id: int,
    only_unprocessed: bool,
) -> list[int]:
    processed_filter = _build_processed_filter(only_unprocessed)
    sql = SQL_GET_BUFFER_IDS_KEYSET.format(processed_filter=processed_filter)

    return (
        conn.execute(
            text(sql),
            {
                "cities": list(cities),
                "limit": limit,
                "last_buffer_id": last_buffer_id,
            },
        )
        .scalars()
        .all()
    )


def run_checks(conn) -> None:
    dup = conn.execute(text(SQL_CHECK_DUPLICATE_KART_ID)).mappings().one()["duplicate_groups"]
    nulls = conn.execute(text(SQL_CHECK_NULL_KART_ID)).mappings().one()["kart_id_nulls"]

    if dup != 0:
        raise RuntimeError(f"QA failed: duplicate kart_id groups={dup}")
    if nulls != 0:
        raise RuntimeError(f"QA failed: kart_id NULL rows={nulls}")


# =========================
# Main
# =========================

def main() -> None:
    if not SETTINGS.cities:
        raise RuntimeError("Список городов пуст. Проверь DTP_CITIES")

    engine = get_engine()

    logger.info(
        "Старт загрузки dtp_main из dtp_buffer. Города: %s",
        ", ".join(SETTINGS.cities),
    )
    logger.info(
        "BATCH=%s | ONLY_UNPROCESSED=%s | MARK_PROCESSED=%s | FULL_REFRESH=%s | DRY_RUN=%s | RUN_CHECKS=%s",
        SETTINGS.batch_size_buffers,
        SETTINGS.only_unprocessed_buffers,
        SETTINGS.mark_buffer_processed,
        SETTINGS.full_refresh,
        SETTINGS.dry_run,
        SETTINGS.run_checks,
    )

    # 1) FULL REFRESH
    with engine.begin() as conn:
        if SETTINGS.full_refresh:
            logger.info("FULL_REFRESH=True: очищаем public.dtp_main перед загрузкой")
            if not SETTINGS.dry_run:
                conn.execute(text(SQL_TRUNCATE_MAIN))
            else:
                logger.info("DRY_RUN=True: truncate пропущен")

    # 2) Keyset loop
    last_buffer_id = 0
    batch_no = 0
    total_buffers = 0

    while True:
        batch_no += 1

        with engine.begin() as conn:
            buffer_ids = fetch_buffer_ids_keyset(
                conn=conn,
                cities=SETTINGS.cities,
                limit=SETTINGS.batch_size_buffers,
                last_buffer_id=last_buffer_id,
                only_unprocessed=SETTINGS.only_unprocessed_buffers,
            )

        if not buffer_ids:
            break

        # advance cursor
        last_buffer_id = buffer_ids[-1]
        total_buffers += len(buffer_ids)

        if SETTINGS.dry_run:
            logger.info(
                "DRY_RUN батч #%s: нашли buffer_id %s..%s (шт=%s) — insert пропущен",
                batch_no,
                buffer_ids[0],
                buffer_ids[-1],
                len(buffer_ids),
            )
            continue

        with engine.begin() as conn:
            conn.execute(text(SQL_INSERT_MAIN_FROM_BUFFER_IDS), {"buffer_ids": list(buffer_ids)})

            if SETTINGS.mark_buffer_processed:
                conn.execute(text(SQL_MARK_BUFFERS_PROCESSED), {"buffer_ids": list(buffer_ids)})

        logger.info(
            "Батч #%s: обработали buffer_id %s..%s (шт=%s)",
            batch_no,
            buffer_ids[0],
            buffer_ids[-1],
            len(buffer_ids),
        )

    # 3) QA checks
    if not SETTINGS.dry_run and SETTINGS.run_checks:
        with engine.begin() as conn:
            run_checks(conn)
        logger.info("QA checks: OK")

    logger.info(
        "Готово ✅ (buffers processed=%s). Повторный запуск безопасен: дубли по kart_id не вставятся.",
        total_buffers,
    )


if __name__ == "__main__":
    main()