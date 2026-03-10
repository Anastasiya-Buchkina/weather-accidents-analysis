""" 
ETL: Загрузка транспортных средств (ТС) из public.dtp_buffer в public.dtp_vehicles (Supabase/Postgres).

Назначение
---------
Скрипт извлекает массив карточек ДТП из `dtp_buffer.raw_json->'tab'`, берёт из каждой карточки
`infoDtp.ts_info[]` (ТС) и вставляет по одной строке на каждое ТС в `public.dtp_vehicles`.

Ключевые принципы
-----------------
1) Привязка к ДТП: `kart_id` (card.KartId) -> `public.dtp_main.kart_id`.
   Поэтому загружаем только те ТС, для которых уже есть запись в `dtp_main`.
2) Идемпотентность: UNIQUE (kart_id, vehicle_num) + ON CONFLICT DO NOTHING.
   Повторный запуск безопасен: дубли не вставятся.
3) Нормализации/справочники:
   - drive_type: нормализуем lower+trim и маппим через `public.dim_drive_type` -> `drive_type_std`.
   - vehicle_class: нормализуем lower+trim и маппим через `public.dim_vehicle_class_group` -> `vehicle_type_group`.
   - stolen_or_hidden: нормализуем lower+trim и маппим через `public.dim_vehicle_escape_status` -> `vehicle_escape_group`.
   - model_normal: lower+trim + схлопывание пробелов.
4) Флаг тех. неисправностей (technical_issue_flag):
   - если technical_state == "технические неисправности отсутствуют" -> "Нет неисправностей"
   - иначе -> "Есть неисправности"

Колонки назначения (public.dtp_vehicles)
---------------------------------------
Порядок колонок в таблице:
vehicle_id, kart_id, vehicle_num, brand, model, model_normal, year, color,
vehicle_class, vehicle_type_group, drive_type, drive_type_std,
technical_state, technical_issue_flag, stolen_or_hidden, vehicle_escape_group,
ownership, owner_type, loaded_at

Запуск
------
python src/load_dtp_vehicles.py

Для автоматизации:
- параметры можно задавать через переменные окружения (см. Config ниже)
- повторные запуски безопасны из-за ON CONFLICT DO NOTHING
- DRY_RUN=true позволяет прогнать отбор батчей без записи в БД
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Загружаем .env сразу при старте модуля
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


# =========================
# Конфигурация
# =========================
@dataclass(frozen=True)
class Config:
    """Параметры запуска (под автоматизацию).

    Значения по умолчанию можно переопределять через переменные окружения:
    - CITIES: строка с городами через запятую (например: "Кемерово,Ставрополь")
    - BATCH_SIZE_BUFFERS: размер батча буферов (например: "50")
    - ONLY_UNPROCESSED_BUFFERS: "true"/"false"
    - MARK_BUFFER_PROCESSED: "true"/"false"
    - DRY_RUN: "true"/"false" (если true — ничего не пишем в БД, только логируем)
    """

    cities: tuple[str, ...] = ("Кемерово", "Ставрополь")
    batch_size_buffers: int = 50
    only_unprocessed_buffers: bool = False
    mark_buffer_processed: bool = False
    dry_run: bool = False

    default_drive_type_std: str = "Не указано"
    default_vehicle_type_group: str = "Прочие/неизвестные"
    default_escape_group: str = "Прочее / неизвестно"

    tech_state_no_issues: str = "технические неисправности отсутствуют"
    flag_no_issues: str = "Нет неисправностей"
    flag_has_issues: str = "Есть неисправности"


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config_from_env() -> Config:
    cities_raw = os.getenv("CITIES")
    cities = (
        tuple([c.strip() for c in cities_raw.split(",") if c.strip()])
        if cities_raw
        else Config().cities
    )
    if not cities:
        raise ValueError("Список городов пуст. Проверь переменную CITIES")

    batch_size = int(os.getenv("BATCH_SIZE_BUFFERS", str(Config().batch_size_buffers)))
    if batch_size < 1:
        raise ValueError("BATCH_SIZE_BUFFERS должен быть >= 1")

    only_unprocessed = _parse_bool(os.getenv("ONLY_UNPROCESSED_BUFFERS"), Config().only_unprocessed_buffers)
    mark_processed = _parse_bool(os.getenv("MARK_BUFFER_PROCESSED"), Config().mark_buffer_processed)
    dry_run = _parse_bool(os.getenv("DRY_RUN"), Config().dry_run)

    return Config(
        cities=cities,
        batch_size_buffers=batch_size,
        only_unprocessed_buffers=only_unprocessed,
        mark_buffer_processed=mark_processed,
        dry_run=dry_run,
    )


# =========================
# Логи
# =========================
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("dtp_vehicles_loader")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(console)
    return logger


logger = setup_logging()


# =========================
# Подключение к БД
# =========================
def get_engine() -> Engine:
    """Создаёт SQLAlchemy Engine.

    Берём DATABASE_URL из .env в корне репозитория.
    pool_pre_ping + connect_timeout помогают при нестабильной сети/пулере.
    """

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Нет DATABASE_URL в .env (в корне проекта)")

    return create_engine(
        db_url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 15},
    )


# =========================
# SQL блоки
# =========================
SQL_GET_BUFFER_IDS = """
select b.buffer_id
from public.dtp_buffer b
where b.city = any(:cities)
  and jsonb_typeof(b.raw_json->'tab') = 'array'
  and jsonb_array_length(b.raw_json->'tab') > 0
  {processed_filter}
order by b.buffer_id
limit :limit
offset :offset
"""

SQL_INSERT_VEHICLES_FROM_BUFFER_IDS = """
with cards as (
    select
        b.buffer_id,
        jsonb_array_elements(b.raw_json->'tab') as card
    from public.dtp_buffer b
    where b.buffer_id = any(:buffer_ids)
),
dtp_cards as (
    select
        (card->>'KartId')::bigint as kart_id,
        card
    from cards
    where (card ? 'KartId')
),
dtp_exists as (
    select d.kart_id, d.card
    from dtp_cards d
    join public.dtp_main m
      on m.kart_id = d.kart_id
),
vehicles_src as (
    select
        e.kart_id,
        jsonb_array_elements(e.card->'infoDtp'->'ts_info') as v
    from dtp_exists e
    where jsonb_typeof(e.card->'infoDtp'->'ts_info') = 'array'
),
prepared as (
    select
        kart_id,

        -- n_ts обычно строка "1", "2"...
        nullif(v->>'n_ts','')::int as vehicle_num,

        nullif(v->>'marka_ts','') as brand,
        nullif(v->>'m_ts','') as model,

        -- нормализованная модель (lower + trim + схлопывание пробелов)
        case
            when nullif(v->>'m_ts','') is not null then
                regexp_replace(lower(btrim(v->>'m_ts')), '\\s+', ' ', 'g')
            else null
        end as model_normal,

        nullif(v->>'f_sob','') as ownership,
        nullif(v->>'o_pf','') as owner_type,

        nullif(v->>'t_ts','') as vehicle_class,

        -- нормализуем vehicle_class для джойна со справочником (lower + trim)
        nullif(lower(btrim(v->>'t_ts')), '') as vehicle_class_normalized,

        -- g_v бывает строкой; аккуратно парсим только если похоже на год
        case
            when (v->>'g_v') ~ '^[0-9]{4}$' then (v->>'g_v')::int
            else null
        end as year,

        nullif(v->>'color','') as color,

        -- raw drive_type как есть
        nullif(v->>'r_rul','') as drive_type,

        -- нормализация drive_type для джойна со справочником (lower + trim)
        nullif(lower(btrim(v->>'r_rul')), '') as drive_type_normalized,

        nullif(v->>'t_n','') as technical_state,

        -- raw stolen_or_hidden как есть
        nullif(v->>'ts_s','') as stolen_or_hidden,

        -- нормализация stolen_or_hidden для джойна со справочником (lower + trim)
        nullif(lower(btrim(v->>'ts_s')), '') as stolen_or_hidden_normalized

    from vehicles_src
    where (v ? 'n_ts')
),
enriched as (
    select
        p.*,

        -- drive_type_std из справочника
        coalesce(dt.drive_type_std, :default_drive_type_std) as drive_type_std,

        -- vehicle_type_group из справочника
        coalesce(vg.vehicle_type_group, :default_vehicle_type_group) as vehicle_type_group,

        -- 2-категорийный флаг по technical_state
        case
            when lower(btrim(coalesce(p.technical_state, ''))) = :tech_state_no_issues then :flag_no_issues
            else :flag_has_issues
        end as technical_issue_flag,

        -- vehicle_escape_group из справочника по stolen_or_hidden
        coalesce(es.escape_group, :default_escape_group) as vehicle_escape_group

    from prepared p

    left join public.dim_drive_type dt
      on dt.drive_type_normalized = p.drive_type_normalized
     and coalesce(dt.is_active, true) = true

    left join public.dim_vehicle_class_group vg
      on lower(btrim(vg.vehicle_class)) = p.vehicle_class_normalized
     and coalesce(vg.is_active, true) = true

    left join public.dim_vehicle_escape_status es
      on es.stolen_or_hidden_normalized = p.stolen_or_hidden_normalized
     and coalesce(es.is_active, true) = true
)
insert into public.dtp_vehicles (
    kart_id,
    vehicle_num,
    brand,
    model,
    model_normal,
    year,
    color,
    vehicle_class,
    vehicle_type_group,
    drive_type,
    drive_type_std,
    technical_state,
    technical_issue_flag,
    stolen_or_hidden,
    vehicle_escape_group,
    ownership,
    owner_type,
    loaded_at
)
select
    kart_id,
    vehicle_num,
    brand,
    model,
    model_normal,
    year,
    color,
    vehicle_class,
    vehicle_type_group,
    drive_type,
    drive_type_std,
    technical_state,
    technical_issue_flag,
    stolen_or_hidden,
    vehicle_escape_group,
    ownership,
    owner_type,
    now()
from enriched
where vehicle_num is not null
on conflict (kart_id, vehicle_num) do nothing;
"""

SQL_MARK_BUFFERS_PROCESSED = """
update public.dtp_buffer
set processed = true,
    processed_at = now()
where buffer_id = any(:buffer_ids)
  and processed is distinct from true;
"""


# =========================
# ETL шаги
# =========================
def build_processed_filter(only_unprocessed: bool) -> str:
    """Часть WHERE для отбора буферов."""
    if not only_unprocessed:
        return ""
    return "and coalesce(b.processed, false) = false"


def fetch_buffer_ids(
    conn,
    cities: tuple[str, ...],
    limit: int,
    offset: int,
    only_unprocessed: bool,
) -> List[int]:
    sql = SQL_GET_BUFFER_IDS.format(processed_filter=build_processed_filter(only_unprocessed))
    return (
        conn.execute(
            text(sql),
            {"cities": list(cities), "limit": limit, "offset": offset},
        )
        .scalars()
        .all()
    )


def load_vehicles_for_buffers(conn, buffer_ids: List[int], cfg: Config) -> int:
    """Вставка ТС для указанного списка buffer_id.

    Возвращает количество вставленных строк (rowcount).
    """
    res = conn.execute(
        text(SQL_INSERT_VEHICLES_FROM_BUFFER_IDS),
        {
            "buffer_ids": buffer_ids,
            "default_drive_type_std": cfg.default_drive_type_std,
            "default_vehicle_type_group": cfg.default_vehicle_type_group,
            "default_escape_group": cfg.default_escape_group,
            "tech_state_no_issues": cfg.tech_state_no_issues,
            "flag_no_issues": cfg.flag_no_issues,
            "flag_has_issues": cfg.flag_has_issues,
        },
    )
    return int(res.rowcount or 0)


def mark_buffers_processed(conn, buffer_ids: List[int]) -> None:
    conn.execute(text(SQL_MARK_BUFFERS_PROCESSED), {"buffer_ids": buffer_ids})


# =========================
# Main
# =========================
def main() -> None:
    cfg = load_config_from_env()
    engine = get_engine()

    logger.info("Старт загрузки dtp_vehicles из dtp_buffer. Города: %s", ", ".join(cfg.cities))
    logger.info(
        "BATCH_SIZE_BUFFERS=%s | ONLY_UNPROCESSED_BUFFERS=%s | MARK_BUFFER_PROCESSED=%s",
        cfg.batch_size_buffers,
        cfg.only_unprocessed_buffers,
        cfg.mark_buffer_processed,
    )
    logger.info("DRY_RUN=%s", cfg.dry_run)

    run_t0 = time.monotonic()
    total_batches = 0
    total_buffers = 0
    total_inserted_rows = 0

    batch_no = 0

    while True:
        batch_no += 1

        with engine.begin() as conn:
            buffer_ids = fetch_buffer_ids(
                conn,
                cities=cfg.cities,
                limit=cfg.batch_size_buffers,
                offset=0,
                only_unprocessed=cfg.only_unprocessed_buffers,
            )

        if not buffer_ids:
            break

        total_batches += 1
        total_buffers += len(buffer_ids)

        t0 = time.monotonic()
        if cfg.dry_run:
            inserted_rows = 0
        else:
            with engine.begin() as conn:
                inserted_rows = load_vehicles_for_buffers(conn, buffer_ids, cfg)

                if cfg.mark_buffer_processed:
                    mark_buffers_processed(conn, buffer_ids)
        elapsed = time.monotonic() - t0

        total_inserted_rows += inserted_rows

        logger.info(
            "Батч #%s: buffer_id %s..%s (шт=%s) | inserted_rows=%s | %.2fs",
            batch_no,
            buffer_ids[0],
            buffer_ids[-1],
            len(buffer_ids),
            inserted_rows,
            elapsed,
        )

        if cfg.dry_run:
            logger.info(
                "DRY_RUN: пропускаем INSERT/UPDATE. Следующий батч будет считаться как обработанный только логически."
            )

    run_elapsed = time.monotonic() - run_t0

    logger.info(
        "Итоги: batches=%s | buffers=%s | inserted_rows_total=%s | %.2fs",
        total_batches,
        total_buffers,
        total_inserted_rows,
        run_elapsed,
    )

    logger.info(
        "Готово ✅ Повторный запуск безопасен: дубли по (kart_id, vehicle_num) не вставятся."
    )


if __name__ == "__main__":
    main()