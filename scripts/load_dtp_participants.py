"""
ETL загрузка участников ДТП (dtp_participants).

Назначение
---------
Скрипт переносит участников ДТП из `public.dtp_buffer` в `public.dtp_participants`.
Спроектирован так, чтобы его было удобно автоматизировать (cron/Airflow/GitHub Actions)
и безопасно перезапускать.

Источник данных
--------------
`public.dtp_buffer.raw_json->'tab'` — массив карточек ДТП.

Извлечение участников
--------------------
1) `infoDtp.ts_info[].ts_uch[]` — участники внутри транспортных средств
2) `infoDtp.uchInfo[]`          — участники вне ТС (пешеходы и т.п.)

Валидации и связи
-----------------
- Загружаем только `kart_id`, которые уже существуют в `public.dtp_main`
  (иначе FK в `dtp_participants` не позволит вставку).

Обогащение и стандартизация
---------------------------
- `injury_status` нормализуется и маппится через `public.dim_injury_status`:
    -> `injury_group`, `severity_level`
- `safety_belt` приводится к канону: 'Да' / 'Нет' / 'Не применимо'
    (ремень применим только к ролям 'Водитель' и 'Пассажир')
- `left_scene` группируется в `left_scene_group`
- `age_clean` (int) вычисляется из `v_st` (сырой текст):
    пусто/NULL -> NULL, '0'/'99' -> NULL, иначе int

Идемпотентность
---------------
- Вставка защищена от дублей: `ON CONFLICT (kart_id, participant_num, participant_source) DO NOTHING`.
- Опционально доступен FULL_REFRESH режим для полной пересборки таблицы.

Параметры окружения (.env в корне проекта)
-----------------------------------------
- DATABASE_URL (обязательно)
- CITIES="Кемерово,Ставрополь" (опционально)
- BATCH_SIZE_BUFFERS=50 (опционально)
- FULL_REFRESH=true|false (опционально)
- START_BUFFER_ID=0 (опционально) — начать обработку с buffer_id > START_BUFFER_ID

Запуск
-----
python src/load_dtp_participants.py
"""

import argparse
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Загружаем .env сразу при старте модуля
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


# =========================
# Configuration
# =========================
@dataclass(frozen=True)
class Config:
    database_url: str
    cities: Tuple[str, ...]
    batch_size_buffers: int
    full_refresh: bool
    start_buffer_id: int
    default_injury_group: str = "Не указано"


def load_config() -> Config:
    """Читает конфиг из .env и (опционально) из аргументов командной строки.

    CLI-аргументы удобны для автоматизации (например, передавать города/батч/фулл-рефреш
    параметрами в планировщике).
    """

    parser = argparse.ArgumentParser(description="Load dtp_participants from dtp_buffer")
    parser.add_argument("--cities", type=str, default=os.getenv("CITIES", ""), help="Список городов через запятую")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE_BUFFERS", "50")), help="Размер батча по buffer")
    parser.add_argument("--full-refresh", type=str, default=os.getenv("FULL_REFRESH", "false"), help="true/false")
    parser.add_argument("--start-buffer-id", type=int, default=int(os.getenv("START_BUFFER_ID", "0")), help="Обрабатывать buffer_id > N")
    args = parser.parse_args()
    if args.batch_size < 1:
        raise ValueError("--batch-size должен быть >= 1")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Нет DATABASE_URL в .env (в корне проекта)")

    cities_raw = (args.cities or "").strip()
    cities = tuple([c.strip() for c in cities_raw.split(",") if c.strip()]) if cities_raw else ("Кемерово", "Ставрополь")

    full_refresh = str(args.full_refresh).lower() in ("1", "true", "yes", "y")

    return Config(
        database_url=db_url,
        cities=cities,
        batch_size_buffers=args.batch_size,
        full_refresh=full_refresh,
        start_buffer_id=args.start_buffer_id,
    )


# =========================
# Logging
# =========================
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("dtp_participants_loader_py")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        logger.handlers.clear()

    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)sZ | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(h)
    return logger


logger = setup_logging()


# =========================
# Database helpers & SQL
# =========================
def get_engine(database_url: str):
    return create_engine(
        database_url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 15},
    )


SQL_TRUNCATE_PARTICIPANTS = "truncate table public.dtp_participants restart identity;"

SQL_GET_BUFFER_IDS = """
select b.buffer_id
from public.dtp_buffer b
where b.city = any(:cities)
  and b.buffer_id > :last_buffer_id
  and jsonb_typeof(b.raw_json->'tab') = 'array'
  and jsonb_array_length(b.raw_json->'tab') > 0
order by b.buffer_id
limit :limit;
"""

SQL_GET_CARDS_FOR_BUFFER_IDS = """
select
    b.buffer_id,
    jsonb_array_elements(b.raw_json->'tab') as card
from public.dtp_buffer b
where b.buffer_id = any(:buffer_ids);
"""

SQL_GET_EXISTING_KART_IDS = """
select m.kart_id
from public.dtp_main m
where m.kart_id = any(:kart_ids);
"""

SQL_GET_DIM_INJURY = """
select
    d.injury_status_normalized,
    d.injury_group,
    d.severity_level
from public.dim_injury_status d
where coalesce(d.is_active, true) = true;
"""

# Вставка: лучше через executemany (SQLAlchemy), без pandas, чтобы было легче автоматизировать
SQL_INSERT_PARTICIPANT = """
insert into public.dtp_participants (
    kart_id,
    vehicle_num,
    participant_num,
    role,
    sex,
    v_st,
    age_clean,
    alco,
    injury_status,
    left_scene,
    safety_belt,
    seat_group,
    injured_card_id,
    injury_group,
    severity_level,
    participant_source,
    left_scene_group,
    loaded_at
)
values (
    :kart_id,
    :vehicle_num,
    :participant_num,
    :role,
    :sex,
    :v_st,
    :age_clean,
    :alco,
    :injury_status,
    :left_scene,
    :safety_belt,
    :seat_group,
    :injured_card_id,
    :injury_group,
    :severity_level,
    :participant_source,
    :left_scene_group,
    now()
)
on conflict (kart_id, participant_num, participant_source) do nothing;
"""


# =========================
# Normalization helpers
# =========================
def normalize_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return s if s != "" else None


def injury_status_normalize(s: Optional[str]) -> Optional[str]:
    s = normalize_text(s)
    return s.lower().strip() if s else None


def left_scene_group(left_scene: Optional[str]) -> str:
    ls = normalize_text(left_scene)
    if ls == "Нет (не скрывался)":
        return "Не скрывался"
    if ls and ls.startswith("Скрылся, впоследствии разыскан"):
        return "Скрылся, разыскан"
    if ls and ls.startswith("Скрылся и впоследствии не установлен"):
        return "Скрылся, не установлен"
    return "Не указано"


def safety_belt_std(role: Optional[str], safety_belt: Optional[str]) -> str:
    r = normalize_text(role)
    sb = normalize_text(safety_belt)
    # Минимально правильная логика:
    if r in ("Водитель", "Пассажир"):
        if sb in ("Да", "Нет"):
            return sb
        return "Не применимо"
    return "Не применимо"


def parse_int_or_none(x: Any) -> Optional[int]:
    x = normalize_text(x)
    if x is None:
        return None
    try:
        return int(x)
    except ValueError:
        return None


def age_clean_from_vst(v_st: Any) -> Optional[int]:
    """Приводим v_st к возрасту (int) для анализа.

    Правила:
    - пусто/NULL -> NULL
    - '0' и '99' -> NULL (как заглушки/не установлен)
    - иначе -> int
    """
    n = parse_int_or_none(v_st)
    if n is None:
        return None
    if n in (0, 99):
        return None
    return n




# =========================
# JSON parsing helpers
# =========================
def iter_ts_uch_participants(card: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    kart_id = card.get("KartId")
    info = card.get("infoDtp") or {}

    ts_info = info.get("ts_info") or []
    if not isinstance(ts_info, list):
        return

    for ts in ts_info:
        if not isinstance(ts, dict):
            continue
        vehicle_num = ts.get("n_ts")  # номер ТС в карточке (может быть строкой)
        ts_uch = ts.get("ts_uch") or []
        if not isinstance(ts_uch, list) or len(ts_uch) == 0:
            continue

        for uch in ts_uch:
            if not isinstance(uch, dict):
                continue
            yield {
                "kart_id": kart_id,
                "vehicle_num": parse_int_or_none(vehicle_num),
                "participant_source": "ts_uch",
                "participant_num": parse_int_or_none(uch.get("N_UCH") or uch.get("n_uch")),
                "role": normalize_text(uch.get("K_UCH") or uch.get("k_uch")),
                "sex": normalize_text(uch.get("POL") or uch.get("pol")),
                "v_st": normalize_text(uch.get("V_ST") or uch.get("v_st")),
                "alco": normalize_text(uch.get("ALCO") or uch.get("alco")),
                "injury_status": normalize_text(uch.get("S_T") or uch.get("s_t")),
                "left_scene": normalize_text(uch.get("S_SM") or uch.get("s_sm")),
                "safety_belt": normalize_text(uch.get("SAFETY_BELT") or uch.get("safety_belt")),
                "seat_group": normalize_text(uch.get("S_SEAT_GROUP") or uch.get("s_seat_group")),
                "injured_card_id": normalize_text(uch.get("INJURED_CARD_ID") or uch.get("injured_card_id")),
            }


def iter_uchinfo_participants(card: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    kart_id = card.get("KartId")
    info = card.get("infoDtp") or {}

    # ВАЖНО: в твоих данных uchInfo внутри infoDtp
    uch_info = info.get("uchInfo") or []
    if not isinstance(uch_info, list):
        return

    for uch in uch_info:
        if not isinstance(uch, dict):
            continue
        yield {
            "kart_id": kart_id,
            "vehicle_num": None,
            "participant_source": "uchInfo",
            "participant_num": parse_int_or_none(uch.get("N_UCH") or uch.get("n_uch")),
            "role": normalize_text(uch.get("K_UCH") or uch.get("k_uch")),
            "sex": normalize_text(uch.get("POL") or uch.get("pol")),
            "v_st": normalize_text(uch.get("V_ST") or uch.get("v_st")),
            "alco": normalize_text(uch.get("ALCO") or uch.get("alco")),
            "injury_status": normalize_text(uch.get("S_T") or uch.get("s_t")),
            "left_scene": normalize_text(uch.get("S_SM") or uch.get("s_sm")),
            "safety_belt": None,
            "seat_group": None,
            "injured_card_id": None,
        }


def extract_participants_from_card(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    out.extend(list(iter_ts_uch_participants(card)))
    out.extend(list(iter_uchinfo_participants(card)))
    return out


# =========================
# Main ETL
# =========================
def main():
    cfg = load_config()
    engine = get_engine(cfg.database_url)

    logger.info("Старт загрузки public.dtp_participants. Города: %s", ", ".join(cfg.cities))
    logger.info("FULL_REFRESH=%s | BATCH_SIZE_BUFFERS=%s", cfg.full_refresh, cfg.batch_size_buffers)

    # 1) dim_injury_status в память (маленький справочник)
    with engine.begin() as conn:
        dim_rows = conn.execute(text(SQL_GET_DIM_INJURY)).mappings().all()

    dim_map = {
        r["injury_status_normalized"]: (
            r["injury_group"],
            r["severity_level"],
        )
        for r in dim_rows
        if r["injury_status_normalized"] is not None
    }

    if cfg.full_refresh:
        logger.info("FULL_REFRESH=True: очищаем public.dtp_participants перед загрузкой")
        with engine.begin() as conn:
            conn.execute(text(SQL_TRUNCATE_PARTICIPANTS))

    last_buffer_id = cfg.start_buffer_id
    batch_no = 0
    attempted_total = 0

    while True:
        batch_no += 1

        with engine.begin() as conn:
            buffer_ids = conn.execute(
                text(SQL_GET_BUFFER_IDS),
                {"cities": list(cfg.cities), "limit": cfg.batch_size_buffers, "last_buffer_id": last_buffer_id}
            ).scalars().all()

        if not buffer_ids:
            break

        # Курсор для keyset pagination
        last_buffer_id = int(buffer_ids[-1])

        # 2) тянем карточки из буфера
        with engine.begin() as conn:
            cards_rows = conn.execute(
                text(SQL_GET_CARDS_FOR_BUFFER_IDS),
                {"buffer_ids": buffer_ids}
            ).mappings().all()

        # 3) распарсить карточки -> участники
        cards: List[Dict[str, Any]] = []
        for r in cards_rows:
            card = r["card"]
            # card уже jsonb (dict-подобный) в supabase/postgres драйвере обычно приходит как dict
            # но иногда может быть строкой, подстрахуемся:
            if isinstance(card, str):
                card = json.loads(card)
            if isinstance(card, dict) and "KartId" in card:
                cards.append(card)

        if not cards:
            continue

        kart_ids = [int(c["KartId"]) for c in cards if normalize_text(c.get("KartId")) and str(c.get("KartId")).isdigit()]

        # 4) FK-фильтр: только те, что есть в dtp_main
        with engine.begin() as conn:
            existing_karts = set(
                conn.execute(text(SQL_GET_EXISTING_KART_IDS), {"kart_ids": kart_ids}).scalars().all()
            )

        cards = [c for c in cards if int(c["KartId"]) in existing_karts]
        if not cards:
            continue

        # 5) готовим строки для вставки
        to_insert: List[Dict[str, Any]] = []
        for card in cards:
            participants = extract_participants_from_card(card)

            for p in participants:
                # базовые проверки
                if p["kart_id"] is None:
                    continue
                if p["participant_num"] is None:
                    # как в SQL: where participant_num is not null
                    continue

                kart_id = int(p["kart_id"])

                inj_norm = injury_status_normalize(p.get("injury_status"))
                injury_group, severity_level = dim_map.get(
                    inj_norm,
                    (cfg.default_injury_group, None)
                )

                row = {
                    "kart_id": kart_id,
                    "vehicle_num": p.get("vehicle_num"),
                    "participant_num": p.get("participant_num"),
                    "role": p.get("role"),
                    "sex": p.get("sex"),
                    "v_st": p.get("v_st"),
                    "age_clean": age_clean_from_vst(p.get("v_st")),
                    "alco": p.get("alco"),
                    "injury_status": p.get("injury_status"),
                    "left_scene": p.get("left_scene"),
                    # Ремень безопасности: приводим к канону 'Да' / 'Нет' / 'Не применимо'
                    # (ремень применим только для ролей 'Водитель' и 'Пассажир')
                    "safety_belt": safety_belt_std(p.get("role"), p.get("safety_belt")),
                    "seat_group": p.get("seat_group"),
                    "injured_card_id": p.get("injured_card_id"),
                    "injury_group": injury_group,
                    "severity_level": severity_level,
                    "participant_source": p.get("participant_source"),
                    "left_scene_group": left_scene_group(p.get("left_scene")),
                }
                to_insert.append(row)

        # 6) вставка пачкой (в одной транзакции)
        if to_insert:
            with engine.begin() as conn:
                conn.execute(text(SQL_INSERT_PARTICIPANT), to_insert)
            attempted_total += len(to_insert)

        logger.info(
            "Батч #%s: buffer_id %s..%s (шт=%s) | valid_kart_ids=%s | attempted_rows=%s",
            batch_no,
            buffer_ids[0],
            buffer_ids[-1],
            len(buffer_ids),
            len(existing_karts),
            len(to_insert),
        )

    logger.info("Готово ✅ attempted_total=%s", attempted_total)


if __name__ == "__main__":
    main()