-- =================================================================================================
-- WEATHER ACCIDENTS ANALYSIS — PHYSICAL DATA MODEL (TABLES, CONSTRAINTS, INDEXES)
-- =================================================================================================
-- Назначение файла:
--   DDL-описание основных таблиц и справочников проекта.
--   Файл включает:
--     * CREATE TABLE
--     * PRIMARY KEY / UNIQUE / FOREIGN KEY
--     * CREATE INDEX


-- =================================================================================================
-- 1. DIMENSIONS / REFERENCE TABLES
-- =================================================================================================

-- -------------------------------------------------------------------------------------------------
-- table: public.dim_city
-- назначение:
--   справочник городов РФ, используемый для нормализации географии,
--   привязки ДТП и погодных данных к единому city_id.
-- -------------------------------------------------------------------------------------------------
create table public.dim_city (
    city_id            bigserial primary key,
    city               text not null,
    region             text null,
    federal_district   text null,
    population         bigint null,
    lat                double precision null,
    lon                double precision null,
    coord_source       text null,
    created_at         timestamptz not null default now(),
    updated_at         timestamptz not null default now(),
    okato              text null,
    oktmo              text null,
    dadata_fias_id     text null,
    dadata_kladr_id    text null,
    dadata_raw         jsonb null,

    constraint uq_city unique (city, region)
);

-- Индекс по координатам для географических проверок / поиска.
create index if not exists ix_dim_city_coords
    on public.dim_city (lat, lon);


-- -------------------------------------------------------------------------------------------------
-- table: public.dim_injury_status
-- назначение:
--   справочник нормализации статусов травм участников ДТП.
-- -------------------------------------------------------------------------------------------------
create table public.dim_injury_status (
    injury_status              text not null primary key,
    injury_status_normalized   text generated always as (nullif(lower(btrim(injury_status)), '')) stored,
    injury_group               text not null,
    severity_level             smallint null,
    is_active                  boolean not null default true,
    note                       text null,
    created_at                 timestamptz not null default now(),
    updated_at                 timestamptz not null default now()
);

create index if not exists idx_dim_injury_status_norm
    on public.dim_injury_status (injury_status_normalized);

create index if not exists idx_dim_injury_status_group
    on public.dim_injury_status (injury_group);


-- -------------------------------------------------------------------------------------------------
-- table: public.dim_vehicle_class_group
-- назначение:
--   справочник нормализации классов ТС -> укрупнённая группа ТС.
-- -------------------------------------------------------------------------------------------------
create table public.dim_vehicle_class_group (
    vehicle_class       text not null primary key,
    vehicle_type_group  text not null,
    is_active           boolean not null default true,
    note                text null,
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now()
);


-- -------------------------------------------------------------------------------------------------
-- table: public.dim_drive_type
-- назначение:
--   справочник нормализации типа привода / компоновки ТС.
-- -------------------------------------------------------------------------------------------------
create table public.dim_drive_type (
    drive_type_normalized  text not null primary key,
    drive_type_std         text not null,
    is_active              boolean not null default true,
    note                   text null,
    created_at             timestamptz not null default now(),
    updated_at             timestamptz not null default now()
);


-- -------------------------------------------------------------------------------------------------
-- table: public.dim_vehicle_escape_status
-- назначение:
--   справочник нормализации статуса скрытия транспортного средства с места ДТП.
-- -------------------------------------------------------------------------------------------------
create table public.dim_vehicle_escape_status (
    stolen_or_hidden_normalized  text not null primary key,
    escape_group                 text not null,
    is_active                    boolean default true,
    created_at                   timestamptz default now(),
    updated_at                   timestamptz default now()
);


-- =================================================================================================
-- 2. BUFFER TABLES
-- =================================================================================================

-- -------------------------------------------------------------------------------------------------
-- table: public.dtp_buffer
-- назначение:
--   буфер сырых JSON-ответов ДТП из внешнего источника.
-- -------------------------------------------------------------------------------------------------
create table public.dtp_buffer (
    buffer_id       bigserial primary key,
    city            text not null,
    region_name     text not null,
    region_id       text not null,
    district_id     text not null,
    request_year    integer not null,
    request_month   integer not null,
    raw_json        jsonb not null,
    loaded_at       timestamptz not null default now(),
    processed       boolean not null default false,
    processed_at    timestamptz null,
    error_message   text null
);


-- -------------------------------------------------------------------------------------------------
-- table: public.weather_buffer
-- назначение:
--   буфер погодных данных по городу и периоду в формате JSON.
-- -------------------------------------------------------------------------------------------------
create table public.weather_buffer (
    buffer_id    bigserial primary key,
    source       text not null,
    city_id      integer not null,
    city         text not null,
    date_from    date not null,
    date_to      date not null,
    payload      jsonb not null,
    loaded_at    timestamptz not null default now(),
    updated_at   timestamptz not null default now(),

    constraint weather_buffer_unique unique (source, city_id, date_from, date_to),
    constraint weather_buffer_city_fk foreign key (city_id)
        references public.dim_city (city_id)
);

-- Быстрые фильтры по городу, периоду и времени загрузки.
create index if not exists ix_weather_buffer_city_id
    on public.weather_buffer (city_id);

create index if not exists ix_weather_buffer_date_range
    on public.weather_buffer (date_from, date_to);

create index if not exists ix_weather_buffer_loaded_at
    on public.weather_buffer (loaded_at);

-- GIN-индекс для поиска по JSON payload.
create index if not exists ix_weather_buffer_payload_gin
    on public.weather_buffer using gin (payload);


-- =================================================================================================
-- 3. CORE FACT TABLES
-- =================================================================================================

-- -------------------------------------------------------------------------------------------------
-- table: public.dtp_main
-- назначение:
--   основная факт-таблица ДТП.
--   1 строка = 1 ДТП (kart_id).
-- -------------------------------------------------------------------------------------------------
create table public.dtp_main (
    dtp_id      bigserial primary key,
    buffer_id   bigint not null,
    kart_id     bigint not null,
    city_id     integer not null,
    dtp_date    date null,
    dtp_time    time null,
    dtp_type    text null,
    district    text null,
    pog         integer null,
    ran         integer null,
    k_ts        integer null,
    k_uch       integer null,
    row_num     integer null,
    loaded_at   timestamptz not null default now(),

    constraint dtp_main_buffer_id_fkey foreign key (buffer_id)
        references public.dtp_buffer (buffer_id)
        on delete cascade,
    constraint dtp_main_city_id_fkey foreign key (city_id)
        references public.dim_city (city_id)
);

-- Уникальный бизнес-ключ карточки ДТП.
create unique index if not exists ux_dtp_main_kart_id
    on public.dtp_main (kart_id);

-- Индексы под фильтрацию по дате / городу.
create index if not exists ix_dtp_main_dtp_date
    on public.dtp_main (dtp_date);

create index if not exists ix_dtp_main_city_date
    on public.dtp_main (city_id, dtp_date);

-- Индекс под join/контроль ETL по буферу.
create index if not exists ix_dtp_main_buffer_id
    on public.dtp_main (buffer_id);


-- -------------------------------------------------------------------------------------------------
-- table: public.dtp_locations
-- назначение:
--   детализация локации и дорожных условий по ДТП.
--   1 строка = 1 ДТП (kart_id).
-- -------------------------------------------------------------------------------------------------
create table public.dtp_locations (
    location_id               bigserial primary key,
    kart_id                   bigint not null,
    city                      text null,
    street                    text null,
    house                     text null,
    km                        integer null,
    m                         integer null,
    road_type                 text null,
    road_category             text null,
    road_defects              text null,
    road_elements             text null,
    objects_near              text null,
    lat_raw                   numeric(10, 6) null,
    lon_raw                   numeric(10, 6) null,
    loaded_at                 timestamptz not null default now(),
    road_category_group       text null,
    light_condition           text null,
    local_weather_condition   text null,
    road_surface_condition    text null,

    constraint uq_dtp_locations_kart unique (kart_id),
    constraint fk_dtp_locations_kart foreign key (kart_id)
        references public.dtp_main (kart_id)
        on delete cascade
);

-- Индексы по наиболее частым аналитическим измерениям.
create index if not exists ix_dtp_locations_city
    on public.dtp_locations (city);

create index if not exists ix_dtp_locations_road_category_group
    on public.dtp_locations (road_category_group);

create index if not exists ix_dtp_locations_light_condition
    on public.dtp_locations (light_condition);

create index if not exists ix_dtp_locations_road_surface_condition
    on public.dtp_locations (road_surface_condition);

create index if not exists ix_dtp_locations_local_weather_condition
    on public.dtp_locations (local_weather_condition);


-- -------------------------------------------------------------------------------------------------
-- table: public.dtp_participants
-- назначение:
--   участники ДТП.
--   1 строка = 1 участник в рамках ДТП.
-- -------------------------------------------------------------------------------------------------
create table public.dtp_participants (
    participant_id      bigint generated always as identity primary key,
    kart_id             bigint not null,
    vehicle_num         integer null,
    participant_num     integer not null,
    role                text null,
    sex                 text null,
    v_st                text null,
    alco                text null,
    injury_status       text null,
    left_scene          text null,
    safety_belt         text null,
    seat_group          text null,
    injured_card_id     text null,
    injury_group        text null,
    severity_level      integer null,
    participant_source  text null,
    left_scene_group    text null,
    loaded_at           timestamptz not null default now(),
    age_clean           integer null,

    constraint dtp_participants_unique unique (kart_id, participant_num, participant_source),
    constraint fk_dtp_participants_kart foreign key (kart_id)
        references public.dtp_main (kart_id)
        on delete cascade
);

-- Часто используемые аналитические срезы.

create index if not exists ix_dtp_participants_injury_group
    on public.dtp_participants (injury_group);

create index if not exists ix_dtp_participants_severity_level
    on public.dtp_participants (severity_level);

create index if not exists ix_dtp_participants_safety_belt
    on public.dtp_participants (safety_belt);

create index if not exists ix_dtp_participants_left_scene_group
    on public.dtp_participants (left_scene_group);

-- Составной индекс под частые фильтры kart_id + role.
create index if not exists ix_dtp_participants_kart_role
    on public.dtp_participants (kart_id, role);


-- -------------------------------------------------------------------------------------------------
-- table: public.dtp_vehicles
-- назначение:
--   транспортные средства, участвовавшие в ДТП.
--   1 строка = 1 ТС в рамках ДТП.
-- -------------------------------------------------------------------------------------------------
create table public.dtp_vehicles (
    vehicle_id             bigint generated by default as identity primary key,
    kart_id                bigint not null,
    vehicle_num            integer not null,
    brand                  text null,
    model                  text null,
    model_normal           text null,
    year                   integer null,
    color                  text null,
    vehicle_class          text null,
    vehicle_type_group     text null,
    drive_type             text null,
    drive_type_std         text null,
    technical_state        text null,
    technical_issue_flag   text null,
    stolen_or_hidden       text null,
    vehicle_escape_group   text null,
    ownership              text null,
    owner_type             text null,
    loaded_at              timestamptz not null,

    constraint dtp_vehicles_kart_vehicle_uk unique (kart_id, vehicle_num),
    constraint fk_dtp_vehicles_kart foreign key (kart_id)
        references public.dtp_main (kart_id)
        on delete cascade
);

-- Аналитические индексы по транспорту.
create index if not exists ix_dtp_vehicles_brand
    on public.dtp_vehicles (brand);

create index if not exists ix_dtp_vehicles_vehicle_type_group
    on public.dtp_vehicles (vehicle_type_group);

create index if not exists idx_dtp_vehicles_drive_type_std
    on public.dtp_vehicles (drive_type_std);


-- -------------------------------------------------------------------------------------------------
-- table: public.weather_conditions
-- назначение:
--   почасовые погодные условия по городу.
--   1 строка = 1 город + 1 час + 1 источник.
-- -------------------------------------------------------------------------------------------------
create table public.weather_conditions (
    weather_condition_id  bigserial primary key,
    observed_at           timestamptz not null,
    city_id               integer not null,
    temperature           numeric(5, 2) null,
    relative_humidity     numeric(5, 2) null,
    precipitation         numeric(8, 3) null,
    rain                  numeric(8, 3) null,
    snowfall              numeric(8, 3) null,
    cloud_cover           numeric(5, 2) null,
    visibility            numeric(10, 2) null,
    wind_speed            numeric(8, 2) null,
    wind_gusts            numeric(8, 2) null,
    source                text not null,
    updated_at            timestamptz not null default now(),

    constraint uq_weather_conditions unique (source, city_id, observed_at),

    constraint fk_weather_conditions_city_id foreign key (city_id)
        references public.dim_city (city_id)
);

-- Составной индекс для аналитических запросов по городу и времени наблюдения
create index if not exists ix_weather_conditions_city_time
    on public.weather_conditions (city_id, observed_at);
