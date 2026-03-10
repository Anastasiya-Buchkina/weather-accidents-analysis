-- =================================================================================================
-- WEATHER ACCIDENTS ANALYSIS — ANALYTICAL VIEWS / DATA MARTS
-- =================================================================================================
-- Назначение файла:
--   DDL-описание аналитических витрин (VIEW), используемых для BI, дашбордов и исследовательского
--   анализа в Jupyter.
--
-- Состав файла:
--   * public.vw_vehicles           — 1 строка = 1 транспортное средство в ДТП
--   * public.vw_participants       — 1 строка = 1 участник ДТП
--   * public.vw_dtp_loc_weather    — 1 строка = 1 ДТП (погода + локация + факты ДТП)
--   * public.vw_agg_vehicles       — 1 строка = 1 ДТП (агрегаты по транспортным средствам)
--   * public.vw_agg_participants   — 1 строка = 1 ДТП (агрегаты по участникам)


-- =================================================================================================
-- VIEW: public.vw_vehicles
-- =================================================================================================
-- Витрина транспортных средств, участвовавших в ДТП.
--
-- Гранулярность:
--   1 строка = 1 транспортное средство в 1 ДТП.
--
-- Источник данных:
--   public.dtp_vehicles
--
-- Логика витрины:
--   Витрина содержит данные по каждому ТС, участвовавшему в ДТП,
--   и включает вычисляемую категорию возраста автомобиля.
--
--   vehicle_age_group:
--       Новые        — 0–3 лет
--       Свежие       — 3–7 лет
--       Подержанные  — 7–15 лет
--       Возрастные   — 15+ лет
--       Неизвестно   — год выпуска отсутствует
--
-- Назначение витрины:
--   Используется для анализа транспортных средств в ДТП
--   и является базовой таблицей для агрегированной витрины
--   public.vw_agg_vehicles (1 строка = 1 ДТП).
--
-- Ключ соединения:
--   kart_id — идентификатор ДТП.
--
-- Описание колонок витрины:
--   vehicle_id             — уникальный идентификатор транспортного средства
--   kart_id                — идентификатор ДТП
--   vehicle_num            — номер транспортного средства в карточке ДТП
--   brand                  — марка транспортного средства
--   model_normal           — нормализованная модель транспортного средства
--   year                   — год выпуска транспортного средства
--   vehicle_age_group      — возрастная категория транспортного средства
--   vehicle_type_group     — укрупнённая группа типа транспортного средства
--   drive_type_std         — нормализованный тип привода
--   technical_issue_flag   — признак наличия технических неисправностей
--   vehicle_escape_group   — статус скрытия транспортного средства с места ДТП
-- =================================================================================================
create or replace view public.vw_vehicles as
select
    v.vehicle_id,
    v.kart_id,
    v.vehicle_num,

    v.brand,
    v.model_normal,
    v.year,

    case
        when v.year is null then 'Неизвестно'
        when extract(year from current_date)::int - v.year < 3 then 'Новые'
        when extract(year from current_date)::int - v.year < 7 then 'Свежие'
        when extract(year from current_date)::int - v.year < 15 then 'Подержанные'
        else 'Возрастные'
    end as vehicle_age_group,

    v.vehicle_type_group,
    v.drive_type_std,
    v.technical_issue_flag,
    v.vehicle_escape_group
from public.dtp_vehicles v;


-- =================================================================================================
-- VIEW: public.vw_participants
-- =================================================================================================
-- Витрина участников ДТП.
--
-- Гранулярность:
--   1 строка = 1 участник ДТП.
--
-- Источник данных:
--   public.dtp_participants
--
-- Логика витрины:
--   Витрина содержит информацию об участниках ДТП и включает
--   нормализованные аналитические поля для последующего анализа.
--
--   Основные категории:
--     role_group   — укрупнённая категория роли участника:
--                    Водитель / Пассажир / Пешеход / Велосипедист / Прочие
--
--     alco_group   — признак алкоголя:
--                    Трезв / Алкоголь обнаружен / Неизвестно
--
-- Назначение витрины:
--   Используется для анализа участников ДТП и является
--   базовой таблицей для агрегированной витрины
--   public.vw_agg_participants (1 строка = 1 ДТП).
--
-- Ключ соединения:
--   kart_id — идентификатор ДТП.
--
-- Описание колонок витрины:
--   participant_id     — уникальный идентификатор участника ДТП
--   kart_id            — идентификатор ДТП
--   vehicle_num        — номер транспортного средства, к которому относится участник
--   participant_num    — номер участника в карточке ДТП
--   participant_source — источник записи об участнике в исходных данных
--   role               — исходная роль участника
--   role_group         — укрупнённая категория роли участника
--   sex                — пол участника
--   alco               — исходный код признака алкоголя
--   alco_group         — нормализованная группа признака алкоголя
--   injury_status      — исходный статус травмы участника
--   injury_group       — укрупнённая группа тяжести травмы
--   left_scene         — исходный признак скрытия с места ДТП
--   left_scene_group   — нормализованная группа скрытия с места ДТП
--   safety_belt        — признак использования ремня безопасности
-- =================================================================================================
create or replace view public.vw_participants as
select
    p.participant_id,
    p.kart_id,
    p.vehicle_num,
    p.participant_num,
    p.participant_source,

    -- исходная роль (как в источнике)
    p.role,

    -- роль-группа (для аналитики)
    case
        when p.role = 'Водитель' then 'Водитель'
        when p.role = 'Пассажир' then 'Пассажир'
        when p.role in (
            'Пешеход',
            'Пешеход, перед ДТП находившийся в (на) ТС в качестве водителя или пешеход, перед ДТП находившийся в (на) ТС в качестве пассажира'
        ) then 'Пешеход'
        when p.role in (
            'Велосипедист',
            'Велосипедист (не применяется)'
        ) then 'Велосипедист'
        else 'Прочие'
    end as role_group,

    p.sex,

    -- исходное значение алкоголя (как в источнике)
    p.alco,

    -- алкоголь-группа (для аналитики)
    case
        when p.alco is null then 'Неизвестно'
        when p.alco = '00' then 'Трезв'
        else 'Алкоголь обнаружен'
    end as alco_group,

    p.injury_status,
    p.injury_group,

    p.left_scene,
    p.left_scene_group,

    p.safety_belt
from public.dtp_participants p;


-- =================================================================================================
-- VIEW: public.vw_dtp_loc_weather
-- =================================================================================================
-- Назначение:
--   Базовая витрина для аналитики ДТП с привязкой:
--     1) фактов ДТП (public.dtp_main)
--     2) локации/условий дороги (public.dtp_locations)
--     3) погоды на час ДТП (public.weather_conditions)
--     4) справочника городов (public.dim_city)
--
-- Гранулярность:
--   1 строка = 1 ДТП (kart_id)
--
-- Ключи и связи:
--   * dtp_main.kart_id = dtp_locations.kart_id
--   * dtp_main.city_id = dim_city.city_id
--   * dtp_main.city_id + dtp_hour_utc = weather_conditions.city_id + weather_conditions.observed_at
--     (плюс фиксируем weather_source)
--
-- Нюанс времени (ВАЖНО):
--   * dtp_main.dtp_date + dtp_main.dtp_time — это локальное время города.
--   * weather_conditions.observed_at хранится в UTC (timestamptz).
--
--   Поэтому для join по погоде:
--     1) собираем dtp_timestamp_local  = dtp_date + dtp_time
--     2) переводим в dtp_timestamp_utc по смещению города (пока hardcode по city_id)
--     3) округляем вниз до часа: dtp_hour_utc = date_trunc('hour', dtp_timestamp_utc)
--     4) left join weather по (city_id, dtp_hour_utc, source)
--
-- Координаты:
--   В витрину отдаём только «валидные» координаты РФ:
--     lat ∈ [41; 82] и lon ∈ [19; 190]
--   Если координаты выходят за диапазон — lat/lon = null, но сохраняются флаги:
--     has_raw_coordinates, coords_valid
--
-- Поля-категории (готовые для BI):
--   * severity_level (fatal / injury / damage_only)
--   * precipitation_type, precipitation_flag
--   * temperature_group, wind_speed_group, cloudiness_level, strong_wind_flag
--   * has_road_defects (road_defects <> 'Не установлены')
--
-- Обновление:
--   VIEW пересчитывается при запросе. Данные обновляются, когда ETL/парсеры
--   обновляют исходные таблицы (dtp_main / dtp_locations / weather_conditions).
--
-- Описание колонок витрины:
--   dtp_id                   — технический идентификатор записи ДТП
--   kart_id                  — бизнес-ключ карточки ДТП
--   city_id                  — идентификатор города
--   city                     — название города
--   population               — численность населения города
--   dtp_date                 — дата ДТП
--   dtp_time                 — время ДТП из источника
--   dtp_timestamp            — локальный timestamp ДТП
--   dtp_hour                 — локальный час ДТП
--   dtp_hour_utc             — час ДТП в UTC для join с погодой
--   dtp_type                 — тип ДТП
--   district                 — район / административная принадлежность ДТП
--   pog                      — количество погибших в ДТП
--   ran                      — количество раненых в ДТП
--   k_ts                     — количество транспортных средств в ДТП
--   k_uch                    — количество участников ДТП
--   severity_level           — категория тяжести ДТП (fatal / injury / damage_only)
--   lat                      — валидная широта ДТП
--   lon                      — валидная долгота ДТП
--   has_raw_coordinates      — признак наличия исходных координат
--   coords_valid             — признак валидности координат
--   street                   — улица
--   house                    — дом / адресный ориентир
--   km                       — километр дороги
--   m                        — метр дороги
--   road_type                — тип дороги
--   road_category            — категория дороги
--   road_category_group      — укрупнённая группа категории дороги
--   road_defects             — исходное описание дефектов дороги
--   has_road_defects         — флаг наличия дефектов дороги
--   road_elements            — элементы дороги / дорожной инфраструктуры
--   objects_near             — объекты рядом с местом ДТП
--   light_condition          — освещённость / световые условия на месте ДТП
--   local_weather_condition  — погодные условия на месте ДТП из карточки ДТП
--   road_surface_condition   — состояние дорожного покрытия
--   weather_observed_at      — timestamp погодного наблюдения (UTC)
--   weather_source           — источник погодных данных
--   has_weather              — признак успешного присоединения погодных данных
--   temperature              — температура воздуха
--   relative_humidity        — относительная влажность
--   precipitation            — общее количество осадков
--   rain                     — количество дождя
--   snowfall                 — количество снегопада
--   cloud_cover              — облачность
--   wind_speed               — скорость ветра
--   wind_gusts               — порывы ветра
--   precipitation_type       — тип осадков
--   precipitation_flag       — флаг наличия осадков
--   temperature_group        — температурная категория
--   wind_speed_group         — категория скорости ветра
--   cloudiness_level         — категория облачности
--   strong_wind_flag         — флаг сильной порывистости ветра
-- =================================================================================================
create or replace view public.vw_dtp_loc_weather as
with base as (
    select
        m.*,

        -- 1) локальный timestamp ДТП
        (m.dtp_date::timestamp + coalesce(m.dtp_time, time '00:00')) as dtp_timestamp_local,

        -- 2) перевод локального времени в UTC
        --    Ставрополь: UTC+3  -> UTC = local - 3h
        --    Кемерово:   UTC+7  -> UTC = local - 7h
        --
        -- ВАЖНО: если появятся новые города — добавим их сюда или вынесем в dim_city с tz_offset.
        (m.dtp_date::timestamp + coalesce(m.dtp_time, time '00:00'))
        - case
            when m.city_id = 446 then interval '3 hour'  -- Ставрополь
            when m.city_id = 965 then interval '7 hour'  -- Кемерово
            else interval '0 hour'                       -- fallback
          end as dtp_timestamp_utc,

        -- 3) час ДТП в UTC (для join с погодой)
        date_trunc(
            'hour',
            (m.dtp_date::timestamp + coalesce(m.dtp_time, time '00:00'))
            - case
                when m.city_id = 446 then interval '3 hour'
                when m.city_id = 965 then interval '7 hour'
                else interval '0 hour'
              end
        ) as dtp_hour_utc
    from public.dtp_main m
)
select
    -- =========================
    -- ДТП (dtp_main)
    -- =========================

    -- ключи
    b.dtp_id,
    b.kart_id,
    b.city_id,

    -- город (из dim_city)
    c.city,
    c.population,

    -- дата и время ДТП (как в источнике)
    b.dtp_date,
    b.dtp_time,

    -- полный timestamp ДТП
    b.dtp_timestamp_local as dtp_timestamp,

    -- час ДТП (локально) — иногда удобно для анализа
    date_trunc('hour', b.dtp_timestamp_local) as dtp_hour,

    -- час ДТП в UTC (техполе для контроля join'а)
    b.dtp_hour_utc,

    -- характеристики ДТП
    b.dtp_type,
    b.district,

    -- факты
    b.pog,
    b.ran,
    b.k_ts,
    b.k_uch,

    -- категория тяжести ДТП
    case
        when coalesce(b.pog, 0) > 0 then 'fatal'
        when coalesce(b.ran, 0) > 0 then 'injury'
        else 'damage_only'
    end as severity_level,

    -- =========================
    -- ЛОКАЦИЯ / ДОРОГА (dtp_locations)
    -- =========================

    -- координаты: в витрине показываем только валидные (иначе null)
    case
        when l.lat_raw between 41 and 82 and l.lon_raw between 19 and 190
            then l.lat_raw::numeric(10,6)
        else null::numeric(10,6)
    end as lat,
    case
        when l.lat_raw between 41 and 82 and l.lon_raw between 19 and 190
            then l.lon_raw::numeric(10,6)
        else null::numeric(10,6)
    end as lon,

    -- флаги по координатам
    (l.lat_raw is not null and l.lon_raw is not null) as has_raw_coordinates,
    (l.lat_raw between 41 and 82 and l.lon_raw between 19 and 190) as coords_valid,

    -- адрес
    l.street,
    l.house,
    l.km,
    l.m,

    -- тип/категория дороги
    l.road_type,
    l.road_category,
    l.road_category_group,

    -- дефекты/элементы/объекты рядом
    l.road_defects,
    case
        when l.road_defects is null then null
        when l.road_defects = 'Не установлены' then false
        else true
    end as has_road_defects,

    l.road_elements,
    l.objects_near,

    -- условия на месте ДТП (из dtp_buffer, сохранены в dtp_locations)
    l.light_condition,
    l.local_weather_condition,
    l.road_surface_condition,

    -- =========================
    -- ПОГОДА (weather_conditions)
    -- =========================

    -- техполя
    w.observed_at as weather_observed_at,
    w.source as weather_source,
    (w.observed_at is not null) as has_weather,

    -- метрики
    w.temperature,
    w.relative_humidity,
    w.precipitation,
    w.rain,
    w.snowfall,
    w.cloud_cover,
    w.wind_speed,
    w.wind_gusts,

    -- -------------------------
    -- категоризации / флаги
    -- -------------------------

    -- Тип осадков
    case
        when coalesce(w.precipitation, 0) = 0 then 'Без осадков'
        when coalesce(w.rain, 0) > 0 and coalesce(w.snowfall, 0) > 0 then 'Смешанные'
        when coalesce(w.rain, 0) > 0 then 'Дождь'
        when coalesce(w.snowfall, 0) > 0 then 'Снег'
        else 'Осадки (прочее)'
    end as precipitation_type,

    -- Флаг осадков
    (coalesce(w.precipitation, 0) > 0)::int as precipitation_flag,

    -- Группы температуры
    case
        when w.temperature is null then null
        when w.temperature < -10 then 'сильный мороз'
        when w.temperature < 0 then 'мороз'
        when w.temperature < 10 then 'холодно'
        when w.temperature < 20 then 'умеренно'
        when w.temperature < 30 then 'тепло'
        else 'жара'
    end as temperature_group,

    -- Группы ветра (по скорости)
    case
        when w.wind_speed is null then null
        when w.wind_speed < 5 then 'слабый'
        when w.wind_speed < 10 then 'умеренный'
        when w.wind_speed < 15 then 'сильный'
        else 'очень сильный'
    end as wind_speed_group,

    -- Облачность
    case
        when w.cloud_cover is null then null
        when w.cloud_cover < 20 then 'ясно'
        when w.cloud_cover < 60 then 'переменная облачность'
        else 'пасмурно'
    end as cloudiness_level,

    -- Порывистость: порывы > скорость + 10 км/ч
    case
        when w.wind_gusts is null or w.wind_speed is null then null
        when w.wind_gusts > (w.wind_speed + 10) then true
        else false
    end as strong_wind_flag
from base b
left join public.dim_city c
    on c.city_id = b.city_id
left join public.dtp_locations l
    on l.kart_id = b.kart_id
left join public.weather_conditions w
    on w.city_id = b.city_id
   and w.observed_at = b.dtp_hour_utc
   and w.source = 'open_meteo_archive';


-- =================================================================================================
-- VIEW: public.vw_agg_vehicles
-- =================================================================================================
-- Агрегированная витрина транспортных средств на уровне ДТП.
--
-- Гранулярность:
--   1 строка = 1 ДТП (kart_id)
--
-- Источник данных:
--   public.vw_vehicles
--   (где 1 строка = 1 транспортное средство, участвовавшее в ДТП)
--
-- Логика:
--   Транспортные средства группируются по kart_id
--   (идентификатор ДТП), после чего рассчитываются
--   агрегированные показатели.
--
-- Назначение витрины:
--   Используется для аналитики транспортных средств
--   на уровне происшествия и соединяется
--   с витриной public.vw_dtp_loc_weather по kart_id.
--
-- Описание колонок витрины:
--   kart_id                    — идентификатор ДТП
--   vehicles_cnt               — общее количество транспортных средств в ДТП
--   cars_cnt                   — количество легковых транспортных средств
--   trucks_cnt                 — количество грузовых транспортных средств
--   buses_cnt                  — количество автобусов
--   motorcycles_cnt            — количество мототранспорта
--   non_motor_cnt              — количество немоторных транспортных средств
--   trailers_cnt               — количество прицепов
--   special_vehicles_cnt       — количество спецтехники
--   other_vehicles_cnt         — количество прочих / неизвестных транспортных средств
--   vehicles_with_issues_cnt   — количество ТС с техническими неисправностями
--   vehicles_no_issues_cnt     — количество ТС без технических неисправностей
--   front_drive_cnt            — количество ТС с передним приводом
--   rear_drive_cnt             — количество ТС с задним приводом
--   awd_drive_cnt              — количество ТС с полным приводом
--   other_drive_cnt            — количество ТС с иным типом привода
--   unknown_drive_cnt          — количество ТС, для которых тип привода не указан
--   vehicle_escape_cnt         — количество ТС, скрывшихся с места ДТП
-- =================================================================================================
create or replace view public.vw_agg_vehicles as
select
    v.kart_id,

    -- 1) Общее число ТС в ДТП
    count(v.vehicle_id)::int as vehicles_cnt,

    -- 2) Кол-во ТС по типу
    sum(case when v.vehicle_type_group = 'Легковые' then 1 else 0 end)::int as cars_cnt,
    sum(case when v.vehicle_type_group = 'Грузовые' then 1 else 0 end)::int as trucks_cnt,
    sum(case when v.vehicle_type_group = 'Автобусы' then 1 else 0 end)::int as buses_cnt,
    sum(case when v.vehicle_type_group = 'Мототранспорт' then 1 else 0 end)::int as motorcycles_cnt,
    sum(case when v.vehicle_type_group = 'Немоторные' then 1 else 0 end)::int as non_motor_cnt,
    sum(case when v.vehicle_type_group = 'Прицепы' then 1 else 0 end)::int as trailers_cnt,
    sum(case when v.vehicle_type_group = 'Спецтехника' then 1 else 0 end)::int as special_vehicles_cnt,
    sum(case when v.vehicle_type_group = 'Прочие/неизвестные' then 1 else 0 end)::int as other_vehicles_cnt,

    -- 3) Технические неисправности
    sum(case when v.technical_issue_flag = 'Есть неисправности' then 1 else 0 end)::int as vehicles_with_issues_cnt,
    sum(case when v.technical_issue_flag = 'Нет неисправностей' then 1 else 0 end)::int as vehicles_no_issues_cnt,

    -- 4) Тип привода
    sum(case when v.drive_type_std = 'Передний' then 1 else 0 end)::int as front_drive_cnt,
    sum(case when v.drive_type_std = 'Задний' then 1 else 0 end)::int as rear_drive_cnt,
    sum(case when v.drive_type_std = 'Полный' then 1 else 0 end)::int as awd_drive_cnt,
    sum(case when v.drive_type_std = 'Иное' then 1 else 0 end)::int as other_drive_cnt,
    sum(case when v.drive_type_std = 'Не указано' then 1 else 0 end)::int as unknown_drive_cnt,

    -- 5) ТС, скрывшиеся с места ДТП
    sum(case when v.vehicle_escape_group = 'Скрылось' then 1 else 0 end)::int as vehicle_escape_cnt
from public.vw_vehicles v
group by v.kart_id;


-- =================================================================================================
-- VIEW: public.vw_agg_participants
-- =================================================================================================
-- Агрегированная витрина участников ДТП на уровне происшествия.
--
-- Гранулярность:
--   1 строка = 1 ДТП (kart_id)
--
-- Источник данных:
--   public.vw_participants
--   (где 1 строка = 1 участник ДТП)
--
-- Логика:
--   Участники группируются по kart_id (идентификатор ДТП),
--   после чего рассчитываются агрегированные показатели.
--
-- Назначение витрины:
--   Используется для аналитики ДТП на уровне происшествия
--   и соединяется с витриной public.vw_dtp_loc_weather по kart_id.
--
-- Описание колонок витрины:
--   kart_id               — идентификатор ДТП
--   participants_cnt      — общее количество участников ДТП
--   drivers_cnt           — количество водителей
--   passengers_cnt        — количество пассажиров
--   pedestrians_cnt       — количество пешеходов
--   cyclists_cnt          — количество велосипедистов
--   others_cnt            — прочие участники
--   injured_cnt           — количество пострадавших (раненые + погибшие)
--   fatal_cnt             — количество погибших
--   alco_positive_cnt     — количество участников с признаком алкоголя
--   left_scene_cnt        — количество участников, скрывшихся с места ДТП
--                           ("Скрылся, разыскан" или "Скрылся, не установлен")
--   seatbelt_no_cnt       — количество участников без ремня безопасности
--                           (учитываются только водители и пассажиры)
-- =================================================================================================
create or replace view public.vw_agg_participants as
select
    p.kart_id,

    -- 1) Общее число участников в ДТП
    count(*)::int as participants_cnt,

    -- 2) Кол-во по ролям
    sum(case when p.role_group = 'Водитель' then 1 else 0 end)::int as drivers_cnt,
    sum(case when p.role_group = 'Пассажир' then 1 else 0 end)::int as passengers_cnt,
    sum(case when p.role_group = 'Пешеход' then 1 else 0 end)::int as pedestrians_cnt,
    sum(case when p.role_group = 'Велосипедист' then 1 else 0 end)::int as cyclists_cnt,
    sum(case when p.role_group = 'Прочие' then 1 else 0 end)::int as others_cnt,

    -- 3) Пострадавшие
    sum(case when p.injury_group in ('Раненый','Погибший') then 1 else 0 end)::int as injured_cnt,
    sum(case when p.injury_group = 'Погибший' then 1 else 0 end)::int as fatal_cnt,

    -- 4) Алкоголь обнаружен
    sum(
        case
            when p.alco is not null
             and p.alco <> '00'
            then 1 else 0
        end
    )::int as alco_positive_cnt,

    -- 5) Участники, скрывшиеся с места ДТП
    sum(
        case
            when p.left_scene_group in ('Скрылся, разыскан', 'Скрылся, не установлен')
            then 1 else 0
        end
    )::int as left_scene_cnt,

    -- 6) Ремень "Нет" (только водитель/пассажир)
    sum(
        case
            when p.role_group in ('Водитель','Пассажир')
             and p.safety_belt = 'Нет'
            then 1 else 0
        end
    )::int as seatbelt_no_cnt
from public.vw_participants p
group by p.kart_id;