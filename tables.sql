drop table if exists records_5min;
with records_overtoom as (
select
    *
from records
where insert_ts > '2024-03-01 00:00'
and (device_id > 30 and device_id < 41)
)
select time,
--         hour,
--        minute_of_hour,
--        minute_of_day,
--        epoch,
--        day_suffix,
--        day_name,
--        day_of_week,
--        day_of_month,
--        mmddyyyy,
--        weekend_indr,
       dev.id as device_id,
--        dev.location_nickname,
--        r.insert_ts,
       avg(r.mass_concentration_pm1p0) as mass_concentration_pm1p0,
       avg(r.mass_concentration_pm2p5) as mass_concentration_pm2p5,
       avg(r.mass_concentration_pm4p0) as mass_concentration_pm4p0,
       avg(r.mass_concentration_pm10p0) as mass_concentration_pm10p0,
       avg(case when r.voc_index = 0 then Null else r.ambient_humidity end) as ambient_humidity,
       avg(case when r.voc_index = 0 then Null else r.ambient_temperature end) as ambient_temperature,
       avg(case when r.voc_index = 0 then Null else r.voc_index end) as voc_index,
       avg(case when r.voc_index = 0 then Null else r.nox_index end) as nox_index
into records_5min
from dates_5min d
cross join devices dev
left join records_overtoom r on insert_ts >= d.time and insert_ts < d.time_plus_1 and dev.id = r.device_id
where dev.id > 30 and dev.id < 41
--   and r.insert_ts is not null
group by time, dev.id
order by time, dev.id
;

drop table if exists records_2min;
with records_overtoom as (
select
    *
from records
where insert_ts > '2024-03-01 00:00'
and (device_id > 30 and device_id < 41)
)
select time,
--         hour,
--        minute_of_hour,
--        minute_of_day,
--        epoch,
--        day_suffix,
--        day_name,
--        day_of_week,
--        day_of_month,
--        mmddyyyy,
--        weekend_indr,
       dev.id as device_id,
--        dev.location_nickname,
--        r.insert_ts,
       avg(r.mass_concentration_pm1p0) as mass_concentration_pm1p0,
       avg(r.mass_concentration_pm2p5) as mass_concentration_pm2p5,
       avg(r.mass_concentration_pm4p0) as mass_concentration_pm4p0,
       avg(r.mass_concentration_pm10p0) as mass_concentration_pm10p0,
       avg(case when r.voc_index = 0 then Null else r.ambient_humidity end) as ambient_humidity,
       avg(case when r.voc_index = 0 then Null else r.ambient_temperature end) as ambient_temperature,
       avg(case when r.voc_index = 0 then Null else r.voc_index end) as voc_index,
       avg(case when r.voc_index = 0 then Null else r.nox_index end) as nox_index
into records_2min
from dates_2min d
cross join devices dev
left join records_overtoom r on insert_ts >= d.time and insert_ts < d.time_plus_1 and dev.id = r.device_id
where dev.id > 30 and dev.id < 41
--   and r.insert_ts is not null
group by time, dev.id
order by time, dev.id
;


drop table if exists  records_5min_clean;

with records_clean as (select time,
       device_id,
       coalesce(mass_concentration_pm1p0, avg(mass_concentration_pm1p0) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm1p0,
       coalesce(mass_concentration_pm2p5,              avg(mass_concentration_pm2p5) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm2p5,
       coalesce(mass_concentration_pm4p0,       avg(mass_concentration_pm4p0) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm4p0,
       coalesce(mass_concentration_pm10p0,       avg(mass_concentration_pm10p0) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm10p0,
       coalesce(ambient_humidity,    avg(ambient_humidity) over (partition by device_id order by time rows between 2 preceding and 2 following)) as ambient_humidity,
       coalesce(ambient_temperature,    avg(ambient_temperature) over (partition by device_id order by time rows between 2 preceding and 2 following)) as ambient_temperature,
       coalesce(voc_index,    avg(voc_index) over (partition by device_id order by time rows between 2 preceding and 2 following)) as voc_index,
       coalesce(nox_index,    avg(nox_index) over (partition by device_id order by time rows between 2 preceding and 2 following)) as nox_index
from records_5min)
select r.*,
               d5.hour,
        d5.minute_of_hour,
        day_of_week,
        day_name
into records_5min_clean
from records_clean as r
left join dates_5min as d5 on r.time = d5.time
where mass_concentration_pm1p0 is not null
;

select device_id, count(*)
from records_5min_clean
group by device_id
;

-- 2 minutes

drop table if exists  records_2min_clean;

with records_clean as (select time,
       device_id,
       coalesce(mass_concentration_pm1p0, avg(mass_concentration_pm1p0) over (partition by device_id order by time rows between 5 preceding and 5 following)) as mass_concentration_pm1p0,
       coalesce(mass_concentration_pm2p5,              avg(mass_concentration_pm2p5) over (partition by device_id order by time rows between 5 preceding and 5 following)) as mass_concentration_pm2p5,
       coalesce(mass_concentration_pm4p0,       avg(mass_concentration_pm4p0) over (partition by device_id order by time rows between 5 preceding and 5 following)) as mass_concentration_pm4p0,
       coalesce(mass_concentration_pm10p0,       avg(mass_concentration_pm10p0) over (partition by device_id order by time rows between 5 preceding and 5 following)) as mass_concentration_pm10p0,
       coalesce(ambient_humidity,    avg(ambient_humidity) over (partition by device_id order by time rows between 5 preceding and 5 following)) as ambient_humidity,
       coalesce(ambient_temperature,    avg(ambient_temperature) over (partition by device_id order by time rows between 5 preceding and 5 following)) as ambient_temperature,
       coalesce(voc_index,    avg(voc_index) over (partition by device_id order by time rows between 5 preceding and 5 following)) as voc_index,
       coalesce(nox_index,    avg(nox_index) over (partition by device_id order by time rows between 5 preceding and 5 following)) as nox_index
from records_2min)
select r.*,
               d5.hour,
        d5.minute_of_hour,
        day_of_week,
        day_name
into records_2min_clean
from records_clean as r
left join dates_2min as d5 on r.time = d5.time
where mass_concentration_pm1p0 is not null
;

select device_id, count(*)
from records_2min_clean
group by device_id
;

drop table if exists  records_5min_smooth;

with records_clean as (select time,
       device_id,
       avg(mass_concentration_pm1p0) over (partition by device_id order by time rows between 3 preceding and current row ) as mass_concentration_pm1p0,
       avg(mass_concentration_pm2p5) over (partition by device_id order by time rows between 3 preceding and current row) as mass_concentration_pm2p5,
       avg(mass_concentration_pm4p0) over (partition by device_id order by time rows between 3 preceding and current row) as mass_concentration_pm4p0,
       avg(mass_concentration_pm10p0) over (partition by device_id order by time rows between 3 preceding and current row) as mass_concentration_pm10p0,
       avg(ambient_humidity) over (partition by device_id order by time rows between 3 preceding and current row) as ambient_humidity,
       avg(ambient_temperature) over (partition by device_id order by time rows between 3 preceding and current row) as ambient_temperature,
       avg(voc_index) over (partition by device_id order by time rows between 3 preceding and current row) as voc_index,
       avg(nox_index) over (partition by device_id order by time rows between 3 preceding and current row) as nox_index
from records_5min)
select r.*,
               d5.hour,
        d5.minute_of_hour,
        day_of_week,
        day_name
into records_5min_smooth
from records_clean as r
left join dates_5min as d5 on r.time = d5.time
where mass_concentration_pm1p0 is not null
;

select device_id, count(*)
from records_5min_smooth
group by device_id
;

drop table if exists  records_2min_smooth;

with records_clean as (select time,
       device_id,
       avg(mass_concentration_pm1p0) over (partition by device_id order by time rows between 3 preceding and current row ) as mass_concentration_pm1p0,
       avg(mass_concentration_pm2p5) over (partition by device_id order by time rows between 3 preceding and current row) as mass_concentration_pm2p5,
       avg(mass_concentration_pm4p0) over (partition by device_id order by time rows between 3 preceding and current row) as mass_concentration_pm4p0,
       avg(mass_concentration_pm10p0) over (partition by device_id order by time rows between 3 preceding and current row) as mass_concentration_pm10p0,
       avg(ambient_humidity) over (partition by device_id order by time rows between 3 preceding and current row) as ambient_humidity,
       avg(ambient_temperature) over (partition by device_id order by time rows between 3 preceding and current row) as ambient_temperature,
       avg(voc_index) over (partition by device_id order by time rows between 3 preceding and current row) as voc_index,
       avg(nox_index) over (partition by device_id order by time rows between 3 preceding and current row) as nox_index
from records_2min)
select r.*,
               d5.hour,
        d5.minute_of_hour,
        day_of_week,
        day_name
into records_2min_smooth
from records_clean as r
left join dates_2min as d5 on r.time = d5.time
where mass_concentration_pm1p0 is not null
;

select device_id, count(*)
from records_2min_smooth
group by device_id
;

drop table if exists  records_5min_pm10;

with records_clean as (select time,
       device_id,
       coalesce(mass_concentration_pm1p0, avg(mass_concentration_pm1p0) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm1p0,
       coalesce(mass_concentration_pm2p5,              avg(mass_concentration_pm2p5) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm2p5,
       coalesce(mass_concentration_pm4p0,       avg(mass_concentration_pm4p0) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm4p0,
       coalesce(mass_concentration_pm10p0,       avg(mass_concentration_pm10p0) over (partition by device_id order by time rows between 2 preceding and 2 following)) as mass_concentration_pm10p0,
       coalesce(ambient_humidity,    avg(ambient_humidity) over (partition by device_id order by time rows between 2 preceding and 2 following)) as ambient_humidity,
       coalesce(ambient_temperature,    avg(ambient_temperature) over (partition by device_id order by time rows between 2 preceding and 2 following)) as ambient_temperature,
       coalesce(voc_index,    avg(voc_index) over (partition by device_id order by time rows between 2 preceding and 2 following)) as voc_index,
       coalesce(nox_index,    avg(nox_index) over (partition by device_id order by time rows between 2 preceding and 2 following)) as nox_index
from records_5min),
    per_device as (
        select time,
        avg(case when device_id = 31 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d31,
        avg(case when device_id = 32 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d32,
        avg(case when device_id = 33 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d33,
        avg(case when device_id = 34 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d34,
        avg(case when device_id = 35 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d35,
        avg(case when device_id = 36 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d36,
        avg(case when device_id = 37 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d37,
        avg(case when device_id = 38 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d38,
        avg(case when device_id = 39 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d39,
        avg(case when device_id = 40 then mass_concentration_pm10p0 end) as mass_concentration_pm10p0_d40
        from records_clean
        where mass_concentration_pm10p0 is not null
        group by time
    )

select r.*,
               d5.hour,
        d5.minute_of_hour,
        day_of_week,
        day_name
into records_5min_pm10
from per_device as r
left join dates_5min as d5 on r.time = d5.time
;

select count(*)
from records_5min_pm10
;
