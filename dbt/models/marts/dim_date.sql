-- Gold: calendar dimension, one row per day 2020-2030, generated (no source table).
-- Same design as dim.Date in Project 1, including the Unknown (-1) row.
--
-- The only model with DIALECT differences: DuckDB and Databricks (Spark SQL) name
-- their date functions differently. dbt's Jinja picks the right version for the target.

with

{% if target.type == 'databricks' %}

days as (
    select explode(sequence(date '2020-01-01', date '2030-12-31', interval 1 day)) as full_date
),
holidays as (
    select * from values
        (1, 1, 'Ano Novo'), (4, 25, 'Dia da Liberdade'), (5, 1, 'Dia do Trabalhador'),
        (6, 10, 'Dia de Portugal'), (8, 15, 'Assunção de Nossa Senhora'),
        (10, 5, 'Implantação da República'), (11, 1, 'Dia de Todos os Santos'),
        (12, 1, 'Restauração da Independência'), (12, 8, 'Imaculada Conceição'),
        (12, 25, 'Natal')
        as t(month_number, day_of_month, holiday_name)
),
calendar as (
    select
        cast(date_format(full_date, 'yyyyMMdd') as int)  as date_key,
        full_date,
        day(full_date)                                   as day_of_month,
        date_format(full_date, 'EEEE')                   as day_name,
        (dayofweek(full_date) + 5) % 7 + 1               as day_of_week,    -- Spark: 1 = Sunday → 1 = Monday
        dayofweek(full_date) in (1, 7)                   as is_weekend,
        weekofyear(full_date)                            as iso_week,
        month(full_date)                                 as month_number,
        date_format(full_date, 'MMMM')                   as month_name,
        date_format(full_date, 'yyyy-MM')                as year_month,
        quarter(full_date)                               as quarter_number,
        concat('Q', quarter(full_date))                  as quarter_name,
        year(full_date)                                  as year,
        h.holiday_name is not null                       as is_holiday,
        h.holiday_name
    from days
    left join holidays h
      on h.month_number = month(full_date) and h.day_of_month = day(full_date)
)

{% else %}

days as (
    select cast(d as date) as full_date
    from range(date '2020-01-01', date '2031-01-01', interval 1 day) as t(d)
),
holidays (month_number, day_of_month, holiday_name) as (
    values (1, 1, 'Ano Novo'), (4, 25, 'Dia da Liberdade'), (5, 1, 'Dia do Trabalhador'),
           (6, 10, 'Dia de Portugal'), (8, 15, 'Assunção de Nossa Senhora'),
           (10, 5, 'Implantação da República'), (11, 1, 'Dia de Todos os Santos'),
           (12, 1, 'Restauração da Independência'), (12, 8, 'Imaculada Conceição'),
           (12, 25, 'Natal')
),
calendar as (
    select
        cast(strftime(full_date, '%Y%m%d') as integer)  as date_key,
        full_date,
        day(full_date)                                  as day_of_month,
        dayname(full_date)                              as day_name,
        isodow(full_date)                               as day_of_week,    -- 1 = Monday
        isodow(full_date) in (6, 7)                     as is_weekend,
        weekofyear(full_date)                           as iso_week,
        month(full_date)                                as month_number,
        monthname(full_date)                            as month_name,
        strftime(full_date, '%Y-%m')                    as year_month,
        quarter(full_date)                              as quarter_number,
        'Q' || quarter(full_date)                       as quarter_name,
        year(full_date)                                 as year,
        h.holiday_name is not null                      as is_holiday,
        h.holiday_name
    from days
    left join holidays h
      on h.month_number = month(full_date) and h.day_of_month = day(full_date)
)

{% endif %}

select * from calendar
union all
select -1, date '1900-01-01', 0, 'Unknown', 0, false, 0, 0, 'Unknown', '1900-01', 0, 'Q0', 1900, false, null
