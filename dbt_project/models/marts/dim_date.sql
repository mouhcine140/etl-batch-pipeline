-- Dimension date générée par spine plutôt que dérivée des seules dates
-- présentes dans les faits, pour couvrir aussi les jours sans vente
-- (utile pour des agrégations "par jour" sans trous côté BI).

with bounds as (

    select
        min(invoice_date_day) as min_date,
        max(invoice_date_day) as max_date
    from {{ ref('stg_online_retail') }}

),

spine as (

    select unnest(generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval 1 day
    )) as date_day

)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week,
    extract(week from date_day) as iso_week,
    extract(dow from date_day) in (0, 6) as is_weekend
from spine
