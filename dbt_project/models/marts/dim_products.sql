-- Un produit (stock_code) par ligne. La description texte n'est pas
-- garantie stable dans le temps sur ce dataset source (fautes de frappe
-- corrigées d'un jour sur l'autre pour un même stock_code) : on garde la
-- plus récente via arg_max plutôt que la première rencontrée.

with lines as (

    select * from {{ ref('stg_online_retail') }}
    where not is_cancellation

),

products as (

    select
        stock_code,
        arg_max(description, invoice_date) as description,
        min(unit_price) as min_unit_price,
        max(unit_price) as max_unit_price,
        count(*) as n_times_sold
    from lines
    group by 1

)

select * from products
