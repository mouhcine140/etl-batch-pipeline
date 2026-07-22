-- Un client par ligne. Les commandes sans customer_id (~invités, cf.
-- README) sont regroupées sous l'identifiant 'UNKNOWN' plutôt
-- qu'exclues : elles représentent un vrai chiffre d'affaires et doivent
-- rester visibles dans les analyses agrégées par pays/période.

with lines as (

    select * from {{ ref('stg_online_retail') }}

),

customers as (

    select
        coalesce(cast(customer_id as varchar), 'UNKNOWN') as customer_id,
        arg_max(country, invoice_date) as country,
        min(invoice_date) as first_seen_at,
        max(invoice_date) as last_seen_at,
        count(distinct invoice_no) as n_orders
    from lines
    group by 1

)

select * from customers
