-- Couche staging : renommage, typage, et calcul des colonnes dérivées de
-- base. Aucune agrégation ici, une ligne source = une ligne en sortie.
--
-- Le dataset source n'a pas de numéro de ligne de facture : (invoice_no,
-- stock_code, invoice_date) suffit presque toujours à identifier une ligne,
-- mais pas toujours - un même produit peut apparaître deux fois sur la même
-- facture à la même seconde avec une quantité différente (ex. facture
-- 536381 / stock_code 71270, deux lignes 1 et 3 unités). Sans le
-- row_number() ci-dessous, le hash de clé produirait un doublon et casserait
-- l'unicité de order_line_id dans fact_order_lines.

with source as (

    select * from {{ source('raw', 'raw_online_retail') }}

),

renamed as (

    select
        invoice_no,
        stock_code,
        trim(description) as description,
        quantity,
        invoice_date,
        cast(invoice_date as date) as invoice_date_day,
        unit_price,
        customer_id,
        country,
        is_cancellation,
        round(quantity * unit_price, 2) as line_amount,
        row_number() over (
            partition by invoice_no, stock_code, invoice_date
            order by unit_price, quantity
        ) as line_seq

    from source

),

with_key as (

    select
        *,
        md5(
            invoice_no || stock_code || cast(invoice_date as varchar) || cast(line_seq as varchar)
        ) as order_line_id
    from renamed

)

select * from with_key
