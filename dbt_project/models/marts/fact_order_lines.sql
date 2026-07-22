-- Grain de la table : une ligne de facture. Les annulations (is_cancellation
-- = true, quantity négatif) sont conservées telles quelles plutôt que
-- nettées contre la commande d'origine : reconstituer ce lien demanderait
-- un rapprochement par (customer_id, stock_code) non fiable à 100% sur ce
-- dataset (pas d'identifiant de commande d'origine sur les avoirs), et le
-- masquer fausserait le chiffre d'affaires réellement encaissé.

with lines as (

    select * from {{ ref('stg_online_retail') }}

)

select
    order_line_id,
    invoice_no,
    stock_code,
    coalesce(cast(customer_id as varchar), 'UNKNOWN') as customer_id,
    invoice_date_day as date_day,
    invoice_date,
    quantity,
    unit_price,
    line_amount,
    is_cancellation

from lines
