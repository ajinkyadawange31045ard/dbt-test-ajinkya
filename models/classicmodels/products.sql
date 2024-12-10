{{ config(
    materialized='incremental',
    unique_key='src_productcode'
) }}

WITH ranked_data AS (
    SELECT
        sd.productcode AS src_productcode,
        sd.productname,
        sd.productline,
        sd.productscale,
        sd.productvendor,
        sd.quantityinstock,
        sd.buyprice,
        sd.msrp,
        COALESCE(pl.dw_product_line_id, ed.dw_product_line_id) AS dw_product_line_id,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        case
            when ed.src_productcode is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        case
            when ed.src_productcode is not null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_update_timestamp,
        ROW_NUMBER() OVER (ORDER BY sd.productcode) + COALESCE(MAX(ed.dw_product_id) OVER (), 0) AS dw_product_id
    FROM
        devstage.products sd
    LEFT JOIN devdw.products ed ON sd.productcode = ed.src_productcode
    LEFT JOIN {{ ref('productlines') }} pl ON sd.productline = pl.productline
    CROSS JOIN etl_metadata.batch_control em
)

SELECT *
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.productline IS NOT NULL  -- Only process new or updated rows
{% endif %}

