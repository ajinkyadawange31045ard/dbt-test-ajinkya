
{{ config(
    materialized='incremental',
    unique_key='productline'
) }}

WITH ranked_data AS (
    SELECT
        sd.productline,
        ROW_NUMBER() OVER (ORDER BY sd.productline) + COALESCE(MAX(ed.dw_product_line_id) OVER (), 0) AS dw_product_line_id,
        sd.create_timestamp as src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        case
            when ed.productline is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        case
            when ed.productline is not null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_update_timestamp
    FROM
        devstage.productlines sd
    LEFT JOIN devdw.productlines ed ON sd.productline = ed.productline
    CROSS JOIN etl_metadata.batch_control em
)

SELECT * FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.productline IS NOT NULL   -- Only process new or updated rows
{% endif %}
