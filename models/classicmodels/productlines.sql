{{ config(
    materialized='incremental',
    unique_key='productline'
) }}

WITH etl_metadata AS (
    SELECT
        etl_batch_no,
        etl_batch_date
    FROM
        etl_metadata.batch_control  -- Replace with your actual table that holds batch info
),

source_data AS (
    SELECT
        a.productline,
        a.create_timestamp,
        a.update_timestamp,
        b.etl_batch_no,
        b.etl_batch_date
    FROM
        devstage.productlines a cross join etl_metadata b
),

existing_data AS (
    SELECT
        productline,
        src_create_timestamp,
        src_update_timestamp,
        etl_batch_no,
        etl_batch_date,
        dw_update_timestamp,
        dw_product_line_id
    FROM
        {{ this }}  -- Refers to the current state of the table created by dbt
),

ranked_data AS (
    SELECT
        sd.productline,
        ROW_NUMBER() OVER (ORDER BY sd.productline) + COALESCE(MAX(ed.dw_product_line_id) OVER (), 0) AS dw_product_line_id,
        CASE
            WHEN sd.productline IS NOT NULL AND ed.productline IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        sd.etl_batch_no,
        sd.etl_batch_date,
        CASE
            WHEN sd.productline IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp
    FROM
        source_data sd
    LEFT JOIN existing_data ed ON sd.productline = ed.productline
)

SELECT *
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.productline IS NOT NULL  -- Only process new or updated rows
{% endif %}
