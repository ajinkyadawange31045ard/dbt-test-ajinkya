{{ config(
    schema='devdw',
    materialized='incremental',
    unique_key='productline'
) }}

WITH source_data AS (
    SELECT
        productline,
        create_timestamp,
        update_timestamp,
        1001 AS etl_batch_no,
        TO_DATE('2001-01-01', 'YYYY-MM-DD') AS etl_batch_date
    FROM
        devstage.productlines
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
        devdw.productlines  -- Refers to the current state of the table created by dbt
),

ranked_data AS (
    SELECT
        source_data.productline,
        ROW_NUMBER() OVER (ORDER BY source_data.productline) + COALESCE(MAX(existing_data.dw_product_line_id) OVER (), 0) AS dw_product_line_id,
        CASE
            WHEN source_data.productline IS NOT NULL AND existing_data.productline IS NULL THEN source_data.create_timestamp
            ELSE existing_data.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(source_data.update_timestamp, existing_data.src_update_timestamp) AS src_update_timestamp,
        1001 AS etl_batch_no,
        TO_DATE('2001-01-01', 'YYYY-MM-DD') AS etl_batch_date,
        CASE
            WHEN source_data.productline IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE existing_data.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp
    FROM
        source_data
    LEFT JOIN existing_data ON source_data.productline = existing_data.productline
)

SELECT *
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.productline IS NOT NULL  -- Only process new or updated rows
{% endif %}
