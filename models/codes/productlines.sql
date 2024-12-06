{{ config(
    materialized='incremental',
    unique_key='productLine',
) }}

-- Subquery to fetch batch metadata
with batch_metadata as (
    select 
        etl_batch_no::integer as etl_batch_no, -- Ensure etl_batch_no is castable to integer
        etl_batch_date::date as etl_batch_date -- Ensure etl_batch_date is castable to date
    from etl_metadata.batch_control
    order by etl_batch_date desc
    limit 1
)

-- Combined updated and inserted records in a single SELECT
select
    case
        when dw.productLine is not null then st.productLine -- Existing record to update
        else st.productLine -- New record to insert
    end as productLine,
    -- Handle updated columns
    case
        when dw.productLine is not null then st.update_timestamp
        else st.create_timestamp
    end as update_timestamp,
    current_timestamp as dw_update_timestamp,
    bm.etl_batch_no,
    bm.etl_batch_date
from {{ ref('devstage_productlines') }} as st
left join {{ this }} as dw
    on st.productLine = dw.productLine
cross join batch_metadata as bm
-- Add condition to select only new or changed records for incremental load
where dw.productLine is null -- New records
    or st.update_timestamp > dw.update_timestamp -- Changed records
