{{ config(
    materialized='incremental',
    unique_key=['src_orderNumber', 'src_productCode']
) }}

-- Fetch the latest batch metadata
WITH ranked_data AS (
    SELECT
        st.orderNumber as src_orderNumber,
        st.productCode as src_productCode,
        st.quantityOrdered,
        st.priceEach,
        st.orderLineNumber,
        st.create_timestamp as src_create_timestamp,
        coalesce(st.update_timestamp, dw.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        case
            when dw.src_ordernumber is null then current_timestamp
            else dw.dw_create_timestamp
        end as dw_create_timestamp,
        case
            when dw.src_ordernumber is not null then current_timestamp
            else dw.dw_update_timestamp
        end as dw_update_timestamp,
        o.dw_order_id,
        p.dw_product_id,
        row_number() over() + coalesce(max(dw.dw_orderdetail_id)over(),0) dw_orderdetail_id
    FROM devstage.orderdetails AS st
    CROSS JOIN etl_metadata.batch_control AS em
    LEFT JOIN devdw.orderdetails AS dw
        ON st.orderNumber = dw.src_orderNumber
        AND st.productCode = dw.src_productCode
    left join {{ref("orders")}} o  on st.ordernumber=o.src_ordernumber
    left join {{ref("products")}} p on st.productcode=p.src_productcode
    WHERE dw.src_orderNumber IS NULL
)
select * from ranked_data

{% if is_incremental() %}
where
    ranked_data.src_ordernumber is not null  -- only process new or updated rows
{% endif %}
