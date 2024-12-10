{{ config(
    materialized='incremental',
    unique_key=['src_customerNumber', 'checkNumber']
) }}

-- Fetch the latest batch metadata
WITH final_data AS (
    SELECT
        sp.customerNumber as src_customerNumber,
        sp.checkNumber,
        sp.paymentDate,
        sp.amount,
        sp.create_timestamp as src_create_timestamp,
        coalesce(sp.update_timestamp, dw.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        case
            when dw.src_customerNumber is null then current_timestamp
            else dw.dw_create_timestamp
        end as dw_create_timestamp,
        case
            when dw.src_customerNumber is not null then current_timestamp
            else dw.dw_update_timestamp
        end as dw_update_timestamp,
        cl.dw_customer_id,
        row_number() over () + coalesce(max(dw.dw_payment_id) over (), 0) as dw_payment_id
    FROM devstage.payments AS sp
    CROSS JOIN etl_metadata.batch_control AS em
    LEFT JOIN devdw.payments AS dw
        ON sp.checkNumber = dw.checkNumber
    LEFT JOIN {{ ref('customers') }} AS cl
        ON sp.customerNumber = cl.src_customerNumber
    WHERE dw.checkNumber IS NULL
)

-- Insert or update records in the target table
SELECT *
FROM final_data

{% if is_incremental() %}
where
    final_data.src_customerNumber is not null  -- only process new or updated rows
{% endif %}
