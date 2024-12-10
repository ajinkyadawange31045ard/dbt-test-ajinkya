{{ config(
    materialized='incremental',
    unique_key='src_ordernumber'
) }}

with ranked_data as (
    select
        c.dw_customer_id,
        sd.ordernumber as src_ordernumber,
        sd.orderdate,
        sd.requireddate,
        sd.shippeddate,
        sd.status,
        sd.customernumber as src_customernumber,
        sd.cancelleddate,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        case
            when ed.src_ordernumber is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        case
            when ed.src_ordernumber is not null then current_timestamp
            else ed.dw_update_timestamp
        end as dw_update_timestamp,
        row_number() over (order by sd.ordernumber) + coalesce(max(ed.dw_order_id) over (), 0) as dw_order_id
    from
        devstage.orders sd
    left join devdw.orders ed on sd.ordernumber = ed.src_ordernumber
    join {{ ref('customers') }} c on sd.customernumber = c.src_customernumber
    cross join etl_metadata.batch_control em
)

select *
from ranked_data

{% if is_incremental() %}
where
    ranked_data.src_ordernumber is not null  -- only process new or updated rows
{% endif %}
