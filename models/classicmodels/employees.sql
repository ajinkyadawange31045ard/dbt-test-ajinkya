{{ config(
    materialized='incremental',
    unique_key='employeenumber'
) }}

{% set batch_no = var('batch_no', 1) %}
{% set batch_date = var('batch_date', '1970-01-01') %}

-- Step 1: Source and existing data
with source_data as (
    select
        sd.employeenumber,
        sd.lastname,
        sd.firstname,
        sd.extension,
        sd.email,
        sd.officecode,
        sd.reportsto,
        sd.jobtitle,
        o.dw_office_id,
        sd.create_timestamp as src_create_timestamp,
        sd.update_timestamp as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date
    from devstage.employees sd
    join {{ ref('offices') }} o on sd.officecode = o.officecode
    cross join etl_metadata.batch_control em 
),

existing_data as (
    select *
    from {{ this }}
),

-- Step 2: Update existing rows
updates as (
    select
        b.employeenumber,
        s.lastname,
        s.firstname,
        s.extension,
        s.email,
        s.officecode,
        s.reportsto,
        s.jobtitle,
        s.dw_office_id,
        b.src_create_timestamp,  -- Preserve original create timestamp
        s.src_update_timestamp,
        b.dw_create_timestamp,
        current_timestamp as dw_update_timestamp,
        b.dw_employee_id,  -- Preserve existing employee ID
        b.dw_reporting_employee_id,  -- Preserve existing reporting relationship
        s.etl_batch_no,
        s.etl_batch_date
    from source_data s
    join existing_data b on s.employeenumber = b.employeenumber
),

-- Step 3: Insert new rows
inserts as (
    select
        s.employeenumber,
        s.lastname,
        s.firstname,
        s.extension,
        s.email,
        s.officecode,
        s.reportsto,
        s.jobtitle,
        s.dw_office_id,
        s.src_create_timestamp,
        s.src_update_timestamp,
        current_timestamp as dw_create_timestamp,
        current_timestamp as dw_update_timestamp,
        row_number() over (order by s.employeenumber) + coalesce(max(b.dw_employee_id) over (), 0) as dw_employee_id,
        cast(null as integer) as dw_reporting_employee_id,  -- Placeholder for reporting relationship
        s.etl_batch_no,
        s.etl_batch_date
    from source_data s
    left join existing_data b on s.employeenumber = b.employeenumber
    where b.employeenumber is null
),

-- Combine updates and inserts
combined as (
    select * from updates
    union all
    select * from inserts
),

-- Step 4: Update reporting relationships
reporting_relationships as (
    select
        c.employeenumber,
        c.lastname,
        c.firstname,
        c.extension,
        c.email,
        c.officecode,
        c.reportsto,
        c.jobtitle,
        c.dw_office_id,
        c.src_create_timestamp,
        c.src_update_timestamp,
        c.dw_create_timestamp,
        c.dw_update_timestamp,
        c.dw_employee_id,
        coalesce(dw2.dw_employee_id, c.dw_reporting_employee_id) as dw_reporting_employee_id, -- Correct reporting relationship
        c.etl_batch_no,
        c.etl_batch_date
    from combined c
    left join combined dw2 on c.reportsto = dw2.employeenumber -- Match `reportsto` with `employeenumber`
)

-- Final output
select *
from reporting_relationships

{% if is_incremental() %}
where reporting_relationships.employeenumber IS NOT NULL  -- Only process new or updated rows

{% endif %}
