-- Standardization applied per rules R006 (proper-case names), R007 (trim), R008 (phone -> E.164-ish)
-- Validation flags computed per rules R001-R005 (see seed: column_rules)
with raw as (
    select * from {{ source('bronze', 'crm_customers_raw') }}
),

standardized as (
    select
        source_system,
        source_record_id,
        -- R006/R007: trim + proper case
        nullif(trim(regexp_replace({{ proper_case('first_name') }}, '\s+', ' ')), '') as first_name,
        nullif(trim(regexp_replace({{ proper_case('last_name') }}, '\s+', ' ')), '') as last_name,
        nullif(trim(email), '') as email,
        -- R008: standardize phone to +1XXXXXXXXXX
        case
            when regexp_replace(coalesce(phone, ''), '[^0-9]', '', 'g') != ''
            then '+1' || right(regexp_replace(phone, '[^0-9]', '', 'g'), 10)
            else null
        end as phone,
        nullif(trim(address_line1), '') as address_line1,
        cast(null as varchar) as address_line2,
        nullif(trim(city), '') as city,
        upper(nullif(trim(state), '')) as state_code,
        nullif(trim(zip), '') as postal_code,
        upper(nullif(trim(country), '')) as country_code,
        try_cast(created_date as date) as source_created_date,
        try_cast(modified_date as date) as source_modified_date
    from raw
)

select
    *,
    -- R001/R002: required fields
    (first_name is null) as err_missing_first_name,
    (last_name is null) as err_missing_last_name,
    -- R003: email format
    (email is null or not regexp_matches(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$')) as err_invalid_email,
    (phone is null) as err_missing_phone,
    -- R004: state reference check
    (state_code is null or state_code not in (select state_code from {{ ref('ref_state_codes') }})) as err_invalid_state,
    -- R005: country reference check
    (country_code is null or country_code not in (select country_code from {{ ref('ref_country_codes') }})) as err_invalid_country
from standardized
