-- Standardization per rules R014 (lowercase email), R015 (phone -> E.164-ish)
-- ERP full_name arrives as "Last, First" -- split into canonical first/last here
-- Validation flags computed per rules R009-R013 (see seed: column_rules)
with raw as (
    select * from {{ source('bronze', 'erp_customers_raw') }}
),

standardized as (
    select
        source_system,
        source_record_id,
        nullif(trim(regexp_replace({{ proper_case("split_part(full_name, ',', 2)") }}, '\s+', ' ')), '') as first_name,
        nullif(trim(regexp_replace({{ proper_case("split_part(full_name, ',', 1)") }}, '\s+', ' ')), '') as last_name,
        -- R014: lowercase email
        lower(nullif(trim(email_addr), '')) as email,
        -- R015: standardize phone to +1XXXXXXXXXX
        case
            when regexp_replace(coalesce(contact_phone, ''), '[^0-9]', '', 'g') != ''
            then '+1' || right(regexp_replace(contact_phone, '[^0-9]', '', 'g'), 10)
            else null
        end as phone,
        nullif(trim(addr1), '') as address_line1,
        nullif(trim(addr2), '') as address_line2,
        nullif(trim(city_code), '') as city,
        upper(nullif(trim(state_code), '')) as state_code,
        nullif(trim(postal_code), '') as postal_code,
        upper(nullif(trim(country_code), '')) as country_code,
        cast(null as date) as source_created_date,
        try_cast(last_updated as date) as source_modified_date
    from raw
)

select
    *,
    -- R009: full_name required (either side missing after split counts as missing)
    (first_name is null or last_name is null) as err_missing_last_name,
    false as err_missing_first_name,
    -- R010: email format
    (email is null or not regexp_matches(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$')) as err_invalid_email,
    -- R011: phone required
    (phone is null) as err_missing_phone,
    -- R012: state reference check
    (state_code is null or state_code not in (select state_code from {{ ref('ref_state_codes') }})) as err_invalid_state,
    -- R013: country reference check
    (country_code is null or country_code not in (select country_code from {{ ref('ref_country_codes') }})) as err_invalid_country
from standardized
