-- Silver = canonical, validated customer records only. Invalid records never reach
-- this table; they are routed to exceptions_queue for the stewardship app instead.
select
    source_system,
    source_record_id,
    first_name,
    last_name,
    email,
    phone,
    address_line1,
    address_line2,
    city,
    state_code,
    postal_code,
    country_code,
    source_created_date,
    source_modified_date,
    current_timestamp as silver_load_ts
from {{ ref('silver_all_staged') }}
where not is_invalid
