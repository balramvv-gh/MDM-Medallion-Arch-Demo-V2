-- Records that failed one or more validation rules land here for a data steward
-- to review, remediate (with AI assist), and re-submit into the pipeline.
select
    md5(source_system || '-' || source_record_id) as exception_id,
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
    reject_reasons,
    'open' as remediation_status,   -- open | in_review | resolved | rejected
    current_timestamp as queued_ts
from {{ ref('silver_all_staged') }}
where is_invalid
