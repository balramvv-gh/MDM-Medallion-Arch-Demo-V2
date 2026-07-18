-- Borderline fuzzy-match candidates (TAU_LOW <= similarity < TAU_HIGH, see
-- scripts/generate_matches.py) that were NOT auto-merged into gold. Presented
-- here for a data steward to confirm ("yes, same customer -- merge them") or
-- reject ("no, coincidence -- keep them separate").
--
-- Mirrors exceptions_queue.sql's pattern deliberately: this table is a full
-- point-in-time snapshot of every borderline pair this pipeline run found.
-- LIVE review status lives in stewardship.match_review_overrides (joined at
-- query time by the API, same as exception_status_overrides), not here --
-- so a pair a steward already decided on keeps showing its history even
-- after this table gets rebuilt by the next `dbt run`.
with candidates as (
    select * from {{ source('gold_prep', 'match_review_candidates') }}
),

side_a as (
    select * from {{ ref('silver_customers') }}
),

side_b as (
    select * from {{ ref('silver_customers') }}
)

select
    c.pair_id,
    c.similarity_score,
    c.blocking_key,
    a.source_system as source_system_a,
    a.source_record_id as source_record_id_a,
    a.first_name as first_name_a,
    a.last_name as last_name_a,
    a.email as email_a,
    a.phone as phone_a,
    a.address_line1 as address_line1_a,
    a.city as city_a,
    a.state_code as state_code_a,
    a.postal_code as postal_code_a,
    b.source_system as source_system_b,
    b.source_record_id as source_record_id_b,
    b.first_name as first_name_b,
    b.last_name as last_name_b,
    b.email as email_b,
    b.phone as phone_b,
    b.address_line1 as address_line1_b,
    b.city as city_b,
    b.state_code as state_code_b,
    b.postal_code as postal_code_b,
    'pending' as review_status,
    c.generated_ts as queued_ts
from candidates c
join side_a a on a.source_system = c.source_system_a and a.source_record_id = c.source_record_id_a
join side_b b on b.source_system = c.source_system_b and b.source_record_id = c.source_record_id_b
