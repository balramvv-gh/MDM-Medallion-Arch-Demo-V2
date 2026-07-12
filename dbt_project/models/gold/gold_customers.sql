-- Survivorship rule (demo-scope): the most-recently-modified source record wins,
-- with CRM preferred as tiebreaker when modified dates are equal or missing.
-- (A production system would typically apply survivorship per-attribute rather
--  than per-record; this is intentionally record-level to keep the demo legible.)
with candidates as (
    select * from {{ ref('gold_match_candidates') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by match_group_id
            order by source_modified_date desc nulls last,
                     case source_system when 'CRM' then 1 else 2 end
        ) as survivor_rank
    from candidates
),

group_stats as (
    select
        match_group_id,
        count(distinct source_system) as source_system_count
    from candidates
    group by match_group_id
)

select
    'GOLD-' || lpad(cast(r.match_group_id as varchar), 5, '0') as golden_id,
    r.first_name,
    r.last_name,
    r.email,
    r.phone,
    r.address_line1,
    r.address_line2,
    r.city,
    r.state_code,
    r.postal_code,
    r.country_code,
    gs.source_system_count,
    r.source_system as survivor_source_system,
    r.source_record_id as survivor_source_record_id,
    current_timestamp as gold_curated_ts
from ranked r
join group_stats gs on gs.match_group_id = r.match_group_id
where r.survivor_rank = 1
