-- This table is the answer to "how do we keep gold linked back to source?" --
-- every source record that contributed to a golden record has a row here,
-- with a match confidence score and a flag for which record was the survivor.
with candidates as (
    select * from {{ ref('gold_match_candidates') }}
),

group_stats as (
    select
        match_group_id,
        count(distinct source_system) as source_system_count
    from candidates
    group by match_group_id
)

select
    'GOLD-' || lpad(cast(c.match_group_id as varchar), 5, '0') as golden_id,
    c.source_system,
    c.source_record_id,
    -- deterministic exact-match on email/phone => high confidence when corroborated
    -- by a second source; single-source records are provisional (unconfirmed) matches
    case when gs.source_system_count > 1 then 1.00 else 0.50 end as match_confidence_score,
    row_number() over (
        partition by c.match_group_id
        order by c.source_modified_date desc nulls last,
                 case c.source_system when 'CRM' then 1 else 2 end
    ) = 1 as is_survivor_record,
    current_timestamp as crosswalk_created_ts
from candidates c
join group_stats gs on gs.match_group_id = c.match_group_id
