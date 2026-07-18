-- This table is the answer to "how do we keep gold linked back to source?" --
-- every source record that contributed to a golden record has a row here,
-- with a match confidence score and a flag for which record was the survivor.
--
-- Confidence score is now genuinely graduated (see scripts/generate_matches.py):
--   1.00            -- connected to another group member via an exact email/phone match (tier 1)
--   <similarity>     -- connected via the fuzzy embedding tier (tier 2), auto-merged or
--                        steward-confirmed -- the actual cosine similarity score, typically 0.80-1.00
--   0.50            -- single-source record, no corroborating match at all ("provisional")
with candidates as (
    select * from {{ ref('gold_match_candidates') }}
),

group_stats as (
    select
        match_group_id,
        count(distinct source_system) as source_system_count
    from candidates
    group by match_group_id
),

edges as (
    select * from {{ source('gold_prep', 'match_edges') }}
),

-- best (max) confidence for each source record from any edge touching it --
-- a record can appear as either side of an edge, so union both directions
edge_confidence as (
    select source_system, source_record_id, max(confidence) as confidence
    from (
        select source_system_a as source_system, source_record_id_a as source_record_id, confidence from edges
        union all
        select source_system_b as source_system, source_record_id_b as source_record_id, confidence from edges
    )
    group by source_system, source_record_id
)

select
    'GOLD-' || lpad(cast(c.match_group_id as varchar), 5, '0') as golden_id,
    c.source_system,
    c.source_record_id,
    coalesce(ec.confidence, case when gs.source_system_count > 1 then 1.00 else 0.50 end) as match_confidence_score,
    row_number() over (
        partition by c.match_group_id
        order by c.source_modified_date desc nulls last,
                 case c.source_system when 'CRM' then 1 else 2 end
    ) = 1 as is_survivor_record,
    current_timestamp as crosswalk_created_ts
from candidates c
join group_stats gs on gs.match_group_id = c.match_group_id
left join edge_confidence ec
  on ec.source_system = c.source_system and ec.source_record_id = c.source_record_id
