-- This table is the answer to "how do we keep gold linked back to source?" --
-- every source record that contributed to a golden record has a row here,
-- with a match confidence score and, since attribute-level survivorship
-- (Data Governance > Rules Configuration > Survivorship Rules), the specific
-- list of gold columns that record's value won (winning_columns) --
-- gold_survivorship_winners.sql picks the winner independently per column, so
-- a single golden record can legitimately draw its email from one source and
-- its address from another. is_survivor_record is kept for backward
-- compatibility (existing UI's "★ survivor" badge) and is now simply
-- "won at least one column" (true whenever winning_columns is non-empty) --
-- a record-level flag derived from the column-level truth, not the other way
-- around, unlike the old design.
--
-- Confidence score is genuinely graduated (see scripts/generate_matches.py),
-- and every value below is read from bus_rules.matching_thresholds (DB-native,
-- maintained via the Rules Configuration screen's maker-checker workflow)
-- rather than hardcoded, so the crosswalk can't drift from the same metadata
-- the batch matching step and the real-time reprocessing path
-- (api/reprocessing.py) use:
--   tier 1's auto_merge_threshold  -- connected to another group member via an
--                                      exact-match tier (e.g. email/phone)
--   <similarity>                   -- connected via a fuzzy tier, auto-merged or
--                                      steward-confirmed -- the actual cosine
--                                      similarity score, typically 0.80-1.00
--   no_match_baseline's value       -- single-source record, no corroborating
--                                      match at all ("provisional")
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
),

-- Single-row lookups from the matching_thresholds seed for the two fallback
-- confidence values (used only when a record has no edge at all touching it --
-- see the comment on the coalesce below).
tier1_fallback as (
    select auto_merge_threshold as confidence
    from {{ source('bus_rules', 'matching_thresholds') }}
    where active and is_match_tier and match_method = 'exact'
    order by tier_order limit 1
),

baseline_fallback as (
    select auto_merge_threshold as confidence
    from {{ source('bus_rules', 'matching_thresholds') }}
    where active and not is_match_tier and match_method = 'no_match_baseline'
    limit 1
),

-- Per-column attribute-level survivorship winners (gold_survivorship_winners.sql),
-- grouped by contributing source record into the list of gold columns that
-- source won -- e.g. a record might win ['email','phone'] while another
-- record in the same group wins ['address_line1','city'].
won_columns as (
    select
        match_group_id, source_system, source_record_id,
        list(target_column order by target_column) as winning_columns
    from {{ ref('gold_survivorship_winners') }}
    group by 1, 2, 3
)

select
    'GOLD-' || lpad(cast(c.match_group_id as varchar), 5, '0') as golden_id,
    c.source_system,
    c.source_record_id,
    -- A record with no edge at all touching it (ec.confidence null) is either
    -- a defensive fallback for a multi-source group that somehow has no
    -- recorded edge (shouldn't normally happen -- tier1_fallback), or a truly
    -- isolated single-source record (baseline_fallback, the "provisional" score).
    coalesce(
        ec.confidence,
        case when gs.source_system_count > 1
             then (select confidence from tier1_fallback)
             else (select confidence from baseline_fallback)
        end
    ) as match_confidence_score,
    coalesce(wc.winning_columns, []) as winning_columns,
    coalesce(len(wc.winning_columns) > 0, false) as is_survivor_record,
    current_timestamp as crosswalk_created_ts
from candidates c
join group_stats gs on gs.match_group_id = c.match_group_id
left join edge_confidence ec
  on ec.source_system = c.source_system and ec.source_record_id = c.source_record_id
left join won_columns wc
  on wc.match_group_id = c.match_group_id
 and wc.source_system = c.source_system
 and wc.source_record_id = c.source_record_id
