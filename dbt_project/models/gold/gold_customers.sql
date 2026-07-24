-- Attribute-level survivorship (Data Governance > Rules Configuration >
-- Survivorship Rules): each column below is independently picked by
-- gold_survivorship_winners.sql, per that column's own active
-- bus_rules.survivorship_rules row (most_common / most_complete / oldest /
-- newest / pattern_match, tie-broken by source_modified_date desc then CRM
-- preferred -- see that model's docstring for the full rule). This replaces
-- the previous record-level survivorship ("the most-recently-modified source
-- record wins in full"); see gold_crosswalk.sql for the per-column "which
-- source won which column" detail this now produces.
with winners as (
    select * from {{ ref('gold_survivorship_winners') }}
),

pivoted as (
    select
        match_group_id,
        max(case when target_column = 'first_name' then winning_value end) as first_name,
        max(case when target_column = 'last_name' then winning_value end) as last_name,
        max(case when target_column = 'email' then winning_value end) as email,
        max(case when target_column = 'phone' then winning_value end) as phone,
        max(case when target_column = 'address_line1' then winning_value end) as address_line1,
        max(case when target_column = 'address_line2' then winning_value end) as address_line2,
        max(case when target_column = 'city' then winning_value end) as city,
        max(case when target_column = 'state_code' then winning_value end) as state_code,
        max(case when target_column = 'postal_code' then winning_value end) as postal_code,
        max(case when target_column = 'country_code' then winning_value end) as country_code
    from winners
    group by match_group_id
),

group_stats as (
    select match_group_id, count(distinct source_system) as source_system_count
    from {{ ref('gold_match_candidates') }}
    group by match_group_id
),

-- "Primary" survivor_source_system/survivor_source_record_id is kept for
-- backward compatibility (existing API/UI fields expect a single survivor
-- per golden record) -- defined as whichever contributing source won the
-- most individual columns, ties broken the same way the per-column rule
-- itself is tie-broken (most recent source_modified_date, then CRM).
column_win_counts as (
    select match_group_id, source_system, source_record_id, count(*) as columns_won
    from winners
    group by 1, 2, 3
),

primary_survivor as (
    select
        c.match_group_id, c.source_system, c.source_record_id,
        row_number() over (
            partition by c.match_group_id
            order by
                c.columns_won desc,
                coalesce(epoch(cast(cand.source_modified_date as timestamp)), -1) desc,
                case c.source_system when 'CRM' then 1 else 2 end
        ) as rn
    from column_win_counts c
    join {{ ref('gold_match_candidates') }} cand
      on cand.match_group_id = c.match_group_id
     and cand.source_system = c.source_system
     and cand.source_record_id = c.source_record_id
)

select
    'GOLD-' || lpad(cast(p.match_group_id as varchar), 5, '0') as golden_id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.address_line1,
    p.address_line2,
    p.city,
    p.state_code,
    p.postal_code,
    p.country_code,
    gs.source_system_count,
    ps.source_system as survivor_source_system,
    ps.source_record_id as survivor_source_record_id,
    current_timestamp as gold_curated_ts
from pivoted p
join group_stats gs on gs.match_group_id = p.match_group_id
join primary_survivor ps on ps.match_group_id = p.match_group_id and ps.rn = 1
