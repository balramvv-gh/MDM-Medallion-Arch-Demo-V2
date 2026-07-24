-- Attribute-level survivorship (Data Governance > Rules Configuration >
-- Survivorship Rules). For each (match_group_id, target_column), this model
-- picks the single contributing source record whose value wins that column,
-- per bus_rules.survivorship_rules -- exactly one active rule per
-- target_column, one of:
--   most_common    -- the non-blank value that appears in the most
--                     contributing records for that column
--   most_complete  -- prefers a non-blank value over a blank/null one
--   oldest/newest  -- by that source record's source_modified_date
--   pattern_match  -- prefers a value matching rule_param (a regex)
-- Whenever a column's configured rule doesn't produce a single clean winner
-- (a genuine tie, or no rule row is configured for that column), the
-- universal, non-configurable tie-break is source_modified_date desc, then
-- CRM preferred -- exactly one primary rule per column plus one fixed
-- tie-break, not an arbitrary stack (see bus_rules.survivorship_rules'
-- docstring in api/db.py for the governance rationale).
--
-- A null/unparseable source_modified_date is treated as the oldest possible
-- (epoch -1), the same convention api/reprocessing.py's _epoch() helper uses
-- for the identical situation on the real-time reprocessing path -- so a
-- missing date never wins 'newest' and never wins a tie-break, but *can*
-- still legitimately win an explicit 'oldest' rule if every other candidate
-- is also missing a date (a demo-scope edge case, consistent with how the
-- rest of this project treats missing dates).
--
-- Downstream: gold_customers.sql pivots this back to one row per golden
-- record; gold_crosswalk.sql groups this by source to report which columns
-- each contributing source record actually won (winning_columns).
with candidates as (
    select * from {{ ref('gold_match_candidates') }}
),

rules as (
    select * from {{ source('bus_rules', 'survivorship_rules') }}
    where active
),

-- Unpivot each editable gold column into long format: one row per
-- (match_group_id, contributing source record, target_column).
long as (
    select match_group_id, source_system, source_record_id, source_modified_date,
           'first_name' as target_column, first_name as value from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'last_name', last_name from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'email', email from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'phone', phone from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'address_line1', address_line1 from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'address_line2', address_line2 from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'city', city from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'state_code', state_code from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'postal_code', postal_code from candidates
    union all
    select match_group_id, source_system, source_record_id, source_modified_date,
           'country_code', country_code from candidates
),

scored as (
    select
        l.*,
        coalesce(r.rule_type, 'newest') as rule_type,
        nullif(trim(l.value), '') as clean_value,
        coalesce(epoch(cast(l.source_modified_date as timestamp)), -1) as date_epoch,
        case coalesce(r.rule_type, 'newest')
            when 'newest' then coalesce(epoch(cast(l.source_modified_date as timestamp)), -1)
            when 'oldest' then -1 * coalesce(epoch(cast(l.source_modified_date as timestamp)), -1)
            when 'most_complete' then
                case when nullif(trim(l.value), '') is not null then 1 else 0 end
            when 'pattern_match' then
                case when nullif(trim(l.value), '') is not null
                          and r.rule_param is not null
                          and regexp_matches(l.value, r.rule_param)
                     then 1 else 0 end
            else 0  -- 'most_common' scored in the freq CTE below
        end as base_rank_key
    from long l
    left join rules r on r.target_column = l.target_column
),

-- 'most_common': frequency of each non-blank value within
-- (match_group_id, target_column). Blank/null values never count toward
-- another value's frequency and can never themselves be "the most common
-- value" -- they fall through to the tie-break like any other non-winner.
freq as (
    select match_group_id, target_column, clean_value, count(*) as value_count
    from scored
    where clean_value is not null
    group by 1, 2, 3
),

ranked as (
    select
        s.*,
        case when s.rule_type = 'most_common' then coalesce(f.value_count, 0)
             else s.base_rank_key
        end as rank_key
    from scored s
    left join freq f
      on f.match_group_id = s.match_group_id
     and f.target_column = s.target_column
     and f.clean_value = s.clean_value
),

winners as (
    select
        *,
        row_number() over (
            partition by match_group_id, target_column
            order by
                rank_key desc,
                date_epoch desc,
                case source_system when 'CRM' then 1 else 2 end
        ) as winner_rank
    from ranked
)

select
    match_group_id,
    target_column,
    source_system,
    source_record_id,
    source_modified_date,
    value as winning_value
from winners
where winner_rank = 1
