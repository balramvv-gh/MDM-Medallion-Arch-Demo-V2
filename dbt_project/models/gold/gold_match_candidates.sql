-- Match strategy (demo-scope, deterministic): two records are considered the same
-- customer if they share the same normalized email OR the same normalized phone.
-- A production system would add fuzzy name/address matching and probabilistic scoring;
-- this is intentionally simple so the survivorship/crosswalk logic stays legible.
with silver as (
    select * from {{ ref('silver_customers') }}
),

match_keys as (
    select
        *,
        lower(trim(email)) as match_email,
        regexp_replace(coalesce(phone, ''), '[^0-9]', '', 'g') as match_phone
    from silver
),

-- assign a match_group_id: union records sharing an email or phone key
groups as (
    select
        *,
        dense_rank() over (order by coalesce(nullif(match_email, ''), match_phone)) as match_group_id
    from match_keys
)

select * from groups
