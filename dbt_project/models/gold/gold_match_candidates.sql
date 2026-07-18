-- Match strategy: hybrid deterministic + embedding-similarity, computed by
-- scripts/generate_matches.py (which must run after silver_customers and
-- before this model -- see that script's docstring for the full algorithm
-- and README.md for the build sequence). Tier 1 is exact match on normalized
-- email OR phone; tier 2 is TF-IDF cosine-similarity over name/address text,
-- clustered via union-find. This model just attaches the match_group_id that
-- step computed to each silver record; it stays its own model (rather than
-- inlining into gold_customers) so lineage/impact analysis can point at "the
-- matching step" as a distinct node, same as before.
with silver as (
    select * from {{ ref('silver_customers') }}
),

groups as (
    select * from {{ source('gold_prep', 'match_groups') }}
)

select
    s.*,
    g.match_group_id
from silver s
join groups g
  on g.source_system = s.source_system
 and g.source_record_id = s.source_record_id
