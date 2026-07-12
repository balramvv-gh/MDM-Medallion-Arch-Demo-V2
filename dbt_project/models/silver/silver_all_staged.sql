with unioned as (
    select * from {{ ref('stg_crm_customers') }}
    union all by name
    select * from {{ ref('stg_erp_customers') }}
)

select
    *,
    (err_missing_first_name or err_missing_last_name or err_invalid_email
        or err_missing_phone or err_invalid_state or err_invalid_country) as is_invalid,
    list_filter(
        [
            case when err_missing_first_name then 'R001/R009: missing first name' end,
            case when err_missing_last_name then 'R002/R009: missing last name' end,
            case when err_invalid_email then 'R003/R010: invalid email format' end,
            case when err_missing_phone then 'R011: missing phone' end,
            case when err_invalid_state then 'R004/R012: invalid state code' end,
            case when err_invalid_country then 'R005/R013: invalid country code' end
        ],
        x -> x is not null
    ) as reject_reasons
from unioned
