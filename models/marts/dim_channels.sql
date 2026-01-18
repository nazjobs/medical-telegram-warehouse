with stg as (
    select * from {{ ref('stg_telegram') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} as channel_key,
    channel_name,
    case 
        when channel_name ilike '%cosmetics%' then 'Cosmetics'
        when channel_name ilike '%pharma%' then 'Pharmaceuticals'
        else 'Medical' 
    end as channel_type
from stg
