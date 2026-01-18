with stg as (
    select * from {{ ref('stg_telegram') }}
),

channels as (
    select * from {{ ref('dim_channels') }}
)

select
    stg.message_id,
    channels.channel_key,
    stg.message_date,
    stg.message_text,
    stg.views,
    stg.forwards,
    stg.has_media,
    length(stg.message_text) as message_length
from stg
join channels on stg.channel_name = channels.channel_name
