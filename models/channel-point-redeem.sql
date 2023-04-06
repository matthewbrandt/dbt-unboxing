with
    clean_data as (
        select safe.parse_json(event_data) as event_data_json
        from `twitch.events`
        where event_type = 'channel-channel_points_custom_reward_redemption-add'
    ),

    json_extracts as (
        select
            string(json_query(event_data_json, '$.id')) as event_id,
            string(json_query(event_data_json, '$.redeemed_at')) as redeemed_at_utc,
            int64(json_query(event_data_json, '$.reward.cost')) as cost,
            string(json_query(event_data_json, '$.reward.prompt')) as prompt,
            string(json_query(event_data_json, '$.reward.title')) as title,
            string(json_query(event_data_json, '$.user_input')) as user_input,
            string(json_query(event_data_json, '$.status')) as status,
            string(json_query(event_data_json, '$.user_id')) as redeemed_user_id,
            string(json_query(event_data_json, '$.user_login')) as redeemed_user_login
        from clean_data
    )

select
    event_id,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', redeemed_at_utc) as redeemed_at_utc,
    cost,
    prompt,
    title,
    nullif(user_input, '') as user_input,
    status,
    redeemed_user_id,
    redeemed_user_login
from json_extracts
