select
    id
    -- *
    -- id,
    -- TIMESTAMP without TIME zone 'epoch' + (user_activity ->> 'created') :: numeric * interval '1 second' as created,
    -- TIMESTAMP without TIME zone 'epoch' + (user_activity ->> 'datetime_epoch') :: numeric * interval '1 second' as datetime,
    -- user_activity ->> 'user_id' AS user_id,
    -- user_activity ->> 'session_name' AS session_name,
    -- user_activity ->> 'ip_address' AS ip_address,
    -- user_activity ->> 'online_status' AS online_status,
    -- user_activity ->> 'device_id' AS device_id,
    -- user_activity ->> 'device_model' AS device_model,
    -- user_activity ->> 'os_version' AS os_version,
    -- user_activity ->> 'client_id' AS client_id,
    -- user_activity ->> 'client_version' AS client_version,
    -- user_activity ->> 'item_id' AS item_id,
    -- user_activity ->> 'page_orientation' AS page_orientation,
    -- REPLACE(REPLACE(user_activity ->> 'page_number', '[', '{'), ']', '}') AS page_number,
    -- user_activity ->> 'duration' AS second,
    -- user_activity ->> 'organization_id' AS organization_id,
    -- user_activity ->> 'catalog_id' AS catalog_id
from
    user_page_view
limit 100