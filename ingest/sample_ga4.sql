-- Preview a few consecutive source days before expanding the range.
-- Keep the event and item grains separate during downstream modeling.
SELECT
    _TABLE_SUFFIX AS source_day,
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    event_bundle_sequence_id,
    event_previous_timestamp,
    traffic_source.source AS first_user_source,
    traffic_source.medium AS first_user_medium,
    device.category AS device_category,
    ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue_in_usd AS purchase_usd,
    TO_JSON_STRING(event_params) AS event_params_json,
    TO_JSON_STRING(items) AS items_json
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201110' AND '20201112';
