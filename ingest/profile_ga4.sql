-- Run as a BigQuery dry run before submitting the query.
-- Use a short, contiguous span and keep the source date in the result.
WITH extracted AS (
    SELECT
        _TABLE_SUFFIX AS source_day,
        event_name,
        event_timestamp,
        user_pseudo_id,
        ecommerce.transaction_id AS transaction_id,
        ecommerce.purchase_revenue_in_usd AS purchase_usd,
        ARRAY(
            SELECT parameter.value.int_value
            FROM UNNEST(event_params) AS parameter
            WHERE parameter.key = 'ga_session_id'
              AND parameter.value.int_value IS NOT NULL
        ) AS session_ids
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20201110' AND '20201112'
)
SELECT
    source_day,
    event_name,
    COUNT(*) AS event_rows,
    COUNTIF(NULLIF(user_pseudo_id, '') IS NULL) AS missing_users,
    COUNTIF(ARRAY_LENGTH(session_ids) = 0) AS missing_sessions,
    COUNTIF(ARRAY_LENGTH(session_ids) > 1) AS repeated_session_parameters,
    COUNTIF(event_timestamp IS NULL) AS missing_timestamps,
    COUNTIF(event_name = 'purchase' AND purchase_usd IS NULL) AS missing_usd,
    COUNTIF(event_name = 'purchase' AND NULLIF(transaction_id, '') IS NULL)
        AS missing_transaction_ids
FROM extracted
GROUP BY source_day, event_name
ORDER BY source_day, event_name;
