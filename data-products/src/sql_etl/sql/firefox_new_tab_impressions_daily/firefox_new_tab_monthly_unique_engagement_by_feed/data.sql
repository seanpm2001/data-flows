{% set sql_engine = "bigquery" %}
{% import 'helpers.j2' as helpers with context %}
{% if for_new_offset %}
    select current_timestamp()
{% else %}
{% macro parse_iso8601(datetime) %}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', '{{ datetime }}')
{% endmacro %}
WITH
  deduplicated AS (
    SELECT
      *   
    FROM
      {% if for_backfill %}
      `moz-fx-data-shared-prod.activity_stream_stable.impression_stats_v1`
    {% else %}
      `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`  
    {% endif %}
    WHERE submission_timestamp >= CAST(DATE_SUB(CAST({{ helpers.parse_iso8601(batch_start) }} as DATE), INTERVAL 1 MONTH) as TIMESTAMP)
    AND submission_timestamp < {{ helpers.parse_iso8601(batch_end) }}
    QUALIFY row_number() over (PARTITION BY DATE(submission_timestamp),
    document_id
    ORDER BY
    submission_timestamp desc) = 1
),
  impression_data AS (
  SELECT
    *,
    CASE
      WHEN ( normalized_country_code IN ('US', 'CA') AND locale IN ('en-CA', 'en-GB', 'en-US') ) THEN 'NEW_TAB_EN_US'
      WHEN ( normalized_country_code IN ('GB',
        'IE')
      AND locale IN ('en-CA',
        'en-GB',
        'en-US') ) THEN 'NEW_TAB_EN_GB'
      WHEN ( normalized_country_code IN ('IN') AND locale IN ('en-CA', 'en-GB', 'en-US') ) THEN 'NEW_TAB_EN_INTL'
      WHEN ( normalized_country_code IN ('DE',
        'CH',
        'AT',
        'BE')
      AND locale IN ('de',
        'de-AT',
        'de-CH') ) THEN 'NEW_TAB_DE_DE'
      WHEN (normalized_country_code IN ('IT') AND locale IN ('it')) THEN 'NEW_TAB_IT_IT'
      WHEN (normalized_country_code IN ('FR')
      AND locale IN ('fr')) THEN 'NEW_TAB_FR_FR'
      WHEN (normalized_country_code IN ('ES') AND locale IN ('es-ES')) THEN 'NEW_TAB_ES_ES'
  END
    AS feed_name
  FROM
    deduplicated
  WHERE
    loaded IS NULL --don't include loaded ping
    AND ARRAY_LENGTH(tiles) >= 1 --make sure data is valid/non-empty
    AND release_channel = 'release'
    AND ( ( normalized_country_code IN ('US',
          'CA',
          'GB',
          'IE',
          'IN')
        AND locale IN ('en-CA',
          'en-GB',
          'en-US') )
      OR ( normalized_country_code IN ('DE',
          'CH',
          'AT',
          'BE')
        AND locale IN ('de',
          'de-AT',
          'de-CH') )
      OR (normalized_country_code IN ('IT')
        AND locale IN ('it'))
      OR (normalized_country_code IN ('FR')
        AND locale IN ('fr'))
      OR (normalized_country_code IN ('ES')
        AND locale IN ('es-ES')) ) ),
  flattened_impression_data AS ( --need this step to filter out >2 clicks from a given client on the same tile within 1 second
  SELECT
    UNIX_SECONDS(submission_timestamp) AS submission_timestamp,
    feed_name,
    --truncate timestamp to seconds
    impression_id AS client_id,
    --client_id renamed to impression_id in GCP
    user_prefs,
    flattened_tiles.id AS tile_id,
    IFNULL(flattened_tiles.pos, alt_pos) AS position,
    --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
    SUM(CASE
        WHEN click IS NULL AND block IS NULL AND pocket IS NULL THEN 1
      ELSE
      0
    END
      ) AS impressions,
    SUM(CASE
        WHEN click IS NOT NULL THEN 1
      ELSE
      0
    END
      ) AS clicks,
    SUM(CASE
        WHEN pocket IS NOT NULL THEN 1
      ELSE
      0
    END
      ) AS pocketed,
    SUM(CASE
        WHEN block IS NOT NULL THEN 1
      ELSE
      0
    END
      ) AS blocked
  FROM
    impression_data
  CROSS JOIN
    UNNEST(impression_data.tiles) AS flattened_tiles
  WITH
  OFFSET
    AS alt_pos
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6 )
SELECT
  DATE_TRUNC(DATE(TIMESTAMP_SECONDS(a.submission_timestamp)), month) AS happened_at,
  feed_name,
  COUNT(DISTINCT
    CASE
      WHEN a.impressions > 0 THEN a.client_id
  END
    ) AS users_viewing_recs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.clicks > 0 THEN a.client_id
  END
    ) AS users_clicking_recs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.impressions > 0 AND a.user_prefs & 4 = 4 AND a.user_prefs & 32 = 32 THEN a.client_id
  END
    ) AS users_eligible_for_spocs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.impressions > 0 AND a.user_prefs & 4 = 4 AND a.user_prefs & 32 = 32 AND t.type = 'spoc' THEN a.client_id
  END
    ) AS users_viewing_spocs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.clicks > 0 AND a.user_prefs & 4 = 4 AND a.user_prefs & 32 = 32 AND t.type = 'spoc' THEN a.client_id
  END
    ) AS users_clicking_spocs_count,
  CAST({{ helpers.parse_iso8601(batch_start) }} as DATE) as aggregation_date
FROM
  flattened_impression_data AS a
LEFT JOIN
  `moz-fx-data-shared-prod.pocket.spoc_tile_ids` AS t
ON
  a.tile_id = t.tile_id
WHERE
  a.clicks < 3
GROUP BY
  1,
  2
ORDER BY
  1,
  2;
{% endif %}