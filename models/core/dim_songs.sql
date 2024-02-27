{{ config(materialized = 'table') }}

SELECT {{ dbt_utils.generate_surrogate_key(['songId']) }} AS songKey,
       *
FROM (

        (
            SELECT song_id AS songId,
                REPLACE(REPLACE(artist_name, '"', ''), '\\', '') AS artistName,
                duration,
                key,
                key_confidence AS keyConfidence,
                loudness,
                song_hotttnesss AS songHotness,
                tempo,
                title,
                year
            FROM {{ source('staging', 'songs') }}
        )

        UNION ALL

        (
            SELECT 'NNNNNNNNNNNNNNNNNNN' AS songId,
                'NA' AS artistName,
                0 AS duration,
                -1 AS key,
                -1 AS keyConfidence,
                -1 AS loudness,
                -1 AS songHotness,
                -1 AS tempo,
                'NA' AS title,
                0 AS year
        )
    )
