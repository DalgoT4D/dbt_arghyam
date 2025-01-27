{{ 
    config(
        materialized='table'
    )
}}


SELECT DISTINCT ON (enc.username)
    enc.username,
    sub.location,
    brd.ward_name,
    brd.block_name,
    brd.district_name,
    brd.gp_name
FROM {{ ref ('dedup_enc') }} AS enc
INNER JOIN {{ ref ('subjects_cdc') }} AS sub ON enc.subject_id = sub.id
INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
ORDER BY enc.username
