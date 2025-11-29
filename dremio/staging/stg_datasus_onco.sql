SELECT
    *
FROM
    minio."oncoped-raw"."raw_datasus_onco.parquet"
WHERE
    IDADE <=19;