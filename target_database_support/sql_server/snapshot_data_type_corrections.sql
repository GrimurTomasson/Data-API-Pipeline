USE [MASTER-API]

SELECT
    ps.TABLE_CATALOG, ps.TABLE_SCHEMA, ps.TABLE_NAME, ps.COLUMN_NAME, ps.DATA_TYPE AS public_data_type, ps.CHARACTER_MAXIMUM_LENGTH AS public_length, sc.DATA_TYPE AS snapshot_data_type, sc.CHARACTER_MAXIMUM_LENGTH AS snapshot_length
    ,'ALTER TABLE ' + sc.TABLE_SCHEMA + '.' + sc.TABLE_NAME + ' ALTER COLUMN ' + sc.COLUMN_NAME + ' ' +
    CASE 
        WHEN ps.data_type IN ('char','nchar','varchar','nvarchar') THEN
            CASE 
                WHEN ps.CHARACTER_MAXIMUM_LENGTH > 0 THEN ps.DATA_TYPE + '(' + CAST (ps.CHARACTER_MAXIMUM_LENGTH AS varchar(4)) + ')'
                ELSE ps.DATA_TYPE + '(8000)' -- max, mjög óæskilegt
            END
        WHEN ps.DATA_TYPE IN ('date','datetime', 'datetime2') AND ps.DATETIME_PRECISION > 0 THEN ps.DATA_TYPE + '(' + CAST (ps.DATETIME_PRECISION AS varchar(4)) + ')'
        WHEN ps.DATA_TYPE IN ('decimal', 'numeric') THEN ps.DATA_TYPE + '(' + CAST (ps.NUMERIC_PRECISION AS varchar(2)) + ',' + CAST (ps.NUMERIC_SCALE AS varchar(2)) + ')'
        ELSE ps.DATA_TYPE
    END
FROM
    [INFORMATION_SCHEMA].COLUMNS ps 
    -- Uppfæra DB vísun !!!
    JOIN [MASTER-API-PRIVATE].[INFORMATION_SCHEMA].[COLUMNS] sc ON sc.TABLE_NAME = ps.TABLE_NAME AND sc.COLUMN_NAME = ps.COLUMN_NAME AND (sc.DATA_TYPE != ps.DATA_TYPE OR sc.CHARACTER_MAXIMUM_LENGTH != ps.CHARACTER_MAXIMUM_LENGTH)
WHERE
    -- Public vensl
    ps.TABLE_SCHEMA = 'Nustada'
    -- Snapshot tafla
    AND sc.TABLE_SCHEMA = 'dbt_snapshots' 