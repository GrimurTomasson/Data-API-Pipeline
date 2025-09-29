-- Plássnotkun taflna og vísa í SQL Server - Grímur
DECLARE @plass_a_disk TABLE(
    object_id INT
    ,database_name sysname
    ,schema_name sysname
    ,table_name SYSNAME
    ,object_name SYSNAME NULL
    ,object_type varchar(5)
    ,structure nvarchar(60)
    ,partition_count INT
    ,rows BIGINT
    ,DATA_COMPRESSION nvarchar(60)
    ,total_space_mb decimal(36,2)
    ,total_space_gb decimal(36,2)
    ,used_space_mb decimal(36,2)
    ,unused_space_mb decimal(36,2)
)
DECLARE @db varchar(100)
DECLARE @sql nvarchar(max)

DECLARE db_cur CURSOR READ_ONLY FAST_FORWARD FOR
    SELECT name FROM SYS.databases WHERE state = 0 -- Í lagi

OPEN db_cur
WHILE 1=1 BEGIN
    FETCH NEXT FROM db_cur INTO @db
    IF @@FETCH_STATUS <> 0
        BREAK

    SET @sql = '    
    SELECT 
        t.object_id
        ,' + QUOTENAME (@db, '''') + ' AS database_name
        ,s.name AS schema_name
        ,t.name AS table_name
        ,i.name AS object_name
        ,CASE WHEN i.type in (0,1,5) THEN ''TABLE'' ELSE ''INDEX'' END AS object_type
        ,i.type_desc AS structure
        ,p.partition_count
        ,p.rows
        ,CASE 
            WHEN p.data_compression_cnt > 1 THEN ''Mixed''
            ELSE ( SELECT DISTINCT p.data_compression_desc FROM sys.partitions p WHERE i.object_id = p.object_id AND i.index_id = p.index_id )
        END AS data_compression
        ,CAST (ROUND (( au.total_pages * (8/1024.00)), 2) AS DECIMAL(36,2)) AS total_space_MB
        ,CAST (ROUND (( au.total_pages * (8/1024.00/1024.00)), 2) AS DECIMAL(36,2)) AS total_space_GB
        ,CAST (ROUND (( au.used_pages * (8/1024.00)), 2) AS DECIMAL(36,2)) AS used_space_MB
        ,CAST (ROUND (((au.total_pages - au.used_pages) * (8/1024.00)), 2) AS DECIMAL(36,2)) AS unused_space_MB
    FROM 
        ' + QUOTENAME (@db) + '.sys.schemas s
        JOIN ' + QUOTENAME (@db) + '.sys.tables t ON t.schema_id = s.schema_id
        JOIN ' + QUOTENAME (@db) + '.sys.indexes i ON i.object_id = t.object_id
        JOIN (
            SELECT 
                object_id, index_id, COUNT (*) AS partition_count, SUM ([rows]) AS [rows], COUNT (DISTINCT data_compression) AS data_compression_cnt
            FROM 
                ' + QUOTENAME (@db) + '.sys.partitions
            GROUP BY 
                object_id, index_id
        ) p ON i.object_id = p.object_id AND i.index_id = p.index_id
        JOIN (
            SELECT 
                p.object_id, p.index_id, SUM (a.total_pages) AS total_pages, SUM (a.used_pages) AS used_pages, SUM (a.data_pages) AS data_pages
            FROM 
                ' + QUOTENAME (@db) + '.sys.partitions p
                JOIN ' + QUOTENAME (@db) + '.sys.allocation_units a ON p.partition_id = a.container_id
            GROUP BY 
                p.object_id, p.index_id
        ) au ON i.object_id = au.object_id AND i.index_id = au.index_id
    WHERE 
        t.is_ms_shipped = 0 -- Not a system table'

    BEGIN TRY
        INSERT INTO @plass_a_disk
            EXEC sp_executesql @sql
    END TRY
    BEGIN CATCH
        PRINT ('Aðgangur að grunni: ' + @db + ' er ekki til staðar!')
    END CATCH
END
CLOSE db_cur
DEALLOCATE db_cur

;WITH
gogn_2 AS (
    SELECT
        s.*
        ,LOWER (s.schema_name) + '_' + LOWER (s.table_name) + '_cci' AS new_index_name
        ,'[' + s.schema_name + '].[' + s.table_name + ']' AS qualified_table_name
        ,'-- Used space (MB): ' + CAST (s.used_space_MB AS varchar(20)) AS space_used_comment
        ,'USE ' + QUOTENAME (s.database_name) + '; ' AS use_database
    FROM
        @plass_a_disk s
),
gogn_3 AS (
    SELECT
        s.*
        ,CASE WHEN s.object_type = 'TABLE' AND s.structure = 'HEAP' THEN
            s.use_database + 'CREATE CLUSTERED COLUMNSTORE INDEX ' + new_index_name + ' ON ' + qualified_table_name + ' ' + space_used_comment
        ELSE NULL END AS conversion_command
        ,CASE WHEN s.object_type = 'TABLE' AND s.structure = 'CLUSTERED COLUMNSTORE' THEN
            s.use_database + 'ALTER INDEX ' + object_name + ' ON ' + qualified_table_name + ' REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON) ' + space_used_comment
        ELSE NULL END AS reorganize_command
        ,CASE WHEN s.object_type = 'TABLE' AND s.structure = 'CLUSTERED COLUMNSTORE' AND s.data_compression != 'COLUMNSTORE_ARCHIVE' THEN -- Add ORDER to this when we upgrade to SQL Server 2022. See: distinct_values_per_column, gives us the correct column list.
            s.use_database + 'CREATE CLUSTERED COLUMNSTORE INDEX ' + object_name + ' ON ' + qualified_table_name + ' WITH (DROP_EXISTING=ON, ONLINE=ON, MAXDOP=1, DATA_COMPRESSION=COLUMNSTORE_ARCHIVE, COMPRESSION_DELAY=0) ' + space_used_comment 
        ELSE NULL END AS compression_command
    FROM
        gogn_2 s
)
SELECT
    s.database_name
    ,s.schema_name
    ,s.table_name
    ,s.object_name
    ,s.object_type
    ,s.structure
    ,s.partition_count
    ,s.[rows]
    ,s.[data_compression]
    ,s.total_space_MB
    ,s.total_space_GB
    ,CAST (s.[rows] / s.total_space_mb AS INT) AS rows_per_MB
    ,s.used_space_MB
    ,s.unused_space_MB
    ,s.conversion_command
    ,s.reorganize_command
    ,s.compression_command
FROM
    gogn_3 s
WHERE 1=1
    AND s.total_space_MB >= 100
    -- AND s.table_name NOT LIKE '%_stg'
    -- AND s.conversion_command IS NOT NULL
    -- AND s.schema_name = 'Snapshot'
    -- AND s.compression_command IS NOT NULL
    -- AND s.structure = 'HEAP'
ORDER BY
    s.total_space_MB DESC


--EXEC sp_estimate_data_compression_savings 'dimension', 'adili', null, null, 'COLUMNSTORE'
