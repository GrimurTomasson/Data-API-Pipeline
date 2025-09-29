-- Við berum saman sameiginlega dálka (heiti) út frá skilgreindum lykli. Lykillinn er skilgreindur handvirkt, mögulega margir dálkar. Við göngum út frá því að töflurnar séu í sama gagnagrunni en mögulega í mismunandi skemum.
-- Ef við viljum ekki bera saman gildi í dálkum þarf handvirkt að hreinsa það út úr síun (where).

-- Uppfærið eftirfarandi þrjú gildi eftir þörfum
DECLARE @relation_schema nvarchar(250) = 'nustada'
DECLARE @first_relation nvarchar(250) = 'adili_v1'
DECLARE @second_relation nvarchar(250) = 'adili_v2'
-----------------------------------------------------------------------------------------------------------
DECLARE @tab CHAR = CHAR(9)
DECLARE @nl CHAR = CHAR(10)
DECLARE @col_name VARCHAR(250)

DECLARE @column_diff varchar(max) = @tab + '''''' + @nl
DECLARE @columns varchar(max) = ''
DECLARE @comparison varchar(max) = ''

DECLARE rd_cursor CURSOR READ_ONLY FAST_FORWARD FOR
    SELECT
        first_c.column_name
    FROM
        INFORMATION_SCHEMA.COLUMNS first_c
        JOIN INFORMATION_SCHEMA.COLUMNS second_c ON first_c.TABLE_SCHEMA = second_c.TABLE_SCHEMA AND first_c.COLUMN_NAME = second_c.COLUMN_NAME -- Eingöngu samnefndir dálkar
    WHERE
        first_c.TABLE_SCHEMA = @relation_schema
        AND first_c.TABLE_NAME = @first_relation
        AND second_c.TABLE_NAME = @second_relation
        AND first_c.column_name NOT IN ('saga_taetigildi', 'stofndagur') -- Það er ekki hjálplegt að bera þetta saman
    ORDER BY
        first_c.ORDINAL_POSITION

OPEN rd_cursor
WHILE 1=1 BEGIN
    FETCH NEXT FROM rd_cursor INTO @col_name
    IF @@FETCH_STATUS <> 0
        BREAK

    SET @column_diff = @column_diff + @tab +'+ CASE WHEN COALESCE (CAST (f.' + @col_name + ' AS varchar(500)), ''NULL'') != COALESCE (CAST (s.' + @col_name + ' AS nvarchar(500)), ''NULL'') THEN ''' + @col_name + ' '' ELSE '''' END' + @nl
    SET @columns = @columns + @tab + ',f.' + @col_name + ' AS f_' + @col_name + ', s.' + @col_name + ' AS s_' + @col_name + @nl
    SET @comparison = @comparison + @tab + 'OR COALESCE (CAST (f.' + @col_name + ' AS varchar(500)), ''NULL'') != COALESCE (CAST (s.' + @col_name + ' AS nvarchar(500)), ''NULL'')' + @nl
END
CLOSE rd_cursor
DEALLOCATE rd_cursor

SET @column_diff = @column_diff + @tab + ' AS dalkar_a_mismun'

PRINT ('SELECT')
PRINT (@column_diff)
PRINT (@columns)
PRINT ('FROM' + @nl + @tab + @relation_schema + '.' + @first_relation + ' f' + @nl + @tab + 'JOIN ' + @relation_schema + '.' + @second_relation + ' s ON s.X = f.X -- Setjið inn réttan lykil!')
PRINT ('WHERE' + @nl + @tab + '1=0' + @nl)
PRINT (@comparison)