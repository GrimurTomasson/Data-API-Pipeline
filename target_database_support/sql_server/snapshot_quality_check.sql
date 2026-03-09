SELECT 'USE [' + DB_NAME () + ']' + CHAR (10) AS gagnaprof
UNION ALL
SELECT 'SELECT * FROM (' + CHAR (10) 
UNION ALL
SELECT
    'SELECT ''' + tafla + ''' AS tafla, i.test, i.dbt_valid_from, i.dbt_valid_to, i.correct_dbt_valid_from, i.correct_dbt_valid_to, i.villufjoldi ,''SELECT ' + lykill + ', dbt_valid_from, dbt_valid_to FROM ' + skema + '.' + tafla + ' WHERE ' 
        + CASE WHEN dalkur_1 IS NOT NULL THEN dalkur_1 + ' = '''''' + ' + dalkur_1 + ' + ''''''' ELSE '' END
        + CASE WHEN dalkur_2 IS NOT NULL THEN dalkur_2 + ' = '''''' + ' + dalkur_2 + ' + ''''''' ELSE '' END
        + CASE WHEN dalkur_3 IS NOT NULL THEN dalkur_3 + ' = '''''' + ' + dalkur_3 + ' + ''''''' ELSE '' END
        + CASE WHEN dalkur_4 IS NOT NULL THEN dalkur_4 + ' = '''''' + ' + dalkur_4 + ' + ''''''' ELSE '' END
        + CASE WHEN dalkur_5 IS NOT NULL THEN dalkur_5 + ' = '''''' + ' + dalkur_5 + ' + ''''''' ELSE '' END 
    + ' ORDER BY dbt_valid_from DESC'' AS fyrirspurn FROM ('
    + CHAR (10) + CHAR (9)
    + 'SELECT ''Non-contiguous date-range'' AS test, ' + lykill + ', dbt_valid_from, dbt_valid_to, NULL AS correct_dbt_valid_from, correct_dbt_valid_to, 1 AS villufjoldi FROM (SELECT * ,LEAD (dbt_valid_from) OVER (PARTITION BY ' + lykill + ' ORDER BY dbt_valid_from) AS correct_dbt_valid_to FROM ' + skema + '.' + tafla + ') i WHERE dbt_valid_to != correct_dbt_valid_to'
    + CHAR (10) + CHAR (9) + 'UNION ALL' + CHAR (10) + CHAR (9)
    + 'SELECT ''Date-range open'' AS test, ' + lykill + ', NULL, NULL, NULL, NULL, COUNT(1) AS villufjoldi FROM ' + skema + '.' + tafla + ' WHERE dbt_valid_to IS NULL GROUP BY ' + lykill + ' HAVING COUNT(1) != 1 '
    + CHAR (10) + CHAR (9) + 'UNION ALL' + CHAR (10) + CHAR (9)
    + 'SELECT ''Overlapping date-ranges'' AS test, *, NULL, 1 FROM (SELECT ' + lykill + ', dbt_valid_from, dbt_valid_to, LAG (dbt_valid_to) OVER (PARTITION BY ' + lykill + ' ORDER BY dbt_valid_from) AS correct_dbt_valid_from FROM ' + skema + '.' + tafla + ') i WHERE DATEDIFF (DAY, correct_dbt_valid_from, dbt_valid_from) != 0'
    + CHAR (10) + CHAR (9) + 'UNION ALL' + CHAR (10) + CHAR (9)
    + 'SELECT ''dbt_valid_from duplications'' AS test, ' + lykill + ', dbt_valid_from, NULL, NULL, NULL, COUNT(1) AS villufjoldi FROM ' + skema + '.' + tafla + ' GROUP BY ' + lykill + ', dbt_valid_from HAVING COUNT (1) > 1'
    + CHAR (10)
    + ') i'
    + CASE WHEN tafla_lead IS NOT NULL THEN CHAR (10) + CHAR (10) + 'UNION ALL' + CHAR (10) ELSE '' END
FROM (
    SELECT 
        *
        ,CONCAT_WS (', ', dalkur_1, dalkur_2, dalkur_3, dalkur_4, dalkur_5) AS lykill
        ,LEAD (tafla) OVER (ORDER BY tafla) AS tafla_lead 
    FROM 
        audit.snapshot_lyklar 
    /*
    WHERE 
        tafla = 'adili_v2'
    */
) i
UNION ALL
SELECT CHAR (10) + ') o ORDER BY tafla, test, dbt_valid_from'
