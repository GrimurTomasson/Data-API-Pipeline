-- https://www.mssqltips.com/sqlservertip/4054/creating-a-date-dimension-or-calendar-table-in-sql-server/

/*
-- Vikan byrjar á mánudagi skv. ISO 8601, viðskiptavikan þ.e., Íslenska, Íslenska er ekki í boði :(
{{
    config(
    pre_hook="SET DATEFIRST 1, DATEFORMAT ymd, LANGUAGE US_ENGLISH") 
}}

WITH dates AS (
	SELECT i.date_key FROM (
        SELECT 
            CAST (DATEADD(DAY, i.sequence_number, CONVERT (date, '01-01-2010', 105)) AS date) AS date_key -- Start date
        FROM (  
            SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1 AS sequence_number
            FROM   [master].[sys].[columns] sc1 -- Just to get a large number
            CROSS JOIN [master].[sys].[columns] sc2) i
    ) i 
    WHERE 
        i.date_key <= CONVERT (date, '31-12-2040', 105) -- End date
),
*/

SET DATEFIRST  1 -- Vikan byrjar á mánudagi skv. ISO 8601, viðskiptavikan þ.e.
SET DATEFORMAT ymd -- Íslenska  
SET LANGUAGE US_ENGLISH -- Íslenska er ekki í boði :(

DECLARE @startDate date = CONVERT (date, '01-01-2010', 105)
DECLARE @endDate date = CONVERT (date, '31-12-2040', 105)

;WITH dates AS (
	SELECT i.date_key FROM (
        SELECT 
            CAST (DATEADD(DAY, i.sequence_number, @startDate) AS date) AS date_key -- Start date
        FROM (  
            SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1 AS sequence_number
            FROM   [master].[sys].[columns] sc1 -- Just to get a large number
            CROSS JOIN [master].[sys].[columns] sc2) i
    ) i 
    WHERE 
        i.date_key <= @endDate
),
manudur_map AS (
	SELECT * FROM ( VALUES
		('Janúar', 'January')
		,('Febrúar', 'February')
		,('Mars', 'March')
		,('Apríl', 'April')
		,('Maí', 'May')
		,('Júní', 'June')
		,('Júlí', 'July')
		,('Ágúst', 'August')
		,('September', 'September')
		,('Október', 'October')
		,('Nóvember', 'November')
		,('Desember', 'December')
	) AS i([is], en)
),
vikudagur_map AS (
	SELECT * FROM ( VALUES
		('Mánudagur', 'Monday', 0)
		,('Þriðjudagur', 'Tuesday', 0)
		,('Miðvikudagur', 'Wednesday', 0)
		,('Fimmtudagur', 'Thursday', 0)
		,('Föstudagur', 'Friday', 0)
		,('Laugardagur', 'Saturday', 1)
		,('Sunnudagur', 'Sunday', 1)
	) AS i([is], en, er_helgi)
),
skref_eitt AS (
	SELECT
		d.date_key AS id
		,DATEPART (WEEKDAY,   d.date_key) AS vikudagur
		,v.[is] AS vikudagur_heiti
		,v.er_helgi
		,CASE WHEN v.er_helgi = 1 THEN 0 ELSE 1 END as er_virkur_dagur
		,DATEPART (WEEK, d.date_key) AS vika_numer_innan_ars
		,DATEPART (ISO_WEEK, d.date_key) AS vika_numer_innan_ars_iso
		,DATEPART (DAY, d.date_key) AS manadardagur
		,DATEPART (MONTH, d.date_key) AS manudur
		,DATEFROMPARTS (YEAR(d.date_key), MONTH(d.date_key), 1) AS manudur_upphafsdagsetning
		,m.[is] AS manudur_heiti
		,DATEPART (Quarter, d.date_key) AS arsfjordungur
		,DATEPART (YEAR, d.date_key) AS ar
		,DATEPART (DAYOFYEAR, d.date_key) AS ar_numer_dags
		,DATEFROMPARTS (YEAR (d.date_key), 1, 1) AS ar_upphafsdagsetning
		,DATEFROMPARTS (YEAR (d.date_key), 12, 31) AS ar_lokadagsetning
	  FROM 
		dates d
		JOIN manudur_map m ON m.en = DATENAME (MONTH, d.date_key)
		JOIN vikudagur_map v ON v.en = DATENAME (WEEKDAY, d.date_key)
),
an_fridaga AS (
	SELECT
		s.id
		-- Dagur
		,s.vikudagur
		,s.vikudagur_heiti
		,s.er_helgi
		,s.er_virkur_dagur
		-- Vika
		,s.vika_numer_innan_ars
		,s.vika_numer_innan_ars_iso
		,CONVERT (tinyint, ROW_NUMBER () OVER (PARTITION BY s.manudur_upphafsdagsetning, s.vikudagur ORDER BY s.id)) AS vika_numer_innan_manadar
		,DATEADD (DAY, 1 - s.vikudagur, s.id) AS vika_fyrsta_dagsetning
		,DATEADD (DAY, 6, DATEADD (DAY, 1 - s.vikudagur, s.id)) AS vika_sidasta_dagsetning
		,CONVERT (tinyint, DENSE_RANK () OVER (PARTITION BY s.ar, s.manudur ORDER BY s.vika_numer_innan_ars)) as vika_numer_manadar
		-- Mánuður
		,s.manadardagur
		,s.manudur
		,s.manudur_upphafsdagsetning
		,MAX (s.id) OVER (PARTITION BY s.ar, s.manudur) AS manudur_lokadagsetning
		,s.manudur_heiti
		,DATEADD (MONTH, 1, s.manudur_upphafsdagsetning) AS upphafsdagsetning_naesta_manudar
		,DATEADD (DAY, -1, DATEADD (MONTH, 2, s.manudur_upphafsdagsetning)) AS lokadagsetning_naesta_manudar
		-- Ársfjórðungur
		,s.arsfjordungur
		,MIN (s.id) OVER (PARTITION BY s.ar, s.arsfjordungur) AS arsfjordungur_upphafsdagsetning
		,MAX (s.id) OVER (PARTITION BY s.ar, s.arsfjordungur) AS arsfjordungur_lokadagsetning
		-- Ár
		,s.ar
		,s.ar - CASE WHEN s.manudur = 1 AND s.vika_numer_innan_ars_iso > 51 THEN 1 WHEN s.manudur= 12 AND s.vika_numer_innan_ars_iso = 1  THEN -1 ELSE 0 END AS ar_iso
		,CONVERT(bit, CASE WHEN (s.ar % 400 = 0) OR (s.ar % 4 = 0 AND s.ar % 100 <> 0) THEN 1 ELSE 0 END) AS er_hlaupar
		,s.ar_numer_dags
		,s.ar_upphafsdagsetning
		,s.ar_lokadagsetning
		,CASE WHEN DATEPART (WEEK, s.ar_lokadagsetning) = 53 THEN 1 ELSE 0 END AS ar_inniheldur_53_vikur
		,CASE WHEN DATEPART (ISO_WEEK, s.ar_lokadagsetning) = 53 THEN 1 ELSE 0 END AS ar_inniheldur_53_iso_vikur
	FROM
		skref_eitt s
)

select * from an_fridaga
order by id


