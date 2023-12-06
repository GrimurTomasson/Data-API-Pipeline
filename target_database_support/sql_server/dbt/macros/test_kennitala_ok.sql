{% test kennitala_ok(model, column_name) %}
WITH gogn AS (
	SELECT
		s.*
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 1, 1) ELSE -1 END AS smu_kt_1
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 2, 1) ELSE -1 END AS smu_kt_2
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 3, 1) ELSE -1 END AS smu_kt_3
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 4, 1) ELSE -1 END AS smu_kt_4
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 5, 1) ELSE -1 END AS smu_kt_5
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 6, 1) ELSE -1 END AS smu_kt_6
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 7, 1) ELSE -1 END AS smu_kt_7
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 8, 1) ELSE -1 END AS smu_kt_8
		,CASE WHEN LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 THEN SUBSTRING (s.{{ column_name }}, 9, 1) ELSE 5000 END AS smu_kt_vartala
	FROM
		{{ model }} s
),
gogn_med_summu AS (
	SELECT
		s.*
		,(s.smu_kt_1 * 3) + (s.smu_kt_2 * 2) + (s.smu_kt_3 * 7) + (s.smu_kt_4 * 6) + (s.smu_kt_5 * 5) + (s.smu_kt_6 * 4) + (s.smu_kt_7 * 3) + (s.smu_kt_8 * 2) AS smu_vartolu_summa
	FROM 
		gogn s
),
gogn_med_vartolu AS (
	SELECT
		s.*
		,CASE s.smu_vartolu_summa % 11 WHEN 0 THEN 0 ELSE 11 - s.smu_vartolu_summa % 11 END AS smu_reiknud_vartala
	FROM
		gogn_med_summu s
)
SELECT s.*, 1 AS null_villa, 0 AS lengdar_villa, 0 AS ekki_numerisk_villa, 0 AS vartolu_villa FROM gogn_med_vartolu s WHERE s.{{ column_name }} IS NULL			
UNION ALL
SELECT s.*, 0, 1, 0, 0 AS villa FROM gogn_med_vartolu s WHERE LEN (s.{{ column_name }}) != 10
UNION ALL
SELECT s.*, 0, 0, 1, 0 AS villa FROM gogn_med_vartolu s WHERE LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 0
UNION ALL
SELECT s.*, 0, 0, 0, 1 AS villa FROM gogn_med_vartolu s WHERE LEN (s.{{ column_name }}) = 10 AND ISNUMERIC (s.{{ column_name }}) = 1 AND s.smu_kt_1 NOT IN (8,9) AND s.smu_kt_vartala != s.smu_reiknud_vartala
{% endtest %}