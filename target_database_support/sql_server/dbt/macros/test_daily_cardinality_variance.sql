{% test daily_cardinality_variance(model, history_column_name, ok_abs_percentage_to, ok_abs_percentage_from=0) %}

WITH rows_per_day AS (
	SELECT 
		{{ history_column_name }} AS history_date, COUNT(*) as row_count
	FROM 
		{{ model }} s
	GROUP BY 
		history_date
)
,rows_per_day_and_last AS (
	SELECT 
		s.*
		,COALESCE (LAG (s.row_count) OVER (ORDER BY s.history_date DESC), s.row_count) AS last_row_count
	from 
		rows_per_day s
)
,all_stats AS (
	SELECT
		s.*
		,ABS (((s.row_count * 100) / s.last_row_count) -100)  AS change_percentage
	FROM
		rows_per_day_and_last s
)
SELECT 
	s.*
FROM
	all_stats s
WHERE
	s.change_percentage NOT BETWEEN {{ ok_abs_percentage_from }} AND {{ ok_abs_percentage_to }}

{% endtest %}