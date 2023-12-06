{% test phonenumber_ok(model, column_name, country_code_model, country_code_column) %}

WITH 
simi AS (
	SELECT 
		* 
	FROM 
		{{ model }}
),
i_lagi AS (
	SELECT -- Símanúmer sem uppfylla E.164 standard. Hefjast á landsnúmeri, innihalda ekkert nema tölustafi (ekki plús fremst, ekki bil, ekki bandstrik o.s.frv.).
		s.* 
	FROM 
		simi s
		JOIN {{ country_code_model }} l ON CHARINDEX (l.{{ country_code_column }}, s.{{ column_name }}) = 1 -- Símanúmerið hefst á landskóða.
	WHERE 
		LEN (s.bodleid) BETWEEN 10 AND 15
		AND s.{{ column_name }} NOT LIKE '%[^0-9]%' -- Ekkert nema tölustafir
)
SELECT * FROM simi
EXCEPT
SELECT * from i_lagi

{% endtest %}