CREATE OR ALTER FUNCTION API_Tools.OfLegalAge (@kyn VARCHAR(1) , @birthday DATE) RETURNS BIT
AS BEGIN
	IF @kyn IS NOT NULL
		IF @kyn IN ('3', '4', '8')
			RETURN 0
		ELSE
			RETURN 1
	IF API_Tools.AgeInYears (@birthday) < 18
		RETURN 0
	RETURN 1
END