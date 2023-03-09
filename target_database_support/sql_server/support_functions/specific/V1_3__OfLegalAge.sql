CREATE OR ALTER FUNCTION dapi_specific.OfLegalAge (@kyn VARCHAR(1) , @birthday DATE) RETURNS BIT
AS BEGIN
	IF @kyn IS NOT NULL
		IF @kyn IN ('3', '4', '8')
			RETURN 0
		ELSE
			RETURN 1
	IF dapi_utils.AgeInYears (@birthday) < 18
		RETURN 0
	RETURN 1
END