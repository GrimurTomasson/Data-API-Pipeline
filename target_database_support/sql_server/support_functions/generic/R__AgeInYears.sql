CREATE OR ALTER FUNCTION [dapi_generic].[AgeInYears] (@dateOfBirth date, @dateOfDeath date = null) RETURNS INT
AS BEGIN
	IF @dateOfDeath IS NULL 
		RETURN FLOOR (DATEDIFF (DAY, @dateOfBirth, CAST (GETDATE() AS DATE)) / 365.2425)
	RETURN FLOOR (DATEDIFF (DAY, @dateOfBirth, @dateOfDeath) / 365.2425)
END