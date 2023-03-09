CREATE OR ALTER FUNCTION dapi_generic.StringToTime (@timeString nvarchar(20), @defaultValue nvarchar(20), @separator nvarchar(1) = ':') RETURNS TIME AS 
BEGIN
	DECLARE @retVal time

	IF @timeString IS NULL
		RETURN TRY_CONVERT (TIME, @defaultValue)

	IF LEN (@timeString) < 3
		SET @timeString = RIGHT('0000' + @timeString, 4)

	IF LEN (@timeString) = 3 -- Vantar 0 fyrir framan
		SET @timeString = '0' + @timeString

	IF LEN (@timeString) >= 4 AND CHARINDEX (@separator, @timeString) != 3 -- Missing hour/minute separator
		SET @timeString = STUFF (@timeString, 3, 0, ':')

	IF LEN (@timeString) > 5 AND CHARINDEX (@separator, REVERSE(@timeString)) != 3 -- Missing hour/minute separator
		SET @timeString = STUFF (@timeString, 6, 0, ':')

	SET @retVal = TRY_CONVERT (TIME, @timeString) 
	IF @retVal IS NOT NULL
		RETURN @retVal
	RETURN TRY_CONVERT (TIME, @defaultValue)
END
