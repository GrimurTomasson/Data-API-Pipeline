CREATE OR ALTER FUNCTION dbo.StringToDate (@dateString VARCHAR(10), @mask VARCHAR(10)) RETURNS DATE
AS BEGIN
	DECLARE @day varchar(2)
	DECLARE @month varchar(2)
	DECLARE @year varchar(4)
	DECLARE @retval date = null

	SET @dateString = TRIM(@dateString)
	IF LEN (@dateString) NOT IN (6, 8) OR LEN (@dateString) != LEN (@mask) -- Óeðlilegt inntak
		RETURN null -- Loggun í villutöflu væri hentug hér

	SET @day = SUBSTRING (@dateString, CHARINDEX ('DD', @mask), 2)
	SET @month = SUBSTRING (@dateString, CHARINDEX ('MM', @mask), 2)
	IF LEN (@dateString) = 6  BEGIN
		SET @year = SUBSTRING (@dateString, CHARINDEX ('YY', @mask), 2)
		SET @retval = CONVERT (date, @day + '-' + @month + '-' + @year, 5)
	END
	IF LEN (@dateString) = 8 BEGIN
		SET @year = SUBSTRING (@dateString, CHARINDEX ('YYYY', @mask), 4)
		SET @retval = CONVERT (date, @day + '-' + @month + '-' + @year, 105)
	END
	RETURN @retval
END