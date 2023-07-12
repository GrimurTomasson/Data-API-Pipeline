-- Create schema if missing
DECLARE @dapiSchema varchar(30) = 'dapi_generic'
IF (SELECT COUNT(1) FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = @dapiSchema) = 0
	EXECUTE( 'CREATE SCHEMA ' + @dapiSchema)
GO

CREATE OR ALTER FUNCTION [dapi_generic].[StringToDate] (@dateString VARCHAR(30), @mask VARCHAR(30)) RETURNS DATE
AS BEGIN
	DECLARE @day varchar(2)
	DECLARE @month varchar(2)
	DECLARE @year varchar(4)
	DECLARE @retval date = null

	SET @dateString = TRIM(@dateString)
	IF  LEN (@dateString) < LEN (@mask) -- Við erum til í að vinna með dagsetningarstrengi sem innihalda t.d. tíma
		RETURN null 

	SET @day = SUBSTRING (@dateString, CHARINDEX ('DD', @mask), 2)
	SET @month = SUBSTRING (@dateString, CHARINDEX ('MM', @mask), 2)
	IF LEN (@dateString) = 6  BEGIN
		SET @year = SUBSTRING (@dateString, CHARINDEX ('YY', @mask), 2)
		SET @retval = CONVERT (date, @day + '-' + @month + '-' + @year, 5)
	END
	IF LEN (@dateString) >= 8 BEGIN
		SET @year = SUBSTRING (@dateString, CHARINDEX ('YYYY', @mask), 4)
		SET @retval = CONVERT (date, @day + '-' + @month + '-' + @year, 105)
	END
	RETURN @retval
END