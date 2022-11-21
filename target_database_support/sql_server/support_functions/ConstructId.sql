-- Create schema if missing
DECLARE @apiTools varchar(30) = 'API_Tools'
IF (SELECT COUNT(1) FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = @apiTools) = 0
	EXECUTE( 'CREATE SCHEMA ' + @apiTools)
GO

CREATE OR ALTER FUNCTION API_Tools.ConstructIdOne (@part1 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END

GO

CREATE OR ALTER FUNCTION API_Tools.ConstructIdTwo (@part1 varchar(250), @part2 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL') 
				+ '_' + COALESCE (UPPER (@part2), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END

GO

CREATE OR ALTER FUNCTION API_Tools.ConstructIdThree (@part1 varchar(250), @part2 varchar(250), @part3 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL') 
				+ '_' + COALESCE (UPPER (@part2), 'NULL') 
				+ '_' + COALESCE (UPPER (@part3), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END

GO

CREATE OR ALTER FUNCTION API_Tools.ConstructIdFour (@part1 varchar(250), @part2 varchar(250), @part3 varchar(250), @part4 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL') 
				+ '_' + COALESCE (UPPER (@part2), 'NULL') 
				+ '_' + COALESCE (UPPER (@part3), 'NULL') 
				+ '_' + COALESCE (UPPER (@part4), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END

GO

CREATE OR ALTER FUNCTION API_Tools.ConstructIdFive (@part1 varchar(250), @part2 varchar(250), @part3 varchar(250), @part4 varchar(250), @part5 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL') 
				+ '_' + COALESCE (UPPER (@part2), 'NULL') 
				+ '_' + COALESCE (UPPER (@part3), 'NULL') 
				+ '_' + COALESCE (UPPER (@part4), 'NULL')
				+ '_' + COALESCE (UPPER (@part5), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END