-- Create schema if missing
DECLARE @dapiSchema varchar(30) = 'dapi'
IF (SELECT COUNT(1) FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = @dapiSchema) = 0
	EXECUTE( 'CREATE SCHEMA ' + @dapiSchema)
GO

CREATE OR ALTER FUNCTION dapi.ConstructIdOne (@part1 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END

GO

CREATE OR ALTER FUNCTION dapi.ConstructIdTwo (@part1 varchar(250), @part2 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL') 
				+ '_' + COALESCE (UPPER (@part2), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END

GO

CREATE OR ALTER FUNCTION dapi.ConstructIdThree (@part1 varchar(250), @part2 varchar(250), @part3 varchar(250)) RETURNS VARCHAR(MAX)
AS BEGIN
	RETURN TRANSLATE (
				COALESCE (UPPER (@part1), 'NULL') 
				+ '_' + COALESCE (UPPER (@part2), 'NULL') 
				+ '_' + COALESCE (UPPER (@part3), 'NULL')
			, ' .,:;-/?!()[]*#$%&=@><|^~"', '__________________________'
			) COLLATE DATABASE_DEFAULT
END

GO

CREATE OR ALTER FUNCTION dapi.ConstructIdFour (@part1 varchar(250), @part2 varchar(250), @part3 varchar(250), @part4 varchar(250)) RETURNS VARCHAR(MAX)
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

CREATE OR ALTER FUNCTION dapi.ConstructIdFive (@part1 varchar(250), @part2 varchar(250), @part3 varchar(250), @part4 varchar(250), @part5 varchar(250)) RETURNS VARCHAR(MAX)
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