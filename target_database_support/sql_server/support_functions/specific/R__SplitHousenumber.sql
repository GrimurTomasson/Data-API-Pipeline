CREATE OR ALTER FUNCTION dapi_specific.SplitHousenumber(@housenumber varchar(100), @type varchar(10)) RETURNS INT
AS BEGIN
	IF LEN (COALESCE (@housenumber, '')) = 0
		RETURN NULL

	DECLARE @numberOfItems INT = (SELECT COUNT(1) FROM STRING_SPLIT(@housenumber, '-'))
	IF @numberOfItems = 1
		RETURN @housenumber
	
	IF @type = 'FROM'
		RETURN (SELECT TOP 1 value FROM STRING_SPLIT(@housenumber, '-') ORDER BY 1 ASC )

	RETURN (SELECT TOP 1 value FROM STRING_SPLIT(@housenumber, '-') ORDER BY 1 DESC )
END