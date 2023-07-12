CREATE or alter FUNCTION dapi_specific.PartOfName (@name varchar(200), @part varchar(20)) RETURNS varchar(200)
AS BEGIN
	SET @name = TRIM(@name)
	DECLARE @firstSpace INT = CHARINDEX (' ', @name)
	DECLARE @lastSpace INT = LEN(@name) + 1 - CHARINDEX (' ', REVERSE (@name))
	
	IF @part = 'FIRST' AND @firstSpace > 1 
		RETURN SUBSTRING (@name, 1, @firstSpace - 1)
	ELSE IF @part = 'MIDDLE'
		IF @firstSpace = @lastSpace --No middle name, only one space
			RETURN ''
		ELSE
			RETURN SUBSTRING (@name, @firstSpace + 1, @lastSpace - @firstSpace - 1)
	ELSE IF @part = 'LAST' AND @firstSpace > 1
		RETURN SUBSTRING (@name, @lastSpace + 1, LEN(@name) - @lastSpace)
	ELSE
		RETURN @name

	RETURN 'Unknown part intput!'
END


