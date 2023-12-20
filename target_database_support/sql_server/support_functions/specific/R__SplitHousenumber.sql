CREATE OR ALTER FUNCTION dapi_specific.SplitHousenumber(@housenumber varchar(100), @type varchar(10)) RETURNS INT
AS BEGIN
	DECLARE @translateFrom VARCHAR(250) = 'aábcdðeéfghiíjklmnoóprstuúvxyýþæözAÁBCDÐEÉFGHIÍJKLMNOÓPRSTUÚVXYÝÞÆÖZÃ*/!,.<>()&'
	DECLARE @translateTo VARCHAR(250) =   '                                                                               '
	DECLARE @retValString VARCHAR(20) = NULL
	SET @housenumber = TRIM ('-' FROM TRIM (TRANSLATE (REPLACE (@housenumber, ' og ', '-'), @translateFrom, @translateTo)))

	IF LEN (COALESCE (@housenumber, '')) = 0
		RETURN NULL

	IF CHARINDEX (' ', @housenumber ) > 0 -- Furðu skráningar með viðbótum fyrir aftan númeraseríu
		SET @housenumber = LEFT (@housenumber, CHARINDEX (' ', @housenumber ) - 1)

	DECLARE @numberOfItems INT = (SELECT COUNT(1) FROM STRING_SPLIT(@housenumber, '-'))
	IF @numberOfItems = 1
		SET @retValString = @housenumber
	ELSE IF @numberOfItems > 1 AND @type = 'FROM'
		SET @retValString = (SELECT TOP 1 VALUE FROM STRING_SPLIT(@housenumber, '-') ORDER BY CAST (VALUE AS INT) ASC )
	ELSE IF @numberOfItems > 1 AND @type = 'TO'
		SET @retValString = (SELECT TOP 1 CAST (VALUE AS INT) FROM STRING_SPLIT(@housenumber, '-') ORDER BY CAST (VALUE AS INT) DESC )

	RETURN CASE WHEN ISNUMERIC (@retValString) = 1 THEN CAST (@retValString AS int) ELSE NULL END
END