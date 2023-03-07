
CREATE OR ALTER FUNCTION dapi_specific.Birthday (@kt varchar(10)) RETURNS VARCHAR(10)
AS BEGIN
	DECLARE @dayOfMonth VARCHAR(2)
	DECLARE @month VARCHAR(2)
	DECLARE @year VARCHAR(4)
	
	IF LEN (@kt) != 10
		RETURN NULL
	
	DECLARE @first INT = CAST (SUBSTRING (@kt, 1, 1) AS INT)
	DECLARE @last INT = CAST (SUBSTRING (@kt, 10, 1) AS INT) -- Við getum vísað beint út af lendar villutékki fyrir framan

	IF  @first > 7 -- Kerfiskennitala, segir okkur ekkert!
		RETURN NULL

	IF @first < 4 -- Ekki fyrirtæki
		SET @dayOfMonth = SUBSTRING (@kt, 1, 2)
	ELSE -- Fyrirtæki
		SET @dayOfMonth = CAST ( @first - 4 AS VARCHAR(1)) + SUBSTRING (@kt, 2, 1)
	
	SET @month = SUBSTRING (@kt, 3, 2)

	IF @last = 8
		SET @year = '18'
	ELSE IF @last = 9
		SET @year = '19'	
	ELSE IF @last = 0
		SET @year = '20'
	ELSE
		RETURN NULL -- Við þekkjum ekki uppbygginguna

	SET @year = @year + SUBSTRING (@kt, 5, 2)

	-- Sökum kennitalna með ólöglegri dagsetningu, t.d. 29-02-1969
	IF ISDATE(@year + '-' + @month + '-' + @dayOfMonth) = 0 
		IF CAST (@month AS INT) = 2 AND CAST (@dayOfMonth AS INT) > 28
			SET @dayOfMonth = '28' -- Mikið gagnlegra en að fá null út
		ELSE
			RETURN NULL

	RETURN CONVERT (DATE, @dayOfMonth + '-' + @month + '-' + @year, 105)
END