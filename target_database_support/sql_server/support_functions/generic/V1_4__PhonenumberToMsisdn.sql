-- https://en.wikipedia.org/wiki/MSISDN
CREATE OR ALTER FUNCTION dapi_generic.PhonenumberToMsisdn (@phoneNumber nvarchar(100), @countryCode nvarchar(3) = '354') RETURNS nvarchar(15) 
AS BEGIN
	SET @phoneNumber = REPLACE (TRANSLATE (@phoneNumber, '- ', '  '), ' ', '') -- + merkið fær að halda sér, hefð.
	IF @phoneNumber IS NULL
		return NULL

	IF LEN (@phoneNumber) = 7
		SET @phoneNumber = @countryCode + @phoneNumber

	IF SUBSTRING (@phoneNumber, 1, 1) != '+'
		SET @phoneNumber = '+' + @phoneNumber

	RETURN @phoneNumber
END