CREATE OR ALTER FUNCTION [dapi_generic].[PhonenumberToE164] (@phoneNumber nvarchar(100), @defaultCountryCode nvarchar(3) = '354') RETURNS nvarchar(15) 
AS BEGIN
	SET @phoneNumber = REPLACE (TRANSLATE (@phoneNumber, '-+', '  '), ' ', '') 
	IF @phoneNumber IS NULL
		return NULL

	IF LEN (@phoneNumber) > 2 AND CHARINDEX ('00', @phoneNumber) = 1
		SET @phoneNumber = SUBSTRING (@phoneNumber, 3, LEN (@phoneNumber) - 2)

	IF LEN (@phoneNumber) < 10
		RETURN @defaultCountryCode + @phoneNumber

	RETURN @phoneNumber
END