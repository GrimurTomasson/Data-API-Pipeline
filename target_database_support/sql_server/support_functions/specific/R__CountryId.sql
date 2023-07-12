
CREATE or ALTER FUNCTION dapi_specific.CountryId (@postalCode VARCHAR(3), @communityCode VARCHAR(4), @address VARCHAR(50)) RETURNS VARCHAR(250)
AS BEGIN
	
	IF LEN (@postalCode) = 3
		RETURN (SELECT i.id FROM Latest@ISO.country_code_v1 i WHERE i.alpha_2_code = 'is')
	
	DECLARE @postalCodeId VARCHAR(250) = (SELECT i.id FROM Latest@ISO.country_code_v1 i WHERE LOWER (i.name_is) = LOWER (@address))
	DECLARE @communityCodeId VARCHAR(250) = (SELECT i.id FROM Latest@ISO.country_code_v1 i WHERE i.alpha_2_code = LOWER (SUBSTRING (COALESCE (@communityCode, '0000'), 3,2)))
	
	IF @communityCodeId IS NOT NULL
		RETURN @communityCodeId

	RETURN @postalCodeId
END