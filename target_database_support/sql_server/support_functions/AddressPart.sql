CREATE OR ALTER FUNCTION [dbo].[AddressPart](@address varchar(500), @type varchar(20)) RETURNS varchar(500) 
AS BEGIN
	DECLARE @raddress varchar(500) = (SELECT REVERSE(@address))
	DECLARE @street varchar(250) = ''
	DECLARE @housenumber varchar(30) = ''
	DECLARE @housenumberAppendix VARCHAR(30) = ''

	-- Viðbót við húsnúmer
	DECLARE @housenumberStarts int = (SELECT PATINDEX ('%[0-9]%', @raddress))
	IF @housenumberStarts > 0 BEGIN
		SET @housenumberAppendix = COALESCE (TRIM (REVERSE (SUBSTRING (@raddress, 1, @housenumberStarts - 1))), '')
		SET @raddress = TRIM (SUBSTRING (@raddress, @housenumberStarts, LEN (@raddress) - (@housenumberStarts - 1)))
	END

	IF @type = 'APPENDIX'
		RETURN @housenumberAppendix
	
	-- Húsnúmer
	DECLARE @streetStarts int = (SELECT PATINDEX ('%[0-9] %', @raddress))
	IF @streetStarts > 0 BEGIN
		SET @housenumber = COALESCE (TRIM (REVERSE (SUBSTRING (@raddress, 1, @streetStarts))), '')
			IF 0 < (SELECT PATINDEX ('%[^0-9-]%', @housenumber))
			SET @housenumber = ''
		ELSE
			SET @raddress = TRIM (SUBSTRING (@raddress, @streetStarts + 1, LEN (@raddress) - (@streetStarts - 1)))
	END

	IF @type = 'NUMBER' 
		RETURN @housenumber	

	SET @street = COALESCE (TRIM (REVERSE (@raddress)), '')
	RETURN @street
END
