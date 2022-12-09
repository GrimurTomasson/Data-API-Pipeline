USE [ReferenceAPI]
GO
/****** Object:  UserDefinedFunction [dbo].[AddressPart]    Script Date: 6.12.2022 11:43:43 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or ALTER FUNCTION API_Tools.[AddressPart](@address varchar(500), @type varchar(20)) RETURNS varchar(500) 
AS BEGIN
	DECLARE @raddress varchar(500) = (SELECT REVERSE(@address))
	DECLARE @street varchar(250) = ''
	DECLARE @housenumber varchar(30) = ''
	DECLARE @housenumberAppendix VARCHAR(30) = ''

	-- Viðbót við húsnúmer
	DECLARE @hasAppendix int = (SELECT PATINDEX ('%[a-öA-Ö][0-9]%', @raddress))
	IF @hasAppendix > 0 AND (LEN (@address) = @hasAppendix + 1 OR SUBSTRING ( @raddress, @hasAppendix - 1, 1) = ' ') BEGIN -- Eitthvað fannst og það er annað hvort við enda strengs eða bil á eftir
		SET @housenumberAppendix = COALESCE (TRIM (SUBSTRING (@raddress, @hasAppendix, 1)), '')
	END

	IF @type = 'APPENDIX'
		return @housenumberAppendix
	
	-- Húsnúmer
	DECLARE @houseNumberIndex int = (SELECT PATINDEX ('%[0-9]%', @address))
	IF @houseNumberIndex > 0 BEGIN
		declare @houseNumberPart varchar(50) = SUBSTRING (@address, @houseNumberIndex, len(@address) - (@houseNumberIndex - 1))
		declare @houseNumberEndsIndex INT = (SELECT PATINDEX ('%[^0-9\-]%', @houseNumberPart))
		IF @houseNumberEndsIndex = 0
			SET @houseNumberEndsIndex = LEN (@address) -- Heimilisfangið endar á húsnúmeri
		SET @houseNumber = SUBSTRING (@houseNumberPart, 1, @houseNumberEndsIndex - 1)
		WHILE PATINDEX ('[^0-9]', SUBSTRING (REVERSE (@houseNumber), 1, 1)) = 1
			SET @houseNumber = SUBSTRING (@houseNumber, 1, LEN (@houseNumber) - 1)
			
		SET @address = SUBSTRING (@address, 1, @houseNumberIndex - 1)
	END

	IF @type = 'NUMBER' 
		return @housenumber

	DECLARE @lodIndex INT = PATINDEX ('%[^a-öA-Ö]lóð%', @address)
	IF @lodIndex > 0 
		SET @address = SUBSTRING (@address, 1, @lodIndex - 1) -- Klippum allt sem snýr að lóð af

	DECLARE @landIndex INT = PATINDEX ('%[^a-öA-Ö\-]land%', @address)
	IF @landIndex > 0 
		SET @address = SUBSTRING (@address, 1, @landIndex - 1) -- Klippum allt sem snýr að landi af

	WHILE PATINDEX ('[^a-öA-Ö \.\)]', SUBSTRING (REVERSE (@address), 1, 1)) = 1 
		SET @address = SUBSTRING (@address, 1, LEN(@address) - 1)

	SET @street = COALESCE (TRIM (@address), '')

	return @street
end 