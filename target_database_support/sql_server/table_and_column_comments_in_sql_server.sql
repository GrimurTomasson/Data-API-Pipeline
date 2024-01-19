
-- SQL Server table/column comment helper functions

CREATE OR ALTER FUNCTION [dbo].[ApiCommentPropertyName]() RETURNS VARCHAR(128)
AS
BEGIN
	RETURN N'API_DESCRIPTION'
END;
GO



CREATE OR ALTER  PROCEDURE [dbo].[AddComment]
	@schemaName NVARCHAR(128) ,@objectName NVARCHAR(128) ,@columnName NVARCHAR(128), @comment SQL_VARIANT, @type NVARCHAR(10)
AS
BEGIN
	DECLARE @objectType VARCHAR(128) = dbo.ObjectType (@schemaName, @objectName)
	DECLARE @propertyName VARCHAR(128) = dbo.ApiCommentPropertyName()

	IF @type = N'COLUMN' 
		EXEC sys.sp_addextendedproperty
			@name	 = @propertyName
			,@value = @comment
			,@level0type = N'SCHEMA'
			,@level0name = @schemaName
			,@level1type = @objectType
			,@level1name = @objectName
			,@level2type = N'COLUMN'
			,@level2name = @columnName
	ELSE IF @type = N'RELATION'
		EXEC sys.sp_addextendedproperty
			@name	 = @propertyName
			,@value = @comment
			,@level0type = N'SCHEMA'
			,@level0name = @schemaName
			,@level1type = @objectType
			,@level1name = @objectName
	ELSE
		THROW 500000, 'Unknown object type to comment.', 16; -- Not optimal
END;
GO

CREATE  OR ALTER PROCEDURE [dbo].[RemoveComment]
	@schemaName NVARCHAR(128) ,@objectName NVARCHAR(128) ,@columnName NVARCHAR(128), @type NVARCHAR(10)
AS
BEGIN
	DECLARE @objectType VARCHAR(128) = dbo.ObjectType (@schemaName, @objectName)
	DECLARE @propertyName VARCHAR(128) = dbo.ApiCommentPropertyName()

	IF @type = N'COLUMN'
		EXEC sys.sp_dropextendedproperty
			@name = @propertyName
			,@level0type = N'SCHEMA'
			,@level0name = @schemaName
			,@level1type = @objectType
			,@level1name = @objectName
			,@level2type = N'COLUMN'
			,@level2name = @columnName
	ELSE IF @type = N'RELATION'
		EXEC sys.sp_dropextendedproperty
			@name = @propertyName
			,@level0type = N'SCHEMA'
			,@level0name = @schemaName
			,@level1type = @objectType
			,@level1name = @objectName
	ELSE
		THROW 500000, 'Unknown object type to remove comment.', 16; -- Not optimal
END;
GO

CREATE  OR ALTER PROCEDURE [dbo].[AddOrReplaceColumnComment]
	@schemaName NVARCHAR(128) ,@objectName NVARCHAR(128) ,@columnName NVARCHAR(128), @comment SQL_VARIANT
AS
BEGIN
	DECLARE @type NVARCHAR(10) = N'COLUMN'
	
	BEGIN TRY
		EXEC RemoveComment @schemaName, @objectName, @columnName, @type
	END TRY
	BEGIN CATCH -- We don't care, if it doesn't exist we can add it.
	END CATCH
	EXEC AddComment @schemaName, @objectName, @columnName, @comment, @type
END;
GO

CREATE  OR ALTER PROCEDURE [dbo].[AddOrReplaceRelationComment]
	@schemaName NVARCHAR(128) ,@objectName NVARCHAR(128), @comment SQL_VARIANT
AS
BEGIN
	DECLARE @type NVARCHAR(10) = N'RELATION'
	
	BEGIN TRY
		EXEC RemoveComment @schemaName, @objectName, N'', @type
	END TRY
	BEGIN CATCH -- We don't care, if it doesn't exist we can add it.
	END CATCH
	EXEC AddComment @schemaName, @objectName, N'', @comment, @type
END;
GO

-- Usage example

CREATE TABLE dbo.some_table(
	first varchar(10)
	,second int
)

EXEC AddOrReplaceRelationComment 'dbo', 'some_table', 'This is our demo table description'
EXEC AddOrReplaceColumnComment 'dbo', 'some_table', 'first', 'This is the first useless column in the relation'
EXEC AddOrReplaceColumnComment 'dbo', 'some_table', 'second', 'Another highly valuable comment'
-- Updating comment
EXEC AddOrReplaceColumnComment 'dbo', 'some_table', 'first', 'A much better comment!'

-- You can either validate the comments through a UI or run the following query
;WITH comment AS (
	SELECT 
		*
	FROM
		sys.extended_properties ep
	WHERE
		ep.major_id = OBJECT_ID ('dbo.some_table')
		AND ep.name = dbo.ApiCommentPropertyName()
)
SELECT 'TABLE' AS [type], '' AS [column], s.value AS comment FROM comment s WHERE s.minor_id = 0
UNION ALL
SELECT 'COLUMN', c.name, s.value FROM comment s JOIN sys.columns c ON c.object_id = s.major_id AND c.column_id = s.minor_id

-- Cleanup
DROP TABLE dbo.some_table