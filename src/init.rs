pub const INITSQL : &str = "
CREATE FN [sys].[ClearTable](t int) AS 
BEGIN 
  EXECUTE( 'DELETE FROM ' | sys.TableName(t) | ' WHERE true' )
END
GO

CREATE FN [sys].[ColName]( table int, colId int ) RETURNS string AS
BEGIN
  DECLARE i int
  SET i = 0
  FOR result = Name FROM sys.Column WHERE Table = table
  BEGIN
    IF i = colId RETURN result
    SET i = i + 1
  END
  RETURN '?bad colId?'  
END
GO

CREATE FN [sys].[ColNames]( table int ) RETURNS string AS
BEGIN
  DECLARE col string
  SET result = '(Id'
  FOR col = Name FROM sys.Column WHERE Table = table
    SET result |= ',' | sys.QuoteName(col)
  RETURN result | ')'
END
GO

CREATE FN [sys].[ColValues]( table int ) RETURNS string AS
BEGIN
  DECLARE col string
  SET result = 'Id'
  FOR col = CASE 
    WHEN Type % 8 = 2 THEN 'sys.SingleQuote(' | Name | ')'
    WHEN Type % 8 = 4 THEN 'sys.FloatLiteral(' | Name | ')'
    WHEN Id = 2 OR Id = 9 THEN '''ALLOCPAGE()'''
    ELSE Name
  END
  FROM sys.Column WHERE Table = table
    SET result |= '|'',''|' | col
  RETURN result
END
GO

CREATE FN [sys].[Cols]( table int ) RETURNS string AS
BEGIN
  DECLARE col string, list string
  FOR col = sys.QuoteName(Name) | ' ' | sys.TypeName(Type)
  FROM sys.Column WHERE Table = table
    SET list |= CASE WHEN  list = '' THEN col ELSE ',' | col END
  RETURN '(' | list | ')'
END
GO

CREATE FN [sys].[Dot]( schema string, name string ) RETURNS string AS
BEGIN
  RETURN sys.QuoteName( schema ) | '.' | sys.QuoteName( name )
END
GO

CREATE FN [sys].[DropColumn]( t int, cname string ) AS 
BEGIN 
  DELETE FROM sys.Column WHERE Table = t AND Name = cname

  /* Could delete browse column info as well (todo)*/
END
GO

CREATE FN [sys].[DropIndex]( ix int ) AS
BEGIN
  /* Note: this should not be called directly, instead use DROP INDEX statement */
  DELETE FROM sys.IndexColumn WHERE Index = ix
  DELETE FROM sys.Index WHERE Id = ix
END
GO

CREATE FN [sys].[DropSchema]( sid int ) AS
/* Note: this should not be called directly, instead use DROP SCHEMA statement */
BEGIN
  DECLARE schema string, name string
  SET schema = Name FROM sys.Schema WHERE Id = sid
  FOR name = Name FROM sys.Function WHERE Schema = sid EXECUTE( 'DROP FN ' | sys.Dot(schema,name) )
  FOR name = Name FROM sys.Table WHERE Schema = sid EXECUTE( 'DROP TABLE ' | sys.Dot(schema,name) )
  DELETE FROM sys.Schema WHERE Id = sid
END
GO

CREATE FN [sys].[DropTable]( t int ) AS 
/* Note: this should not be called directly, instead use DROP TABLE statement */
BEGIN
  /* Delete the rows */
  EXECUTE( 'DELETE FROM ' | sys.TableName(t) | ' WHERE true' )

  DECLARE id int
  /* Delete the Index data */
  FOR id = Id FROM sys.Index WHERE Table = t
  BEGIN
    DELETE FROM sys.IndexColumn WHERE Index = id
  END
  DELETE FROM sys.Index WHERE Table = t
   /* Delete the column data */
  FOR id = Id FROM sys.Column WHERE Table = t
  BEGIN
    DELETE FROM browse.Column WHERE Id = id
  END
  /* Delete other data */
  DELETE FROM browse.Table WHERE Id = t
  DELETE FROM sys.Column WHERE Table = t
  DELETE FROM sys.Table WHERE Id = t
END
GO

CREATE FN [sys].[FloatLiteral]( x float ) RETURNS string AS 
BEGIN
   RETURN 'PARSEFLOAT(' | sys.SingleQuote( '' | x ) | ')'
END
GO

CREATE FN [sys].[IncludeSchema]( mode int, s string ) RETURNS bool AS 
BEGIN
  IF s = 'sys' OR s = 'date' OR s = 'web' OR s = 'log' OR s = 'handler' OR s = 'browse'
    OR s = 'email' OR s = 'timed' OR s = 'login'
  RETURN mode = 2

  ELSE
  RETURN mode = 1
END
GO

CREATE FN [sys].[IndexCols]( index int ) RETURNS string AS
BEGIN
  DECLARE table int, list string, col string
  SET table = Table FROM sys.Index WHERE Id = index
  FOR col = sys.QuoteName(sys.ColName( table, ColId )) FROM sys.IndexColumn WHERE Index = index
    SET list |= CASE WHEN  list = '' THEN col ELSE ',' | col END
  RETURN '(' | list | ')'
END
GO

CREATE FN [sys].[IndexName]( index int ) RETURNS string AS
BEGIN
  SET result = sys.QuoteName(Name) FROM sys.Index WHERE Id = index
END
GO

CREATE FN [sys].[QuoteName]( s string ) RETURNS string AS
BEGIN
  RETURN '[' | REPLACE( s, ']', ']]' ) | ']'
END
GO

CREATE FN [sys].[SchemaName]( schema int) RETURNS string AS 
BEGIN 
  SET result = Name FROM sys.Schema WHERE Id = schema
END
GO

CREATE FN [sys].[ScriptBrowse]( t int ) AS
BEGIN
  -- Script browse information for Table t.
  -- Looks up Table and Column Id values (tid,cid) by name in case they change.
  DECLARE sid int, tname string, sname string
  SET sid = Schema, tname = Name FROM sys.Table WHERE Id = t
  SET sname = Name FROM sys.Schema WHERE Id = sid

  SELECT '
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = ' | sys.SingleQuote(sname) | '
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = ' | sys.SingleQuote(tname) 
SELECT '
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'
    | sys.SingleQuote(NameFunction) |','|sys.SingleQuote(SelectFunction) 
    | ',' | sys.SingleQuote(DefaultOrder) | ',' | sys.SingleQuote(Title) | ',' 
    | sys.SingleQuote(Description) | ',' | Role | ')'
  FROM browse.Table WHERE Id = t

  DECLARE cid int, cname string, ref int, rtname string, rs int, rsname string
  FOR cid=Id, cname=Name FROM sys.Column WHERE Table = t
  BEGIN
    SET ref= 0 SET ref = RefersTo FROM browse.Column WHERE Id = cid
    SET rtname = '', rs=0 SET rtname = Name, rs = Schema FROM sys.Table WHERE Id = ref
    SET rsname = '' SET rsname = Name FROM sys.Schema WHERE Id = rs

    SELECT '
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = ' | sys.SingleQuote(cname) | '
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = ' | sys.SingleQuote(rsname) | ' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ' | sys.SingleQuote(rtname) | '

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, '
      |Position|','|sys.SingleQuote(Label)|','|sys.SingleQuote(Description)
      |',rt,'|sys.SingleQuote(Default)|','|InputCols|','|InputRows|','|sys.SingleQuote(InputFunction)|','|Datatype|')'
    FROM browse.Column WHERE Id = cid
  END
  SELECT '
GO'
END
GO

CREATE FN [sys].[ScriptData]( t int, mode int ) AS
BEGIN
    DECLARE filter string, tname string, schema int, sname string

    IF t < 6 SET filter = CASE 
       WHEN t = 1 THEN ' WHERE Id != 1' -- Sys
       WHEN t = 2 THEN ' WHERE Id > 6' -- Table
       WHEN t = 3 THEN ' WHERE Table > 6' -- Field
       WHEN t = 4 THEN ' WHERE Id > 7' -- Index
       WHEN t = 5 THEN ' WHERE Index > 7' -- IndexColumn
       ELSE '' END
    ELSE IF mode = 1
    BEGIN
      SET tname = sys.TableName(t) 
      SET filter = CASE
        WHEN tname = '[log].[Transaction]'
          OR tname = '[browse].[Column]' OR tname = '[browse].[Table]' THEN ' WHERE false'
        ELSE '' END
    END  
    ELSE IF mode = 2
    BEGIN
      SET tname = sys.TableName(t) 
      SET schema = Schema FROM sys.Table WHERE Id = t
      SET sname = sys.SchemaName(schema)
      SET filter = CASE
        WHEN sname = 'log' OR sname = 'email' OR sname = 'login' OR sname = 'timed'
          OR tname = '[browse].[Column]' OR tname = '[browse].[Table]' THEN ' WHERE false'
        ELSE '' END
    END    

    SELECT '
INSERT INTO ' | sys.TableName(t) | sys.ColNames(t) | ' VALUES 
'       

    EXECUTE( 'SELECT ''(''|' | sys.ColValues(t) | '|'')
''' | ' FROM ' | sys.TableName(t) | filter )

    SELECT 'GO
'
END
GO

CREATE FN [sys].[ScriptSchema]( s int, mode int ) AS
BEGIN
  DECLARE sname string SET sname = sys.SchemaName(s)

  /* Create the schema, tables, indexes */
  
  IF sname != 'sys'
  BEGIN
    SELECT '
--############################################
CREATE SCHEMA ' | sys.QuoteName( sname ) | '
GO
'

    DECLARE t int
    FOR t = Id FROM sys.Table WHERE Schema = s ORDER BY Name
    BEGIN
      EXEC sys.ScriptTable(t)
    END
  END

  /******* Script functions *******/

  SELECT '
CREATE FN ' | sys.Dot( sname,Name) | Def | '
GO
' 
  FROM sys.Function  WHERE Schema = s ORDER BY Name

  /******* Script Data *******/

  IF sname != 'sys'
  BEGIN
    FOR t = Id FROM sys.Table WHERE Schema = s ORDER BY Name
      EXEC sys.ScriptData(t,mode)
  END
END
GO

CREATE FN [sys].[ScriptSchemaBrowse]( s int ) AS
BEGIN
  DECLARE t int
  FOR t = Id FROM sys.Table WHERE Schema = s ORDER BY Name
  BEGIN
    EXEC sys.ScriptBrowse(t)
  END
END
GO

CREATE FN [sys].[ScriptTable]( t int ) AS
BEGIN
  SELECT '
CREATE TABLE ' | sys.TableName(t) | sys.Cols(t) | ' 
GO
'
  DECLARE ix int, name string
  FOR ix = Id, name = Name FROM sys.Index WHERE Table = t
  BEGIN
    SELECT '
CREATE INDEX ' | sys.QuoteName(name) | ' ON ' | sys.TableName(t) | sys.IndexCols(ix) | '
GO
'
  END
END
GO

CREATE FN [sys].[SingleQuote]( s string ) RETURNS string AS
BEGIN
  RETURN '''' | REPLACE( s, '''', '''''' ) | ''''
END
GO

CREATE FN [sys].[TableName]( table int ) RETURNS string AS
BEGIN
  DECLARE schema int, name string
  SET schema = Schema, name = Name FROM sys.Table WHERE Id = table
  IF name = '' RETURN ''
  SET result = sys.Dot( Name, name ) FROM sys.Schema WHERE Id = schema
END
GO

CREATE FN [sys].[TypeName]( t int ) RETURNS string AS 
BEGIN 
  DECLARE p int
  SET p = t / 8
  RETURN CASE 
    WHEN t = 0 THEN 'none'
    WHEN t = 13 THEN 'bool'
    WHEN t = 36 THEN 'float' 
    WHEN t = 68 THEN 'double'
    WHEN t = 67 THEN 'int'
    WHEN t = 129 THEN 'binary'
    WHEN t = 130 THEN 'string'
    ELSE 
    CASE 
       WHEN t % 8 = 1 THEN 'binary(' | (p-1) | ')'
       WHEN t % 8 = 2 THEN 'string(' | (p-1) | ')'
       WHEN t % 8 = 3 THEN 'int(' | p | ')'
       ELSE '???'
    END
  END
END
GO

--############################################
CREATE SCHEMA [date]
GO

CREATE FN [date].[DaysToString]( date int ) RETURNS string AS
BEGIN
  RETURN -- date.WeekDayToString( 1 + (date+5) % 7 ) | ' ' | 
    date.YearMonthDayToString( date.DaysToYearMonthDay( date ) )
END
GO

CREATE FN [date].[DaysToYearDay]( days int ) RETURNS int AS
BEGIN
  -- Given a date represented by the number of days since 1 Jan 0000
  -- calculate a date in Year/Day representation stored as
  -- year * 512 + day where day is 1..366, the day in the year.
  
  DECLARE year int, day int, cycle int
  -- 146097 is the number of the days in a 400 year cycle ( 400 * 365 + 97 leap years )
  SET cycle = days / 146097
  SET days = days - 146097 * cycle -- Same as days % 146097
  SET year = days / 365
  SET day = days - year * 365 -- Same as days % 365
  -- Need to adjust day to allow for leap years.
  -- Leap years are 0, 4, 8, 12 ... 96, not 100, 104 ... not 200... not 300, 400, 404 ... not 500.
  -- Adjustment as function of y is 0 => 0, 1 => 1, 2 =>1, 3 => 1, 4 => 1, 5 => 2 ..
  SET day = day - ( year + 3 ) / 4 + ( year + 99 ) / 100 - ( year + 399 ) / 400
  
  IF day < 0
  BEGIN
    SET year = year - 1
    SET day = day + CASE WHEN date.IsLeapYear( year ) THEN 366 ELSE 365 END
  END
  RETURN 512 * ( cycle * 400 + year ) + day + 1
END
GO

CREATE FN [date].[DaysToYearMonthDay]( days int ) RETURNS int AS
BEGIN
  RETURN date.YearDayToYearMonthDay( date.DaysToYearDay( days ) )
END
GO

CREATE FN [date].[IsLeapYear]( y int ) RETURNS bool AS
BEGIN
  RETURN y % 4 = 0 AND ( y % 100 != 0 OR y % 400 = 0 )
END
GO

CREATE FN [date].[MicroSecToString](micro int) RETURNS string AS
BEGIN
  DECLARE day int, sec int, min int, hour int
  SET sec = micro / 1000000
  SET day = sec / 86400 -- 86400 = 24 * 60 * 60, seconds in a day.
  SET sec = sec % 86400
  SET min = sec / 60
  SET sec = sec % 60
  SET hour = min / 60
  SET min = min % 60
  RETURN date.DaysToString(  day ) | ' ' | hour | ':' | min | ':' | sec
END
GO

CREATE FN [date].[MonthToString]( m int ) RETURNS string AS
BEGIN
  RETURN CASE
    WHEN m = 1 THEN 'Jan'
    WHEN m = 2 THEN 'Feb'
    WHEN m = 3 THEN 'Mar'
    WHEN m = 4 THEN 'Apr'
    WHEN m = 5 THEN 'May'
    WHEN m = 6 THEN 'Jun'
    WHEN m = 7 THEN 'Jul'
    WHEN m = 8 THEN 'Aug'
    WHEN m = 9 THEN 'Sep'
    WHEN m = 10 THEN 'Oct'
    WHEN m = 11 THEN 'Nov'
    WHEN m = 12 THEN 'Dec'
    ELSE '???'
  END
END
GO

CREATE FN [date].[NowString]() RETURNS string AS
BEGIN
  RETURN date.MicroSecToString( date.Ticks() )
END
GO

CREATE FN [date].[StringToDays]( s string ) RETURNS int AS
BEGIN
  -- Typical input is 'Feb 2 2020'
  DECLARE ms string, month int
  SET ms = SUBSTRING( s, 1, 3 )
  SET month = CASE 
    WHEN ms = 'Jan' THEN 1
    WHEN ms = 'Feb' THEN 2
    WHEN ms = 'Mar' THEN 3
    WHEN ms = 'Apr' THEN 4
    WHEN ms = 'May' THEN 5
    WHEN ms = 'Jun' THEN 6
    WHEN ms = 'Jul' THEN 7
    WHEN ms = 'Aug' THEN 8
    WHEN ms = 'Sep' THEN 9
    WHEN ms = 'Oct' THEN 10
    WHEN ms = 'Nov' THEN 11
    WHEN ms = 'Dec' THEN 12
    ELSE 0
  END  
  IF month = 0 THROW 'Unknown month parsing date ' | web.Attr(ms)
  DECLARE six int -- Index of first space
  SET six = 4
  WHILE true
  BEGIN
    IF six > LEN(s) BREAK
    IF SUBSTRING( s, six, 1 ) = ' ' BREAK
    SET six = six + 1
  END
  DECLARE ssix int
  SET ssix = six+1
  WHILE true
  BEGIN
    IF ssix > LEN(s) BREAK
    IF SUBSTRING( s, ssix, 1 ) = ' ' BREAK
    SET ssix = ssix + 1
  END
 
  DECLARE day int, year int
  SET day = PARSEINT( SUBSTRING( s, six+1, ssix - six - 1) )
  IF day < 1 OR day > 31 THROW 'Day must be 1..31 parsing date ' | web.Attr(''|day)
  SET year = PARSEINT( SUBSTRING( s, ssix + 1, LEN(s) ) )
  RETURN date.YearMonthDayToDays( date.YearMonthDay( year, month, day ) )
END
GO

CREATE FN [date].[StringToTime]( s string ) RETURNS int AS
BEGIN
  -- Typical input is 'Feb 2 2020 20:15:31'
  DECLARE month int, day int, year int, hour int, min int, sec int

  DECLARE ms string
  SET ms = SUBSTRING( s, 1, 3 )
  SET month = CASE 
    WHEN ms = 'Jan' THEN 1
    WHEN ms = 'Feb' THEN 2
    WHEN ms = 'Mar' THEN 3
    WHEN ms = 'Apr' THEN 4
    WHEN ms = 'May' THEN 5
    WHEN ms = 'Jun' THEN 6
    WHEN ms = 'Jul' THEN 7
    WHEN ms = 'Aug' THEN 8
    WHEN ms = 'Sep' THEN 9
    WHEN ms = 'Oct' THEN 10
    WHEN ms = 'Nov' THEN 11
    WHEN ms = 'Dec' THEN 12
    ELSE 0
  END  
  IF month = 0 THROW 'Unknown month parsing date ' | web.Attr(ms)
  DECLARE dix int -- Index of space beforee day string
  SET dix = 4
  WHILE true
  BEGIN
    IF dix > LEN(s) BREAK
    IF SUBSTRING( s, dix, 1 ) = ' ' BREAK
    SET dix = dix + 1
  END
  DECLARE yix int -- Index of space before year string
  SET yix = dix+1
  WHILE true
  BEGIN
    IF yix > LEN(s) BREAK
    IF SUBSTRING( s, yix, 1 ) = ' ' BREAK
    SET yix = yix + 1
  END

  DECLARE hix int -- Index of space before hour string
  SET hix = yix+1
  WHILE true
  BEGIN
    IF hix > LEN(s) BREAK
    IF SUBSTRING( s, hix, 1 ) = ' ' BREAK
    SET hix = hix + 1
  END

  DECLARE mix int -- Index of colon before hour string
  SET mix = hix+1
  WHILE true
  BEGIN
    IF mix > LEN(s) BREAK
    IF SUBSTRING( s, mix, 1 ) = ':' BREAK
    SET mix = mix + 1
  END

  DECLARE six int -- Index of colon before seconds string
  SET six = mix+1
  WHILE true
  BEGIN
    IF six > LEN(s) BREAK
    IF SUBSTRING( s, six, 1 ) = ':' BREAK
    SET six = six + 1
  END
 
  SET day = PARSEINT( SUBSTRING( s, dix+1, yix - dix - 1) )
  IF day < 1 OR day > 31 THROW 'Day must be 1..31 parsing date ' | web.Attr(''|day)
  SET year = PARSEINT( SUBSTRING( s, yix + 1, hix - yix - 1 ) )
  SET hour = PARSEINT( SUBSTRING( s, hix + 1, mix - hix - 1 ) )
  IF hour > 23 THROW 'Hour must be 0..23 parsing time ' | web.Attr(''|hour)
  SET min = PARSEINT( SUBSTRING( s, mix + 1, six - mix - 1 ) )
  IF min > 59 THROW 'Minutes must be 0..59 parsing time ' | web.Attr(''|min)
  SET sec = PARSEINT( SUBSTRING( s, six + 1, LEN(s) ) )
  IF sec > 59 THROW 'Secondss must be 0..59 parsing time ' | web.Attr(''|sec)
  

  SET result = date.YearMonthDayToDays( date.YearMonthDay( year, month, day ) )
  SET result = result * 24 * 60 * 60 + hour * 3600 + min * 60 + sec
  SET result = result * 1000000
END
GO

CREATE FN [date].[StringToYearMonthDay]( s string ) RETURNS int AS
BEGIN
  RETURN date.DaysToYearMonthDay( date.StringToDays( s ) )
END
GO

CREATE FN [date].[Test]( y int, m int, d int, n int ) AS 
BEGIN
  DECLARE ymd int, days int
  SET ymd = date.YearMonthDay( y, m, d )
  SET days = date.YearMonthDayToDays( ymd )
  DECLARE i int
  SET i = 0
  WHILE i < n
  BEGIN
    SELECT '<br>' | date.DaysToString( days + i )
    SET i = i + 1
  END
END
GO

CREATE FN [date].[TestRoundTrip]() AS
BEGIN
  DECLARE day int
  SET day = 0
  WHILE day < 1000000
  BEGIN
    IF date.YearMonthDayToDays( date.DaysToYearMonthDay(day) ) != day
    BEGIN
      SELECT 'Test failed day = ' | day
      BREAK
    END
    SET day = day + 1
  END
  SELECT 'Finished test day=' | day | ' date=' | date.DaysToString(day)
END
GO

CREATE FN [date].[Ticks]() RETURNS int AS
BEGIN
  -- Microseconds since 1 Jan 0000
  RETURN GLOBAL(0) + 62135596800000000 /* 719162 * 24 * 3600 * 1000000 */
     + 366 * 24 * 3600 * 1000000
END
GO

CREATE FN [date].[Today]() RETURNS int AS
BEGIN
  DECLARE sec int, day int
  SET sec = date.Ticks() / 1000000
  SET day = sec / 86400
  RETURN day
END
GO

CREATE FN [date].[TodayYMD]() RETURNS int AS 
BEGIN
  SET result = date.DaysToYearMonthDay(date.Today())
END
GO

CREATE FN [date].[WeekDayToString]( wd int ) RETURNS string AS
BEGIN
  RETURN CASE
    WHEN wd = 1 THEN 'Mon'
    WHEN wd = 2 THEN 'Tue'
    WHEN wd = 3 THEN 'Wed'
    WHEN wd = 4 THEN 'Thu'
    WHEN wd = 5 THEN 'Fri'
    WHEN wd = 6 THEN 'Sat'
    WHEN wd = 7 THEN 'Sun'
    ELSE '?weekday?'
    END
END
GO

CREATE FN [date].[YearDay]( year int, day int ) RETURNS int AS
BEGIN
  RETURN year * 512 + day
END
GO

CREATE FN [date].[YearDayToDays]( yd int ) RETURNS int AS
BEGIN
  -- Given a date in Year/Day representation stored as y * 512 + d where 1 <= d <= 366 ( so d is day in year )
  -- returns the number of days since \"day zero\" (1 Jan 0000)
  -- using the Gregorian calendar where days divisible by 4 are leap years, except if divisible by 100, except if divisible by 400.
  DECLARE y int, d int, cycle int
  -- Extract y and d from yd.
  SET y = yd / 512, d = yd % 512 - 1
  SET cycle = y / 400, y = y % 400 -- The Gregorian calendar repeats every 400 years.
 
  -- Result days come from cycles, from years having at least 365 days, from leap years and finally d.
  -- 146097 is the number of the days in a 400 year cycle ( 400 * 365 + 97 leap years ).
  RETURN cycle * 146097 
    + y * 365 
    + ( y + 3 ) / 4 - ( y + 99 ) / 100 + ( y + 399 ) / 400
    + d
END
GO

CREATE FN [date].[YearDayToString]( yd int ) RETURNS string AS
BEGIN
   RETURN date.YearMonthDayToString( date.YearDayToYearMonthDay( yd ) )  
END
GO

CREATE FN [date].[YearDayToYearMonthDay]( yd int ) RETURNS int AS
BEGIN
  DECLARE y int, d int, leap bool, fdm int, m int, dim int
  SET y = yd / 512
  SET d = yd % 512 - 1
  SET leap = date.IsLeapYear( y )
  -- Jan = 0..30, Feb = 0..27 or 0..28  
  IF NOT leap AND d >= 59 SET d = d + 1
  SET fdm = CASE 
    WHEN d < 31 THEN 0 -- Jan
    WHEN d < 60 THEN 31 -- Feb
    WHEN d < 91 THEN 60 -- Mar
    WHEN d < 121 THEN 91 -- Apr
    WHEN d < 152 THEN 121 -- May
    WHEN d < 182 THEN 152 -- Jun
    WHEN d < 213 THEN 182 -- Jul
    WHEN d < 244 THEN 213 -- Aug
    WHEN d < 274 THEN 244 -- Sep
    WHEN d < 305 THEN 274 -- Oct
    WHEN d < 335 THEN 305 -- Nov
    ELSE 335 -- Dec
    END
  SET dim = d - fdm
  SET m = ( d - dim + 28 ) / 31
  RETURN date.YearMonthDay( y, m+1, dim+1 )
END
GO

CREATE FN [date].[YearMonthDay]( year int, month int, day int ) RETURNS int AS
BEGIN
  RETURN year * 512 + month * 32 + day
END
GO

CREATE FN [date].[YearMonthDayToDays]( ymd int ) RETURNS int AS
BEGIN
  RETURN date.YearDayToDays( date.YearMonthDayToYearDay( ymd ) )
END
GO

CREATE FN [date].[YearMonthDayToString]( ymd int ) RETURNS string AS
BEGIN
  DECLARE y int, m int, d int
  SET d = ymd % 32
  SET m = ymd / 32
  SET y = m / 16
  SET m = m % 16
  RETURN date.MonthToString(m) | ' ' | d | ' ' |  y
END
GO

CREATE FN [date].[YearMonthDayToYearDay]( ymd int ) RETURNS int AS
BEGIN
  DECLARE y int, m int, d int
  -- Extract y, m, d from ymd
  SET d = ymd % 32, m = ymd / 32  
  SET y = m / 16, m = m % 16
  -- Incorporate m into d ( assuming Feb has 29 days ).
  SET d = d + CASE
    WHEN m = 1 THEN 0 -- Jan
    WHEN m = 2 THEN 31 -- Feb
    WHEN m = 3 THEN 60 -- Mar
    WHEN m = 4 THEN 91 -- Apr
    WHEN m = 5 THEN 121 -- May
    WHEN m = 6 THEN 152 -- Jun
    WHEN m = 7 THEN 182 -- Jul
    WHEN m = 8 THEN 213 -- Aug
    WHEN m = 9 THEN 244 -- Sep
    WHEN m = 10 THEN 274 -- Oct
    WHEN m = 11 THEN 305 -- Nov
    ELSE 335 -- Dec
    END
  -- Allow for Feb being only 28 days in a non-leap-year.
  IF m >= 3 AND NOT date.IsLeapYear( y ) SET d = d - 1
  RETURN date.YearDay( y, d )
END
GO

--############################################
CREATE SCHEMA [web]
GO

CREATE TABLE [web].[File]([Path] string,[ContentType] string,[Content] binary) 
GO

CREATE INDEX [ByPath] ON [web].[File]([Path])
GO

CREATE FN [web].[Attr]( s string ) RETURNS string AS
BEGIN
  SET s = REPLACE( s, '&', '&amp;' )
  SET s = REPLACE( s, '\"', '&quot;' )
  RETURN '\"' | s | '\"'
END
GO

CREATE FN [web].[Cookie]( name string ) RETURNS string AS
BEGIN
  RETURN ARG( 3, name )
END
GO

CREATE FN [web].[Encode]( s string ) RETURNS string AS
BEGIN
  SET s = REPLACE( s,'&', '&amp;' )
  SET s = REPLACE( s, '<', '&lt;' )
  RETURN s
END
GO

CREATE FN [web].[Form]( name string ) RETURNS string AS
BEGIN
  RETURN ARG( 2, name )
END
GO

CREATE FN [web].[Head]( title string ) AS 
BEGIN 
  EXEC web.SetContentType( 'text/html;charset=utf-8' )

  DECLARE back string SET back = browse.backurl()

  SELECT '<html>
<head>
<meta http-equiv=\"Content-type\" content=\"text/html;charset=UTF-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>' | title | '</title>
<style>
   body{font-family:sans-serif;}
   body{ max-width:60em; }
</style>
</head>
<body>
<div style=\"color:white;background:lightblue;padding:4px;\">
' | CASE WHEN back != '' THEN '<a href=\"' | back| '\">Back</a> | ' ELSE '' END | '
<a href=/Menu>Menu</a> 
| <a target=_blank href=/Menu>New Window</a>
| <a href=/Manual>Manual</a>
| <a href=/Logout>Logout</a>
| <a target=_blank href=\"/browse/EditFunc?s=handler&n=' | web.Path() | '\">Code</a> ' | date.NowString() | ' UTC</div>'
END
GO

CREATE FN [web].[Main]() AS 
BEGIN 
  DECLARE path string SET path = web.Path()
  DECLARE ok string, schema int SET ok = Name, schema = Schema FROM sys.Function WHERE Name = path /* AND Schema = 6 */
  IF ok = path
  BEGIN
    EXECUTE( 'EXEC ' | sys.Dot(sys.SchemaName(schema),path) | '()' )
    DECLARE ex string
    SET ex = EXCEPTION()
    IF ex != ''
    BEGIN
      EXEC web.pubhead( 'Error' )
      SELECT '<h1>Error</h1><pre>'
      SELECT web.Encode( ex )
      SELECT '</pre>'
      EXEC web.pubtrail()
    END
  END
  ELSE
  BEGIN
    DECLARE ct string, content binary
    SET ok = Path, ct = ContentType, content = Content FROM web.File WHERE Path = path
    IF ok = path
    BEGIN
      EXEC web.SendBinary( ct, content )
    END    
    ELSE
    BEGIN
      EXEC web.pubhead( 'Unknown page')
      SELECT 'Unknown page Path=' | path
      EXEC web.pubtrail()
    END
  END
END
GO

CREATE FN [web].[Path]() RETURNS string AS
BEGIN
  RETURN ARG(0,'')
END
GO

CREATE FN [web].[Query]( name string ) RETURNS string AS
BEGIN
  RETURN ARG( 1, name )
END
GO

CREATE FN [web].[Redirect]( url string ) AS
BEGIN
  DECLARE x int
  SET x = HEADER( 'location', url )
  SET x = STATUSCODE( 303 )
END
GO

CREATE FN [web].[SendBinary]( contenttype string, content binary ) AS
BEGIN
  DECLARE cu int SET cu = login.user()
  EXEC web.SetContentType( contenttype )
  SELECT content
END
GO

CREATE FN [web].[SetContentType]( ct string ) AS
BEGIN
  DECLARE x int
  SET x = HEADER( 'Content-Type', ct )
END
GO

CREATE FN [web].[SetCookie]( name string, value string, expires string ) AS
BEGIN
  /* Expires can be either in seconds e.g. Max-Age=1000000000
     or Expires=Wed, 09 Jun 2021 10:18:14 GMT
     or blank for temporary cookie

     To delete a cookie use e.g.

     EXEC web.SetCookie('username','','Max-Age=0')
  */
  DECLARE x int
  SET x = HEADER( 'set-cookie', name | '=' | value | '; ' | expires )
END
GO

CREATE FN [web].[SetDos]( uid int ) RETURNS int AS
BEGIN
  DECLARE ok int
  SET ok = SETDOS
  ( 'u' | uid, 
     1000, 
     1000000000000, 
     1000000000,
     1000000000000 
  )
  IF ok = 0
  BEGIN
     DECLARE x int
     SET x = STATUSCODE( 429 )
  END
  RETURN ok
END
GO

CREATE FN [web].[SetUser]() AS 
BEGIN 
  DECLARE dummy int
  SET dummy = login.user()
END
GO

CREATE FN [web].[Trailer]() AS
BEGIN
  SELECT '</body></html>'
END
GO

CREATE FN [web].[UrlEncode]( s string ) RETURNS string AS
BEGIN
  /* Would probably be better to do this using builtin function */
  SET s = REPLACE( s, '%', '%25' )
  SET s = REPLACE( s, '&', '%26' )
  SET s = REPLACE( s, '=', '%3D' )
  --SET s = REPLACE( s, '?', '%3F' )
  --SET s = REPLACE( s, '/', '%2F' )
  --SET s = REPLACE( s, '#', '%23' )
  RETURN s
END
GO

CREATE FN [web].[pubhead](title string) AS
BEGIN 
  EXEC web.SetContentType( 'text/html;charset=utf-8' )
  SELECT '<html>
<head>
<meta http-equiv=\"Content-type\" content=\"text/html;charset=UTF-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<link rel=\"shortcut icon\" href=\"/favicon.ico\" type=\"image/x-icon\">
<title>' | title | '</title>
<style>
   body{font-family:sans-serif;}
</style>
</head>
<body>
<p><a href=\"/shop/\">Home</a>

'
END
GO

CREATE FN [web].[pubtrail]() AS 
BEGIN 

SELECT  '<div class=outer3><div>Copyright © ' | ( date.TodayYMD() / 512 ) | '  Whatever Limited</div></div></body></html>'

END
GO

INSERT INTO [web].[File](Id,[Path],[ContentType],[Content]) VALUES 
(16,'/favicon.ico','image/x-icon',0x00000100010010100000010020002804000016000000280000001000000020000000010020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008ff840008ffc60008ffbd0008ffc60008ff830000000000000000000000000000000000000000000000000000000000000000000000000000ff190009ffef0008ff620009ff38000000000009ff380008ff620009ffec0000ff150000000000000000000000000000000000000000000000000000ff160008ffc30000ff0f00000000000000000000000000000000000000000000ff120008ffc30000ff1400000000000000000000000000000000000000000009ffcb0005ff33000000000000000000000000000000000000000000000000000000000005ff370009ffc7000000000000000000000000000000000006ff5c0007ff8e0000000000000000000000000000000000000000000000000000000000000000000000000007ff910006ff580000000000000000000000000009ffac0006ff520000000000000000000000000000000000000000000000000000000000000000000000000009ff550008ffa90000000000000000000000000008ffd60006ff280000000000000000000000000000000000000000000000000000000000000000000000000006ff2a0008ffd40000000000000000000000000008ffbd00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008ffbd0000000000000000000000000008ffd60006ff280000000000000000000000000000000000000000000000000000000000000000000000000006ff2a0008ffd40000000000000000000000000009ffac0006ff520000000000000000000000000000000000000000000000000000000000000000000000000009ff550008ffa90000000000000000000000000005ff5d0007ff8b0000000000000000000000000000000000000000000000000000000000000000000000000007ff8e0006ff5a000000000000000000000000000000000009ffcd0005ff31000000000000000000000000000000000000000000000000000000000005ff350009ffc900000000000000000000000000000000000000000000ff180008ffc20000ff0b00000000000000000000000000000000000000000000ff0e0008ffc20000ff170000000000000000000000000000000000000000000000000009ff1c0008fff30008ff600005ff37000000000005ff370008ff600008fff00000ff180000000000000000000000000000000000000000000000000000000000000000000000000009ff870009ffc80008ffbf0009ffc80008ff86000000000000000000000000000000000000000000000000)
GO

--############################################
CREATE SCHEMA [browse]
GO

CREATE TABLE [browse].[Column]([Position] int,[Label] string,[Description] string,[RefersTo] int,[Default] string,[InputCols] int,[InputFunction] string,[InputRows] int,[Datatype] int) 
GO

CREATE INDEX [ByRefersTo] ON [browse].[Column]([RefersTo])
GO

CREATE TABLE [browse].[Datatype]([Name] string,[DataKind] int,[SqlFn] string) 
GO

CREATE TABLE [browse].[Table]([NameFunction] string,[SelectFunction] string,[DefaultOrder] string,[Title] string,[Description] string,[Role] int) 
GO

CREATE FN [browse].[/browse/AddChild]() AS
BEGIN
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE c int SET c = browse.fieldid()

  DECLARE p int SET p = PARSEINT( web.Query('p') )
  DECLARE t int SET t = Table FROM sys.Column WHERE Id = c
  DECLARE ex string
  IF web.Form( '$submit' ) != '' 
  BEGIN
    EXECUTE( browse.InsertSql( t, c, p ) ) 
    SET ex = EXCEPTION()
    IF ex = '' 
    BEGIN
      EXEC web.Redirect( browse.backurl() )       
      RETURN 
    END
  END
 
  DECLARE title string SET title = 'Add ' | browse.TableTitle( t )
  EXEC web.Head( title )
  SELECT '<b>' | title | '</b><br>'
  IF ex != '' SELECT '<p>Error: ' | ex
  SELECT '<form method=post>' 
  EXECUTE( browse.FormInsertSql( t, c ) )
  SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
  EXEC web.Trailer()
END
GO

CREATE FN [browse].[/browse/AddRow]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE t int SET t = browse.tableid()

  DECLARE ex string
  IF web.Form( '$submit' ) != '' 
  BEGIN
    DECLARE lastid int
    SET lastid = LASTID()
    EXECUTE( browse.InsertSql( t, 0, 0 ) ) 
    SET ex = EXCEPTION()
    IF ex = '' 
    BEGIN
      DECLARE ba string SET ba = browse.backargs()
      EXEC web.Redirect( '/browse/Row?' | browse.tablearg(t) | '&k=' | LASTID() | ba )
      RETURN
    END
  END
  
  EXEC web.Head( 'Add ' | browse.TableTitle( t ) )
  IF ex != '' SELECT '<p>Error: ' | web.Encode( ex )
  SELECT '<form method=post enctype=\"multipart/form-data\">' 
  EXECUTE( browse.FormInsertSql( t, 0 ) )

  SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
  EXEC web.Trailer()
END
GO

CREATE FN [browse].[/browse/ColInfo]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE tid int SET tid = 8
  DECLARE c int SET c = PARSEINT( web.Query( 'k' ) )
  DECLARE t int, colName string
  SET t = Table, colName = Name FROM sys.Column WHERE Id = c

  DECLARE ok int SET ok = 0
  SET ok = Id FROM browse.Column WHERE Id = c
  IF ok != c INSERT INTO browse.Column( Id ) VALUES ( c )

  IF web.Form( '$submit' ) != '' 
  BEGIN
    EXECUTE( browse.UpdateSql( tid, c ) ) 
    EXEC web.Redirect( browse.backurl() )  
  END
  ELSE
  BEGIN
    EXEC web.Head( 'Column ' | colName )
    SELECT '<h1>Column ' | colName | '</h1><form method=post>' 
    EXECUTE( browse.FormUpdateSql( tid, c ) )
    SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
    EXEC web.Trailer()
  END
END
GO

CREATE FN [browse].[/browse/EditFunc]() AS
BEGIN
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE s string SET s = web.Query('s')
  DECLARE n string SET n = web.Query('n')
  DECLARE sid int SET sid = Id FROM sys.Schema WHERE Name = s
  DECLARE def string, ex string SET def = web.Form('def')
  IF def != '' 
  BEGIN
    EXECUTE( 'ALTER FN ' | sys.Dot(s,n) | def )
    SET ex = EXCEPTION()
  END
  ELSE SET def = Def FROM sys.Function WHERE Schema = sid AND Name = n 
  EXEC web.Head( 'Edit ' | n )
  IF ex != '' SELECT '<p>Error: ' | web.Encode( ex )
  SELECT 
     '<p><form method=post>'
     | '<input type=submit value=\"ALTER\"> <a href=/browse/Schema?s=' | s | '>' | s | '</a> . ' | n 
     | CASE WHEN s = 'handler' THEN ' <a href=' | n | '>Go</a>' ELSE '' END
     | '<br><textarea name=def rows=40 cols=150>' | web.Encode(def) | '</textarea>' 
     | '</form>' 
  EXEC web.Trailer()
END
GO

CREATE FN [browse].[/browse/EditRow]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN
  DECLARE t int SET t = browse.tableid()
  DECLARE k int SET k = PARSEINT( web.Query('k') )
  DECLARE ex string
  DECLARE submit string SET submit = web.Form( '$submit' )
  IF submit != '' 
  BEGIN
    IF submit = 'Save'
    BEGIN
      EXECUTE( browse.UpdateSql( t, k ) ) 
      SET ex = EXCEPTION()
      IF ex = '' 
      BEGIN
        EXEC web.Redirect( browse.backurl() )
        RETURN
      END
    END
    ELSE IF submit = 'Delete'
    BEGIN
      EXECUTE( 'DELETE FROM ' | sys.TableName( t ) | ' WHERE Id =' | k )
      EXEC web.Redirect( browse.backurl() )
      RETURN
    END      
  END
 
  EXEC web.Head( 'Edit ' | browse.TableTitle( t ) )
  IF ex != '' SELECT '<p>Error: ' | web.Encode(ex)

  SELECT '<form method=post enctype=\"multipart/form-data\">'  
  EXECUTE( browse.FormUpdateSql( t, k ) )
  SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'

  SELECT '<form method=post><input name=\"$submit\" type=submit value=Delete></form>'
  EXEC web.Trailer()
END
GO

CREATE FN [browse].[/browse/File]() AS 
BEGIN
   DECLARE k int SET k = PARSEINT( web.Query('k'))
   DECLARE c int SET c = PARSEINT( web.Query('c'))
   DECLARE t int
   DECLARE cname string, ctname string
   DECLARE id int
   SET t = Table, cname = Name FROM sys.Column WHERE Id = c

   FOR id = Id FROM sys.Column WHERE Table = t
   BEGIN
     DECLARE def string SET def = ''
     SET def = Default FROM browse.Column WHERE Id = id AND Datatype = 10
     IF def = cname 
     BEGIN
       SET ctname = Name FROM sys.Column WHERE Id = id
       BREAK
     END
   END

   DECLARE sql string
   SET sql = '
DECLARE content binary
DECLARE ct string
SET content = ' | cname | ', ct=' | ctname | ' 
FROM ' | sys.TableName(t) | '
WHERE Id = ' | k | '
EXEC web.SetContentType(ct)
SELECT content
'

   EXECUTE( sql )
   
END
GO

CREATE FN [browse].[/browse/Image]() AS 
BEGIN
   DECLARE k int SET k = PARSEINT( web.Query('k'))
   DECLARE c int SET c = PARSEINT( web.Query('c'))
   DECLARE t int
   DECLARE cname string, ctname string
   DECLARE id int
   SET t = Table, cname = Name FROM sys.Column WHERE Id = c

   FOR id = Id FROM sys.Column WHERE Table = t
   BEGIN
     DECLARE def string SET def = ''
     SET def = Default FROM browse.Column WHERE Id = id AND Datatype = 10
     IF def = cname 
     BEGIN
       SET ctname = Name FROM sys.Column WHERE Id = id
       BREAK
     END
   END

   DECLARE sql string
   SET sql = '
DECLARE content binary
DECLARE ct string
SET content = ' | cname | ', ct=' | ctname | ' 
FROM ' | sys.TableName(t) | '
WHERE Id = ' | k | '
EXEC web.SetContentType(ct)
SELECT content
'

   EXECUTE( sql )
   
END
GO

CREATE FN [browse].[/browse/Info]() AS 
BEGIN
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE k int SET k = browse.tableid()

  DECLARE tid int SET tid = 9
  DECLARE ok int SET ok = 0
  SET ok = Id FROM browse.Table WHERE Id = k
  IF ok != k INSERT INTO browse.Table( Id ) VALUES ( k )
  IF web.Form( '$submit' ) != '' 
  BEGIN
    EXECUTE( browse.UpdateSql( tid, k ) ) 
    EXEC web.Redirect( '/browse/Table?' | browse.tablearg(k) )
  END
  ELSE
  BEGIN
    EXEC web.Head( 'Browse Info for ' | sys.TableName(k) )
    SELECT '<form method=post>' 
    EXECUTE( browse.FormUpdateSql( tid, k ) )
    SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
    EXEC web.Trailer()
  END
END
GO

CREATE FN [browse].[/browse/Row]() AS 
BEGIN
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE t int SET t = browse.tableid()

  DECLARE k int SET k = PARSEINT( web.Query('k') )  

  EXECUTE( browse.ShowSql(t,k) )
END
GO

CREATE FN [browse].[/browse/Schema]() AS
BEGIN
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE ba string SET ba = browse.backargs()

  DECLARE s string SET s = web.Query('s')
  DECLARE sid int SET sid = Id FROM sys.Schema WHERE Name = s
  EXEC web.Head( 'Schema ' | s )
  SELECT '<h1>Schema ' | s | '</h1>'

  SELECT '<p><a target=_blank href=\"/ScriptSchema?s=' | s | '\">Script</a>'
 
  SELECT '<h2>Tables</h2>'
  SELECT '<p><a href=\"/browse/Table?' | browse.tablearg(Id) | ba | '\">' | Name | '</a>'
  FROM sys.Table WHERE Schema = sid ORDER BY Name
  SELECT '<h2>Functions</h2>' 
  SELECT '<p><a href=\"/browse/EditFunc?s=' | s | '&n=' | Name | ba | '\">' | Name | '</a>'
  FROM sys.Function WHERE Schema = sid ORDER BY Name
  EXEC web.Trailer()
END
GO

CREATE FN [browse].[/browse/Table]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE ba string SET ba = browse.backargs()

  DECLARE t int SET t = browse.tableid()

  DECLARE title string SET title = browse.TableTitle( t )
  SET title = title | ' Table'
  EXEC web.Head( title )
  SELECT '<b>' | title | '</b> <a href=/browse/Info?' | browse.tablearg(t) | ba | '>Settings</a>'   
    | '<p><b>Columns:</b> ' | browse.ColNames( t, ba )
/*
  SELECT '<p><b>Indexes</b>'
  SELECT '<br>' | sys.QuoteName(Name) | ' ' | sys.IndexCols(Id)
  FROM sys.Index WHERE Table = t
*/
  SELECT '<p><b>Rows</b> <a href=\"/browse/AddRow?' | browse.tablearg(t) | ba | '\">Add</a>'

  DECLARE orderBy string SET orderBy = DefaultOrder FROM browse.Table WHERE Id = t
  DECLARE sql string SET sql ='SELECT ''<br><a href=\"/browse/Row?' | browse.tablearg(t)
    | '&k=''| Id | ''' | ba |'\">Show</a> ''| ''''|' 
    | browse.ColValues(Id,ba)  
    | ' FROM ' 
    | sys.TableName(Id)
    | CASE WHEN orderBy != '' THEN ' ORDER BY ' | orderBy ELSE '' END
  FROM sys.Table WHERE Id = t

  EXECUTE( sql )

  EXEC web.Trailer()
END
GO

CREATE FN [browse].[AlterSql]() AS 
BEGIN
  DECLARE f string, sql string

  FOR 
     f |= '
    WHEN t=' | Id | ' THEN ' | SqlFn | '(kind,colid)' FROM browse.Datatype
  BEGIN
  END

 SET sql = 
'ALTER FN browse.Sql( kind int, colid int, t int ) RETURNS string AS
BEGIN
  RETURN CASE ' | f | '
    ELSE ''browseSqlInvalidDatatype'' 
    END 
END
'
  EXECUTE( sql )
END
GO

CREATE FN [browse].[BrowseColumnName]( k int ) RETURNS string AS 
BEGIN
  SET result = sys.TableName( Table ) | '.' | sys.QuoteName( Name )
  FROM sys.Column WHERE Id = k
END
GO

CREATE FN [browse].[ChildSql]( colId int, k int, ba string ) RETURNS string AS 
BEGIN 
  /* Returns SQL to display a child table, with hyperlinks where a column refers to another table */
  DECLARE col string, colid int, colName string, type int, th string, ob string
  DECLARE table int SET table = Table FROM sys.Column WHERE Id = colId
  
  SET ob = DefaultOrder FROM browse.Table WHERE Id = table
  FOR colid = Id, type = Type,
    col = CASE WHEN Type % 8 = 2 THEN 'web.Encode(' | Name | ')' ELSE Name END, colName = Name
  FROM sys.Column WHERE Table = table AND Id != colId
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, nf string, label string
    SET ref = 0, nf = '', label = ''
    SET ref = RefersTo, label = Label FROM browse.Column WHERE Id = colid
    IF ref > 0 SET nf = NameFunction FROM browse.Table WHERE Id = ref
    SET ob = DefaultOrder FROM browse.Table WHERE Id = ref
    SET result |= '|''<TD' | CASE WHEN type % 8 != 2 THEN ' align=right' ELSE '' END | '>''|'
      | CASE 
        WHEN nf != '' THEN '''<a href=\"/browse/Row?' | browse.tablearg(ref) | '&k=''|' | col | '|''' | ba | '\">''|' | nf | '(' | col | ')' | '|''</a>''' 
        ELSE browse.Sql(2,colid,browse.GetDatatype(type,colid))
        END,
        th = th | '<TH>' | CASE WHEN label != '' THEN label ELSE colName END
  END
  DECLARE kcol string SET kcol = sys.QuoteName(Name) FROM sys.Column WHERE Id = colId
  RETURN 
   'SELECT ''<TABLE><TR><TH>' | th | ''' '
   | 'SELECT ' | '''<TR><TD><a href=\"/browse/Row?' | browse.tablearg(table) | '&k=''| Id | ''' | ba | '\">Show</a> '''
     | result | ' FROM ' | sys.TableName( table ) | ' WHERE ' | kcol | ' = ' | k | CASE WHEN ob != '' THEN ' ORDER BY ' | ob ELSE '' END
   | ' SELECT ''</TABLE>'''
END
GO

CREATE FN [browse].[ColNames]( table int, ba string ) RETURNS string AS
BEGIN
  DECLARE col string
  FOR col = '<a href=\"/browse/ColInfo?k=' | Id | ba | '\">' | Name | '</a>' 
    | ' ' | sys.TypeName(Type) /* | ' pos=' | browse.ColPos(Id) */
  FROM sys.Column WHERE Table = table
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    SET result |= CASE WHEN result = '' THEN '' ELSE ', ' END | col
  END
END
GO

CREATE FN [browse].[ColPos]( c int ) RETURNS int AS
BEGIN
  DECLARE pos int
  SET pos = Position FROM browse.Column WHERE Id = c
  RETURN pos
END
GO

CREATE FN [browse].[ColValues]( table int, ba string ) RETURNS string AS
BEGIN
  DECLARE col string, colid int, type int
  FOR colid = Id, type=Type, col = CASE 
    WHEN Type % 8 = 2 THEN 'web.Encode(sys.SingleQuote(' | Name | '))'
    ELSE Name
  END
  FROM sys.Column WHERE Table = table 
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, nf string, datatype int
    SET ref = 0, nf = '', datatype = 0
    SET ref = RefersTo, datatype = Datatype
    FROM browse.Column WHERE Id = colid

    IF datatype = 0 SET datatype = browse.DefaultDataType(type)

    IF ref > 0 SET nf = NameFunction FROM browse.Table WHERE Id = ref

    SET result |= CASE WHEN result = '' THEN '' ELSE '|'', ''|' END | 
      CASE 
      WHEN nf != '' 
      THEN '''<a href=\"/browse/Row?' | browse.tablearg(ref) | '&k=''|' | col | '|'''|ba|'\">''|' | nf | '(' | col | ')' | '|''</a>''' 

      ELSE browse.Sql(1,colid,datatype)
      END
  END
END
GO

CREATE FN [browse].[DatatypeName]( datatype int ) RETURNS string AS
BEGIN
  SET result = Name FROM browse.Datatype WHERE Id = datatype
END
GO

CREATE FN [browse].[DatatypeSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId
  DECLARE opt string, options string
  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | web.Encode( browse.DatatypeName(Id) ) | '</option>'
  FROM browse.Datatype
  ORDER BY browse.DatatypeName(Id)
  SET options |= opt
  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options | 
     '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
     | '</select>'
END
GO

CREATE FN [browse].[DefaultDataType](type int) RETURNS int AS 
BEGIN
    SET result = CASE
       WHEN type % 8 = 3 THEN 1 /* int */
       WHEN type % 8 = 2 THEN 2 /* string */
       WHEN type % 8 = 5 THEN 6 /* bool */
       WHEN type % 8 = 1 THEN 9 /* binary - todo */
       WHEN type % 8 = 4 THEN 8 /* float */
       ELSE 0
    END
END
GO

CREATE FN [browse].[DownloadLink](id int,colid int) RETURNS string AS
BEGIN 
   RETURN '<a target=_blank href=\"/browse/File?k=' | id | '&c=' | colid |'\">Download</a>'
END
GO

CREATE FN [browse].[FormInsertSql]( table int, pc int ) RETURNS string AS
BEGIN
  DECLARE sql string, col string, type int, colId int
  FOR col = Name, type = Type, colId = Id FROM sys.Column 
    WHERE Table = table AND Id != pc
    ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, inf string, default string
    SET ref = 0, inf = '', default = ''
    SET ref = RefersTo,  inf = InputFunction, default = Default FROM browse.Column WHERE Id = colId
    IF ref > 0 AND inf = '' SET inf = SelectFunction FROM browse.Table WHERE Id = ref
    IF default = '' SET default = '0'   
 
    DECLARE inp string
    SET inp = CASE WHEN inf = '' 
        THEN browse.Sql( 3, colId, browse.GetDatatype(type,colId) )
        ELSE 'browse.Label(' | colId | ') | ' | inf | '(' | colId | ',' | default | ')'
        END

    IF inp != '' 
    BEGIN
      SET sql |= CASE WHEN sql = '' THEN '' ELSE ' | ' END | inp
    END
  END
  RETURN CASE WHEN sql = '' THEN '' ELSE 'SELECT ' | sql END
END
GO

CREATE FN [browse].[FormUpdateSql]( table int, k int ) RETURNS string AS
BEGIN
  DECLARE sql string, col string, colId int, type int
  FOR col = Name, colId = Id, type = Type FROM sys.Column WHERE Table = table
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, inf string
    SET ref = 0, inf = ''
    SET ref = RefersTo, inf = InputFunction FROM browse.Column WHERE Id = colId
    IF ref > 0 AND inf = '' SET inf = SelectFunction FROM browse.Table WHERE Id = ref

    DECLARE inp string
    SET inp = CASE WHEN inf = '' 
        THEN browse.Sql( 4, colId, browse.GetDatatype(type,colId) )
        ELSE 'browse.Label(' | colId | ') | ' | inf | '(' | colId | ',' | sys.QuoteName(col) | ')'
        END

    IF inp != ''
    BEGIN
      SET sql |= CASE WHEN sql = '' THEN '' ELSE ' | ' END | inp
    END
  END
  RETURN 'SELECT ' | sql | ' FROM ' | sys.TableName( table ) | ' WHERE Id =' | k
END
GO

CREATE FN [browse].[GetDatatype]( type int, colid int ) RETURNS int AS
BEGIN
  SET result = Datatype FROM browse.Column WHERE Id = colid
  IF result = 0
  BEGIN
    SET result = CASE
       WHEN type % 8 = 3 THEN 1 /* int */
       WHEN type % 8 = 2 THEN 2 /* string */
       WHEN type % 8 = 5 THEN 6 /* bool */
       WHEN type % 8 = 1 THEN 9 /* binary - todo */
       WHEN type % 8 = 4 THEN 8 /* float */
       ELSE 0
    END
  END
END
GO

CREATE FN [browse].[InputBinary]( colid int, value binary ) RETURNS string AS 
BEGIN 
  DECLARE cn string SET cn = Name FROM sys.Column WHERE Id = colid
  DECLARE size int SET size = InputCols FROM browse.Column WHERE Id = colid
  IF size = 0 SET size = 50
  RETURN browse.Label(colid) | '<input id=\"' | cn | '\" name=\"' | cn | '\" size=' | size | ' value=\"' | value | '\">'
END
GO

CREATE FN [browse].[InputBool]( colId int, value bool ) RETURNS string AS
BEGIN
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colId
  RETURN browse.Label(colId) | '<input type=checkbox id=\"' | cn | '\" name=\"' | cn | '\"' | CASE WHEN value THEN ' checked' ELSE '' END | '>'
END
GO

CREATE FN [browse].[InputDouble]( colid int, value double ) RETURNS string AS 
BEGIN 
  DECLARE cn string SET cn = Name FROM sys.Column WHERE Id = colid
  DECLARE size int 
  SET size = InputCols FROM browse.Column WHERE Id = colid
  IF size = 0 SET size = 15
  RETURN browse.Label(colid) | '<input id=\"' | cn | '\" name=\"' | cn | '\" size=\"' | size | '\"' | ' value=\"' | value | '\">'
END
GO

CREATE FN [browse].[InputFile]( colid int ) RETURNS string AS 
BEGIN 
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colid

  RETURN browse.Label(colid) | '<input type=file id=\"' | cn | '\" name=\"' | cn | '\">'
END
GO

CREATE FN [browse].[InputInt]( colId int, value int) RETURNS string AS 
BEGIN 
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colId
  DECLARE size int
  SET size = InputCols FROM browse.Column WHERE Id = colId
  IF size = 0 SET size = 10
  RETURN browse.Label(colId) | '<input type=number id=\"' | cn | '\" name=\"' | cn | '\" size=' | size | ' value=' | value | '>'
END
GO

CREATE FN [browse].[InputString]( colId int, value string ) RETURNS string AS 
BEGIN 
  DECLARE cn string SET cn = Name FROM sys.Column WHERE Id = colId 
  DECLARE cols int, rows int, description string
  SET cols = InputCols, rows = InputRows, description = Description
  FROM browse.Column WHERE Id = colId
  IF cols = 0 SET cols = 50
  IF rows > 0
    RETURN '<textarea id=\"' | cn | '\" name=\"' | cn | '\" cols=\"' | cols | '\"' | '\" rows=\"' | rows |'\"'
      | CASE WHEN value = '' THEN 'placeholder=' | web.Attr(description) ELSE '' END
      | '\">' | web.Encode(value) | '</textarea>'
  ELSE
    RETURN browse.Label(colId) | '<input id=\"' | cn | '\" name=\"' | cn | '\" size=\"' | cols | '\"' | ' value=' | web.Attr(value) | '>'
END
GO

CREATE FN [browse].[InputTime]( colId int, value int) RETURNS string AS 
BEGIN 
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colId
  DECLARE size int
  SET size = InputCols FROM browse.Column WHERE Id = colId
  IF size = 0 SET size = 20
  RETURN browse.Label(colId) | '<input id=\"' | cn | '\" name=\"' | cn | '\" size=' | size | ' value=' | web.Attr(date.MicroSecToString(value)) | '>'
END
GO

CREATE FN [browse].[InputVersionCheck]( colid int, value int ) RETURNS string AS
BEGIN
   DECLARE name string SET name = Name FROM sys.Column WHERE Id = colid
   RETURN '<input type=hidden name=' | name | ' value=' | value | '>'
END
GO

CREATE FN [browse].[InputYearMonthDay]( colId int, value int) RETURNS string AS 
BEGIN 
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colId
  DECLARE size int
  SET size = InputCols FROM browse.Column WHERE Id = colId
  IF size = 0 SET size = 10
  RETURN browse.Label(colId) | '<input id=\"' | cn | '\" name=\"' | cn | '\" size=' | size | ' value=' | web.Attr(date.YearMonthDayToString(value)) | '>'
END
GO

CREATE FN [browse].[InsertContentType]( colid int ) RETURNS string AS 
BEGIN
  DECLARE cname string
  SET cname = Default FROM browse.Column WHERE Id = colid

  DECLARE x int
  WHILE true
  BEGIN
    DECLARE name string
    SET name = FILEATTR(x,0)
    IF name = cname RETURN FILEATTR(x,1)
    IF name = '' BREAK
    SET x = x + 1
  END    
  RETURN ''
END
GO

CREATE FN [browse].[InsertFile]( cname string ) RETURNS binary AS 
BEGIN
  DECLARE x int
  WHILE true
  BEGIN
    DECLARE name string
    SET name = FILEATTR(x,0)
    IF name = cname RETURN FILECONTENT(x)
    IF name = '' BREAK
    SET x = x + 1
  END    
  RETURN 0x
END
GO

CREATE FN [browse].[InsertFileName]( colid int ) RETURNS string AS 
BEGIN
  DECLARE cname string
  SET cname = Default FROM browse.Column WHERE Id = colid

  DECLARE x int
  WHILE true
  BEGIN
    DECLARE name string
    SET name = FILEATTR(x,0)
    IF name = cname RETURN '/' | FILEATTR(x,2)
    IF name = '' BREAK
    SET x = x + 1
  END    
  RETURN ''
END
GO

CREATE FN [browse].[InsertNames]( table int ) RETURNS string AS
BEGIN
  DECLARE col string
  FOR col = Name FROM sys.Column WHERE Table = table
    SET result |= CASE WHEN result = '' THEN '' ELSE ',' END | sys.QuoteName(col)
  RETURN '(' | result | ')'
END
GO

CREATE FN [browse].[InsertSql]( table int, pc int, p int ) RETURNS string AS
BEGIN
  DECLARE vlist string, names string, type int, colid int, name string

  FOR type = Type, colid = Id, name = Name FROM sys.Column WHERE Table = table 
  BEGIN
    DECLARE sql string SET sql = CASE 
      WHEN colid = pc THEN '' | p
      ELSE browse.Sql( 5, colid, browse.GetDatatype(type,colid) )
    END

    IF sql != '' 
    BEGIN
       SET vlist |= CASE WHEN vlist = '' THEN '' ELSE ' , ' END | sql
       SET names |= CASE WHEN names = '' THEN '' ELSE ' , ' END | name
    END
  END

  RETURN 'INSERT INTO ' | sys.TableName( table ) | '(' | names | ') VALUES (' | vlist | ')'
END
GO

CREATE FN [browse].[Label]( colid int ) RETURNS string AS
BEGIN
  DECLARE name string, label string
  SET name = Name FROM sys.Column WHERE Id = colid
  SET label = Label FROM browse.Column WHERE Id = colid
  IF label = '' SET label = name
  RETURN '<p><label for=' | name | '>' | label | '</label>: '
END
GO

CREATE FN [browse].[ParseBool]( s string ) RETURNS bool AS
BEGIN
  RETURN s = 'on'
END
GO

CREATE FN [browse].[SchemaSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId
  DECLARE opt string, options string, sels string
  SET sels = web.Form( col )
  IF sels != '' SET sel = PARSEINT( sels )
  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | web.Encode( Name ) | '</option>'
  FROM sys.Schema
  ORDER BY Name
  SET options |= opt
  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options | 
     '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
     | '</select>'
END
GO

CREATE FN [browse].[ShowImage](id int,colid int) RETURNS string AS
BEGIN 
   RETURN '<img style=\"max-width:300px;\" src=\"/browse/File?k=' | id | '&c=' | colid |'\">'
END
GO

CREATE FN [browse].[ShowSql](table int, k int) RETURNS string AS
BEGIN
  DECLARE ba string SET ba = browse.backargs()

  DECLARE cols string, col string, colname string, colid int
  FOR colid = Id, colname = Name, col = CASE 
    WHEN Type % 8 = 2 THEN 'web.Encode(' | Name | ')'
    ELSE Name
    END
  FROM sys.Column WHERE Table = table 
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, nf string, datatype int
    SET ref = 0, nf = '', datatype = 0
    SET ref = RefersTo, datatype = Datatype
    FROM browse.Column WHERE Id = colid
    IF ref > 0 SET nf = NameFunction FROM browse.Table WHERE Id = ref ELSE SET nf = ''
    SET cols |= 
      CASE WHEN cols = '' THEN '' ELSE ' | ' END
      | '''<p>' | colname | ': '' | '
      | CASE 
        WHEN datatype != 0 THEN browse.Sql(2,colid,datatype)
        WHEN nf != '' THEN '''<a href=\"/browse/Row?' | browse.tablearg(ref)| '&k=''|' | col | '|''' | ba | '\">''|' | nf | '(' | col | ')' | '|''</a>''' 
        ELSE col
        END
  END
  DECLARE namefunc string SET namefunc = NameFunction FROM browse.Table WHERE Id = table
  RETURN '  
    DECLARE t int SET t = '|table|'
    DECLARE k int SET k = '|k|'

    DECLARE ok int SET ok = Id FROM ' | sys.TableName(table) | ' WHERE Id = k
    IF ok = k
    BEGIN
      DECLARE title string SET title = browse.TableTitle( t )' 
        | CASE WHEN namefunc = '' THEN '' ELSE ' | '' '' | ' | namefunc | '(k)' END | '
      EXEC web.Head( title )
      SELECT ''<b>'' | title | ''</b><br>''
  '
  | ' SELECT ' | cols | ' FROM ' | sys.TableName(table) | ' WHERE Id = k'
  | ' SELECT ''<p><a href=\"/browse/EditRow?'' | browse.tablearg(t) | ''&k='' | k | '''| ba |'\">Edit</a>'''
  | '
    DECLARE col int
    FOR col = Id FROM browse.Column WHERE RefersTo = t
    BEGIN
      SELECT ''<p><b>'' | browse.TableTitle( Table ) | ''</b>''
       | '' <a href=\"/browse/AddChild?'' | browse.fieldarg(col) | ''&p='' | k | '''|ba|'\">Add</a>''
      FROM sys.Column WHERE Id = col
      EXECUTE( browse.ChildSql( col, k, '''|ba|''' ) )
    END
    SELECT ''<p><a href=\"/browse/Table?'' | browse.tablearg(t) | ''\">'' | browse.TableTitle(t) | '' Table</a>''
    EXEC web.Trailer()
  END
  ELSE
  BEGIN
    EXEC web.Redirect( browse.backurl() )
  END
'
END
GO

CREATE FN [browse].[Sql]( kind int, colid int, t int ) RETURNS string AS
BEGIN
  RETURN CASE 
    WHEN t=1 THEN browse.SqlInteger(kind,colid)
    WHEN t=2 THEN browse.SqlString(kind,colid)
    WHEN t=3 THEN browse.SqlTime(kind,colid)
    WHEN t=4 THEN browse.SqlDate(kind,colid)
    WHEN t=5 THEN browse.SqlFile(kind,colid)
    WHEN t=6 THEN browse.SqlBool(kind,colid)
    WHEN t=7 THEN browse.SqlPassword(kind,colid)
    WHEN t=8 THEN browse.SqlFloat(kind,colid)
    WHEN t=9 THEN browse.SqlBinary(kind,colid)
    WHEN t=10 THEN browse.SqlContentType(kind,colid)
    WHEN t=11 THEN browse.SqlImage(kind,colid)
    WHEN t=12 THEN browse.SqlFileName(kind,colid)
    WHEN t=13 THEN browse.SqlVersionCheck(kind,colid)
    ELSE 'browseSqlInvalidDatatype' 
    END 
END
GO

CREATE FN [browse].[SqlBinary]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 SET default = Default 
   FROM browse.Column WHERE Id = colid

   IF default = '' SET default = ''''''
 
   SET result = CASE
     WHEN kind = 1 THEN Name
     WHEN kind = 2 THEN Name
     WHEN kind = 3 THEN  'browse.InputString(' | colid | ',' | default | ')'
     WHEN kind = 4 THEN  'browse.InputString(' | colid | ',' | Name | ')' 
     WHEN kind = 5 OR kind = 6 THEN  'web.Form(' | sys.SingleQuote(Name) | ')' 
     ELSE 'SqlBinaryBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlBool]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */
 
   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN Name 
     WHEN kind = 4 THEN  'browse.InputBool(' | colid | ',' | Name | ')' 
     WHEN kind = 5 OR kind = 6 THEN  'browse.ParseBool(web.Form(' | sys.SingleQuote(Name) | '))' 
     ELSE 'SqlBoolBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlContentType]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 SET default = Default 
   FROM browse.Column WHERE Id = colid

   IF default = '' SET default = ''''''
 
   SET result = CASE
     WHEN kind = 1 THEN 'sys.SingleQuote(web.Encode(' | Name | '))' 
     WHEN kind = 2 THEN 'web.Encode(' | Name | ')' 
     WHEN kind = 3 THEN  ''
     WHEN kind = 4 THEN  '' 
     WHEN kind = 5 THEN  'browse.InsertContentType(' | colid | ')'
     WHEN kind = 6 THEN  'browse.UpdateContentType(' | colid | ',' | Name | ')'
     ELSE 'SqlContentTypeBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlDate]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 SET default = CASE WHEN Default = '' THEN 'date.DaysToYearMonthDay(date.Today())' ELSE Default END
   FROM browse.Column WHERE Id = colid
  
   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN 'date.YearMonthDayToString(' | Name | ')' 
     WHEN kind = 3 THEN  'browse.InputYearMonthDay(' | colid | ',' | default | ')'
     WHEN kind = 4 THEN  'browse.InputYearMonthDay(' | colid | ',' | Name | ')'
     WHEN kind = 5 OR kind = 6 THEN  'date.StringToYearMonthDay(web.Form(' | sys.SingleQuote(Name) | '))' 
     ELSE 'SqlDateBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlFile]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN 'browse.DownloadLink(Id,' | colid | ')'
     WHEN kind = 3 OR kind = 4 THEN   'browse.InputFile(' | colid | ')'
     WHEN kind = 5 THEN  'browse.InsertFile(' | sys.SingleQuote(Name) | ')' 
     WHEN kind = 6 THEN  'browse.UpdateFile(' | sys.SingleQuote(Name) | ',' | Name | ')' 
     ELSE 'SqlFileBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlFileName]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 SET default = Default 
   FROM browse.Column WHERE Id = colid

   IF default = '' SET default = ''''''
 
   SET result = CASE
     WHEN kind = 1 THEN 'sys.SingleQuote(web.Encode(' | Name | '))' 
     WHEN kind = 2 THEN 'web.Encode(' | Name | ')' 
     WHEN kind = 3 THEN  ''
     WHEN kind = 4 THEN  'browse.InputString(' | colid | ',' | Name | ')' 
     WHEN kind = 5 THEN  'browse.InsertFileName(' | colid | ')'
     WHEN kind = 6 THEN  'web.Form(' | sys.SingleQuote(Name) | ')' 
     ELSE 'SqlFileNameBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlFloat]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 
   BEGIN
      SET default = Default FROM browse.Column WHERE Id = colid
      IF default = '' SET default = 'PARSEFLOAT(''0.0'')'
   END
 
   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN Name 
     WHEN kind = 3 THEN  'browse.InputDouble(' | colid | ',' | default | ')'
     WHEN kind = 4 THEN  'browse.InputDouble(' | colid | ',' | Name | ')' 
     WHEN kind = 5 OR kind = 6 THEN  'PARSEFLOAT(web.Form(' | sys.SingleQuote(Name) | '))' 
     ELSE 'SqlFloatBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlImage]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   SET result = CASE
     WHEN kind = 1 THEN 'BINLEN(' | Name | ')'
     WHEN kind = 2 THEN 'browse.ShowImage(Id,' | colid | ')'
     WHEN kind = 3 OR kind = 4 THEN   'browse.InputFile(' | colid | ')'
     WHEN kind = 5 THEN  'browse.InsertFile(' | sys.SingleQuote(Name) | ')' 
     WHEN kind = 6 THEN  'browse.UpdateFile(' | sys.SingleQuote(Name) | ',' | Name | ')' 
     ELSE 'SqlFileBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlInteger]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 
   BEGIN
      SET default = Default FROM browse.Column WHERE Id = colid
      IF default = '' SET default = '0'
   END
 
   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN Name 
     WHEN kind = 3 THEN  'browse.InputInt(' | colid | ',' | default | ')'
     WHEN kind = 4 THEN  'browse.InputInt(' | colid | ',' | Name | ')' 
     WHEN kind = 5 OR kind = 6 THEN  'PARSEINT(web.Form(' | sys.SingleQuote(Name) | '))' 
     ELSE 'SqlIntegerBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlPassword]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 SET default = Default 
   FROM browse.Column WHERE Id = colid

   IF default = '' SET default = ''''''
 
   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN Name

     WHEN kind = 3 OR kind = 5 THEN  '' /* Password has to be set after creating user as Id is included as salt */ 

     WHEN kind = 4 THEN  'browse.InputString(' | colid | ',' | '''''' | ')' 

     /* If no new password is entered, leave password unchanged */
     WHEN kind = 6 THEN  'login.Update( ' | Name | ', web.Form(' | sys.SingleQuote(Name) | '),Id)' 
     ELSE 'SqlPasswordBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlString]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 SET default = Default 
   FROM browse.Column WHERE Id = colid

   IF default = '' SET default = ''''''
 
   SET result = CASE
     WHEN kind = 1 THEN 'sys.SingleQuote(web.Encode(' | Name | '))' 
     WHEN kind = 2 THEN 'web.Encode(' | Name | ')' 
     WHEN kind = 3 THEN  'browse.InputString(' | colid | ',' | default | ')'
     WHEN kind = 4 THEN  'browse.InputString(' | colid | ',' | Name | ')' 
     WHEN kind = 5 OR kind = 6 THEN  'web.Form(' | sys.SingleQuote(Name) | ')' 
     ELSE 'SqlStringBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlTime]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */

   DECLARE default string
   IF kind = 3 SET default = CASE WHEN Default = '' THEN 'date.Ticks()' ELSE Default END
   FROM browse.Column WHERE Id = colid
 
   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN 'date.MicroSecToString(' | Name | ')'
     WHEN kind = 3 THEN   'browse.InputTime(' | colid | ',' | default | ')'
     WHEN kind = 4 THEN   'browse.InputTime(' | colid | ',' | Name | ')'
     WHEN kind = 5 OR kind = 6 THEN  'date.StringToTime(web.Form(' | sys.SingleQuote(Name) | '))' 
     ELSE 'SqlTimeBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[SqlVersionCheck]( kind int, colid int ) RETURNS string AS
BEGIN
   /* kind values: 
      List=1, Show=2, Input(insert)=3, Input(update)=4, Parse(insert)=5, Parse(update) = 6 
   */


   SET result = CASE
     WHEN kind = 1 OR kind = 2 THEN 'date.MicroSecToString(' | Name | ')'
     WHEN kind = 3 THEN  ''
     WHEN kind = 4 THEN  'browse.InputVersionCheck(' | colid | ',' | Name | ')' 
     WHEN kind = 5 THEN  'date.Ticks()'
     WHEN kind = 6 THEN  'browse.VersionCheck(' | Name | ',PARSEINT(web.Form(' | sys.SingleQuote(Name) | ')))' 
     ELSE 'SqlIntegerBADKIND'
   END

   FROM sys.Column WHERE Id = colid
END
GO

CREATE FN [browse].[TableSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId
  DECLARE opt string, options string
  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | web.Encode( sys.TableName(Id) ) | '</option>'
  FROM sys.Table
  ORDER BY sys.TableName(Id)
  SET options |= opt
  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options | 
     '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
     | '</select>'
END
GO

CREATE FN [browse].[TableTitle]( table int ) RETURNS string AS
BEGIN
  SET result = Title FROM browse.Table WHERE Id = table
  IF result = '' SET result = Name FROM sys.Table WHERE Id = table
END
GO

CREATE FN [browse].[UpdateContentType]( colid int, old string ) RETURNS string AS 
BEGIN
  DECLARE cname string
  SET cname = Default FROM browse.Column WHERE Id = colid

  DECLARE x int
  WHILE true
  BEGIN
    DECLARE name string
    SET name = FILEATTR(x,0)
    IF name = cname RETURN FILEATTR(x,1)
    IF name = '' BREAK
    SET x = x + 1
  END    
  RETURN old
END
GO

CREATE FN [browse].[UpdateFile]( cname string, old binary ) RETURNS binary AS 
BEGIN
  DECLARE x int
  WHILE true
  BEGIN
    DECLARE name string
    SET name = FILEATTR(x,0)
    IF name = cname RETURN FILECONTENT(x)
    IF name = '' BREAK
    SET x = x + 1
  END    
  RETURN old
END
GO

CREATE FN [browse].[UpdateSql]( table int, k int ) RETURNS string AS
BEGIN
  DECLARE alist string, col string, type int, colId int
  FOR colId = Id, col = Name, type = Type FROM sys.Column WHERE Table = table
  BEGIN
    SET alist |= CASE WHEN alist = '' THEN '' ELSE ' , ' END
      | sys.QuoteName(col) | ' = ' 
      | browse.Sql( 6, colId, browse.GetDatatype(type,colId) )
  END
  RETURN 'UPDATE ' | sys.TableName( table ) | ' SET ' | alist | ' WHERE Id =' | k
END
GO

CREATE FN [browse].[VersionCheck]( latest int, check int ) RETURNS int AS
BEGIN
   IF check != latest THROW 'Version check error - record has been changed by another user'
   RETURN date.Ticks()
END
GO

CREATE FN [browse].[backargs]() RETURNS string AS 
BEGIN 
  DECLARE keep string

  /* Cleaner approach would be to iterate over all query args except b[n] */
  DECLARE n int, v string
  SET n = 1
  WHILE n < 6
  BEGIN
    DECLARE name string
    SET name = CASE 
      WHEN n = 2 THEN 'n'
      WHEN n = 3 THEN 'k'
      WHEN n = 4 THEN 'p'
      WHEN n = 5 THEN 'f'
      ELSE 's'     
    END
    SET v = web.Query(name)
    IF v != '' SET keep = keep | CASE WHEN keep = '' THEN '?' ELSE '&' END | name | '=' | v
    SET n = n + 1
  END      

  SET keep = web.Path() | keep

  SET n = 1
  WHILE 1 = 1
  BEGIN
    SET v = web.Query( 'b' | n )
    IF v = ''
      RETURN result | '&b' | n | '=' | web.UrlEncode(keep)
    ELSE
      SET result = result | '&b' | n | '=' | web.UrlEncode(v)
    SET n = n + 1
  END
END
GO

CREATE FN [browse].[backurl]() RETURNS string AS
BEGIN
  DECLARE n int
  SET n = 1
  WHILE 1 = 1
  BEGIN
    DECLARE v string, pv string, bs string
    SET v = web.Query('b' | n )
    IF v = '' RETURN pv | bs

    IF pv != '' SET  bs = bs | '&b' | (n-1) | '=' | web.UrlEncode(pv)
    SET pv = v

    SET n = n + 1
  END
END
GO

CREATE FN [browse].[fieldarg](f int) RETURNS string AS
BEGIN
  DECLARE t int, fname string
  SET t = Table, fname = Name FROM sys.Column WHERE Id = f
  RETURN browse.tablearg(t) | '&f=' | fname
END
GO

CREATE FN [browse].[fieldid]() RETURNS int AS
BEGIN 
  DECLARE t int SET t = browse.tableid()
  DECLARE fname string SET fname = web.Query('f')
  DECLARE f int SET f = Id FROM sys.Column WHERE Table = t AND Name = fname
  RETURN f
END
GO

CREATE FN [browse].[tablearg]( t int ) RETURNS string AS
BEGIN
  DECLARE sid int, s string, n string
  SET n = Name, sid = Schema FROM sys.Table WHERE Id = t
  SET s = Name FROM sys.Schema WHERE Id = sid
  RETURN 's=' | s | '&n=' | n
END
GO

CREATE FN [browse].[tableid]() RETURNS int AS
BEGIN
  DECLARE sname string, tname string, sid int, tid int
  SET sname = web.Query('s')
  SET tname = web.Query('n')

  SET sid = Id FROM sys.Schema WHERE Name = sname

  SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = tname

  RETURN tid
END
GO

INSERT INTO [browse].[Column](Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Datatype]) VALUES 
GO

INSERT INTO [browse].[Datatype](Id,[Name],[DataKind],[SqlFn]) VALUES 
(1,'Integer',3,'browse.SqlInteger')
(2,'String',2,'browse.SqlString')
(3,'Time',3,'browse.SqlTime')
(4,'Date',3,'browse.SqlDate')
(5,'File',1,'browse.SqlFile')
(6,'Bool',5,'browse.SqlBool')
(7,'Password',1,'browse.SqlPassword')
(8,'Float',4,'browse.SqlFloat')
(9,'Binary',1,'browse.SqlBinary')
(10,'ContentType',2,'browse.SqlContentType')
(11,'Image',1,'browse.SqlImage')
(12,'FileName',2,'browse.SqlFileName')
(13,'VersionCheck',3,'browse.SqlVersionCheck')
GO

INSERT INTO [browse].[Table](Id,[NameFunction],[SelectFunction],[DefaultOrder],[Title],[Description],[Role]) VALUES 
GO

--############################################
CREATE SCHEMA [handler]
GO

CREATE FN [handler].[/CheckAll]() AS 
BEGIN
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  EXEC web.Head('Check All Functions compile')
  DECLARE sid int, sname string, fname string, err int, n int

  FOR sid = Id, sname = sys.QuoteName(Name) FROM sys.Schema
  BEGIN
    FOR fname = sys.QuoteName(Name) FROM sys.Function WHERE Schema = sid
    BEGIN
      -- SELECT '<br>Checking ' | sname | '.' | fname
      EXECUTE( 'CHECK ' | sname | '.' | fname )
      DECLARE ex string SET ex = EXCEPTION()
      IF ex != '' 
      BEGIN
        SELECT '<br>Error : ' | web.Encode(ex)
        SET err = err + 1
      END
      SET n = n + 1
    END
  END
  SELECT '<p>' | n | ' functions checked, errors=' | err | '.'
  EXEC web.Trailer()
END
GO

CREATE FN [handler].[/Execute]() AS 
BEGIN
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE sql string SET sql = web.Form('sql')
  EXEC web.Head( 'Execute' )
  SELECT 
     '<p><form method=post>'
     | 'SQL to <input type=submit value=Execute>'
     | '<br><textarea name=sql rows=20 cols=100' | CASE WHEN sql='' THEN ' placeholder=\"Enter SQL here. See Manual for details.\"' ELSE '' END | '>' | web.Encode(sql) | '</textarea>' 
     | '</form>' 
  IF sql != '' 
  BEGIN
    -- EXEC SETMODE( 1 ) -- Causes result tables to be displayed as HTML tables
    EXECUTE( sql ) 
    -- EXEC SETMODE( 0 )
    DECLARE ex string SET ex = EXCEPTION()
    IF ex != '' SELECT '<p>Error : ' | web.Encode(ex)
  END
  SELECT '<p>Example SQL:'
     | '<br>SELECT dbo.CustName(Id) AS Name, Age FROM dbo.Cust'
     | '<br>SELECT Cust, Total FROM dbo.Order'
     | '<br>SELECT EMAILTX()'
     | '<br>EXEC date.Test( 2020, 1, 1, 60 )'
     | '<br>EXEC date.TestRoundTrip()'
     | '<br>CREATE TABLE dbo.Cust( LastName string, Age int )'
     | '<br>CREATE FN handler.[/MyPage]() AS BEGIN END'
     | '<br>SELECT ''hash='' | ARGON( ''argon2i!'', ''delicious salt'' )'
     | '<br>EXEC web.SetCookie(''username'',''fred'',''Max-Age=1000000000'')'
     | '<br>EXEC rtest.OneTest()'
     | '<br>CREATE INDEX ByCust ON dbo.Order(Cust)'
     | '<br>DROP INDEX ByCust ON dbo.Order'  
     | '<br>ALTER TABLE dbo.Cust MODIFY FirstName string(20), ADD [City] string, PostCode string'
     | '<br>ALTER TABLE dbo.Cust DROP Postcode'
     | '<br>DROP TABLE dbo.Cust'
     | '<br>SELECT VERIFYDB()'
     | '<br>SELECT REPACKFILE(0,''dbo'',''Order'')'
     | '<br>EXEC dbo.MakeOrders(50000)'
     | '<br>DELETE FROM dbo.Order WHERE true'
     | '<br>SELECT ''&lt;p>Id='' | Id | '' Len='' | BINLEN(data) FROM log.Transaction'

   EXEC web.Trailer()
END
GO

CREATE FN [handler].[/Logout]() AS 
BEGIN 
    EXEC web.SetCookie( 'uid', '', '' )
    EXEC web.SetCookie( 'hpw', '', '' )
    EXEC web.Head( 'Logout' )
    SELECT '<p>Logged out.'
    EXEC web.Trailer()
END
GO

CREATE FN [handler].[/Manual]() AS BEGIN

DECLARE cu int SET cu = login.get(0) IF cu = 0 RETURN

EXEC web.Head('Manual')
SELECT '<h1>Manual</h1>
<p>This manual describes the various SQL statements that are available. Where syntax is described, optional elements are enclosed in square brackets.
<h2>Schema definition</h2>
<h3>CREATE SCHEMA</h3>
<p>CREATE SCHEMA name
<p>Creates a new schema. Every database object (Table, Function) has an associated schema. Schemas are used to organise database objects into logical categories.
<h2>Table definition</h2>
<h3>CREATE TABLE</h3><p>CREATE TABLE schema.tablename ( Colname1 Coltype1, Colname2 Coltype2, ... )
<p>Creates a new base table. Every base table is automatically given an Id column, which auto-increments on INSERT ( if no explicit value is supplied).<p>The data types are as follows:
<ul>
<li>int(n), 1 <= n <= 8. Signed n-byte integer. Default is 8 bytes.</li>
<li>float, double : floating point numbers of size 4 and 8 bytes respectively.</li>
<li>string(n) : a variable length string of unicode characters. n (optional, default 15) specifies number of bytes stored inline.</li>
<li>binary(n) : a variable length string of bytes. n (optional, default 15) specifies number of bytes stored inline.</li>
<li>bool : boolean ( true or false ).</li>
</ul>

<p>Each data type has a default value : zero for numbers, a zero length string for string and binary, and false for the boolean type. The variable length data types are stored in a special system table if the length exceeds the reserved inline storage, meaning they are slightly slower to store and retrieve. Local float and integer variables and arithmetic operations are all 64 bits (8 bytes). The lower precision only applies when a value is stored in column of a table.
<h3>ALTER TABLE</h3>
<p>ALTER TABLE schema.tablename action1, action2 .... <p>The actions are as follows:
<ul>
<li>ADD Colname Coltype : a new column is added to the table.</li>
<li>MODIFY Colname Coltype : the datatype of an existing column is changed. The only changes allowed are between the different sizes of integers, between float and double, and modification of the number of bytes stored inline for binary and string.</li>
<li>DROP Colname : the column is removed from the table.</li>
</ul>
<p>Note: currently, any indexes that have been added to a table need to be dropped before using ALTER TABLE. They can be added again afterwards.
</ul>
<h2>Data manipulation statements</h2>
<h3>INSERT</h3>
<p>INSERT INTO schema.tablename ( Colname1, Colname2 ... ) VALUES ( Val1, Val2... ) [,] ( Val3, Val4 ...) ...
<p>The specified values are inserted into the table. The values may be any expressions ( possibly involving local variables or function calls ).
<h3>SELECT</h3><p>SELECT expressions FROM source-table [WHERE bool-exp ] [ORDER BY expressions]
<p>A new table is computed, based on the list of expressions and the WHERE and ORDER BY clauses.
<p>If the keyword DESC is placed after an ORDER BY expression, the order is reversed ( descending order ).
<p>The SELECT expressions can be given names using AS.
<p>When used as a stand-alone statement, the results are passed to the code that invoked the batch, and may be displayed to a user or sent to a client for further processing and eventual display. 
<h3>UPDATE</h3><p>UPDATE schema.tablename SET Colname1 = Exp1, Colname2 = Exp2 .... WHERE bool-exp
<p>Rows in the table which satisfy the WHERE condition are updated.
<h3>DELETE</h3><p>DELETE FROM schema.tablename WHERE bool-exp
<p>Rows in the table which satisfy the WHERE condition are removed.
<h2>Local variable declaration and assignment statements</h2>
<h3>DECLARE</h3><p>DECLARE name1 type1, name2 type2 ....
<p>Local variables are declared with the specified types. The variables are initialised to default values ( but only once, not each time the DECLARE is encountered if there is a loop ).
<h3>SET</h3>
<p>SET name1 = exp1, name2 = exp2 .... [ FROM table ] [ WHERE bool-exp ]
<p>Local variables are assigned. If the FROM clause is specified, the values are taken from a table row which satisfies the WHERE condition. If there is no such row, the values of the local variables remain unchanged.
<h3>FOR</h3><p>FOR name1 = exp1, name2 = exp2 .... FROM table [ WHERE bool-exp ] [ORDER BY expressions] Statement
<p>Statement is repeatedly executed for each row from the table which satisfies the WHERE condition, with the named local variables being assigned expressions which depend on the rows.
<h2>Control flow statements</h2>
<h3>BEGIN .. END</h3><p>BEGIN Statement1 Statement2 ... END
<p>The statements are executed in order. A BEGIN..END compound statement can be used whenever a single statement is allowed.
<h3>IF .. THEN ... ELSE ...</h3>
<p>IF bool-exp THEN Statement1 [ ELSE Statement2 ]
<p>If bool-exp evaluates to true Statement1 is executed, otherwise Statement2 ( if specified ) is executed.
<h3>WHILE</h3><p>WHILE bool-exp Statement
<p>Statement is repeatedly executed as long as bool-exp evaluates to true. See also BREAK.
<h3>GOTO</h3><p>GOTO label
<p>Control is transferred to the labelled statement. A label consists of a name followed by a colon (:)
<h3>BREAK</h3><p>BREAK
<p>Execution of the enclosing FOR or WHILE loop is terminated.
<h2>Batch execution</h2><p>EXECUTE ( string-expression )
<p>Evaluates the string expression, and then executes the result ( which should be a list of SQL statements ).
<p>Note that database objects ( tables, function ) must be created in a prior batch before being used. A GO statement may be used to signify the start of a new batch.
<h2>Stored Functions</h2>
<h3>CREATE FN</h3><p>CREATE FN schema.name ( param1 type1, param2 type2... ) AS BEGIN statements END
<p>A stored function ( no return value ) is created, which can later be called by an EXEC statement.
<h3>EXEC</h3><p>EXEC schema.name( exp1, exp2 ... )
<p>The stored function is called with the supplied parameters.
<h3>Exceptions</h3><p>An exception will terminate the execution of a function or batch. EXCEPTION() can be used to obtain a string describing the most recent exception (and clears the exception string). If any exception occurs, the database is left unchanged.
<h3>THROW</h3>
<p>THROW string-expression 
<p>An exception is raised, with the error message being set to the string.
<h3>CREATE FN</h3><p>CREATE FN schema.name ( param1 type1, param2 type2... ) RETURNS type AS BEGIN statements END
<p>A stored function is created which can later be used in expressions.
<h3>RETURN</h3>
<p>RETURN expression
<p>Returns a value from a stored function. RETURN with no expression returns from a stored function with no return value.
<p>The pre-defined local variable result can be assigned instead to set the return value.
<h3>CHECK</h3>
<p>CHECK schema.name
<p>Checks that a function compiles ok. EXCEPTION() should be used to check if there is any error.

<h2>Expressions</h2>
<p>Expressions are composed from literals, named local variables, local parameters and named columns from tables. These may be combined using operators, stored functions, pre-defined functions. There is also the CASE expression, which has syntax CASE WHEN bool1 THEN exp1 WHEN bool2 THEN exp2 .... ELSE exp END - the result is the expression associated with the first bool expression which evaluates to true.
<h3>Literals</h3>
<p>String literals are written enclosed in single quotes. If a single quote is needed in a string literal, it is written as two single quotes. Binary literals are written in hexadecimal preceded by 0x. Integers are a list of digits (0-9). The bool literals are true and false.
<h3>Names</h3><p>Names are enclosed in square brackets and are case sensitive ( although language keywords such as CREATE SELECT are case insensitive, and are written without the square brackets, often in upper case only by convention ). The square brackets can be omitted if the name consists of only letters (A-Z,a-z).
<h3>Operators</h3>
<p>The operators ( all binary, except for - which can be unary, and NOT which is only unary ) in order of precedence, high to low, are as follows:
<ul>
<li>*  / % : multiplication, division and remainder (after division) of numbers. Remainder only applies to integers.</li>
<li>+ - : addition, subtraction of numbers.</li>
<li>| : concatenation of string/binary values. The second expression is automatically converted to string/binary if necessary.</li>
<li>= != > < >= <= : comparison of any data type.</li>
<li>NOT : boolean negation ( result is true if arg is false, false if arg is true ).</li>
<li>AND : boolean operator ( result is true if both args are true )</li>
<li>OR : boolean operator  ( result is true if either arg is true )</li>
</ul>
<p>Brackets can be used where necessary, for example ( a + b ) * c.
<h3>Pre-defined functions</h3>
<ul>
<li>LEN( s string ) : returns the length of s, which must be a string expression.</li>
<li>BINLEN( s binary ) : returns the length of s, which must be a binary expression.</li> 
<li>SUBSTRING( s string, start int, len int ) : returns the substring of s from start (1-based) length len.</li>
<li>BINSUBSTRING( s binary, start int, len int ) : binary version of SUBSTRING.</li>
<li>REPLACE( s string, pat string, sub string ) : returns a copy of s where every occurrence of pat is replaced with sub.</li>
<li>LASTID() : returns the last Id value allocated by an INSERT statement.</li>
<li>PARSEINT( s string ) : parses an integer from s.</li>
<li>PARSEFLOAT( s string ) : parses a floating point number from s.</li>
<li>EXCEPTION() returns a string with any error that occurred during an EXECUTE statement.</li>
<li>REPACKFILE(k,schema,table) : A file is re-packed to free up pages. The result is an integer, the number of pages freed, or -1 if the table or index does not exist. k=0 => main file, k=1.. => an index, k in -4..-1 => byte storage files. 
<li>VERIFYDB() : verifies the logical page structure of the database. , the result is a string. Note: this needs exclusive access to the database to give consistent results, as it can observe update activity in shared data structures. Calling it while another process is updating the database may result in an exception.
<li>See the web schema for functions that can be used to access http requests.</li>
</ul>
<h3>Conversions</h3>
<p>To be decided. Currently the only implicit conversion is to string for operands of string concatenation.
<h2>Indexes
<h3>CREATE INDEX</h3><p>CREATE INDEX indexname ON schema.tablename( Colname1, Colname2 ... )<p>Creates a new index. Indexes allow efficient access to rows other than by Id values. 
<p>For example, <br>CREATE INDEX ByCust ON dbo.Order(Cust) 
<br>creates an index allowing the orders associated with a particular customer to be efficiently retrieved without scanning the entire order table.
<h2>Drop</h2>
<h3>DROP object-type object-name</h3><p>object-type can be any one of SCHEMA,TABLE or FUNCTION.
<p>The specified object is removed from the database. In the case of a SCHEMA, all objects in the SCHEMA are also removed. In the case of TABLE, all the rows in the table are also removed.
<h3>DROP INDEX</h3><p>DROP INDEX indexname ON schema.tablename<p>The specified index is removed from the database.
<h2>Comments</h2>
<p>There are two kinds of comments. Single line comments start with -- and extend to the end of the line. Delimited comments start with /* and are terminated by */. Comments have no effect, they are simply to help document the code.
<h2>Comparison with other SQL implementations</h2><p>There is a single variable length string datatype \"string\" for unicode strings ( equivalent to nvarchar(max) in MSSQL ), no fixed length strings.
<p>Similarly there is a single binary datatype \"binary\" equivalent to varbinary(max) in MSSQL.
<p>Every table automatically gets an integer Id field ( it does not have to be specified ), which is automatically filled in if not specified in an INSERT statement. Id values must be unique ( an attempt to insert or assign a duplicate Id will raise an exception ). 
<p>WHERE condition is not optional in UPDATE and DELETE statements - WHERE true can be used if you really want to UPDATE or DELETE all rows. This is a \"safety\" feature.
<p>Local variables cannot be assigned with SELECT, instead SET or FOR is used, can be FROM a table, e.g.
<p>DECLARE s string SET s = Name FROM sys.Schema WHERE Id = schema
<p>No cursors ( use FOR instead ).
<p>Local variables cannot be assigned in a DECLARE statement.
<p>No default schemas. Schema of tables and functions must always be stated explicitly.
<p>No nulls. Columns are initialised to default a value if not specified by INSERT, or when new columns are added to a table by ALTER TABLE.
<p>No triggers. No joins. No outer references.

<h2>Guide to the system schemas</h2>
<h3>sys</h3>
<p>Has core system tables for language objects and related functions.
<h3>web</h3>
<p>Has the function that handles web requests ( web.main ) and other functions related to handling web requests.
<h3>handler</h3>
<p>System page handling functions.
<h3>htm</h3>
<p>Has functions related to encoding html.
<h3>browse</h3><p>Has tables and functions for displaying, editing arbitrary tables in the database.
<h3>date</h3><p>Has functions for manipulating dates - conversions between Days ( from year 0 ), Year-Day, Year-Month-Day and string.
<h3>email</h3><p>Tables and functions for sending email.
<h3>log</h3><p>Transaction logging for database replication.
<h3>timed</h3><p>Timed jobs.

' 
EXEC web.Trailer()
END
GO

CREATE FN [handler].[/Menu]() AS
BEGIN
   DECLARE cu int SET cu = login.get(0) IF cu = 0 RETURN

   EXEC web.Head('System Menu')
SELECT '
<p><a target=_blank href=\"/\">Public Home Page</a>
<h3>System</h3>
<p><a href=/Execute>Execute SQL</a>
<p><a href=/browse/Table?s=login&n=user>Logins</a>
<p><a href=/browse/Table?s=web&n=File>Files</a>
<p><a target=_blank href=/ScriptAll>Script All</a> 
  | <a target=_blank href=/ScriptSystem>Script System</a>    
  | <a target=_blank href=/log/getall>Exact</a>
<p><a href=/CheckAll>Check all functions compile ok</a> 
<h3>Schemas</h3>'

   SELECT '<a href=/browse/Schema?s=' | Name | '>' | Name | '</a> | ' FROM sys.Schema ORDER BY Name

   EXEC web.Trailer()
END
GO

CREATE FN [handler].[/ScriptAll]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(0) IF cu = 0 RETURN

  EXEC web.SetContentType( 'text/plain;charset=utf-8' )

  DECLARE mode int SET mode = 1

  DECLARE s int
  FOR s = Id FROM sys.Schema
    EXEC sys.ScriptSchema(s,mode)
  FOR s = Id FROM sys.Schema
    EXEC sys.ScriptSchemaBrowse(s)
END
GO

CREATE FN [handler].[/ScriptSchema]() AS BEGIN 

  DECLARE cu int SET cu = login.get(0) IF cu = 0 RETURN

  DECLARE sname string SET sname = web.Query('s')
  DECLARE s int SET s = Id FROM sys.Schema WHERE Name = sname

  EXEC web.SetContentType( 'text/plain; charset=utf-8' )

  DECLARE mode int SET mode = CASE WHEN sys.IncludeSchema(1,sname) THEN 1 ELSE 2 END

  EXEC sys.ScriptSchema(s,mode)

  EXEC sys.ScriptSchemaBrowse(s)

END
GO

CREATE FN [handler].[/ScriptSystem]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(0) IF cu = 0 RETURN

  EXEC web.SetContentType( 'text/plain; charset=utf-8' )

  DECLARE mode int SET mode = 2

  DECLARE s int
  FOR s = Id FROM sys.Schema WHERE sys.IncludeSchema(mode,Name)
    EXEC sys.ScriptSchema(s,mode)
  FOR s = Id FROM sys.Schema WHERE sys.IncludeSchema(mode,Name)
    EXEC sys.ScriptSchemaBrowse(s)
END
GO

--############################################
CREATE SCHEMA [email]
GO

CREATE TABLE [email].[Delayed]([msg] int,[error] string,[time] int) 
GO

CREATE TABLE [email].[Msg]([from] string,[to] string,[title] string,[body] string,[format] int(1),[account] int,[status] int) 
GO

CREATE TABLE [email].[Queue]([msg] int) 
GO

CREATE TABLE [email].[SendError]([msg] int,[error] string,[time] int) 
GO

CREATE TABLE [email].[SmtpAccount]([server] string,[username] string,[password] string) 
GO

CREATE FN [email].[LogSendError]( id int, retry int, error string ) AS

BEGIN
  DELETE FROM email.Queue WHERE msg = id

  IF retry = 0
  BEGIN
    INSERT INTO email.SendError( msg, error, time )
    VALUES ( id, error, date.Ticks() )
  END
  ELSE
  BEGIN
    INSERT INTO email.Delayed( msg, error, time )
    VALUES ( id, error, date.Ticks() )
    EXEC email.Retry() -- Will update timed.Job table with time for next retry.
    EXEC timed.Sleep() -- Set sleep time based on timed.Job table.
  END
END
GO

CREATE FN [email].[MsgName](id int) RETURNS string AS
BEGIN
  SET result = '' | id
END
GO

CREATE FN [email].[MsgSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId

  DECLARE opt string, options string

  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | web.Encode( email.MsgName(Id) ) | '</option>'
  FROM email.Msg
  ORDER BY Id
  SET options |= opt

  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options 
    | '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
    | '</select>'
END
GO

CREATE FN [email].[Retry]() AS 
BEGIN 
  DECLARE now int SET now = date.Ticks()
 
  -- Find a Delayed email that is due to be sent.
  -- Transient failures are retried after 600 seconds = ten minutes. 
  DECLARE id int, t int, r int
  FOR id = Id, t = time + 600 * 1000000 FROM email.Delayed 
  BEGIN
    IF now >= t
    BEGIN
      SET r = id
      BREAK
    END
  END

  IF r != 0
  BEGIN
    DECLARE m int
    SET m = msg FROM email.Delayed WHERE Id = r
    DELETE FROM email.Delayed WHERE Id = r
    INSERT INTO email.Queue( msg ) VALUES ( m )
    DECLARE dummy int SET dummy = EMAILTX()
  END

  -- Calculate time to for next call to email.Retry.
  DECLARE next int SET next = now + 24 * 3600 * 1000000
  FOR t = time + 600 * 1000000 FROM email.Delayed
  BEGIN
    IF t < next SET next = t
  END
 
  -- Minimum time for next call to email.Retry is 10 seconds.
  IF next < now + 10 * 1000000 SET next = now + 10 * 1000000

  UPDATE timed.Job SET at = next WHERE fn = 'email.Retry'

END
GO

CREATE FN [email].[Sent](id int) AS
BEGIN
  DELETE FROM email.Queue WHERE msg = id

  -- Test retry.
  -- EXEC email.LogSendError( id, 1, 'Testing retry!' )
END
GO

CREATE FN [email].[SmtpAccountName](id int) RETURNS string AS
BEGIN
  SET result = '' | id
END
GO

CREATE FN [email].[SmtpAccountSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId

  DECLARE opt string, options string

  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | web.Encode( email.SmtpAccountName(Id) ) | '</option>'
  FROM email.SmtpAccount
  ORDER BY Id
  SET options |= opt

  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options 
    | '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
    | '</select>'
END
GO

INSERT INTO [email].[Delayed](Id,[msg],[error],[time]) VALUES 
GO

INSERT INTO [email].[Msg](Id,[from],[to],[title],[body],[format],[account],[status]) VALUES 
GO

INSERT INTO [email].[Queue](Id,[msg]) VALUES 
GO

INSERT INTO [email].[SendError](Id,[msg],[error],[time]) VALUES 
GO

INSERT INTO [email].[SmtpAccount](Id,[server],[username],[password]) VALUES 
GO

--############################################
CREATE SCHEMA [login]
GO

CREATE TABLE [login].[user]([Name] string,[HashedPassword] binary) 
GO

CREATE FN [login].[Update]( old binary, new string, id int ) RETURNS binary AS
BEGIN
   RETURN
   CASE 
   WHEN new = '' THEN old
   ELSE login.hash(new | id)
   END
END
GO

CREATE FN [login].[get]( role int ) RETURNS int AS
BEGIN
  /* Get the current logged in user, if none, output login form. Note: role is not yet checked */

  /*
     Login is initially disabled. Remove or comment out the line below enable Login after Login password has been setup for some user.
     In addition, the salt string in login.Hash should be changed.
  */
  RETURN 1 -- Login disabled.

  DECLARE uid int
  SET uid = login.user()
  IF uid = 0
  BEGIN
    EXEC web.Head( 'Login' )
    SELECT '<form method=post>User Name <input name=username><br>Password <input type=password name=password><br><input type=submit value=Login></form>'
    EXEC web.Trailer()
  END
  RETURN uid
END
GO

CREATE FN [login].[hash](s string) RETURNS binary AS
BEGIN
  SET result = ARGON(s,'Sep 14 2022 saltiness')
END
GO

CREATE FN [login].[user]() RETURNS int AS
BEGIN
  DECLARE username string SET username = web.Form('username')
  DECLARE uid int

  IF username != ''
  BEGIN
    DECLARE password string SET password = web.Form('password')
    SET result = Id FROM login.user WHERE Name = username
    DECLARE hpw binary SET hpw = login.hash( password|result )
    SET uid = Id FROM login.user WHERE Id = result AND HashedPassword = hpw
    IF uid > 0
    BEGIN
      EXEC web.SetCookie( 'uid', '' | uid, '' )
      EXEC web.SetCookie( 'hpw', '' | hpw, '' )
      IF web.SetDos(uid) = 0 RETURN 0
      RETURN uid
    END
  END

  DECLARE uids string SET uids = web.Cookie('uid')
  DECLARE hpwf string SET hpwf = web.Cookie('hpw')

  IF uids != ''
  BEGIN
    SET uid = PARSEINT(uids)
    DECLARE hpwt binary SET hpwt = HashedPassword FROM login.user WHERE Id = uid
    IF hpwf = '' | hpwt 
    BEGIN
      IF web.SetDos(uid) = 0 RETURN 0
      RETURN uid
    END
  END
  RETURN 0
END
GO

INSERT INTO [login].[user](Id,[Name],[HashedPassword]) VALUES 
GO

--############################################
CREATE SCHEMA [timed]
GO

CREATE TABLE [timed].[Job]([fn] string,[at] int) 
GO

CREATE FN [timed].[Run]() AS 
/* 
  This function is called by the Rust program.
  The time interval (in milliseconds) is set using the built-in SLEEP function.
*/
BEGIN 

  DECLARE now int SET now = date.Ticks()

  DECLARE f string, a int
  FOR f = fn, a = at FROM timed.Job
  BEGIN
    IF now >= a
      EXECUTE( 'EXEC ' | f | '()' )
  END
   
  EXEC timed.Sleep()
END
GO

CREATE FN [timed].[Sleep]() AS 
BEGIN 
  /* Set sleep time based on timed.Job table */

  DECLARE now int, next int
  SET now = date.Ticks()

  SET next = now + 24 * 3600 * 1000000 -- 24 hours

  DECLARE t int
  FOR t = at FROM timed.Job
  BEGIN
    IF t < next SET next = t
  END

  DECLARE dummy int
  SET dummy = SLEEP( next-now )
END
GO

INSERT INTO [timed].[Job](Id,[fn],[at]) VALUES 
GO

--############################################
CREATE SCHEMA [log]
GO

CREATE TABLE [log].[Transaction]([data] binary) 
GO

CREATE FN [log].[/log/get]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  DECLARE k int SET k = PARSEINT( web.Query('k') )

  DECLARE id int, d binary

  SET id = Id, d = data FROM log.Transaction WHERE Id = k

  IF id = k 
    SELECT d
  ELSE
  BEGIN
    DECLARE dummy int SET dummy = TRANSWAIT()
  END
END
GO

CREATE FN [log].[/log/getall]() AS 
BEGIN 
  DECLARE cu int SET cu = login.get(1) IF cu = 0 RETURN

  EXEC web.SetContentType( 'text/plain; charset=utf-8' )

  DECLARE t int
  FOR t = Id FROM sys.Table
    EXEC sys.ScriptData(t,3)
END
GO

INSERT INTO [log].[Transaction](Id,[data]) VALUES 
GO

DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Column'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Table'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'sys' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Table'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Type'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Function'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Schema'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'sys' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Schema'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Index'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'sys.IndexName','','','','',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Table'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'sys' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Table'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'IndexColumn'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Index'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'sys' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Index'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Schema'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'sys.SchemaName','browse.SchemaSelect','','','',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Table'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'sys.TableName','browse.TableSelect','','','',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Schema'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'sys' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Schema'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'web'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'File'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Path'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'Content',0,0,'',12)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'ContentType'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'Content',0,0,'',10)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Content'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',5)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'browse'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Column'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'RefersTo'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'sys' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Table'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, -1,'','',rt,'',0,0,'',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'InputFunction'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 100,'','',rt,'',0,0,'',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Datatype'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'browse' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Datatype'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, -2,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'browse'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Datatype'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'browse.DatatypeName','browse.DatatypeSelect','','','',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'DataKind'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'browse'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Table'
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'email'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Delayed'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'msg'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'email' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Msg'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'time'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'email'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Msg'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'email.MsgName','email.MsgSelect','','','',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'format'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','0 means body is plain text, 1 means HTML.',rt,'',0,0,'',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'account'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'email' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'SmtpAccount'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'email'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Queue'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'msg'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'email' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Msg'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'email'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'SendError'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'msg'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = 'email' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = 'Msg'

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',1)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'error'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',2)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'time'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',3)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'email'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'SmtpAccount'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'email.SmtpAccountName','email.SmtpAccountSelect','','','',0)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'login'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'user'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'HashedPassword'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'',0,0,'',7)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'timed'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Job'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'at'
SET rs = 0 SET rs =Id FROM sys.Schema WHERE Name = '' 
SET rt = 0 SET rt =Id FROM sys.Table WHERE Schema = rs AND Name = ''

INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputRows],[InputFunction],[Datatype]) 
VALUES (cid, 0,'','',rt,'date.Ticks()',0,0,'browse.InputTime',3)
GO
DECLARE tid int, sid int, cid int, rs int, rt int
SET sid = Id FROM sys.Schema WHERE Name = 'log'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Transaction'
GO";