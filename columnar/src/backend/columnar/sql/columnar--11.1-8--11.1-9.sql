-- columnar--11.1-8--11.1-9.sql

-- count

CREATE FUNCTION vemptycount(int8) RETURNS int8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE FUNCTION vanycount(int8, "any") RETURNS int8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;

CREATE AGGREGATE vcount(*) (SFUNC = vemptycount, STYPE = int8, INITCOND="0");
CREATE AGGREGATE vcount("any") (SFUNC = vanycount, STYPE = int8, INITCOND="0");

-- int2 / int4

CREATE FUNCTION vint2int4avg(internal) RETURNS numeric AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;

-- int2

CREATE FUNCTION vint2sum(int8, int2) RETURNS int8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vsum(int2) (SFUNC = vint2sum, STYPE = int8, INITCOND="0");

CREATE FUNCTION vint2acc(internal, int2) RETURNS internal AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vavg(int2) (SFUNC = vint2acc, STYPE = internal, FINALFUNC = vint2int4avg, INITCOND = '{0,0}');

CREATE FUNCTION vint2larger(int2, int2) RETURNS int2 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmax(int2) (SFUNC = vint2larger, STYPE = int2, INITCOND="-32768");

CREATE FUNCTION vint2smaller(int2, int2) RETURNS int2 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmin(int2) (SFUNC = vint2smaller, STYPE = int2, INITCOND="32767");

-- int4

CREATE FUNCTION vint4sum(int8, int4) RETURNS int8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vsum(int4) (SFUNC = vint4sum, STYPE = int8, INITCOND="0");

CREATE FUNCTION vint4acc(internal, int4) RETURNS internal AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vavg(int4) (SFUNC = vint4acc, STYPE = internal, FINALFUNC = vint2int4avg, INITCOND = '{0,0}');

CREATE FUNCTION vint4larger(int4, int4) RETURNS int4 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmax(int4) (SFUNC = vint4larger, STYPE = int4, INITCOND="-2147483648");

CREATE FUNCTION vint4smaller(int4, int4) RETURNS int4 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmin(int4) (SFUNC = vint4smaller, STYPE = int4, INITCOND="2147483647");


-- int8

CREATE FUNCTION vint8acc(internal, int8) RETURNS internal AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;

CREATE FUNCTION vint8sum(internal) RETURNS numeric AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vsum(int8) (SFUNC = vint8acc, STYPE = internal, FINALFUNC = vint8sum, 
                             SERIALFUNC = int8_avg_serialize, DESERIALFUNC = int8_avg_deserialize);

CREATE FUNCTION vint8avg(internal) RETURNS numeric AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vavg(int8) (SFUNC = vint8acc, STYPE = internal, FINALFUNC = vint8avg, 
                             SERIALFUNC = int8_avg_serialize, DESERIALFUNC = int8_avg_deserialize);

CREATE FUNCTION vint8larger(int8, int8) RETURNS int8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmax(int8) (SFUNC = vint8larger, STYPE = int8, INITCOND="-9223372036854775808");

CREATE FUNCTION vint8smaller(int8, int8) RETURNS int8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmin(int8) (SFUNC = vint8smaller, STYPE = int8, INITCOND="9223372036854775807");

-- date

CREATE FUNCTION vdatelarger(date, date) RETURNS date AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmax(date) (SFUNC = vdatelarger, STYPE = date, INITCOND="-infinity");

CREATE FUNCTION vdatesmaller(date, date) RETURNS date AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE vmin(date) (SFUNC = vdatesmaller, STYPE = date, INITCOND="infinity");
