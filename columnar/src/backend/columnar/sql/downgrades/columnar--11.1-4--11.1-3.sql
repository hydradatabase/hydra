-- columnar--11.1-4--11.1-3.sql

DROP FUNCTION public.vchareq(character, character);
DROP FUNCTION public.vcharge(character, character);
DROP FUNCTION public.vchargt(character, character);
DROP FUNCTION public.vcharle(character, character);
DROP FUNCTION public.vcharlt(character, character);
DROP FUNCTION public.vcharne(character, character);

DROP FUNCTION public.vdate_eq(date, date);
DROP FUNCTION public.vdate_ge(date, date);
DROP FUNCTION public.vdate_gt(date, date);
DROP FUNCTION public.vdate_le(date, date);
DROP FUNCTION public.vdate_lt(date, date);

DROP FUNCTION public.vint24eq(smallint, integer);
DROP FUNCTION public.vint24ge(smallint, integer);
DROP FUNCTION public.vint24gt(smallint, integer);
DROP FUNCTION public.vint24le(smallint, integer);
DROP FUNCTION public.vint24lt(smallint, integer);
DROP FUNCTION public.vint24ne(smallint, integer);

DROP FUNCTION public.vint28eq(smallint, bigint);
DROP FUNCTION public.vint28ge(smallint, bigint);
DROP FUNCTION public.vint28gt(smallint, bigint);
DROP FUNCTION public.vint28le(smallint, bigint);
DROP FUNCTION public.vint28lt(smallint, bigint);
DROP FUNCTION public.vint28ne(smallint, bigint);

DROP FUNCTION public.vint2eq(smallint, smallint);
DROP FUNCTION public.vint2ge(smallint, smallint);
DROP FUNCTION public.vint2gt(smallint, smallint);
DROP FUNCTION public.vint2le(smallint, smallint);
DROP FUNCTION public.vint2lt(smallint, smallint);
DROP FUNCTION public.vint2ne(smallint, smallint);

DROP FUNCTION public.vint42eq(integer, smallint);
DROP FUNCTION public.vint42ge(integer, smallint);
DROP FUNCTION public.vint42gt(integer, smallint);
DROP FUNCTION public.vint42le(integer, smallint);
DROP FUNCTION public.vint42lt(integer, smallint);
DROP FUNCTION public.vint42ne(integer, smallint);

DROP FUNCTION public.vint48eq(integer, bigint);
DROP FUNCTION public.vint48ge(integer, bigint);
DROP FUNCTION public.vint48gt(integer, bigint);
DROP FUNCTION public.vint48le(integer, bigint);
DROP FUNCTION public.vint48lt(integer, bigint);
DROP FUNCTION public.vint48ne(integer, bigint);

DROP FUNCTION public.vint4eq(integer, integer);
DROP FUNCTION public.vint4ge(integer, integer);
DROP FUNCTION public.vint4gt(integer, integer);
DROP FUNCTION public.vint4le(integer, integer);
DROP FUNCTION public.vint4lt(integer, integer);
DROP FUNCTION public.vint4ne(integer, integer);

DROP FUNCTION public.vint82eq(bigint, smallint);
DROP FUNCTION public.vint82ge(bigint, smallint);
DROP FUNCTION public.vint82gt(bigint, smallint);
DROP FUNCTION public.vint82le(bigint, smallint);
DROP FUNCTION public.vint82lt(bigint, smallint);
DROP FUNCTION public.vint82ne(bigint, smallint);

DROP FUNCTION public.vint84eq(bigint, integer);
DROP FUNCTION public.vint84ge(bigint, integer);
DROP FUNCTION public.vint84gt(bigint, integer);
DROP FUNCTION public.vint84le(bigint, integer);
DROP FUNCTION public.vint84lt(bigint, integer);
DROP FUNCTION public.vint84ne(bigint, integer);

DROP FUNCTION public.vint8eq(bigint, bigint);
DROP FUNCTION public.vint8ge(bigint, bigint);
DROP FUNCTION public.vint8gt(bigint, bigint);
DROP FUNCTION public.vint8le(bigint, bigint);
DROP FUNCTION public.vint8lt(bigint, bigint);
DROP FUNCTION public.vint8ne(bigint, bigint);

DROP FUNCTION public.vtime_eq(time without time zone, time without time zone);
DROP FUNCTION public.vtime_ge(time without time zone, time without time zone);
DROP FUNCTION public.vtime_gt(time without time zone, time without time zone);
DROP FUNCTION public.vtime_le(time without time zone, time without time zone);
DROP FUNCTION public.vtime_lt(time without time zone, time without time zone);
DROP FUNCTION public.vtime_ne(time without time zone, time without time zone);