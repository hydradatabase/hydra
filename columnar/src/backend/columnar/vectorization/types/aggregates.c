
#include "postgres.h"

#include "fmgr.h"
#include "nodes/execnodes.h"
#include "utils/date.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"

#include "pg_version_constants.h"
#include "columnar/vectorization/types/types.h"
#include "columnar/vectorization/types/numeric.h"

/* count */

PG_FUNCTION_INFO_V1(vemptycount);
Datum vemptycount(PG_FUNCTION_ARGS)
{
	int64 arg = PG_GETARG_INT64(0);
	PG_RETURN_INT64(arg);
}

PG_FUNCTION_INFO_V1(vanycount);
Datum
vanycount(PG_FUNCTION_ARGS)
{
	int64 arg = PG_GETARG_INT64(0);
	int64 result = arg;
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	int i;

	for (i = 0; i <  arg1->dimension; i++) 
	{
		if (arg1->isnull[i])
			continue;

		result++;
	}

	PG_RETURN_INT64(result);
}

/* int2 */

PG_FUNCTION_INFO_V1(vint2sum);
Datum
vint2sum(PG_FUNCTION_ARGS)
{
	int64 sumX = PG_GETARG_INT64(0);
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i;

	int16 *vectorValue = (int16*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
			sumX += (int64) vectorValue[i];
	}

	PG_RETURN_INT64(sumX);
}

PG_FUNCTION_INFO_V1(vint2acc);
Datum
vint2acc(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	Int64AggState *transdata;
	int i;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	transdata = (Int64AggState *) ARR_DATA_PTR(transarray);

	int16 *vectorValue = (int16*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			transdata->N++;
			transdata->sumX += (int64) vectorValue[i];
		}
	}

	PG_RETURN_ARRAYTYPE_P(transarray);
}

PG_FUNCTION_INFO_V1(vint2larger);
Datum vint2larger(PG_FUNCTION_ARGS)
{
	int16 maxValue = PG_GETARG_INT16(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int16 result = maxValue;
	int i = 0;

	int16 *vectorValue = (int16*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Max(result, vectorValue[i]);
	}

	maxValue = Max(maxValue, result);

	PG_RETURN_INT16(maxValue);
}

PG_FUNCTION_INFO_V1(vint2smaller);
Datum vint2smaller(PG_FUNCTION_ARGS)
{
	int16 minValue = PG_GETARG_INT32(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int16 result = minValue;
	int i = 0;

	int16 *vectorValue = (int16*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Min(result, vectorValue[i]);
	}

	minValue = Min(minValue, result);

	PG_RETURN_INT16(minValue);
}

/* int4 */

PG_FUNCTION_INFO_V1(vint4sum);
Datum
vint4sum(PG_FUNCTION_ARGS)
{
	int64 sumX = PG_GETARG_INT64(0);
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i;

	int32 *vectorValue = (int32*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
			sumX += (int64) vectorValue[i];
	}

	PG_RETURN_INT64(sumX);
}

PG_FUNCTION_INFO_V1(vint4acc);
Datum
vint4acc(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	Int64AggState *transdata;
	int i;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	transdata = (Int64AggState *) ARR_DATA_PTR(transarray);

	int32 *vectorValue = (int32*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			transdata->N++;
			transdata->sumX += (int64) vectorValue[i];
		}
	}

	PG_RETURN_ARRAYTYPE_P(transarray);
}

PG_FUNCTION_INFO_V1(vint4larger);
Datum vint4larger(PG_FUNCTION_ARGS)
{
	int32 maxValue = PG_GETARG_INT32(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int32 result = maxValue;
	int i = 0;

	int32 *vectorValue = (int32*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Max(result, vectorValue[i]);
	}

	maxValue = Max(maxValue, result);

	PG_RETURN_INT32(maxValue);
}

PG_FUNCTION_INFO_V1(vint4smaller);
Datum vint4smaller(PG_FUNCTION_ARGS)
{
	int32 minValue = PG_GETARG_INT32(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int32 result = minValue;
	int i = 0;

	int32 *vectorValue = (int32*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Min(result, vectorValue[i]);
	}

	minValue = Min(minValue, result);

	PG_RETURN_INT32(minValue);
}

/* int2 / int4 */

PG_FUNCTION_INFO_V1(vint2int4avg);
Datum
vint2int4avg(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Int64AggState *transdata;

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int64AggState))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int64AggState *) ARR_DATA_PTR(transarray);

	/* SQL defines AVG of no values to be NULL */
	if (transdata->N == 0)
		PG_RETURN_NULL();

#if PG_VERSION_NUM >= PG_VERSION_14
	Numeric sumNumeric, nNumeric, res;
	nNumeric = int64_to_numeric(transdata->N);
	sumNumeric = int64_to_numeric(transdata->sumX);
	res = numeric_div_opt_error(sumNumeric, nNumeric, NULL);
	PG_RETURN_NUMERIC(res);
#else
	Datum countd, sumd;
	countd = DirectFunctionCall1(int8_numeric,
								 Int64GetDatumFast(transdata->N));
	sumd = DirectFunctionCall1(int8_numeric,
							   Int64GetDatumFast(transdata->sumX));
	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
#endif

}


/* int8 */

PG_FUNCTION_INFO_V1(vint8acc);
Datum
vint8acc(PG_FUNCTION_ARGS)
{
	Int128AggState *state;
	int i;

	state = PG_ARGISNULL(0) ? NULL : (Int128AggState *) PG_GETARG_POINTER(0);
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);

	MemoryContext aggContext;
	MemoryContext oldContext;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "aggregate function called in non-aggregate context");

	oldContext = MemoryContextSwitchTo(aggContext);

	/* Create the state data on the first call */
	if (state == NULL)
	{
		state = palloc0(sizeof(Int128AggState));
		state->calcSumX2 = false;
	}

	int64 *vectorValue = (int64*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			state->N++;
			state->sumX += (int128) vectorValue[i];
		}
	}

	MemoryContextSwitchTo(oldContext);

	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(vint8sum);
Datum
vint8sum(PG_FUNCTION_ARGS)
{
	Int128AggState *state;

	state = PG_ARGISNULL(0) ? NULL : (Int128AggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	Numeric res = int128_to_numeric(state->sumX);

	PG_RETURN_NUMERIC(res);
}

PG_FUNCTION_INFO_V1(vint8avg);
Datum
vint8avg(PG_FUNCTION_ARGS)
{
	Int128AggState *state;
	Numeric sumNumeric, nNumeric, res;

	state = PG_ARGISNULL(0) ? NULL : (Int128AggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	sumNumeric = int128_to_numeric(state->sumX);
	nNumeric = int128_to_numeric(state->N);
	res = numeric_div_opt_error(sumNumeric, nNumeric, NULL);

	PG_RETURN_NUMERIC(res);
}

PG_FUNCTION_INFO_V1(vint8larger);
Datum vint8larger(PG_FUNCTION_ARGS)
{
	int64 maxValue = PG_GETARG_INT64(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int64 result = 0;
	int i = 0;

	int64 *vectorValue = (int64*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Max(maxValue, vectorValue[i]);
	}

	maxValue = Max(maxValue, result);

	PG_RETURN_INT64(maxValue);
}

PG_FUNCTION_INFO_V1(vint8smaller);
Datum vint8smaller(PG_FUNCTION_ARGS)
{
	int64 minValue = PG_GETARG_INT64(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int64 result = minValue;
	int i = 0;

	int64 *vectorValue = (int64*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Min(result, vectorValue[i]);
	}

	minValue = Min(minValue, result);

	PG_RETURN_INT64(minValue);
}

// date

PG_FUNCTION_INFO_V1(vdatelarger);
Datum vdatelarger(PG_FUNCTION_ARGS)
{
	int32 maxValue = PG_GETARG_INT32(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int32 result = maxValue;
	int i = 0;

	DateADT *vectorValue = (DateADT*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Max(result, vectorValue[i]);
	}

	maxValue = Max(maxValue, result);

	PG_RETURN_INT32(maxValue);
}

PG_FUNCTION_INFO_V1(vdatesmaller);
Datum vdatesmaller(PG_FUNCTION_ARGS)
{
	int32 minValue = PG_GETARG_INT32(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int32 result = minValue;
	int i = 0;

	DateADT *vectorValue = (DateADT*) arg2->value;

	for (i = 0; i < arg2->dimension; i++) 
	{
		if (arg2->isnull[i])
			continue;

		result = Min(result, vectorValue[i]);
	}

	minValue = Min(minValue, result);

	PG_RETURN_INT32(minValue);
}