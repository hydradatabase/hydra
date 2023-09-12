/*-------------------------------------------------------------------------
 *
 * types.h
 *
 * Defines for operator construction
 *
 * Copyright (c) Hydra, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_TYPES_H
#define COLUMNAR_TYPES_H

#include "postgres.h"

#include "columnar/vectorization/columnar_vector_types.h"

#define _BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, OPSYM, OPSTR)				\
PG_FUNCTION_INFO_V1(v##FNAME##OPSTR);										\
Datum v##FNAME##OPSTR(PG_FUNCTION_ARGS) 									\
{																			\
	VectorFnArgument *left = (VectorFnArgument *) PG_GETARG_POINTER(0);		\
	VectorFnArgument *right = (VectorFnArgument *) PG_GETARG_POINTER(1);	\
																			\
	VectorColumn *vectorColumn = NULL;										\
	VectorColumn *res = NULL;												\
	int i = 0;																\
																			\
	if (left->type == VECTOR_FN_ARG_VAR &&									\
		right->type == VECTOR_FN_ARG_CONSTANT)								\
	{																		\
		vectorColumn = (VectorColumn*) left->arg;							\
		RTYPE constValue = right->arg;										\
																			\
		res = BuildVectorColumn(vectorColumn->dimension, 1, true, NULL);	\
																			\
		LTYPE *vectorValue = (LTYPE *) vectorColumn->value;					\
		bool *vectorNull = (bool *) vectorColumn->isnull;					\
		bool *resIdx = (bool *) res->value;									\
		bool *resNull = (bool *) res->isnull;								\
																			\
		for (i = 0; i < vectorColumn->dimension; i++)						\
		{																	\
			resNull[i] = vectorNull[i];										\
			resIdx[i] = !vectorNull[i] && vectorValue[i] OPSYM constValue;	\
		}																	\
																			\
		res->dimension = vectorColumn->dimension;							\
	}																		\
	else if (left->type == VECTOR_FN_ARG_CONSTANT &&						\
			 right->type == VECTOR_FN_ARG_VAR)								\
	{																		\
		vectorColumn = (VectorColumn*) right->arg;							\
		LTYPE constValue = left->arg;										\
																			\
		res = BuildVectorColumn(vectorColumn->dimension, 1, true, NULL);	\
																			\
		RTYPE *vectorValue = (RTYPE *) vectorColumn->value;					\
		bool *vectorNull = (bool *) vectorColumn->isnull;					\
		bool *resIdx = (bool *) res->value;									\
		bool *resNull = (bool *) res->isnull;								\
																			\
		for (i = 0; i < vectorColumn->dimension; i++)						\
		{																	\
			resNull[i] = vectorNull[i];										\
			resIdx[i] = !vectorNull[i] && vectorValue[i] OPSYM constValue;	\
		}																	\
																			\
		res->dimension = vectorColumn->dimension;							\
	}																		\
	else																	\
	{																		\
																			\
	}																		\
																			\
	PG_RETURN_POINTER(res);													\
}																			\

#define BUILD_CMP_OPERATOR_INT(FNAME, LTYPE, RTYPE)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, ==, eq)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, !=, ne)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE,  >, gt)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE,  <, lt)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, <=, le)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, >=, ge)		\


typedef struct Int128AggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */
	int64		N;				/* count of processed numbers */
	int128		sumX;			/* sum of processed numbers */
	int128		sumX2;			/* sum of squares of processed numbers */
} Int128AggState;

typedef struct Int64AggState
{
	int64		N;				/* count of processed numbers */
	int64		sumX;			/* sum of processed numbers */
} Int64AggState;

#endif

