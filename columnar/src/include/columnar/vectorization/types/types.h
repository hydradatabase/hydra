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

#define _BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST, OPSYM, OPSTR)	\
PG_FUNCTION_INFO_V1(v##FNAME##OPSTR);										\
Datum v##FNAME##OPSTR(PG_FUNCTION_ARGS) 									\
{																			\
	VectorColumn *arg1 = (VectorColumn*) LCAST(0); 							\
	RTYPE arg2 = RCAST(1); 													\
	VectorColumn *res = BuildVectorColumn(arg1->dimension, 1, true);		\
																			\
	int i = 0;																\
	LTYPE *arg1Idx = (LTYPE *) arg1->value;									\
	bool *resIdx = (bool *) res->value;										\
																			\
	for (i = 0; i < arg1->dimension; i++)									\
	{																		\
		res->isnull[i] = arg1->isnull[i];									\
		resIdx[i] = arg1Idx[i] OPSYM arg2;									\
	}																		\
																			\
	res->dimension = arg1->dimension;										\
	PG_RETURN_POINTER(res);													\
}																			\

#define BUILD_CMP_OPERATOR_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST, ==, eq)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST, !=, ne)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST,  >, gt)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST,  <, lt)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST, <=, le)		\
	_BUILD_CMP_OP_INT(FNAME, LTYPE, RTYPE, LCAST, RCAST, >=, ge)		\

#endif