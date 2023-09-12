/*-------------------------------------------------------------------------
 *
 * columnar_vector_types.h
 *
 * Structures used in vectorization execution
 *
 * Copyright (c) Hydra, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_VECTOR_TYPES_H
#define COLUMNAR_VECTOR_TYPES_H

#include "postgres.h"
#include "fmgr.h"

#include "executor/tuptable.h"
#include "nodes/bitmapset.h"
#include "nodes/primnodes.h"

/* DEFAULT_CHUNK_ROW_COUNT */
#define COLUMNAR_VECTOR_COLUMN_SIZE 10000

typedef struct VectorTupleTableSlot
{
	/* TupleTableSlot structure */
	TupleTableSlot tts;
	/* How many tuples does this slot contain */ 
	uint32 dimension;
	/* Keep array to represent filtered tuples */
	bool keep[COLUMNAR_VECTOR_COLUMN_SIZE];
	/* Row Number */
	uint64 rowNumber[COLUMNAR_VECTOR_COLUMN_SIZE];
} VectorTupleTableSlot;

extern TupleTableSlot * CreateVectorTupleTableSlot(TupleDesc tupleDesc);

typedef struct VectorColumn
{
	uint32	dimension;
	uint16	columnTypeLen;
	bool 	columnIsVal;
	Datum	*value;
	bool	isnull[COLUMNAR_VECTOR_COLUMN_SIZE];
	uint64	*rowNumber;
} VectorColumn;

extern VectorColumn * BuildVectorColumn(int16 columnDimension,
										int16 columnTypeLen,
										bool columnIsVal,
										uint64 *rowNumber);
extern void ExtractTupleFromVectorSlot(TupleTableSlot *out, 
									   VectorTupleTableSlot *vectorSlot, 
									   int32 index,
									   List *attrNeededList);
extern void WriteTupleToVectorSlot(TupleTableSlot *in,
								   VectorTupleTableSlot *vectorSlot,
								   int32 index);
extern void CleanupVectorSlot(VectorTupleTableSlot *vectorSlot);

typedef enum VectorQualType
{
	VECTOR_QUAL_BOOL_EXPR,
	VECTOR_QUAL_EXPR
} VectorQualTypeEnum;


typedef enum VectorArgType
{
	VECTOR_FN_ARG_CONSTANT,
	VECTOR_FN_ARG_VAR
} VectorFnArgTypeEnum;


typedef struct VectorFnArgument
{
	VectorFnArgTypeEnum type;
	Datum arg;
} VectorFnArgument;


typedef struct VectorQual
{
	VectorQualTypeEnum vectorQualType;
	union
	{
		struct 
		{
			FmgrInfo *fmgrInfo;
			FunctionCallInfo fcInfo;
			VectorFnArgument *vectorFnArguments;
		} expr;
		struct
		{
			BoolExprType boolExprType;
			List *vectorQualExprList;
		} boolExpr;
	} u;
} VectorQual;

#endif
