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
	/* Skip array to represent filtered tuples */
	bool skip[COLUMNAR_VECTOR_COLUMN_SIZE];
} VectorTupleTableSlot;

extern TupleTableSlot * CreateVectorTupleTableSlot(TupleDesc tupleDesc);

typedef struct VectorColumn
{
	uint32	dimension;
	uint16	columnTypeLen;
	bool 	columnIsVal;
	Datum	*value;
	bool	isnull[COLUMNAR_VECTOR_COLUMN_SIZE];
} VectorColumn;

extern VectorColumn * BuildVectorColumn(int16 columnDimension,
										int16 columnTypeLen,
										bool columnIsVal);
extern void extractTupleFromVectorSlot(TupleTableSlot *out, 
									   VectorTupleTableSlot *vectorSlot, 
									   int32 index,
									   Bitmapset *attrNeeded);

typedef enum VectorQualType
{
	VECTOR_QUAL_BOOL_EXPR,
	VECTOR_QUAL_EXPR
} VectorQualTypeEnum;

typedef struct VectorQual
{
	VectorQualTypeEnum vectorQualType;
	union
	{
		struct 
		{
			FmgrInfo *fmgrInfo;
			FunctionCallInfo fcInfo;
		} expr;
		struct
		{
			BoolExprType boolExprType;
			List *vectorQualExprList;
		} boolExpr;
	} u;
} VectorQual;

#endif
