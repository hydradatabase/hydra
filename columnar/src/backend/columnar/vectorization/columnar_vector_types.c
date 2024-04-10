/*-------------------------------------------------------------------------
 *
 * columnar_vector_types.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/lsyscache.h"
#include "nodes/bitmapset.h"

#include "columnar/columnar.h"
#include "columnar/vectorization/columnar_vector_types.h"

#include "columnar/utils/listutils.h"

VectorColumn *
BuildVectorColumn(int16 columnDimension, int16 columnTypeLen, 
				  bool columnIsVal, uint64 *rowNumber)
{
	VectorColumn *vectorColumn;

	vectorColumn = palloc0(sizeof(VectorColumn));

	vectorColumn->dimension = 0;
	vectorColumn->value = palloc0(columnTypeLen * COLUMNAR_VECTOR_COLUMN_SIZE);
	vectorColumn->columnTypeLen = columnTypeLen;
	vectorColumn->columnIsVal = columnIsVal;
	vectorColumn->rowNumber = rowNumber;

	return vectorColumn;
}

TupleTableSlot * 
CreateVectorTupleTableSlot(TupleDesc tupleDesc)
{
	int						i;
	TupleTableSlot			*slot;
	VectorTupleTableSlot	*vectorTTS;
	VectorColumn			*vectorColumn;

	static TupleTableSlotOps tts_ops;
	tts_ops = TTSOpsVirtual;
	tts_ops.base_slot_size = sizeof(VectorTupleTableSlot);

	slot = MakeTupleTableSlot(CreateTupleDescCopy(tupleDesc), &tts_ops);

	TupleDesc slotTupleDesc  = slot->tts_tupleDescriptor;

	/* Vectorized TTS */
	vectorTTS = (VectorTupleTableSlot*) slot;
	
	/* All tuples should be skipped in initialization */
	memset(vectorTTS->keep, false, COLUMNAR_VECTOR_COLUMN_SIZE);

	for (i = 0; i < slotTupleDesc->natts; i++)
	{		
		Oid columnTypeOid = TupleDescAttr(slotTupleDesc, i)->atttypid;
		
		int16 columnTypeLen = get_typlen(columnTypeOid);

		int16 vectorColumnTypeLen = 
			columnTypeLen == -1 ?  sizeof(Datum) : columnTypeLen;

		/* 
		 * We consider that type is passed by val also for cases where we have 
		 * typlen == -1. This is because we use pointer to VARLEN type and don't
		 * construct our own object.
		*/
		bool vectorColumnIsVal = vectorColumnTypeLen <= sizeof(Datum);

		vectorColumn = BuildVectorColumn(COLUMNAR_VECTOR_COLUMN_SIZE,
										 vectorColumnTypeLen,
										 vectorColumnIsVal,
										 vectorTTS->rowNumber);

		vectorTTS->tts.tts_values[i] = PointerGetDatum(vectorColumn);
		vectorTTS->tts.tts_isnull[i] = false;
	}

	vectorTTS->tts.tts_nvalid = tupleDesc->natts;

	return slot;
}


void
ExtractTupleFromVectorSlot(TupleTableSlot *out, VectorTupleTableSlot *vectorSlot, 
						   int32 index, List *attrNeededList)
{
	int attno;
	foreach_int(attno, attrNeededList)
	{
		if (!out->tts_tupleDescriptor->attrs[attno].attisdropped)
		{
			VectorColumn *column = (VectorColumn *) vectorSlot->tts.tts_values[attno];
			int8 *rawColumRawData = (int8*) column->value + column->columnTypeLen * index;
			out->tts_values[attno] = fetch_att(rawColumRawData, column->columnIsVal, column->columnTypeLen);
			out->tts_isnull[attno] = column->isnull[index];
		}
	}

	ExecStoreVirtualTuple(out);
}

void
WriteTupleToVectorSlot(TupleTableSlot *in, VectorTupleTableSlot *vectorSlot, 
					   int32 index)
{
	TupleDesc tupDesc = in->tts_tupleDescriptor;

	int i;

	//vectorSlot->keep[index] = true;

	for (i = 0; i < tupDesc->natts; i++)
	{
		VectorColumn *column = (VectorColumn *) vectorSlot->tts.tts_values[i];

		if (!in->tts_isnull[i])
		{
			column->isnull[column->dimension] = false;

			if (column->columnIsVal)
			{
				int8 *writeColumnRowPosition = (int8 *) column->value + column->columnTypeLen * index;

				store_att_byval(writeColumnRowPosition, in->tts_values[i], column->columnTypeLen);
			}
			else
			{
				Pointer val = DatumGetPointer(in->tts_values[i]);

				Size data_length = VARSIZE_ANY(val);

				Datum *varLenTypeContainer = NULL;

				varLenTypeContainer = palloc0(sizeof(int8) * data_length);
				memcpy(varLenTypeContainer, val, data_length);

				*(Datum *) ((int8 *) column->value + column->columnTypeLen * index) = 
					PointerGetDatum(varLenTypeContainer);
			}
		}

		column->dimension++;
	}
}

void
CleanupVectorSlot(VectorTupleTableSlot *vectorSlot)
{
	TupleDesc tupDesc = vectorSlot->tts.tts_tupleDescriptor;

	int i;

	for (i = 0; i < tupDesc->natts; i++)
	{
		VectorColumn *column = (VectorColumn *) vectorSlot->tts.tts_values[i];
		memset(column->isnull, true, COLUMNAR_VECTOR_COLUMN_SIZE);
		column->dimension = 0;
	}
	
	memset(vectorSlot->keep, true, COLUMNAR_VECTOR_COLUMN_SIZE);
	vectorSlot->dimension = 0;
}
