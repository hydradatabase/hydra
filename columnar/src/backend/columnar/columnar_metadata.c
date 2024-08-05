/*-------------------------------------------------------------------------
 *
 * columnar_metadata.c
 *
 * Copyright (c) Citus Data, Inc.
 * Copyright (c) Hydra, Inc.
 *
 * Manages metadata for columnar relations in separate, shared metadata tables
 * in the "columnar" schema.
 *
 *   * holds basic stripe information including data size and row counts
 *   * holds basic chunk and chunk group information like data offsets and
 *     min/max values (used for Chunk Group Filtering)
 *   * useful for fast VACUUM operations (e.g. reporting with VACUUM VERBOSE)
 *   * useful for stats/costing
 *   * maps logical row numbers to stripe IDs
 *   * TODO: visibility information
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "citus_version.h"
#include "columnar/columnar.h"
#include "columnar/columnar_metadata.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_version_compat.h"
#include "columnar/utils/listutils.h"

#include <sys/stat.h>
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "lib/stringinfo.h"
#if PG_VERSION_NUM >= PG_VERSION_16
#include "parser/parse_relation.h"
#endif
#include "port.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#if PG_VERSION_NUM >= PG_VERSION_16
#include "storage/relfilelocator.h"
#include "utils/relfilenumbermap.h"
#else
#include "utils/relfilenodemap.h"
#endif

typedef struct
{
	Relation rel;
	EState *estate;
	ResultRelInfo *resultRelInfo;
} ModifyState;

/* RowNumberLookupMode to be used in StripeMetadataLookupRowNumber */
typedef enum RowNumberLookupMode
{
	/*
	 * Find the stripe whose firstRowNumber is less than or equal to given
	 * input rowNumber.
	 */
	FIND_LESS_OR_EQUAL,

	/*
	 * Find the stripe whose firstRowNumber is greater than input rowNumber.
	 */
	FIND_GREATER
} RowNumberLookupMode;

static void InsertEmptyStripeMetadataRow(uint64 storageId, uint64 stripeId,
										 uint32 columnCount, uint32 chunkGroupRowCount,
										 uint64 firstRowNumber);
static void GetHighestUsedAddressAndId(uint64 storageId,
									   uint64 *highestUsedAddress,
									   uint64 *highestUsedId);
static StripeMetadata * UpdateStripeMetadataRow(uint64 storageId, uint64 stripeId,
												bool *update, Datum *newValues);
static List * ReadDataFileStripeList(uint64 storageId, Snapshot snapshot,
									 ScanDirection scanDirection);
static StripeMetadata * BuildStripeMetadata(Relation columnarStripes,
											HeapTuple heapTuple);
static void ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe,
									uint32 chunkGroupCount,
									uint32 **chunkGroupRowCounts,
									uint32 **chunkGroupDeletedRows,
									Snapshot snapshot);
static Oid ColumnarStorageIdSequenceRelationId(void);
static Oid ColumnarStripeRelationId(void);
static Oid ColumnarStripePKeyIndexRelationId(void);
static Oid ColumnarStripeFirstRowNumberIndexRelationId(void);
static Oid ColumnarOptionsRelationId(void);
static Oid ColumnarOptionsIndexRegclass(void);
static Oid ColumnarChunkRelationId(void);
static Oid ColumnarChunkGroupRelationId(void);
static Oid ColumnarRowMaskRelationId(void);
static Oid ColumnarRowMaskSeqId(void);
static Oid ColumnarChunkIndexRelationId(void);
static Oid ColumnarChunkGroupIndexRelationId(void);
static Oid ColumnarRowMaskIndexRelationId(void);
static Oid ColumnarRowMaskStripeIndexRelationId(void);
static Oid ColumnarNamespaceId(void);
static uint64 GetHighestUsedRowNumber(uint64 storageId);
static void DeleteStorageFromColumnarMetadataTable(Oid metadataTableId,
												   AttrNumber storageIdAtrrNumber,
												   Oid storageIdIndexId,
												   uint64 storageId);
static void DeleteStripeFromColumnarMetadataTable(Oid metadataTableId,
												  AttrNumber storageIdAtrrNumber,
												  AttrNumber stripeIdAttrNumber,
												  Oid storageIdIndexId,
												  uint64 storageId,
												  uint64 stripeId);
static ModifyState * StartModifyRelation(Relation rel);
static void InsertTupleAndEnforceConstraints(ModifyState *state, Datum *values,
											 bool *nulls);
static void DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple);
static void FinishModifyRelation(ModifyState *state);
static bytea * DatumToBytea(Datum value, Form_pg_attribute attrForm);
static Datum ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm);
static bool WriteColumnarOptions(Oid regclass, ColumnarOptions *options, bool overwrite);
static StripeMetadata * StripeMetadataLookupRowNumber(Relation relation, uint64 rowNumber,
													  Snapshot snapshot,
													  RowNumberLookupMode lookupMode);
static void CheckStripeMetadataConsistency(StripeMetadata *stripeMetadata);

PG_FUNCTION_INFO_V1(columnar_relation_storageid);
PG_FUNCTION_INFO_V1(create_table_row_mask);

/* constants for columnar.options */
#define Natts_columnar_options 5
#define Anum_columnar_options_regclass 1
#define Anum_columnar_options_chunk_group_row_limit 2
#define Anum_columnar_options_stripe_row_limit 3
#define Anum_columnar_options_compression_level 4
#define Anum_columnar_options_compression 5

/* ----------------
 *		columnar.options definition.
 * ----------------
 */
typedef struct FormData_columnar_options
{
	Oid regclass;
	int32 chunk_group_row_limit;
	int32 stripe_row_limit;
	int32 compressionLevel;
	NameData compression;

#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_columnar_options;
typedef FormData_columnar_options *Form_columnar_options;


/* constants for columnar.stripe */
#define Natts_columnar_stripe 9
#define Anum_columnar_stripe_storageid 1
#define Anum_columnar_stripe_stripe 2
#define Anum_columnar_stripe_file_offset 3
#define Anum_columnar_stripe_data_length 4
#define Anum_columnar_stripe_column_count 5
#define Anum_columnar_stripe_chunk_row_count 6
#define Anum_columnar_stripe_row_count 7
#define Anum_columnar_stripe_chunk_count 8
#define Anum_columnar_stripe_first_row_number 9

/* constants for columnar.chunk_group */
#define Natts_columnar_chunkgroup 5
#define Anum_columnar_chunkgroup_storageid 1
#define Anum_columnar_chunkgroup_stripe 2
#define Anum_columnar_chunkgroup_chunk 3
#define Anum_columnar_chunkgroup_row_count 4
#define Anum_columnar_chunkgroup_deleted_rows 5

/* constants for columnar.chunk */
#define Natts_columnar_chunk 14
#define Anum_columnar_chunk_storageid 1
#define Anum_columnar_chunk_stripe 2
#define Anum_columnar_chunk_attr 3
#define Anum_columnar_chunk_chunk 4
#define Anum_columnar_chunk_minimum_value 5
#define Anum_columnar_chunk_maximum_value 6
#define Anum_columnar_chunk_value_stream_offset 7
#define Anum_columnar_chunk_value_stream_length 8
#define Anum_columnar_chunk_exists_stream_offset 9
#define Anum_columnar_chunk_exists_stream_length 10
#define Anum_columnar_chunk_value_compression_type 11
#define Anum_columnar_chunk_value_compression_level 12
#define Anum_columnar_chunk_value_decompressed_size 13
#define Anum_columnar_chunk_value_count 14

/* constants for columnar.row_mask */
#define Natts_columnar_row_mask 8
#define Anum_columnar_row_mask_id 1
#define Anum_columnar_row_mask_storage_id 2
#define Anum_columnar_row_mask_stripe_id 3
#define Anum_columnar_row_mask_chunk_id 4
#define Anum_columnar_row_mask_start_row_number 5
#define Anum_columnar_row_mask_end_row_number 6
#define Anum_columnar_row_mask_deleted_rows 7
#define Anum_columnar_row_mask_mask 8


/*
 * InitColumnarOptions initialized the columnar table options. Meaning it writes the
 * default options to the options table if not already existing.
 */
void
InitColumnarOptions(Oid regclass)
{
	/*
	 * When upgrading we retain options for all columnar tables by upgrading
	 * "columnar.options" catalog table, so we shouldn't do anything here.
	 */
	if (IsBinaryUpgrade)
	{
		return;
	}

	ColumnarOptions defaultOptions = {
		.chunkRowCount = columnar_chunk_group_row_limit,
		.stripeRowCount = columnar_stripe_row_limit,
		.compressionType = columnar_compression,
		.compressionLevel = columnar_compression_level
	};

	WriteColumnarOptions(regclass, &defaultOptions, false);
}


/*
 * SetColumnarOptions writes the passed table options as the authoritive options to the
 * table irregardless of the optiones already existing or not. This can be used to put a
 * table in a certain state.
 */
void
SetColumnarOptions(Oid regclass, ColumnarOptions *options)
{
	WriteColumnarOptions(regclass, options, true);
}


/*
 * WriteColumnarOptions writes the options to the catalog table for a given regclass.
 *  - If overwrite is false it will only write the values if there is not already a record
 *    found.
 *  - If overwrite is true it will always write the settings
 *
 * The return value indicates if the record has been written.
 */
static bool
WriteColumnarOptions(Oid regclass, ColumnarOptions *options, bool overwrite)
{
	/*
	 * When upgrading we should retain the options from the previous
	 * cluster and don't write new options.
	 */
	Assert(!IsBinaryUpgrade);

	bool written = false;

	bool nulls[Natts_columnar_options] = { 0 };
	Datum values[Natts_columnar_options] = {
		ObjectIdGetDatum(regclass),
		Int32GetDatum(options->chunkRowCount),
		Int32GetDatum(options->stripeRowCount),
		Int32GetDatum(options->compressionLevel),
		0, /* to be filled below */
	};

	NameData compressionName = { 0 };
	namestrcpy(&compressionName, CompressionTypeStr(options->compressionType));
	values[Anum_columnar_options_compression - 1] = NameGetDatum(&compressionName);

	/* create heap tuple and insert into catalog table */
	Relation columnarOptions = relation_open(ColumnarOptionsRelationId(),
											 RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(columnarOptions);

	/* find existing item to perform update if exist */
	ScanKeyData scanKey[1] = { 0 };
	ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(regclass));

	Relation index = index_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		if (overwrite)
		{
			/* TODO check if the options are actually different, skip if not changed */
			/* update existing record */
			bool update[Natts_columnar_options] = { 0 };
			update[Anum_columnar_options_chunk_group_row_limit - 1] = true;
			update[Anum_columnar_options_stripe_row_limit - 1] = true;
			update[Anum_columnar_options_compression_level - 1] = true;
			update[Anum_columnar_options_compression - 1] = true;

			HeapTuple tuple = heap_modify_tuple(heapTuple, tupleDescriptor,
												values, nulls, update);
			CatalogTupleUpdate(columnarOptions, &tuple->t_self, tuple);
			written = true;
		}
	}
	else
	{
		/* inserting new record */
		HeapTuple newTuple = heap_form_tuple(tupleDescriptor, values, nulls);
		CatalogTupleInsert(columnarOptions, newTuple);
		written = true;
	}

	if (written)
	{
		CommandCounterIncrement();
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	relation_close(columnarOptions, RowExclusiveLock);

	return written;
}


/*
 * DeleteColumnarTableOptions removes the columnar table options for a regclass. When
 * missingOk is false it will throw an error when no table options can be found.
 *
 * Returns whether a record has been removed.
 */
bool
DeleteColumnarTableOptions(Oid regclass, bool missingOk)
{
	bool result = false;

	/*
	 * When upgrading we shouldn't delete or modify table options and
	 * retain options from the previous cluster.
	 */
	Assert(!IsBinaryUpgrade);

	Relation columnarOptions = try_relation_open(ColumnarOptionsRelationId(),
												 RowExclusiveLock);
	if (columnarOptions == NULL)
	{
		/* extension has been dropped */
		return false;
	}

	/* find existing item to remove */
	ScanKeyData scanKey[1] = { 0 };
	ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(regclass));

	Relation index = index_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		CatalogTupleDelete(columnarOptions, &heapTuple->t_self);
		CommandCounterIncrement();

		result = true;
	}
	else if (!missingOk)
	{
		ereport(ERROR, (errmsg("missing options for regclass: %d", regclass)));
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	relation_close(columnarOptions, RowExclusiveLock);

	return result;
}


bool
ReadColumnarOptions(Oid regclass, ColumnarOptions *options)
{
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(regclass));

	Oid columnarOptionsOid = ColumnarOptionsRelationId();
	Relation columnarOptions = try_relation_open(columnarOptionsOid, AccessShareLock);
	if (columnarOptions == NULL)
	{
		/*
		 * Extension has been dropped. This can be called while
		 * dropping extension or database via ObjectAccess().
		 */
		return false;
	}

	Relation index = try_relation_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
	if (index == NULL)
	{
		table_close(columnarOptions, AccessShareLock);

		/* extension has been dropped */
		return false;
	}

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
															1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		Form_columnar_options tupOptions = (Form_columnar_options) GETSTRUCT(heapTuple);

		options->chunkRowCount = tupOptions->chunk_group_row_limit;
		options->stripeRowCount = tupOptions->stripe_row_limit;
		options->compressionLevel = tupOptions->compressionLevel;
		options->compressionType = ParseCompressionType(NameStr(tupOptions->compression));
	}
	else
	{
		/* populate options with system defaults */
		options->compressionType = columnar_compression;
		options->stripeRowCount = columnar_stripe_row_limit;
		options->chunkRowCount = columnar_chunk_group_row_limit;
		options->compressionLevel = columnar_compression_level;
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	relation_close(columnarOptions, AccessShareLock);

	return true;
}


/*
 * SaveStripeSkipList saves chunkList for a given stripe as rows
 * of columnar.chunk.
 */
void
SaveStripeSkipList(RelFileLocator relfilelocator, uint64 stripe, StripeSkipList *chunkList,
				   TupleDesc tupleDescriptor)
{
	uint32 columnIndex = 0;
	uint32 chunkIndex = 0;
	uint32 columnCount = chunkList->columnCount;

	uint64 storageId = LookupStorageId(relfilelocator);
	Oid columnarChunkOid = ColumnarChunkRelationId();
	Relation columnarChunk = table_open(columnarChunkOid, RowExclusiveLock);
	ModifyState *modifyState = StartModifyRelation(columnarChunk);

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		for (chunkIndex = 0; chunkIndex < chunkList->chunkCount; chunkIndex++)
		{
			ColumnChunkSkipNode *chunk =
				&chunkList->chunkSkipNodeArray[columnIndex][chunkIndex];

			Datum values[Natts_columnar_chunk] = {
				UInt64GetDatum(storageId),
				Int64GetDatum(stripe),
				Int32GetDatum(columnIndex + 1),
				Int32GetDatum(chunkIndex),
				0, /* to be filled below */
				0, /* to be filled below */
				Int64GetDatum(chunk->valueChunkOffset),
				Int64GetDatum(chunk->valueLength),
				Int64GetDatum(chunk->existsChunkOffset),
				Int64GetDatum(chunk->existsLength),
				Int32GetDatum(chunk->valueCompressionType),
				Int32GetDatum(chunk->valueCompressionLevel),
				Int64GetDatum(chunk->decompressedValueSize),
				Int64GetDatum(chunk->rowCount)
			};

			bool nulls[Natts_columnar_chunk] = { false };

			if (chunk->hasMinMax)
			{
				values[Anum_columnar_chunk_minimum_value - 1] =
					PointerGetDatum(DatumToBytea(chunk->minimumValue,
												 &tupleDescriptor->attrs[columnIndex]));
				values[Anum_columnar_chunk_maximum_value - 1] =
					PointerGetDatum(DatumToBytea(chunk->maximumValue,
												 &tupleDescriptor->attrs[columnIndex]));
			}
			else
			{
				nulls[Anum_columnar_chunk_minimum_value - 1] = true;
				nulls[Anum_columnar_chunk_maximum_value - 1] = true;
			}

			InsertTupleAndEnforceConstraints(modifyState, values, nulls);
		}
	}

	FinishModifyRelation(modifyState);
	table_close(columnarChunk, RowExclusiveLock);
}


/*
 * SaveChunkGroups saves the metadata for given chunk groups in columnar.chunk_group.
 */
void
SaveChunkGroups(RelFileLocator relfilelocator, uint64 stripe,
				List *chunkGroupRowCounts)
{
	uint64 storageId = LookupStorageId(relfilelocator);
	Oid columnarChunkGroupOid = ColumnarChunkGroupRelationId();
	Relation columnarChunkGroup = table_open(columnarChunkGroupOid, RowExclusiveLock);
	ModifyState *modifyState = StartModifyRelation(columnarChunkGroup);

	ListCell *lc = NULL;
	int chunkId = 0;

	foreach(lc, chunkGroupRowCounts)
	{
		int64 rowCount = lfirst_int(lc);
		Datum values[Natts_columnar_chunkgroup] = {
			UInt64GetDatum(storageId),
			Int64GetDatum(stripe),
			Int32GetDatum(chunkId),
			Int64GetDatum(rowCount),
			Int32GetDatum(0)
		};

		bool nulls[Natts_columnar_chunkgroup] = { false };

		InsertTupleAndEnforceConstraints(modifyState, values, nulls);
		chunkId++;
	}

	FinishModifyRelation(modifyState);
	table_close(columnarChunkGroup, RowExclusiveLock);
}


/*
 * SaveEmptyRowMask saves the metadata for inserted rows in columnar.mask_row
 */
extern bool 
SaveEmptyRowMask(uint64 storageId, uint64 stripeId,
				 uint64 stripeStartRowNumber, List *chunkGroupRowCounts)
{
	Oid columnarRowMaskOid = ColumnarRowMaskRelationId();
	Oid columnarRowMaskSeq = ColumnarRowMaskSeqId();
	Relation columnarRowMask = table_open(columnarRowMaskOid, RowExclusiveLock);
	ModifyState *modifyState = StartModifyRelation(columnarRowMask);

	uint64 chunkIterationStartRowNumber = stripeStartRowNumber;
	uint64 chunkIterationEndRowNumber = stripeStartRowNumber -  1;

	ListCell *lc = NULL;
	int chunkId = 0;

	bool chunkInserted = true;

	foreach(lc, chunkGroupRowCounts)
	{
		if (!chunkInserted)
			break;

		int64 rowCount = lfirst_int(lc);

		uint16 rowMaskIterations = 
			(rowCount % COLUMNAR_ROW_MASK_CHUNK_SIZE) ? 
				(rowCount / COLUMNAR_ROW_MASK_CHUNK_SIZE + 1) :
				(rowCount / COLUMNAR_ROW_MASK_CHUNK_SIZE);

		for(uint16 n = 0; n < rowMaskIterations; n++)
		{
			uint16 maskSize;

			/* Last iteration */
			if (n == (rowMaskIterations - 1))
			{
				uint16 lastIterationRowSize = rowCount % COLUMNAR_ROW_MASK_CHUNK_SIZE ? 
						rowCount % COLUMNAR_ROW_MASK_CHUNK_SIZE : COLUMNAR_ROW_MASK_CHUNK_SIZE;
				chunkIterationEndRowNumber += lastIterationRowSize;
				maskSize = (lastIterationRowSize) % 8 ? 
								lastIterationRowSize / 8 + 1 :
								lastIterationRowSize / 8;
			}
			else
			{
				chunkIterationEndRowNumber += COLUMNAR_ROW_MASK_CHUNK_SIZE;
				maskSize = COLUMNAR_ROW_MASK_CHUNK_SIZE / 8;
			}

			bytea *initialLookupRecord = (bytea *) palloc0(maskSize + VARHDRSZ);
			SET_VARSIZE(initialLookupRecord, maskSize + VARHDRSZ);

			int64 nextSeqId = nextval_internal(columnarRowMaskSeq, false);

			Datum values[Natts_columnar_row_mask] = {
				Int64GetDatum(nextSeqId),
				Int64GetDatum(storageId),
				Int64GetDatum(stripeId),
				Int32GetDatum(chunkId),
				Int64GetDatum(chunkIterationStartRowNumber),
				Int64GetDatum(chunkIterationEndRowNumber),
				Int32GetDatum(0), 
				0, /* to be filled below */
			};

			values[Anum_columnar_row_mask_mask - 1] =
				PointerGetDatum(initialLookupRecord);

			bool nulls[Natts_columnar_row_mask] = { false };

			/*
			 * columnar.row_mask has UNIQUE constraint which can throw
			 * error so we need to catch this and return to function caller
			 * if saving empty row mask was succesful or not.
			 */
			PG_TRY();
			{
				InsertTupleAndEnforceConstraints(modifyState, values, nulls);
			}
			
			PG_CATCH();
			{
				FlushErrorState();
				chunkInserted = false;
				break;
			}

			PG_END_TRY();

			chunkIterationStartRowNumber += COLUMNAR_ROW_MASK_CHUNK_SIZE;
		}

		chunkIterationStartRowNumber = chunkIterationEndRowNumber + 1;
		chunkId++;
	}

	FinishModifyRelation(modifyState);
	table_close(columnarRowMask, RowExclusiveLock);

	return chunkInserted;
}


/*
 * ReadStripeSkipList fetches chunk metadata for a given stripe.
 */
StripeSkipList *
ReadStripeSkipList(RelFileLocator relfilelocator, uint64 stripe, TupleDesc tupleDescriptor,
				   uint32 chunkCount, Snapshot snapshot)
{
	int32 columnIndex = 0;
	int32 chunkGroupIndex = 0;
	int32 chunkGroupRowOffsetAcc = 0;
	HeapTuple heapTuple = NULL;
	uint32 columnCount = tupleDescriptor->natts;
	ScanKeyData scanKey[2];

	uint64 storageId = LookupStorageId(relfilelocator);

	Oid columnarChunkOid = ColumnarChunkRelationId();
	Relation columnarChunk = table_open(columnarChunkOid, AccessShareLock);
	Relation index = index_open(ColumnarChunkIndexRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_columnar_chunk_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunk_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(stripe));

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarChunk, index,
															snapshot, 2, scanKey);

	StripeSkipList *chunkList = palloc0(sizeof(StripeSkipList));
	chunkList->chunkCount = chunkCount;
	chunkList->columnCount = columnCount;
	chunkList->chunkSkipNodeArray = palloc0(columnCount * sizeof(ColumnChunkSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		chunkList->chunkSkipNodeArray[columnIndex] =
			palloc0(chunkCount * sizeof(ColumnChunkSkipNode));
	}

	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 ForwardScanDirection)))
	{
		Datum datumArray[Natts_columnar_chunk];
		bool isNullArray[Natts_columnar_chunk];

		heap_deform_tuple(heapTuple, RelationGetDescr(columnarChunk), datumArray,
						  isNullArray);

		int32 attr = DatumGetInt32(datumArray[Anum_columnar_chunk_attr - 1]);
		int32 chunkIndex = DatumGetInt32(datumArray[Anum_columnar_chunk_chunk - 1]);

		if (attr <= 0 || attr > columnCount)
		{
			ereport(ERROR, (errmsg("invalid columnar chunk entry"),
							errdetail("Attribute number out of range: %d", attr)));
		}

		if (chunkIndex < 0 || chunkIndex >= chunkCount)
		{
			ereport(ERROR, (errmsg("invalid columnar chunk entry"),
							errdetail("Chunk number out of range: %d", chunkIndex)));
		}

		columnIndex = attr - 1;

		ColumnChunkSkipNode *chunk =
			&chunkList->chunkSkipNodeArray[columnIndex][chunkIndex];
		chunk->rowCount = DatumGetInt64(datumArray[Anum_columnar_chunk_value_count -
												   1]);
		chunk->valueChunkOffset =
			DatumGetInt64(datumArray[Anum_columnar_chunk_value_stream_offset - 1]);
		chunk->valueLength =
			DatumGetInt64(datumArray[Anum_columnar_chunk_value_stream_length - 1]);
		chunk->existsChunkOffset =
			DatumGetInt64(datumArray[Anum_columnar_chunk_exists_stream_offset - 1]);
		chunk->existsLength =
			DatumGetInt64(datumArray[Anum_columnar_chunk_exists_stream_length - 1]);
		chunk->valueCompressionType =
			DatumGetInt32(datumArray[Anum_columnar_chunk_value_compression_type - 1]);
		chunk->valueCompressionLevel =
			DatumGetInt32(datumArray[Anum_columnar_chunk_value_compression_level - 1]);
		chunk->decompressedValueSize =
			DatumGetInt64(datumArray[Anum_columnar_chunk_value_decompressed_size - 1]);

		if (isNullArray[Anum_columnar_chunk_minimum_value - 1] ||
			isNullArray[Anum_columnar_chunk_maximum_value - 1])
		{
			chunk->hasMinMax = false;
		}
		else
		{
			bytea *minValue = DatumGetByteaP(
				datumArray[Anum_columnar_chunk_minimum_value - 1]);
			bytea *maxValue = DatumGetByteaP(
				datumArray[Anum_columnar_chunk_maximum_value - 1]);

			chunk->minimumValue =
				ByteaToDatum(minValue, &tupleDescriptor->attrs[columnIndex]);
			chunk->maximumValue =
				ByteaToDatum(maxValue, &tupleDescriptor->attrs[columnIndex]);

			chunk->hasMinMax = true;
		}
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarChunk, AccessShareLock);

	ReadChunkGroupRowCounts(storageId, stripe, chunkCount,
							&chunkList->chunkGroupRowCounts,
							&chunkList->chunkGroupDeletedRows,
							snapshot);

	chunkList->chunkGroupRowOffset = palloc0(chunkCount * sizeof(uint32));

	for (chunkGroupIndex = 0; chunkGroupIndex < chunkCount; chunkGroupIndex++)
	{
		chunkList->chunkGroupRowOffset[chunkGroupIndex] = chunkGroupRowOffsetAcc;
		chunkGroupRowOffsetAcc += chunkList->chunkGroupRowCounts[chunkGroupIndex];
	}

	return chunkList;
}


/*
 * ReadChunkRowMask fetches chunk row mask for columnar relation.
 */
bytea *
ReadChunkRowMask(RelFileLocator relfilelocator, Snapshot snapshot,
				 MemoryContext cxt,
				 uint64 stripeFirstRowNumber, int rowCount)
{
	HeapTuple heapTuple = NULL;
	ScanKeyData scanKey[3];

	uint64 storageId = LookupStorageId(relfilelocator);

	Oid columnarRowMaskOid = ColumnarRowMaskRelationId();
	Relation columnarRowMask = table_open(columnarRowMaskOid, AccessShareLock);
	Relation index = index_open(ColumnarRowMaskIndexRelationId(), AccessShareLock);

	MemoryContext oldContext = MemoryContextSwitchTo(cxt);

	uint16 chunkMaskSize = 
		(rowCount % COLUMNAR_ROW_MASK_CHUNK_SIZE) ?
			(rowCount / 8 + 1) :
			(rowCount / 8);

	bytea *chunkRowMaskBytea = (bytea *) palloc0(chunkMaskSize + VARHDRSZ);
	SET_VARSIZE(chunkRowMaskBytea, chunkMaskSize + VARHDRSZ);

	ScanKeyInit(&scanKey[0], Anum_columnar_row_mask_storage_id,
				BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_row_mask_start_row_number,
				BTGreaterEqualStrategyNumber, F_INT8GE, UInt64GetDatum(stripeFirstRowNumber));
	ScanKeyInit(&scanKey[2], Anum_columnar_row_mask_end_row_number,
				BTLessEqualStrategyNumber, F_INT8LE, UInt64GetDatum(stripeFirstRowNumber + rowCount - 1));

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarRowMask,
															index,
															SnapshotSelf, 3, scanKey);

	int pos = 0;
	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 ForwardScanDirection)))
	{
		Datum datumArray[Natts_columnar_row_mask];
		bool isNullArray[Natts_columnar_row_mask];

		heap_deform_tuple(heapTuple, RelationGetDescr(columnarRowMask), datumArray, isNullArray);
		bytea * currentRowMask = DatumGetByteaP(datumArray[Anum_columnar_row_mask_mask - 1]);

		memcpy(VARDATA(chunkRowMaskBytea) + pos,
			   VARDATA(currentRowMask),
			   VARSIZE_ANY_EXHDR(currentRowMask));

		pos += VARSIZE_ANY_EXHDR(currentRowMask);
	}

	MemoryContextSwitchTo(oldContext);

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarRowMask, AccessShareLock);

	return chunkRowMaskBytea;
}


bool
UpdateRowMask(RelFileLocator relfilelocator, uint64 storageId,
			  Snapshot snapshot, uint64 rowNumber)
{
	bytea *rowMask = NULL;

#if PG_VERSION_NUM >= PG_VERSION_16
	RowMaskWriteStateEntry *rowMaskEntry = 
		RowMaskFindWriteState(relfilelocator.relNumber, GetCurrentSubTransactionId(), rowNumber);
#else
	RowMaskWriteStateEntry *rowMaskEntry = 
		RowMaskFindWriteState(relfilelocator.relNode, GetCurrentSubTransactionId(), rowNumber);
#endif

	if (rowMaskEntry == NULL)
	{
		HeapTuple rowMaskHeapTuple;
		ScanKeyData scanKey[3];

		Oid columnarRowMaskOid = ColumnarRowMaskRelationId();
		Relation columnarRowMask = table_open(columnarRowMaskOid, AccessShareLock);
		TupleDesc tupleDescriptor = RelationGetDescr(columnarRowMask);

		Relation index = index_open(ColumnarRowMaskIndexRelationId(), AccessShareLock);

		ScanKeyInit(&scanKey[0], Anum_columnar_row_mask_storage_id,
					BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(storageId));
		ScanKeyInit(&scanKey[1], Anum_columnar_row_mask_start_row_number,
					BTLessEqualStrategyNumber, F_INT8LE, UInt64GetDatum(rowNumber));
		ScanKeyInit(&scanKey[2], Anum_columnar_row_mask_end_row_number,
					BTGreaterEqualStrategyNumber, F_INT8GE, UInt64GetDatum(rowNumber));

		SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarRowMask,
																index,
																NULL, 3, scanKey);

		rowMaskHeapTuple = systable_getnext_ordered(scanDescriptor, BackwardScanDirection);

		if (HeapTupleIsValid(rowMaskHeapTuple))
		{
			bool isnull;

			Datum mask =
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_mask,
								tupleDescriptor, &isnull);
		
#if PG_VERSION_NUM >= PG_VERSION_16
			rowMaskEntry = RowMaskInitWriteState(relfilelocator.relNumber,
												 storageId,
												 GetCurrentSubTransactionId(),
												 DatumGetByteaP(mask));
#else
			rowMaskEntry = RowMaskInitWriteState(relfilelocator.relNode,
												 storageId,
												 GetCurrentSubTransactionId(),
												 DatumGetByteaP(mask));
#endif
			/* 
			 * Populate row mask cache with values from heap table
			 */
			rowMaskEntry->id = DatumGetUInt64(
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_id,
								tupleDescriptor, &isnull));

			rowMaskEntry->storageId = DatumGetUInt64(
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_storage_id,
								tupleDescriptor, &isnull));
			
			rowMaskEntry->stripeId = DatumGetUInt64(
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_stripe_id,
								tupleDescriptor, &isnull));
			
			rowMaskEntry->chunkId = DatumGetInt32(
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_chunk_id,
								tupleDescriptor, &isnull));

			rowMaskEntry->startRowNumber = DatumGetInt64(
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_start_row_number,
								tupleDescriptor, &isnull));

			rowMaskEntry->deletedRows = DatumGetInt32(
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_deleted_rows,
								tupleDescriptor, &isnull));

			rowMaskEntry->endRowNumber = DatumGetInt64(
					fastgetattr(rowMaskHeapTuple, Anum_columnar_row_mask_end_row_number,
								tupleDescriptor, &isnull));

			rowMask = rowMaskEntry->mask;
		}
		else
		{
			/*
			* If the heap tuple is invalid, that likely means that we have
			* encountered a speculative insert.
			*/
			systable_endscan_ordered(scanDescriptor);
			index_close(index, AccessShareLock);
			table_close(columnarRowMask, AccessShareLock);
			return false;
		}

		systable_endscan_ordered(scanDescriptor);
		index_close(index, AccessShareLock);
		table_close(columnarRowMask, AccessShareLock);
	}
	else
	{
		rowMask = rowMaskEntry->mask;
	}

	int16 rowByteMask = rowNumber - rowMaskEntry->startRowNumber;

	/* 
	 * IF we have been blocked by advisory lock for storage, maybe row
	 * was delete by some other transaction.
	 */
	if (VARDATA(rowMask)[rowByteMask / 8] & (1 << (rowByteMask % 8)))
		return false;

	VARDATA(rowMask)[rowByteMask / 8] |= 1 << (rowByteMask % 8);

	rowMaskEntry->deletedRows++;

	CommandCounterIncrement();

	return true;
}


void FlushRowMaskCache(RowMaskWriteStateEntry *rowMaskEntry)
{
	HeapTuple oldHeapTuple = NULL;

	ScanKeyData scanKey;

	Oid columnarChunkGroupMaskOid = ColumnarRowMaskRelationId();
	Relation columnarChunkGroupMask =
		table_open(columnarChunkGroupMaskOid, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(columnarChunkGroupMask);

	Relation index = index_open(ColumnarRowMaskIndexRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey, Anum_columnar_row_mask_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(rowMaskEntry->id));

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarChunkGroupMask,
															index,
															NULL, 1, &scanKey);

	oldHeapTuple = systable_getnext_ordered(scanDescriptor, BackwardScanDirection);

	index_close(index, AccessShareLock);

	if (HeapTupleIsValid(oldHeapTuple))
	{
		bool update[Natts_columnar_row_mask] = { 0 };
		bool nulls[Natts_columnar_row_mask] = { 0 };
		Datum values[Natts_columnar_row_mask] = { 0 };

		// Update deleted row count
		update[Anum_columnar_row_mask_deleted_rows - 1] = true;
		values[Anum_columnar_row_mask_deleted_rows - 1] = rowMaskEntry->deletedRows;

		// Update mask byte array
		update[Anum_columnar_row_mask_mask - 1] = true;
		values[Anum_columnar_row_mask_mask - 1] = PointerGetDatum(rowMaskEntry->mask);

		HeapTuple newHeapTuple = heap_modify_tuple(oldHeapTuple, tupleDescriptor,
												   values, nulls, update);

		CatalogTupleUpdate(columnarChunkGroupMask, &oldHeapTuple->t_self, newHeapTuple);

		heap_freetuple(newHeapTuple);
	}

	systable_endscan_ordered(scanDescriptor);
	table_close(columnarChunkGroupMask, AccessShareLock);

	CommandCounterIncrement();
}


/*
 * FindStripeByRowNumber returns StripeMetadata for the stripe whose
 * firstRowNumber is greater than given rowNumber. If no such stripe
 * exists, then returns NULL.
 */
StripeMetadata *
FindNextStripeByRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot)
{
	return StripeMetadataLookupRowNumber(relation, rowNumber, snapshot, FIND_GREATER);
}


/*
 * FindStripeByRowNumber returns StripeMetadata for the stripe that contains
 * the row with rowNumber. If no such stripe exists, then returns NULL.
 */
StripeMetadata *
FindStripeByRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot)
{
	StripeMetadata *stripeMetadata =
		FindStripeWithMatchingFirstRowNumber(relation, rowNumber, snapshot);
	if (!stripeMetadata)
	{
		return NULL;
	}

	if (rowNumber > StripeGetHighestRowNumber(stripeMetadata))
	{
		return NULL;
	}

	return stripeMetadata;
}


/*
 * FindStripeWithMatchingFirstRowNumber returns a StripeMetadata object for
 * the stripe that has the greatest firstRowNumber among the stripes whose
 * firstRowNumber is smaller than or equal to given rowNumber. If no such
 * stripe exists, then returns NULL.
 *
 * Note that this doesn't mean that found stripe certainly contains the tuple
 * with given rowNumber. This is because, it also needs to be verified if
 * highest row number that found stripe contains is greater than or equal to
 * given rowNumber. For this reason, unless that additional check is done,
 * this function is mostly useful for checking against "possible" constraint
 * violations due to concurrent writes that are not flushed by other backends
 * yet.
 */
StripeMetadata *
FindStripeWithMatchingFirstRowNumber(Relation relation, uint64 rowNumber,
									 Snapshot snapshot)
{
	return StripeMetadataLookupRowNumber(relation, rowNumber, snapshot,
										 FIND_LESS_OR_EQUAL);
}

/* FindNextStripeForParallelWorker returns next stripe that should be assigned
 * for worker. Approach here is to calculate module of stripe id with total number
 * of workers that are running for execution. 
 */

StripeMetadata * 
FindNextStripeForParallelWorker(Relation relation,
								Snapshot snapshot,
								uint64 nextStripeId,
								uint64 * nextHigherStripeId)
{
	StripeMetadata *foundStripeMetadata = NULL;

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	ScanKeyData scanKey[2];

	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));

	ScanKeyInit(&scanKey[1], Anum_columnar_stripe_stripe,
				BTGreaterEqualStrategyNumber, F_INT8GE, UInt64GetDatum(nextStripeId));

	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);

	Relation index = index_open(ColumnarStripePKeyIndexRelationId(),
								AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															snapshot, 2,
															scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		foundStripeMetadata = BuildStripeMetadata(columnarStripes, heapTuple);
		*nextHigherStripeId = foundStripeMetadata->id;
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	return foundStripeMetadata;
}


/*
 * StripeWriteState returns write state of given stripe.
 */
StripeWriteStateEnum
StripeWriteState(StripeMetadata *stripeMetadata)
{
	if (stripeMetadata->aborted)
	{
		return STRIPE_WRITE_ABORTED;
	}
	else if (stripeMetadata->rowCount > 0)
	{
		return STRIPE_WRITE_FLUSHED;
	}
	else
	{
		return STRIPE_WRITE_IN_PROGRESS;
	}
}


/*
 * StripeGetHighestRowNumber returns rowNumber of the row with highest
 * rowNumber in given stripe.
 */
uint64
StripeGetHighestRowNumber(StripeMetadata *stripeMetadata)
{
	return stripeMetadata->firstRowNumber + stripeMetadata->rowCount - 1;
}


/*
 * StripeMetadataLookupRowNumber returns StripeMetadata for the stripe whose
 * firstRowNumber is less than or equal to (FIND_LESS_OR_EQUAL), or is
 * greater than (FIND_GREATER) given rowNumber by doing backward index
 * scan on stripe_first_row_number_idx.
 * If no such stripe exists, then returns NULL.
 */
static StripeMetadata *
StripeMetadataLookupRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot,
							  RowNumberLookupMode lookupMode)
{
	Assert(lookupMode == FIND_LESS_OR_EQUAL || lookupMode == FIND_GREATER);

	StripeMetadata *foundStripeMetadata = NULL;

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(storageId));

	StrategyNumber strategyNumber = InvalidStrategy;
	RegProcedure procedure = InvalidOid;
	if (lookupMode == FIND_LESS_OR_EQUAL)
	{
		strategyNumber = BTLessEqualStrategyNumber;
		procedure = F_INT8LE;
	}
	else if (lookupMode == FIND_GREATER)
	{
		strategyNumber = BTGreaterStrategyNumber;
		procedure = F_INT8GT;
	}
	ScanKeyInit(&scanKey[1], Anum_columnar_stripe_first_row_number,
				strategyNumber, procedure, UInt64GetDatum(rowNumber));


	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);
	Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
								AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															snapshot, 2,
															scanKey);

	ScanDirection scanDirection = NoMovementScanDirection;
	if (lookupMode == FIND_LESS_OR_EQUAL)
	{
		scanDirection = BackwardScanDirection;
	}
	else if (lookupMode == FIND_GREATER)
	{
		scanDirection = ForwardScanDirection;
	}
	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, scanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		foundStripeMetadata = BuildStripeMetadata(columnarStripes, heapTuple);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	return foundStripeMetadata;
}


/*
 * CheckStripeMetadataConsistency first decides if stripe write operation for
 * given stripe is "flushed", "aborted" or "in-progress", then errors out if
 * its metadata entry contradicts with this fact.
 *
 * Checks performed here are just to catch bugs, so it is encouraged to call
 * this function whenever a StripeMetadata object is built from an heap tuple
 * of columnar.stripe. Currently, BuildStripeMetadata is the only function
 * that does this.
 */
static void
CheckStripeMetadataConsistency(StripeMetadata *stripeMetadata)
{
	bool stripeLooksInProgress =
		stripeMetadata->rowCount == 0 && stripeMetadata->chunkCount == 0 &&
		stripeMetadata->fileOffset == ColumnarInvalidLogicalOffset &&
		stripeMetadata->dataLength == 0;

	/*
	 * Even if stripe is flushed, fileOffset and dataLength might be equal
	 * to 0 for zero column tables, but those two should still be consistent
	 * with respect to each other.
	 */
	bool stripeLooksFlushed =
		stripeMetadata->rowCount > 0 && stripeMetadata->chunkCount > 0 &&
		((stripeMetadata->fileOffset != ColumnarInvalidLogicalOffset &&
		  stripeMetadata->dataLength > 0) ||
		 (stripeMetadata->fileOffset == ColumnarInvalidLogicalOffset &&
		  stripeMetadata->dataLength == 0));

	StripeWriteStateEnum stripeWriteState = StripeWriteState(stripeMetadata);
	if (stripeWriteState == STRIPE_WRITE_FLUSHED && stripeLooksFlushed)
	{
		/*
		 * If stripe was flushed to disk, then we expect stripe to store
		 * at least one tuple.
		 */
		return;
	}
	else if (stripeWriteState == STRIPE_WRITE_IN_PROGRESS && stripeLooksInProgress)
	{
		/*
		 * If stripe was not flushed to disk, then values of given four
		 * fields should match the columns inserted by
		 * InsertEmptyStripeMetadataRow.
		 */
		return;
	}
	else if (stripeWriteState == STRIPE_WRITE_ABORTED && (stripeLooksInProgress ||
														  stripeLooksFlushed))
	{
		/*
		 * Stripe metadata entry for an aborted write can be complete or
		 * incomplete. We might have aborted the transaction before or after
		 * inserting into stripe metadata.
		 */
		return;
	}

	ereport(ERROR, (errmsg("unexpected stripe state, stripe metadata "
						   "entry for stripe with id=" UINT64_FORMAT
						   " is not consistent", stripeMetadata->id)));
}


/*
 * FindStripeWithHighestRowNumber returns StripeMetadata for the stripe that
 * has the row with highest rowNumber by doing backward index scan on
 * stripe_first_row_number_idx. If given relation is empty, then returns NULL.
 */
StripeMetadata *
FindStripeWithHighestRowNumber(Relation relation, Snapshot snapshot)
{
	StripeMetadata *stripeWithHighestRowNumber = NULL;

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(storageId));

	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);
	Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
								AccessShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															snapshot, 1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, BackwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		stripeWithHighestRowNumber = BuildStripeMetadata(columnarStripes, heapTuple);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	return stripeWithHighestRowNumber;
}


/*
 * ReadChunkGroupRowCounts returns an array of row counts of chunk groups and 
 * deleted rows count for each chunk group for given stripe.
 * Arrays that are updated will be allocated in this function.
 */
static void
ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe, uint32 chunkGroupCount,
						uint32 **chunkGroupRowCounts, uint32 **chunkGroupDeletedRows,
						Snapshot snapshot)
{
	Oid columnarChunkGroupOid = ColumnarChunkGroupRelationId();
	Relation columnarChunkGroup = table_open(columnarChunkGroupOid, AccessShareLock);
	Relation index = index_open(ColumnarChunkGroupIndexRelationId(), AccessShareLock);

	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_chunkgroup_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunkgroup_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(stripe));

	SysScanDesc scanDescriptor =
		systable_beginscan_ordered(columnarChunkGroup, index, snapshot, 2, scanKey);

	HeapTuple heapTuple = NULL;

	*chunkGroupRowCounts = palloc0(chunkGroupCount * sizeof(uint32));
	*chunkGroupDeletedRows = palloc0(chunkGroupCount * sizeof(uint32));

	/*
	 * Since we have now updates of `chunk_group`, there could be multiple tuples
	 * retrieved with changed only deleted_row count. We expect that last modified
	 * version will be retrieved last so it we just update information based on tuple
	 * chunk group index.
	 */
	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 ForwardScanDirection)))
	{
		if (HeapTupleHeaderIsHotUpdated(heapTuple->t_data))
			continue;

		Datum datumArray[Natts_columnar_chunkgroup];
		bool isNullArray[Natts_columnar_chunkgroup];

		heap_deform_tuple(heapTuple,
						  RelationGetDescr(columnarChunkGroup),
						  datumArray, isNullArray);

		uint32 tupleChunkGroupIndex =
			DatumGetUInt32(datumArray[Anum_columnar_chunkgroup_chunk - 1]);

		if (tupleChunkGroupIndex > chunkGroupCount)
		{
			elog(WARNING, "Tuple chunk group higher than chunk group count: %d, %d (storage_id = %ld, stripe_id = %ld)", tupleChunkGroupIndex, chunkGroupCount, UInt64GetDatum(storageId), Int64GetDatum(stripe));
			tupleChunkGroupIndex = chunkGroupCount;
		}

		(*chunkGroupRowCounts)[tupleChunkGroupIndex] =
			(uint32) DatumGetUInt64(datumArray[Anum_columnar_chunkgroup_row_count - 1]);

		(*chunkGroupDeletedRows)[tupleChunkGroupIndex] =
			(uint32) DatumGetUInt64(datumArray[Anum_columnar_chunkgroup_deleted_rows - 1]);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarChunkGroup, AccessShareLock);
}


/*
 * UpdateChunkGroupDeletedRows updates `deleted_rows` column for each chunk group.
 */
void
UpdateChunkGroupDeletedRows(uint64 storageId, uint64 stripe, 
							uint32 chunkGroupId, uint32 deletedRowNumber)
{

	HeapTuple oldHeapTuple = NULL;

	Oid columnarChunkGroupOid = ColumnarChunkGroupRelationId();
	Relation columnarChunkGroup = table_open(columnarChunkGroupOid, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(columnarChunkGroup);
	Relation index = index_open(ColumnarChunkGroupIndexRelationId(), AccessShareLock);

	ScanKeyData scanKey[3];
	ScanKeyInit(&scanKey[0], Anum_columnar_chunkgroup_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunkgroup_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(stripe));
	ScanKeyInit(&scanKey[2], Anum_columnar_chunkgroup_chunk,
				BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(chunkGroupId));

	SysScanDesc scanDescriptor =
		systable_beginscan_ordered(columnarChunkGroup, index, NULL, 3, scanKey);

	oldHeapTuple = systable_getnext_ordered(scanDescriptor, BackwardScanDirection);

	index_close(index, AccessShareLock);

	if (HeapTupleIsValid(oldHeapTuple))
	{
		bool update[Natts_columnar_chunkgroup] = { 0 };
		bool nulls[Natts_columnar_chunkgroup] = { 0 };
		Datum values[Natts_columnar_chunkgroup] = { 0 };

		// Update deleted row count
		update[Anum_columnar_chunkgroup_deleted_rows - 1] = true;
		values[Anum_columnar_chunkgroup_deleted_rows - 1] = (deletedRowNumber);

		HeapTuple newHeapTuple = heap_modify_tuple(oldHeapTuple, tupleDescriptor,
												   values, nulls, update);

		CatalogTupleUpdate(columnarChunkGroup, &oldHeapTuple->t_self, newHeapTuple);

		heap_freetuple(newHeapTuple);
	}

	systable_endscan_ordered(scanDescriptor);
	table_close(columnarChunkGroup, AccessShareLock);

	CommandCounterIncrement();
}


/*
 * InsertEmptyStripeMetadataRow adds a row to columnar.stripe for the empty
 * stripe reservation made for stripeId.
 */
static void
InsertEmptyStripeMetadataRow(uint64 storageId, uint64 stripeId, uint32 columnCount,
							 uint32 chunkGroupRowCount, uint64 firstRowNumber)
{
	bool nulls[Natts_columnar_stripe] = { false };

	Datum values[Natts_columnar_stripe] = { 0 };
	values[Anum_columnar_stripe_storageid - 1] =
		UInt64GetDatum(storageId);
	values[Anum_columnar_stripe_stripe - 1] =
		UInt64GetDatum(stripeId);
	values[Anum_columnar_stripe_column_count - 1] =
		UInt32GetDatum(columnCount);
	values[Anum_columnar_stripe_chunk_row_count - 1] =
		UInt32GetDatum(chunkGroupRowCount);
	values[Anum_columnar_stripe_first_row_number - 1] =
		UInt64GetDatum(firstRowNumber);

	/* stripe has no rows yet, so initialize rest of the columns accordingly */
	values[Anum_columnar_stripe_row_count - 1] =
		UInt64GetDatum(0);
	values[Anum_columnar_stripe_file_offset - 1] =
		UInt64GetDatum(ColumnarInvalidLogicalOffset);
	values[Anum_columnar_stripe_data_length - 1] =
		UInt64GetDatum(0);
	values[Anum_columnar_stripe_chunk_count - 1] =
		UInt32GetDatum(0);

	Oid columnarStripesOid = ColumnarStripeRelationId();
	Relation columnarStripes = table_open(columnarStripesOid, RowExclusiveLock);

	ModifyState *modifyState = StartModifyRelation(columnarStripes);

	InsertTupleAndEnforceConstraints(modifyState, values, nulls);

	FinishModifyRelation(modifyState);

	table_close(columnarStripes, RowExclusiveLock);
}


/*
 * StripesForRelfilenode returns a list of StripeMetadata for stripes
 * of the given relfilelocator.
 */
List *
StripesForRelfilenode(RelFileLocator relfilelocator, ScanDirection scanDirection)
{
	uint64 storageId = LookupStorageId(relfilelocator);

	return ReadDataFileStripeList(storageId, GetTransactionSnapshot(), scanDirection);
}


/*
 * DeletedRowsForStripe returns number of deleted rows for stripe
 * of the given relfilelocator.
 */
uint32
DeletedRowsForStripe(RelFileLocator relfilelocator, uint32 chunkCount, uint64 stripeId)
{
	uint64 storageId = LookupStorageId(relfilelocator);

	uint32 *chunkGroupRowCounts;
	uint32 *chunkGroupDeletedRows;
	int i;

	uint32 deletedRows = 0;
	
	ReadChunkGroupRowCounts(storageId, stripeId, chunkCount,
							&chunkGroupRowCounts, &chunkGroupDeletedRows,
							GetTransactionSnapshot());

	for (i = 0; i < chunkCount; i++)
	{
		deletedRows += chunkGroupDeletedRows[i];
	}

	pfree(chunkGroupRowCounts);
	pfree(chunkGroupDeletedRows);

	return deletedRows;
}

/*
 * DecompressedLengthForStripe returns total size of all decompressed rows and chunk
 * for given stripe
 */
Size
DecompressedLengthForStripe(RelFileLocator relfilelocator, uint64 stripeId)
{
	HeapTuple heapTuple = NULL;
	ScanKeyData scanKey[2];

	uint64 storageId = LookupStorageId(relfilelocator);

	Oid columnarChunkOid = ColumnarChunkRelationId();
	Relation columnarChunk = table_open(columnarChunkOid, AccessShareLock);
	Relation index = index_open(ColumnarChunkIndexRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_columnar_chunk_storageid,
				BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_chunk_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(stripeId));

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarChunk, index,
															GetTransactionSnapshot(), 2, scanKey);

	Size decompressedChunkSize = 0;

	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 ForwardScanDirection)))
	{
		Datum datumArray[Natts_columnar_chunk];
		bool isNullArray[Natts_columnar_chunk];

		heap_deform_tuple(heapTuple, RelationGetDescr(columnarChunk), datumArray,
						  isNullArray);

		decompressedChunkSize +=
			DatumGetInt64(datumArray[Anum_columnar_chunk_value_decompressed_size - 1]);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarChunk, AccessShareLock);

	return decompressedChunkSize;
}


/*
 * GetHighestUsedAddress returns the highest used address for the given
 * relfilelocator across all active and inactive transactions.
 *
 * This is used by truncate stage of VACUUM, and VACUUM can be called
 * for empty tables. So this doesn't throw errors for empty tables and
 * returns 0.
 */
uint64
GetHighestUsedAddress(RelFileLocator relfilelocator)
{
	uint64 storageId = LookupStorageId(relfilelocator);

	uint64 highestUsedAddress = 0;
	uint64 highestUsedId = 0;
	GetHighestUsedAddressAndId(storageId, &highestUsedAddress, &highestUsedId);

	return highestUsedAddress;
}


/*
 * GetHighestUsedAddressAndId returns the highest used address and id for
 * the given relfilelocator across all active and inactive transactions.
 */
static void
GetHighestUsedAddressAndId(uint64 storageId,
						   uint64 *highestUsedAddress,
						   uint64 *highestUsedId)
{
	ListCell *stripeMetadataCell = NULL;

	SnapshotData SnapshotDirty;
	InitDirtySnapshot(SnapshotDirty);

	List *stripeMetadataList = ReadDataFileStripeList(storageId, &SnapshotDirty,
													  ForwardScanDirection);

	*highestUsedId = 0;

	/* file starts with metapage */
	*highestUsedAddress = COLUMNAR_BYTES_PER_PAGE;

	foreach(stripeMetadataCell, stripeMetadataList)
	{
		StripeMetadata *stripe = lfirst(stripeMetadataCell);
		uint64 lastByte = stripe->fileOffset + stripe->dataLength - 1;
		*highestUsedAddress = Max(*highestUsedAddress, lastByte);
		*highestUsedId = Max(*highestUsedId, stripe->id);
	}
}


/*
 * ReserveEmptyStripe reserves an empty stripe for given relation
 * and inserts it into columnar.stripe. It is guaranteed that concurrent
 * writes won't overwrite the returned stripe.
 */
EmptyStripeReservation *
ReserveEmptyStripe(Relation rel, uint64 columnCount, uint64 chunkGroupRowCount,
				   uint64 stripeRowCount)
{
	EmptyStripeReservation *stripeReservation = palloc0(sizeof(EmptyStripeReservation));

	uint64 storageId = ColumnarStorageGetStorageId(rel, false);

	stripeReservation->stripeId = ColumnarStorageReserveStripeId(rel);
	stripeReservation->stripeFirstRowNumber =
		ColumnarStorageReserveRowNumber(rel, stripeRowCount);

	/*
	 * XXX: Instead of inserting a dummy entry to columnar.stripe and
	 * updating it when flushing the stripe, we could have a hash table
	 * in shared memory for the bookkeeping of ongoing writes.
	 */
	InsertEmptyStripeMetadataRow(storageId, stripeReservation->stripeId,
								 columnCount, chunkGroupRowCount,
								 stripeReservation->stripeFirstRowNumber);

	return stripeReservation;
}


/*
 * CompleteStripeReservation completes reservation of the stripe with
 * stripeId for given size and in-place updates related stripe metadata tuple
 * to complete reservation.
 */
StripeMetadata *
CompleteStripeReservation(Relation rel, uint64 stripeId, uint64 sizeBytes,
						  uint64 rowCount, uint64 chunkCount)
{
	uint64 resLogicalStart = ColumnarStorageReserveData(rel, sizeBytes);
	uint64 storageId = ColumnarStorageGetStorageId(rel, false);

	bool update[Natts_columnar_stripe] = { false };
	update[Anum_columnar_stripe_file_offset - 1] = true;
	update[Anum_columnar_stripe_data_length - 1] = true;
	update[Anum_columnar_stripe_row_count - 1] = true;
	update[Anum_columnar_stripe_chunk_count - 1] = true;

	Datum newValues[Natts_columnar_stripe] = { 0 };
	newValues[Anum_columnar_stripe_file_offset - 1] = Int64GetDatum(resLogicalStart);
	newValues[Anum_columnar_stripe_data_length - 1] = Int64GetDatum(sizeBytes);
	newValues[Anum_columnar_stripe_row_count - 1] = UInt64GetDatum(rowCount);
	newValues[Anum_columnar_stripe_chunk_count - 1] = Int32GetDatum(chunkCount);

	return UpdateStripeMetadataRow(storageId, stripeId, update, newValues);
}


/*
 * UpdateStripeMetadataRow updates stripe metadata tuple for the stripe with
 * stripeId according to given newValues and update arrays.
 * Note that this function shouldn't be used for the cases where any indexes
 * of stripe metadata should be updated according to modifications done.
 */
static StripeMetadata *
UpdateStripeMetadataRow(uint64 storageId, uint64 stripeId, bool *update,
						Datum *newValues)
{
	SnapshotData dirtySnapshot;
	InitDirtySnapshot(dirtySnapshot);

	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], Anum_columnar_stripe_stripe,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(stripeId));

	Oid columnarStripesOid = ColumnarStripeRelationId();

	Relation columnarStripes = table_open(columnarStripesOid, AccessShareLock);
	Relation columnarStripePkeyIndex = index_open(ColumnarStripePKeyIndexRelationId(),
												  AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes,
															columnarStripePkeyIndex,
															&dirtySnapshot, 2, scanKey);

	HeapTuple oldTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	if (!HeapTupleIsValid(oldTuple))
	{
		ereport(ERROR, (errmsg("attempted to modify an unexpected stripe, "
							   "columnar storage with id=" UINT64_FORMAT
							   " does not have stripe with id=" UINT64_FORMAT,
							   storageId, stripeId)));
	}

	/*
	 * heap_inplace_update already doesn't allow changing size of the original
	 * tuple, so we don't allow setting any Datum's to NULL values.
	 */
	bool newNulls[Natts_columnar_stripe] = { false };
	TupleDesc tupleDescriptor = RelationGetDescr(columnarStripes);
	HeapTuple modifiedTuple = heap_modify_tuple(oldTuple, tupleDescriptor,
												newValues, newNulls, update);

	heap_inplace_update(columnarStripes, modifiedTuple);

	/*
	 * Existing tuple now contains modifications, because we used
	 * heap_inplace_update().
	 */
	HeapTuple newTuple = oldTuple;

	/*
	 * Must not pass modifiedTuple, because BuildStripeMetadata expects a real
	 * heap tuple with MVCC fields.
	 */
	StripeMetadata *modifiedStripeMetadata = BuildStripeMetadata(columnarStripes,
																 newTuple);

	CommandCounterIncrement();

	systable_endscan_ordered(scanDescriptor);
	index_close(columnarStripePkeyIndex, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	/* return StripeMetadata object built from modified tuple */
	return modifiedStripeMetadata;
}


/*
 * ReadDataFileStripeList reads the stripe list for a given storageId
 * in the given snapshot.
 */
static List *
ReadDataFileStripeList(uint64 storageId, Snapshot snapshot, ScanDirection scanDirection)
{
	List *stripeMetadataList = NIL;
	ScanKeyData scanKey[1];
	HeapTuple heapTuple;

	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(storageId));

	Oid columnarStripesOid = ColumnarStripeRelationId();

	Relation columnarStripes = table_open(columnarStripesOid, AccessShareLock);
	Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
								AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															snapshot, 1,
															scanKey);

	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 scanDirection)))
	{
		StripeMetadata *stripeMetadata = BuildStripeMetadata(columnarStripes, heapTuple);
		stripeMetadataList = lappend(stripeMetadataList, stripeMetadata);
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);

	return stripeMetadataList;
}


/*
 * BuildStripeMetadata builds a StripeMetadata object from given heap tuple.
 *
 * NB: heapTuple must be a proper heap tuple with MVCC fields.
 */
static StripeMetadata *
BuildStripeMetadata(Relation columnarStripes, HeapTuple heapTuple)
{
	Assert(RelationGetRelid(columnarStripes) == ColumnarStripeRelationId());

	Datum datumArray[Natts_columnar_stripe];
	bool isNullArray[Natts_columnar_stripe];
	heap_deform_tuple(heapTuple, RelationGetDescr(columnarStripes),
					  datumArray, isNullArray);

	StripeMetadata *stripeMetadata = palloc0(sizeof(StripeMetadata));
	stripeMetadata->id = DatumGetInt64(datumArray[Anum_columnar_stripe_stripe - 1]);
	stripeMetadata->fileOffset = DatumGetInt64(
		datumArray[Anum_columnar_stripe_file_offset - 1]);
	stripeMetadata->dataLength = DatumGetInt64(
		datumArray[Anum_columnar_stripe_data_length - 1]);
	stripeMetadata->columnCount = DatumGetInt32(
		datumArray[Anum_columnar_stripe_column_count - 1]);
	stripeMetadata->chunkCount = DatumGetInt32(
		datumArray[Anum_columnar_stripe_chunk_count - 1]);
	stripeMetadata->chunkGroupRowCount = DatumGetInt32(
		datumArray[Anum_columnar_stripe_chunk_row_count - 1]);
	stripeMetadata->rowCount = DatumGetInt64(
		datumArray[Anum_columnar_stripe_row_count - 1]);
	stripeMetadata->firstRowNumber = DatumGetUInt64(
		datumArray[Anum_columnar_stripe_first_row_number - 1]);

	/*
	 * If there is unflushed data in a parent transaction, then we would
	 * have already thrown an error before starting to scan the table.. If
	 * the data is from an earlier subxact that committed, then it would
	 * have been flushed already. For this reason, we don't care about
	 * subtransaction id here.
	 */
	TransactionId entryXmin = HeapTupleHeaderGetXmin(heapTuple->t_data);
	stripeMetadata->aborted = !TransactionIdIsInProgress(entryXmin) &&
							  TransactionIdDidAbort(entryXmin);
	stripeMetadata->insertedByCurrentXact =
		TransactionIdIsCurrentTransactionId(entryXmin);

	CheckStripeMetadataConsistency(stripeMetadata);

	return stripeMetadata;
}


/*
 * DeleteMetadataRows removes the rows with given relfilelocator from columnar
 * metadata tables.
 */
void
DeleteMetadataRows(RelFileLocator relfilelocator)
{
	/*
	 * During a restore for binary upgrade, metadata tables and indexes may or
	 * may not exist.
	 */
	if (IsBinaryUpgrade)
	{
		return;
	}

	uint64 storageId = LookupStorageId(relfilelocator);

	DeleteStorageFromColumnarMetadataTable(ColumnarStripeRelationId(),
										   Anum_columnar_stripe_storageid,
										   ColumnarStripePKeyIndexRelationId(),
										   storageId);
	DeleteStorageFromColumnarMetadataTable(ColumnarChunkGroupRelationId(),
										   Anum_columnar_chunkgroup_storageid,
										   ColumnarChunkGroupIndexRelationId(),
										   storageId);
	DeleteStorageFromColumnarMetadataTable(ColumnarChunkRelationId(),
										   Anum_columnar_chunk_storageid,
										   ColumnarChunkIndexRelationId(),
										   storageId);
	DeleteStorageFromColumnarMetadataTable(ColumnarRowMaskRelationId(),
										   Anum_columnar_row_mask_storage_id,
										   ColumnarRowMaskIndexRelationId(),
										   storageId);
}


/*
 * DeleteMetadataRowsForStripeId removes the rows with given relfilelocator and 
 * stripe id from columnar metadata tables.
 */
void
DeleteMetadataRowsForStripeId(RelFileLocator relfilelocator, uint64 stripeId)
{
	/*
	 * During a restore for binary upgrade, metadata tables and indexes may or
	 * may not exist.
	 */
	if (IsBinaryUpgrade)
	{
		return;
	}

	uint64 storageId = LookupStorageId(relfilelocator);

	DeleteStripeFromColumnarMetadataTable(
		ColumnarStripeRelationId(),
		Anum_columnar_stripe_storageid,
		Anum_columnar_stripe_stripe,
		ColumnarStripePKeyIndexRelationId(),
		storageId, stripeId);
	DeleteStripeFromColumnarMetadataTable(
		ColumnarChunkGroupRelationId(),
		Anum_columnar_chunkgroup_storageid,
		Anum_columnar_chunkgroup_stripe,
		ColumnarChunkGroupIndexRelationId(),
		storageId, stripeId);
	DeleteStripeFromColumnarMetadataTable(
		ColumnarChunkRelationId(),
		Anum_columnar_chunk_storageid,
		Anum_columnar_chunk_stripe,
		ColumnarChunkIndexRelationId(),
		storageId, stripeId);
	DeleteStripeFromColumnarMetadataTable(
		ColumnarRowMaskRelationId(),
		Anum_columnar_row_mask_storage_id,
		Anum_columnar_row_mask_stripe_id,
		ColumnarRowMaskStripeIndexRelationId(),
		storageId, stripeId);
}


/*
 * DeleteStorageFromColumnarMetadataTable removes the rows with given
 * storageId from given columnar metadata table.
 */
static void
DeleteStorageFromColumnarMetadataTable(Oid metadataTableId,
									   AttrNumber storageIdAtrrNumber,
									   Oid storageIdIndexId, uint64 storageId)
{
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], storageIdAtrrNumber, BTEqualStrategyNumber,
				F_INT8EQ, UInt64GetDatum(storageId));

	Relation metadataTable = try_relation_open(metadataTableId, AccessShareLock);
	if (metadataTable == NULL)
	{
		/* extension has been dropped */
		return;
	}

	Relation index = index_open(storageIdIndexId, AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(metadataTable, index, NULL,
															1, scanKey);

	ModifyState *modifyState = StartModifyRelation(metadataTable);

	HeapTuple heapTuple;
	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 ForwardScanDirection)))
	{
		DeleteTupleAndEnforceConstraints(modifyState, heapTuple);
	}

	systable_endscan_ordered(scanDescriptor);

	FinishModifyRelation(modifyState);

	index_close(index, AccessShareLock);
	table_close(metadataTable, AccessShareLock);
}


/*
 * DeleteStripeFromColumnarMetadataTable removes the rows in columnar
 * metadata table that match storageId and stripeId.
 */
static void
DeleteStripeFromColumnarMetadataTable(Oid metadataTableId,
									  AttrNumber storageIdAtrrNumber,
									  AttrNumber stripeIdAttrNumber,
									  Oid storageIdIndexId,
									  uint64 storageId,
									  uint64 stripeId)
{
	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], storageIdAtrrNumber, BTEqualStrategyNumber,
				F_INT8EQ, UInt64GetDatum(storageId));
	ScanKeyInit(&scanKey[1], stripeIdAttrNumber, BTEqualStrategyNumber,
				F_INT8EQ, UInt64GetDatum(stripeId));

	Relation metadataTable = try_relation_open(metadataTableId, RowShareLock);
	if (metadataTable == NULL)
	{
		/* extension has been dropped */
		return;
	}

	Relation index = index_open(storageIdIndexId, RowShareLock);
	SysScanDesc scanDescriptor = systable_beginscan_ordered(metadataTable, index, NULL,
															2, scanKey);

	ModifyState *modifyState = StartModifyRelation(metadataTable);

	HeapTuple heapTuple;
	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 ForwardScanDirection)))
	{
		DeleteTupleAndEnforceConstraints(modifyState, heapTuple);
	}

	systable_endscan_ordered(scanDescriptor);

	FinishModifyRelation(modifyState);

	index_close(index, RowShareLock);
	table_close(metadataTable, RowShareLock);
}


/*
 * StartModifyRelation allocates resources for modifications.
 */
static ModifyState *
StartModifyRelation(Relation rel)
{
	EState *estate = create_estate_for_relation(rel);

#if PG_VERSION_NUM >= PG_VERSION_14
	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);
#else
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
#endif

	/* ExecSimpleRelationInsert, ... require caller to open indexes */
	ExecOpenIndices(resultRelInfo, false);

	ModifyState *modifyState = palloc(sizeof(ModifyState));
	modifyState->rel = rel;
	modifyState->estate = estate;
	modifyState->resultRelInfo = resultRelInfo;

	return modifyState;
}


/*
 * InsertTupleAndEnforceConstraints inserts a tuple into a relation and makes
 * sure constraints are enforced and indexes are updated.
 */
static void
InsertTupleAndEnforceConstraints(ModifyState *state, Datum *values, bool *nulls)
{
	TupleDesc tupleDescriptor = RelationGetDescr(state->rel);
	HeapTuple tuple = heap_form_tuple(tupleDescriptor, values, nulls);

	TupleTableSlot *slot = ExecInitExtraTupleSlot(state->estate, tupleDescriptor,
												  &TTSOpsHeapTuple);

	/*
	 * The tuple has no other reference.  Therefore, we can safely set
	 * shouldFree to true, which can avoid duplicate memory allocation.
	 *
	 * When working with low-memory machines, it's important to be mindful
	 * of duplicate memory allocation for large values.  This can lead to
	 * out-of-memory errors and cause issues with the performance of the
	 * machine.
	 */
	ExecStoreHeapTuple(tuple, slot, true);

	/* use ExecSimpleRelationInsert to enforce constraints */
	ExecSimpleRelationInsert_compat(state->resultRelInfo, state->estate, slot);
}


/*
 * DeleteTupleAndEnforceConstraints deletes a tuple from a relation and
 * makes sure constraints (e.g. FK constraints) are enforced.
 */
static void
DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple)
{
	EState *estate = state->estate;
	ResultRelInfo *resultRelInfo = state->resultRelInfo;

	ItemPointer tid = &(heapTuple->t_self);
	simple_heap_delete(state->rel, tid);

	/* execute AFTER ROW DELETE Triggers to enforce constraints */
#if PG_VERSION_NUM >= PG_VERSION_15
	ExecARDeleteTriggers(estate, resultRelInfo, tid, NULL, NULL, true);
#else
	ExecARDeleteTriggers(estate, resultRelInfo, tid, NULL, NULL);
#endif
}


/*
 * FinishModifyRelation cleans up resources after modifications are done.
 */
static void
FinishModifyRelation(ModifyState *state)
{
	ExecCloseIndices(state->resultRelInfo);

	AfterTriggerEndQuery(state->estate);
#if PG_VERSION_NUM >= PG_VERSION_14
	ExecCloseResultRelations(state->estate);
	ExecCloseRangeTableRelations(state->estate);
#else
	ExecCleanUpTriggerState(state->estate);
#endif
	ExecResetTupleTable(state->estate->es_tupleTable, false);
	FreeExecutorState(state->estate);

	CommandCounterIncrement();
}


/*
 * Based on a similar function from
 * postgres/src/backend/replication/logical/worker.c.
 *
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
EState *
create_estate_for_relation(Relation rel)
{
	EState *estate = CreateExecutorState();

	RangeTblEntry *rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;
#if PG_VERSION_NUM >= PG_VERSION_16
	List *perminfos = NIL;
	addRTEPermissionInfo(&perminfos, rte);
	ExecInitRangeTable(estate, list_make1(rte), perminfos);
#else
	ExecInitRangeTable(estate, list_make1(rte));
#endif

#if PG_VERSION_NUM < PG_VERSION_14
	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
#endif

	estate->es_output_cid = GetCurrentCommandId(true);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}


/*
 * DatumToBytea serializes a datum into a bytea value.
 */
static bytea *
DatumToBytea(Datum value, Form_pg_attribute attrForm)
{
	int datumLength = att_addlength_datum(0, attrForm->attlen, value);
	bytea *result = palloc0(datumLength + VARHDRSZ);

	SET_VARSIZE(result, datumLength + VARHDRSZ);

	if (attrForm->attlen > 0)
	{
		if (attrForm->attbyval)
		{
			Datum tmp;
			store_att_byval(&tmp, value, attrForm->attlen);
			memcpy(VARDATA(result), &tmp, attrForm->attlen);
		}
		else
		{
			memcpy(VARDATA(result), DatumGetPointer(value), attrForm->attlen);
		}
	}
	else
	{
		memcpy(VARDATA(result), DatumGetPointer(value), datumLength);
	}

	return result;
}


/*
 * ByteaToDatum deserializes a value which was previously serialized using
 * DatumToBytea.
 */
static Datum
ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm)
{
	/*
	 * We copy the data so the result of this function lives even
	 * after the byteaDatum is freed.
	 */
	char *binaryDataCopy = palloc0(VARSIZE_ANY_EXHDR(bytes));
	memcpy(binaryDataCopy, /*VARSIZE_ANY_EXHDR(bytes),*/
			 VARDATA_ANY(bytes), VARSIZE_ANY_EXHDR(bytes));

	return fetch_att(binaryDataCopy, attrForm->attbyval, attrForm->attlen);
}


/*
 * ColumnarStorageIdSequenceRelationId returns relation id of columnar.stripe.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStorageIdSequenceRelationId(void)
{
	return get_relname_relid("storageid_seq", ColumnarNamespaceId());
}


/*
 * ColumnarStripeRelationId returns relation id of columnar.stripe.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripeRelationId(void)
{
	return get_relname_relid("stripe", ColumnarNamespaceId());
}


/*
 * ColumnarStripePKeyIndexRelationId returns relation id of columnar.stripe_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripePKeyIndexRelationId(void)
{
	return get_relname_relid("stripe_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarStripeFirstRowNumberIndexRelationId returns relation id of
 * columnar.stripe_first_row_number_idx.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripeFirstRowNumberIndexRelationId(void)
{
	return get_relname_relid("stripe_first_row_number_idx", ColumnarNamespaceId());
}


/*
 * ColumnarOptionsRelationId returns relation id of columnar.options.
 */
static Oid
ColumnarOptionsRelationId(void)
{
	return get_relname_relid("options", ColumnarNamespaceId());
}


/*
 * ColumnarOptionsIndexRegclass returns relation id of columnar.options_pkey.
 */
static Oid
ColumnarOptionsIndexRegclass(void)
{
	return get_relname_relid("options_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarChunkRelationId returns relation id of columnar.chunk.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkRelationId(void)
{
	return get_relname_relid("chunk", ColumnarNamespaceId());
}


/*
 * ColumnarChunkGroupRelationId returns relation id of columnar.chunk_group.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkGroupRelationId(void)
{
	return get_relname_relid("chunk_group", ColumnarNamespaceId());
}


/*
 * ColumnarRowMaskRelationId returns relation id of columnar.row_mask.
 */
static Oid
ColumnarRowMaskRelationId(void)
{
	return get_relname_relid("row_mask", ColumnarNamespaceId());
}


/*
 * ColumnarRowMaskSeqId returns relation id of columnar.row_mask_seq.
 */
static Oid
ColumnarRowMaskSeqId(void)
{
	return get_relname_relid("row_mask_seq", ColumnarNamespaceId());
}



/*
 * ColumnarChunkIndexRelationId returns relation id of columnar.chunk_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkIndexRelationId(void)
{
	return get_relname_relid("chunk_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarChunkGroupIndexRelationId returns relation id of columnar.chunk_group_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkGroupIndexRelationId(void)
{
	return get_relname_relid("chunk_group_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarRowMaskIndexRelationId returns relation id 
 * of columnar.row_mask_pkey
 */
static Oid
ColumnarRowMaskIndexRelationId(void)
{
	return get_relname_relid("row_mask_pkey", ColumnarNamespaceId());
}

/*
 * ColumnarRowMaskStripeIndexRelationId returns relation id 
 * of columnar.columnar_row_mask_stripe_unique
 */
static Oid
ColumnarRowMaskStripeIndexRelationId(void)
{
	return get_relname_relid("row_mask_stripe_unique", ColumnarNamespaceId());
}


/*
 * ColumnarNamespaceId returns namespace id of the schema we store columnar
 * related tables.
 */
static Oid
ColumnarNamespaceId(void)
{
	return get_namespace_oid("columnar", false);
}


/*
 * LookupStorageId reads storage metapage to find the storage ID for the given relfilelocator. It returns
 * false if the relation doesn't have a meta page yet.
 */
uint64
LookupStorageId(RelFileLocator relfilelocator)
{
	Oid relationId = RelidByRelfilenumber(RelationTablespace_compat(relfilelocator),
										  RelationPhysicalIdentifierNumber_compat(
											  relfilelocator));

	Relation relation = relation_open(relationId, AccessShareLock);
	uint64 storageId = ColumnarStorageGetStorageId(relation, false);
	table_close(relation, AccessShareLock);

	return storageId;
}


/*
 * ColumnarMetadataNewStorageId - create a new, unique storage id and return
 * it.
 */
uint64
ColumnarMetadataNewStorageId()
{
	return nextval_internal(ColumnarStorageIdSequenceRelationId(), false);
}


/*
 * columnar_relation_storageid returns storage id associated with the
 * given relation id, or -1 if there is no associated storage id yet.
 */
Datum
columnar_relation_storageid(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	Relation relation = relation_open(relationId, AccessShareLock);
	if (!IsColumnarTableAmTable(relationId))
	{
		elog(ERROR, "relation \"%s\" is not a columnar table",
			 RelationGetRelationName(relation));
	}

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);

	relation_close(relation, AccessShareLock);

	PG_RETURN_INT64(storageId);
}

/*
 * create_table_row_mask creates empty row mask for table
 */
Datum
create_table_row_mask(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation relation = relation_open(relationId, AccessShareLock);

	if (!IsColumnarTableAmTable(relationId))
	{
		elog(ERROR, "relation \"%s\" is not a columnar table",
			 RelationGetRelationName(relation));
	}

	uint64 storageId = ColumnarStorageGetStorageId(relation, false);

	ScanKeyData scanKey[1];
	StripeMetadata *stripeMetadata = NULL;

	ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
				BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(storageId));

	Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);
	
	Relation index = index_open(ColumnarStripePKeyIndexRelationId(),
								AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
															SnapshotSelf, 1, scanKey);

	HeapTuple heapTuple = NULL;

	bool created = true;

	while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
																 ForwardScanDirection)))
	{
		stripeMetadata = BuildStripeMetadata(columnarStripes, heapTuple);

		List *chunkGroupRowCount = NIL;

		int64 lastChunkRowCount = stripeMetadata->rowCount % stripeMetadata->chunkGroupRowCount ?
									stripeMetadata->rowCount % stripeMetadata->chunkGroupRowCount :
									stripeMetadata->chunkGroupRowCount;

		for (int i = 0; i < stripeMetadata->chunkCount - 1; i++)
		{
			chunkGroupRowCount = 
				lappend_int(chunkGroupRowCount, stripeMetadata->chunkGroupRowCount);
		}

		chunkGroupRowCount = 
				lappend_int(chunkGroupRowCount, lastChunkRowCount);

		if(!SaveEmptyRowMask(storageId, stripeMetadata->id,
							 stripeMetadata->firstRowNumber, chunkGroupRowCount))
		{
			elog(WARNING, "relation \"%s\" already has columnar.row_mask populated.",
				 RelationGetRelationName(relation));
			created = false;
			break;
		}
	}

	systable_endscan_ordered(scanDescriptor);
	index_close(index, AccessShareLock);
	table_close(columnarStripes, AccessShareLock);
	relation_close(relation, AccessShareLock);

	PG_RETURN_BOOL(created);
}

/*
 * ColumnarStorageUpdateIfNeeded - upgrade columnar storage to the current version by
 * using information from the metadata tables.
 */
void
ColumnarStorageUpdateIfNeeded(Relation rel, bool isUpgrade)
{
	if (ColumnarStorageIsCurrent(rel))
	{
		return;
	}

	/*
	 * RelationGetSmgr was added in 15, but only backported to 13.10 and 14.07
	 * leaving other versions requiring something like this.
	 */
	if (unlikely(rel->rd_smgr == NULL))
	{
	#if PG_VERSION_NUM >= PG_VERSION_16
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_locator, rel->rd_backend));
	#else
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
	#endif
	}

	BlockNumber nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	if (nblocks < 2)
	{
		ColumnarStorageInit(rel->rd_smgr, ColumnarMetadataNewStorageId());
		return;
	}

	uint64 storageId = ColumnarStorageGetStorageId(rel, true);

	uint64 highestId;
	uint64 highestOffset;
	GetHighestUsedAddressAndId(storageId, &highestOffset, &highestId);

	uint64 reservedStripeId = highestId + 1;
	uint64 reservedOffset = highestOffset + 1;
	uint64 reservedRowNumber = GetHighestUsedRowNumber(storageId) + 1;
	ColumnarStorageUpdateCurrent(rel, isUpgrade, reservedStripeId,
								 reservedRowNumber, reservedOffset);
}


/*
 * GetHighestUsedRowNumber returns the highest used rowNumber for given
 * storageId. Returns COLUMNAR_INVALID_ROW_NUMBER if storage with
 * storageId has no stripes.
 * Note that normally we would use ColumnarStorageGetReservedRowNumber
 * to decide that. However, this function is designed to be used when
 * building the metapage itself during upgrades.
 */
static uint64
GetHighestUsedRowNumber(uint64 storageId)
{
	uint64 highestRowNumber = COLUMNAR_INVALID_ROW_NUMBER;

	List *stripeMetadataList = ReadDataFileStripeList(storageId,
													  GetTransactionSnapshot(),
													  ForwardScanDirection);
	StripeMetadata *stripeMetadata = NULL;
	foreach_ptr(stripeMetadata, stripeMetadataList)
	{
		highestRowNumber = Max(highestRowNumber,
							   StripeGetHighestRowNumber(stripeMetadata));
	}

	return highestRowNumber;
}

/*
 * RewriteMetadataRowWithNewValues rewrites a given metadata entry
 * for a storageId and a stripeId in place with a new offset,
 * rowCount, sizeBytes, and chunkCount.
 *
 * This is used in the vacuum UDF to fill any existing holes
 * if possible.
 */
StripeMetadata *
RewriteStripeMetadataRowWithNewValues(Relation rel, uint64 stripeId,
              uint64 sizeBytes, uint64 fileOffset, uint64 rowCount, uint64 chunkCount)
{
	uint64 storageId = ColumnarStorageGetStorageId(rel, false);

	bool update[Natts_columnar_stripe] = { false };
	update[Anum_columnar_stripe_file_offset - 1] = true;
	update[Anum_columnar_stripe_data_length - 1] = true;
	update[Anum_columnar_stripe_row_count - 1] = true;
	update[Anum_columnar_stripe_chunk_count - 1] = true;

	Datum newValues[Natts_columnar_stripe] = { 0 };
	newValues[Anum_columnar_stripe_file_offset - 1] = Int64GetDatum(fileOffset);
	newValues[Anum_columnar_stripe_data_length - 1] = Int64GetDatum(sizeBytes);
	newValues[Anum_columnar_stripe_row_count - 1] = UInt64GetDatum(rowCount);
	newValues[Anum_columnar_stripe_chunk_count - 1] = Int32GetDatum(chunkCount);

	return UpdateStripeMetadataRow(storageId, stripeId, update, newValues);
}
