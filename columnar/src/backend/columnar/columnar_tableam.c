#include "citus_version.h"

#include "postgres.h"
#include "fmgr.h"

#include <math.h>

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "storage/lockdefs.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
#if PG_VERSION_NUM >= 130000
#include "access/detoast.h"
#else
#include "access/tuptoaster.h"
#endif
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "safe_lib.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "columnar/columnar.h"
#include "columnar/columnar_customscan.h"
#include "columnar/columnar_metadata.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_version_compat.h"
#include "columnar/utils/listutils.h"

#include "columnar/vectorization/columnar_vector_types.h"
#include "columnar/columnar_metadata.h"

/*
 * Timing parameters for truncate locking heuristics.
 *
 * These are the same values from src/backend/access/heap/vacuumlazy.c
 */
#define VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL 50       /* ms */
#define VACUUM_TRUNCATE_LOCK_TIMEOUT 4500               /* ms */

/*
 * ColumnarScanDescData is the scan state passed between beginscan(),
 * getnextslot(), rescan(), and endscan() calls.
 */
typedef struct ColumnarScanDescData
{
	TableScanDescData cs_base;
	ColumnarReadState *cs_readState;

	/*
	 * We initialize cs_readState lazily in the first getnextslot() call. We
	 * need the following for initialization. We save them in beginscan().
	 */
	MemoryContext scanContext;
	Bitmapset *attr_needed;
	List *scanQual;

	/* Parallel Scan Data */
	ParallelColumnarScan parallelColumnarScan;

	/* Vectorization */
	bool returnVectorizedTuple;
} ColumnarScanDescData;


/*
 * IndexFetchColumnarData is the scan state passed between index_fetch_begin,
 * index_fetch_reset, index_fetch_end, index_fetch_tuple calls.
 */
typedef struct IndexFetchColumnarData
{
	IndexFetchTableData cs_base;
	ColumnarReadState *cs_readState;
	Bitmapset *attr_needed;
	List *stripeMetadataList;
	bool is_select_query; /* CustomIndexScan only gets planned with SELECT query */

	/*
	 * We initialize cs_readState lazily in the first columnar_index_fetch_tuple
	 * call. However, we want to do memory allocations in a sub MemoryContext of
	 * columnar_index_fetch_begin. For this reason, we store scanContext in
	 * columnar_index_fetch_begin.
	 */
	MemoryContext scanContext;
} IndexFetchColumnarData;

/* available to other extensions using find_rendezvous_variable() */
static ColumnarTableSetOptions_hook_type ColumnarTableSetOptions_hook = NULL;

static object_access_hook_type PrevObjectAccessHook = NULL;
static ProcessUtility_hook_type PrevProcessUtilityHook = NULL;

/* forward declaration for static functions */
static MemoryContext CreateColumnarScanMemoryContext(void);
static void ColumnarTableDropHook(Oid tgid);
static void ColumnarTriggerCreateHook(Oid tgid);
static void ColumnarTableAMObjectAccessHook(ObjectAccessType access, Oid classId,
											Oid objectId, int subId,
											void *arg);
static void ColumnarProcessUtility(PlannedStmt *pstmt,
								   const char *queryString,
#if PG_VERSION_NUM >= PG_VERSION_14
								   bool readOnlyTree,
#endif
								   ProcessUtilityContext context,
								   ParamListInfo params,
								   struct QueryEnvironment *queryEnv,
								   DestReceiver *dest,
								   QueryCompletionCompat *completionTag);
static bool ConditionalLockRelationWithTimeout(Relation rel, LOCKMODE lockMode,
											   int timeout, int retryInterval,
											   bool acquire);
static List * NeededColumnsList(TupleDesc tupdesc, Bitmapset *attr_needed);
static void LogRelationStats(Relation rel, int elevel);
static void TruncateColumnar(Relation rel, int elevel);
static bool TruncateAndCombineColumnarStripes(Relation rel, int elevel);
static HeapTuple ColumnarSlotCopyHeapTuple(TupleTableSlot *slot);
static void ColumnarCheckLogicalReplication(Relation rel);
static Datum * detoast_values(TupleDesc tupleDesc, Datum *orig_values, bool *isnull);
static uint64 tid_to_row_number(ItemPointerData tid);
static void ErrorIfInvalidRowNumber(uint64 rowNumber);
static void ColumnarReportTotalVirtualBlocks(Relation relation, Snapshot snapshot,
											 int progressArrIndex);
static BlockNumber ColumnarGetNumberOfVirtualBlocks(Relation relation, Snapshot snapshot);
static ItemPointerData ColumnarGetHighestItemPointer(Relation relation,
													 Snapshot snapshot);
static double ColumnarReadRowsIntoIndex(TableScanDesc scan,
										Relation indexRelation,
										IndexInfo *indexInfo,
										bool progress,
										IndexBuildCallback indexCallback,
										void *indexCallbackState,
										EState *estate, ExprState *predicate);
static void ColumnarReadMissingRowsIntoIndex(TableScanDesc scan, Relation indexRelation,
											 IndexInfo *indexInfo, EState *estate,
											 ExprState *predicate,
											 ValidateIndexState *state);
static ItemPointerData TupleSortSkipSmallerItemPointers(Tuplesortstate *tupleSort,
														ItemPointer targetItemPointer);


/* Custom tuple slot ops used for columnar. Initialized in columnar_tableam_init(). */
static TupleTableSlotOps TTSOpsColumnar;

/* Previous cache enabled state. */
static bool previousCacheEnabledState = false;

static const TupleTableSlotOps *
columnar_slot_callbacks(Relation relation)
{
	return &TTSOpsColumnar;
}


static TableScanDesc
columnar_beginscan(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc parallel_scan,
				   uint32 flags)
{
	previousCacheEnabledState = columnar_enable_page_cache;

	int natts = relation->rd_att->natts;

	/* attr_needed represents 0-indexed attribute numbers */
	Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

	TableScanDesc scandesc = columnar_beginscan_extended(relation, snapshot, nkeys, key,
														 parallel_scan,
														 flags, attr_needed, NULL,
														 NULL,
														 false);

	bms_free(attr_needed);

	return scandesc;
}


TableScanDesc
columnar_beginscan_extended(Relation relation, Snapshot snapshot,
							int nkeys, ScanKey key,
							ParallelTableScanDesc parallel_scan,
							uint32 flags, Bitmapset *attr_needed, List *scanQual,
							ParallelColumnarScan parallelColumnarScan,
							bool returnVectorizedTuple)
{
	previousCacheEnabledState = columnar_enable_page_cache;
#if PG_VERSION_NUM >= PG_VERSION_16
	Oid relfilelocator = relation->rd_locator.relNumber;
#else
	Oid relfilelocator = relation->rd_node.relNode;
#endif
	/*
	 * A memory context to use for scan-wide data, including the lazily
	 * initialized read state. We assume that beginscan is called in a
	 * context that will last until end of scan.
	 */
	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	ColumnarScanDesc scan = palloc0(sizeof(ColumnarScanDescData));
	scan->cs_base.rs_rd = relation;
	scan->cs_base.rs_snapshot = snapshot;
	scan->cs_base.rs_nkeys = nkeys;
	scan->cs_base.rs_key = key;
	scan->cs_base.rs_flags = flags;
	scan->cs_base.rs_parallel = parallel_scan;

	/*
	 * We will initialize this lazily in first tuple, where we have the actual
	 * tuple descriptor to use for reading. In some cases like ALTER TABLE ...
	 * ALTER COLUMN ... TYPE, the tuple descriptor of relation doesn't match
	 * the storage which we are reading, so we need to use the tuple descriptor
	 * of "slot" in first read.
	 */
	scan->cs_readState = NULL;
	scan->attr_needed = bms_copy(attr_needed);
	scan->scanQual = copyObject(scanQual);
	scan->scanContext = scanContext;

	/* Parallel execution scan data */;
	scan->parallelColumnarScan = parallelColumnarScan;

	/* Vectorized result */
	scan->returnVectorizedTuple = returnVectorizedTuple;

	if (PendingWritesInUpperTransactions(relfilelocator, GetCurrentSubTransactionId()))
	{
		elog(ERROR,
			 "cannot read from table when there is unflushed data in upper transactions");
	}

	MemoryContextSwitchTo(oldContext);

	return ((TableScanDesc) scan);
}

/*
 * CreateColumnarScanMemoryContext creates a memory context to store
 * ColumnarReadStare in it.
 */
static MemoryContext
CreateColumnarScanMemoryContext(void)
{
	return AllocSetContextCreate(CurrentMemoryContext, "Columnar Scan Context",
								 ALLOCSET_DEFAULT_SIZES);
}


/*
 * init_columnar_read_state initializes a column store table read and returns the
 * state.
 */
static ColumnarReadState *
init_columnar_read_state(Relation relation, TupleDesc tupdesc, Bitmapset *attr_needed,
						 List *scanQual, MemoryContext scanContext, Snapshot snapshot,
						 bool randomAccess,
						 ParallelColumnarScan parallelColumnarScan)
{
	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	List *neededColumnList = NeededColumnsList(tupdesc, attr_needed);
	ColumnarReadState *readState = ColumnarBeginRead(relation, tupdesc, neededColumnList,
													 scanQual, scanContext, snapshot,
													 randomAccess,
													 parallelColumnarScan);

	MemoryContextSwitchTo(oldContext);

	return readState;
}


static void
columnar_endscan(TableScanDesc sscan)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;
	if (scan->cs_readState != NULL)
	{
		ColumnarEndRead(scan->cs_readState);
		scan->cs_readState = NULL;
	}

	if (scan->cs_base.rs_flags & SO_TEMP_SNAPSHOT)
	{
		UnregisterSnapshot(scan->cs_base.rs_snapshot);
	}

	/* clean up any caches. */
	if (columnar_enable_page_cache == true)
	{
		ColumnarResetCache();
	}

	MemoryContextDelete(scan->scanContext);

	columnar_enable_page_cache = previousCacheEnabledState;
}


static void
columnar_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	/* XXX: hack to pass in new quals that aren't actually scan keys */
	List *scanQual = (List *) key;

	if (scan->cs_readState != NULL)
	{
		ColumnarRescan(scan->cs_readState, scanQual);
	}
}


static bool
columnar_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	/*
	 * if this is the first row, initialize read state.
	 */
	if (scan->cs_readState == NULL)
	{
		bool randomAccess = false;
		scan->cs_readState =
			init_columnar_read_state(scan->cs_base.rs_rd, slot->tts_tupleDescriptor,
									 scan->attr_needed, scan->scanQual,
									 scan->scanContext, scan->cs_base.rs_snapshot,
									 randomAccess,
									 scan->parallelColumnarScan);
	}

	ExecClearTuple(slot);

	if (scan->returnVectorizedTuple)
	{
		VectorTupleTableSlot * vectorTTS = (VectorTupleTableSlot *) slot;

		int newVectorSize = 0;

		bool nextRowFound = ColumnarReadNextVector(scan->cs_readState,
												   vectorTTS->tts.tts_values,
												   vectorTTS->tts.tts_isnull,
												   vectorTTS->rowNumber,
												   &newVectorSize);

		if (!nextRowFound)
			return false;

		vectorTTS->dimension = newVectorSize;

		memset(vectorTTS->keep, true, newVectorSize);

		ExecStoreVirtualTuple(slot);
	}
	else
	{
		uint64 rowNumber;
		bool nextRowFound = ColumnarReadNextRow(scan->cs_readState, slot->tts_values,
												slot->tts_isnull, &rowNumber);

		if (!nextRowFound)
		{
			return false;
		}

		ExecStoreVirtualTuple(slot);
		slot->tts_tid = row_number_to_tid(rowNumber);
	}

	return true;
}


/*
 * row_number_to_tid maps given rowNumber to ItemPointerData.
 */
ItemPointerData
row_number_to_tid(uint64 rowNumber)
{
	ErrorIfInvalidRowNumber(rowNumber);

	ItemPointerData tid = { 0 };
	ItemPointerSetBlockNumber(&tid, rowNumber / VALID_ITEMPOINTER_OFFSETS);
	ItemPointerSetOffsetNumber(&tid, rowNumber % VALID_ITEMPOINTER_OFFSETS +
							   FirstOffsetNumber);
	return tid;
}


/*
 * tid_to_row_number maps given ItemPointerData to rowNumber.
 */
static uint64
tid_to_row_number(ItemPointerData tid)
{
	uint64 rowNumber = ItemPointerGetBlockNumber(&tid) * VALID_ITEMPOINTER_OFFSETS +
					   ItemPointerGetOffsetNumber(&tid) - FirstOffsetNumber;

	ErrorIfInvalidRowNumber(rowNumber);

	return rowNumber;
}


/*
 * ErrorIfInvalidRowNumber errors out if given rowNumber is invalid.
 */
static void
ErrorIfInvalidRowNumber(uint64 rowNumber)
{
	if (rowNumber == COLUMNAR_INVALID_ROW_NUMBER)
	{
		/* not expected but be on the safe side */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("unexpected row number for columnar table")));
	}
	else if (rowNumber > COLUMNAR_MAX_ROW_NUMBER)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("columnar tables can't have row numbers "
							   "greater than " UINT64_FORMAT,
							   (uint64) COLUMNAR_MAX_ROW_NUMBER),
						errhint("Consider using VACUUM FULL for your table")));
	}
}


static Size
columnar_parallelscan_estimate(Relation rel)
{
	elog(ERROR, "columnar_parallelscan_estimate not implemented");
}


static Size
columnar_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnar_parallelscan_initialize not implemented");
}


static void
columnar_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnar_parallelscan_reinitialize not implemented");
}


static IndexFetchTableData *
columnar_index_fetch_begin(Relation rel)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	Oid relfilelocator = rel->rd_locator.relNumber;
#else
	Oid relfilelocator = rel->rd_node.relNode;
#endif
	if (PendingWritesInUpperTransactions(relfilelocator, GetCurrentSubTransactionId()))
	{
		/* XXX: maybe we can just flush the data and continue */
		elog(ERROR, "cannot read from index when there is unflushed data in "
					"upper transactions");
	}

	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	IndexFetchColumnarData *scan = palloc0(sizeof(IndexFetchColumnarData));
	scan->cs_base.rel = rel;
	scan->cs_readState = NULL;
	scan->stripeMetadataList = NIL;
	scan->scanContext = scanContext;
	scan->is_select_query = false;

	MemoryContextSwitchTo(oldContext);

	return &scan->cs_base;
}

IndexFetchTableData *
columnar_index_fetch_begin_extended(Relation rel, Bitmapset *attr_needed)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	Oid relfilenode = rel->rd_locator.relNumber;
#else
	Oid relfilenode = rel->rd_node.relNode;
#endif
	if (PendingWritesInUpperTransactions(relfilenode, GetCurrentSubTransactionId()))
	{
		/* XXX: maybe we can just flush the data and continue */
		elog(ERROR, "cannot read from index when there is unflushed data in "
					"upper transactions");
	}

	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	IndexFetchColumnarData *scan = palloc0(sizeof(IndexFetchColumnarData));
	scan->cs_base.rel = rel;
	scan->cs_readState = NULL;
	scan->stripeMetadataList = NIL;
	scan->scanContext = scanContext;

	scan->attr_needed = bms_copy(attr_needed);
	scan->is_select_query = true;

	MemoryContextSwitchTo(oldContext);

	return &scan->cs_base;
}


static void
columnar_index_fetch_reset(IndexFetchTableData *sscan)
{
	/* no-op */
}


static void
columnar_index_fetch_end(IndexFetchTableData *sscan)
{
	columnar_index_fetch_reset(sscan);

	IndexFetchColumnarData *scan = (IndexFetchColumnarData *) sscan;
	if (scan->cs_readState)
	{
		ColumnarEndRead(scan->cs_readState);
		scan->cs_readState = NULL;
	}

	bms_free(scan->attr_needed);

	/* clean up any caches. */
	if (columnar_enable_page_cache == true)
	{
		ColumnarResetCache();
	}

	MemoryContextDelete(scan->scanContext);
}

static StripeMetadata *
FindStripeMetadataFromListBinarySearch(IndexFetchColumnarData *scan, uint64 rowNumber)
{
	ListCell *lc = NULL;

	int high = scan->stripeMetadataList->length - 1;
	int low = 0;

	while(low <= high)
	{
		int mid = low + (high - low) / 2;

		lc = list_nth_cell(scan->stripeMetadataList, mid);

		StripeMetadata *stripeMetadata = lc->ptr_value;

		if (rowNumber >= stripeMetadata->firstRowNumber &&
			rowNumber < stripeMetadata->firstRowNumber + stripeMetadata->rowCount)
		{
			return stripeMetadata;
		}

		if (stripeMetadata->firstRowNumber > rowNumber)
		{
			high = mid - 1;
		}
		else
		{
			low = mid + 1;
		}
	}
	return NULL;
}


static bool
columnar_index_fetch_tuple(struct IndexFetchTableData *sscan,
						   ItemPointer tid,
						   Snapshot snapshot,
						   TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	/* no HOT chains are possible in columnar, directly set it to false */
	*call_again = false;

	/*
	 * Initialize all_dead to false if passed to be non-NULL.
	 *
	 * XXX: For aborted writes, we should set all_dead to true but this would
	 * require implementing columnar_index_delete_tuples for simple deletion
	 * of dead tuples (TM_IndexDeleteOp.bottomup = false).
	 */
	if (all_dead)
	{
		*all_dead = false;
	}

	ExecClearTuple(slot);

	IndexFetchColumnarData *scan = (IndexFetchColumnarData *) sscan;
	Relation columnarRelation = scan->cs_base.rel;

	/* initialize read state for the first row */
	if (scan->cs_readState == NULL)
	{
		/* no quals for index scan */
		List *scanQual = NIL;

		if (bms_is_empty(scan->attr_needed))
		{
			/* we need all columns */
			int natts = columnarRelation->rd_att->natts;
			bms_free(scan->attr_needed);
			scan->attr_needed = bms_add_range(NULL, 0, natts - 1);
		}

		bool randomAccess = true;
		scan->cs_readState = init_columnar_read_state(columnarRelation,
													  slot->tts_tupleDescriptor,
													  scan->attr_needed, scanQual,
													  scan->scanContext,
													  snapshot, randomAccess,
													  NULL);
		if (scan->is_select_query)
#if PG_VERSION_NUM >= PG_VERSION_16
			scan->stripeMetadataList =
				StripesForRelfilenode(columnarRelation->rd_locator, ForwardScanDirection);
#else
			scan->stripeMetadataList =
				StripesForRelfilenode(columnarRelation->rd_node, ForwardScanDirection);
#endif

	}

	uint64 rowNumber = tid_to_row_number(*tid);

	StripeMetadata *stripeMetadata = NULL;
	
	if (scan->is_select_query)
		stripeMetadata = FindStripeMetadataFromListBinarySearch(scan, rowNumber);
	else
		stripeMetadata = FindStripeWithMatchingFirstRowNumber(columnarRelation, rowNumber, snapshot);
	
	if (!stripeMetadata)
	{
		/* it is certain that tuple with rowNumber doesn't exist */
		return false;
	}

	StripeWriteStateEnum stripeWriteState = StripeWriteState(stripeMetadata);

	if (stripeWriteState == STRIPE_WRITE_FLUSHED &&
		!ColumnarReadRowByRowNumber(scan->cs_readState, rowNumber,
									slot->tts_values, slot->tts_isnull))
	{
		/*
		 * FindStripeWithMatchingFirstRowNumber doesn't verify upper row
		 * number boundary of found stripe. For this reason, we didn't
		 * certainly know if given row number belongs to one of the stripes.
		 */
		return false;
	}
	else if (stripeWriteState == STRIPE_WRITE_ABORTED)
	{
		/*
		 * We only expect to see un-flushed stripes when checking against
		 * constraint violation. In that case, indexAM provides dirty
		 * snapshot to index_fetch_tuple callback.
		 */
		pfree(stripeMetadata);
		Assert(snapshot->snapshot_type == SNAPSHOT_DIRTY);
		return false;
	}
	else if (stripeWriteState == STRIPE_WRITE_IN_PROGRESS)
	{
		if (stripeMetadata->insertedByCurrentXact)
		{
			/*
			 * Stripe write is in progress and its entry is inserted by current
			 * transaction, so obviously it must be written by me. Since caller
			 * might want to use tupleslot datums for some reason, do another
			 * look-up, but this time by first flushing our writes.
			 *
			 * XXX: For index scan, this is the only case that we flush pending
			 * writes of the current backend. If we have taught reader how to
			 * read from WriteStateMap. then we could guarantee that
			 * index_fetch_tuple would never flush pending writes, but this seem
			 * to be too much work for now, but should be doable.
			 */
			ColumnarReadFlushPendingWrites(scan->cs_readState);

			/*
			 * Fill the tupleslot and fall through to return true, it
			 * certainly exists.
			 */
			ColumnarReadRowByRowNumberOrError(scan->cs_readState, rowNumber,
											  slot->tts_values, slot->tts_isnull);
		}
		else
		{
			/* similar to aborted writes, it should be dirty snapshot */
			Assert(snapshot->snapshot_type == SNAPSHOT_DIRTY);

			/*
			 * Stripe that "might" contain the tuple with rowNumber is not
			 * flushed yet. Here we set all attributes of given tupleslot to NULL
			 * before returning true and expect the indexAM callback that called
			 * us --possibly to check against constraint violation-- blocks until
			 * writer transaction commits or aborts, without requiring us to fill
			 * the tupleslot properly.
			 *
			 * XXX: Note that the assumption we made above for the tupleslot
			 * holds for "unique" constraints defined on "btree" indexes.
			 *
			 * For the other constraints that we support, namely:
			 * * exclusion on btree,
			 * * exclusion on hash,
			 * * unique on btree;
			 * we still need to fill tts_values.
			 *
			 * However, for the same reason, we should have already flushed
			 * single tuple stripes when inserting into table for those three
			 * classes of constraints.
			 *
			 * This is annoying, but this also explains why this hack works for
			 * unique constraints on btree indexes, and also explains how we
			 * would never end up with "else" condition otherwise.
			 */
			memset(slot->tts_isnull, true, slot->tts_nvalid * sizeof(bool));
		}
	}
	else
	{
		/*
		 * At this point, we certainly know that stripe is flushed and
		 * ColumnarReadRowByRowNumber successfully filled the tupleslot.
		 */
		Assert(stripeWriteState == STRIPE_WRITE_FLUSHED);
	}

	if (!scan->is_select_query)
		pfree(stripeMetadata);
	slot->tts_tableOid = RelationGetRelid(columnarRelation);
	slot->tts_tid = *tid;
	ExecStoreVirtualTuple(slot);

	return true;
}


static bool
columnar_fetch_row_version(Relation relation,
						   ItemPointer tid,
						   Snapshot snapshot,
						   TupleTableSlot *slot)
{
	uint64 rowNumber = tid_to_row_number(*tid);
	ColumnarReadState **readState =
		FindReadStateCache(relation, GetCurrentSubTransactionId());

	if (readState == NULL)
	{
		readState = InitColumnarReadStateCache(relation, GetCurrentSubTransactionId());

		int natts = relation->rd_att->natts;
		Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

		List *scanQual = NIL;

		bool randomAccess = false;

		*readState = init_columnar_read_state(relation,
											  slot->tts_tupleDescriptor,
											  attr_needed, scanQual,
											  GetColumnarReadStateCache(),
											  snapshot, randomAccess,
											  NULL);
	}

	MemoryContext oldContext = MemoryContextSwitchTo(GetColumnarReadStateCache());
	ColumnarReadRowByRowNumber(*readState, rowNumber,
							   slot->tts_values, slot->tts_isnull);
	MemoryContextSwitchTo(oldContext);

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *tid;

	if (TTS_EMPTY(slot))
		ExecStoreVirtualTuple(slot);

	return true;
}


static void
columnar_get_latest_tid(TableScanDesc sscan,
						ItemPointer tid)
{
	elog(ERROR, "columnar_get_latest_tid not implemented");
}


static bool
columnar_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "columnar_tuple_tid_valid not implemented");
}


static bool
columnar_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	uint64 rowNumber = tid_to_row_number(slot->tts_tid);
	StripeMetadata *stripeMetadata = FindStripeByRowNumber(rel, rowNumber, snapshot);
	return stripeMetadata != NULL;
}


#if PG_VERSION_NUM >= PG_VERSION_14
static TransactionId
columnar_index_delete_tuples(Relation rel,
							 TM_IndexDeleteOp *delstate)
{
	previousCacheEnabledState = columnar_enable_page_cache;
	columnar_enable_page_cache = false;

	/*
	 * XXX: We didn't bother implementing index_delete_tuple for neither of
	 * simple deletion and bottom-up deletion cases. There is no particular
	 * reason for that, just to keep things simple.
	 *
	 * See the rest of this function to see how we deal with
	 * index_delete_tuples requests made to columnarAM.
	 */

	if (delstate->bottomup)
	{
		/*
		 * Ignore any bottom-up deletion requests.
		 *
		 * Currently only caller in postgres that does bottom-up deletion is
		 * _bt_bottomupdel_pass, which in turn calls _bt_delitems_delete_check.
		 * And this function is okay with ndeltids being set to 0 by tableAM
		 * for bottom-up deletion.
		 */
		delstate->ndeltids = 0;
		return InvalidTransactionId;
	}
	else
	{
		/*
		 * TableAM is not expected to set ndeltids to 0 for simple deletion
		 * case, so here we cannot do the same trick that we do for
		 * bottom-up deletion.
		 * See the assertion around table_index_delete_tuples call in pg
		 * function index_compute_xid_horizon_for_tuples.
		 *
		 * For this reason, to avoid receiving simple deletion requests for
		 * columnar tables (bottomup = false), columnar_index_fetch_tuple
		 * doesn't ever set all_dead to true in order to prevent triggering
		 * simple deletion of index tuples. But let's throw an error to be on
		 * the safe side.
		 */
		elog(ERROR, "columnar_index_delete_tuples not implemented for simple deletion");
	}
}


#else
static TransactionId
columnar_compute_xid_horizon_for_tuples(Relation rel,
										ItemPointerData *tids,
										int nitems)
{
	elog(ERROR, "columnar_compute_xid_horizon_for_tuples not implemented");
}


#endif


static void
columnar_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					  int options, BulkInsertState bistate)
{
	previousCacheEnabledState = columnar_enable_page_cache;
	columnar_enable_page_cache = false;

	/*
	 * columnar_init_write_state allocates the write state in a longer
	 * lasting context, so no need to worry about it.
	 */
	ColumnarWriteState *writeState = columnar_init_write_state(relation,
															   RelationGetDescr(relation),
																 slot->tts_tableOid,
															   GetCurrentSubTransactionId());
	MemoryContext oldContext = MemoryContextSwitchTo(ColumnarWritePerTupleContext(
														 writeState));

	ColumnarCheckLogicalReplication(relation);

	slot_getallattrs(slot);

	Datum *values = detoast_values(slot->tts_tupleDescriptor,
								   slot->tts_values, slot->tts_isnull);

	uint64 writtenRowNumber = ColumnarWriteRow(writeState, values, slot->tts_isnull);
	slot->tts_tid = row_number_to_tid(writtenRowNumber);

	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(ColumnarWritePerTupleContext(writeState));

	pgstat_count_heap_insert(relation, 1);
}


static void
columnar_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								  CommandId cid, int options,
								  BulkInsertState bistate, uint32 specToken)
{
	previousCacheEnabledState = columnar_enable_page_cache;
	columnar_enable_page_cache = false;

	/*
	 * columnar_init_write_state allocates the write state in a longer
	 * lasting context, so no need to worry about it.
	 */
	ColumnarWriteState *writeState = columnar_init_write_state(relation,
															   RelationGetDescr(relation),
																 slot->tts_tableOid,
															   GetCurrentSubTransactionId());
	MemoryContext oldContext = MemoryContextSwitchTo(ColumnarWritePerTupleContext(
														 writeState));

	ColumnarCheckLogicalReplication(relation);

	slot_getallattrs(slot);

	Datum *values = detoast_values(slot->tts_tupleDescriptor,
								   slot->tts_values, slot->tts_isnull);

#if PG_VERSION_NUM >= PG_VERSION_16
	uint64 storageId = LookupStorageId(relation->rd_locator);
#else
	uint64 storageId = LookupStorageId(relation->rd_node);
#endif
	uint64 writtenRowNumber = ColumnarWriteRow(writeState, values, slot->tts_isnull);
#if PG_VERSION_NUM >= PG_VERSION_16
	UpdateRowMask(relation->rd_locator, storageId,  NULL, writtenRowNumber);
#else
	UpdateRowMask(relation->rd_node, storageId,  NULL, writtenRowNumber);
#endif
	slot->tts_tid = row_number_to_tid(writtenRowNumber);

	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(ColumnarWritePerTupleContext(writeState));

	pgstat_count_heap_insert(relation, 1);
}


static void
columnar_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
									uint32 specToken, bool succeeded)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	uint64 storageId = LookupStorageId(relation->rd_locator);
#else
	uint64 storageId = LookupStorageId(relation->rd_node);
#endif
	/* Set lock for relation until transaction ends */
	DirectFunctionCall1(pg_advisory_xact_lock_int8,
						Int64GetDatum((int64) storageId));

	columnar_enable_page_cache = previousCacheEnabledState;
}

static void
columnar_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					  CommandId cid, int options, BulkInsertState bistate)
{
	ColumnarWriteState *writeState = columnar_init_write_state(relation,
															   RelationGetDescr(relation),
																 slots[0]->tts_tableOid,
															   GetCurrentSubTransactionId());

	ColumnarCheckLogicalReplication(relation);

	MemoryContext oldContext = MemoryContextSwitchTo(ColumnarWritePerTupleContext(
														 writeState));

	for (int i = 0; i < ntuples; i++)
	{
		TupleTableSlot *tupleSlot = slots[i];

		slot_getallattrs(tupleSlot);

		Datum *values = detoast_values(tupleSlot->tts_tupleDescriptor,
									   tupleSlot->tts_values, tupleSlot->tts_isnull);

		uint64 writtenRowNumber = ColumnarWriteRow(writeState, values,
												   tupleSlot->tts_isnull);

		EState *estate = create_estate_for_relation(relation);

#if PG_VERSION_NUM >= PG_VERSION_14
		ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo, relation, 1, NULL, 0);
#else
		ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
#endif

		ExecOpenIndices(resultRelInfo, false);

		if (relation->rd_att->constr)
			ExecConstraints(resultRelInfo, tupleSlot, estate);

		ExecCloseIndices(resultRelInfo);

		AfterTriggerEndQuery(estate);
#if PG_VERSION_NUM >= PG_VERSION_14
		ExecCloseResultRelations(estate);
		ExecCloseRangeTableRelations(estate);
#else
		ExecCleanUpTriggerState(estate);
#endif
		ExecResetTupleTable(estate->es_tupleTable, false);
		FreeExecutorState(estate);

		tupleSlot->tts_tid = row_number_to_tid(writtenRowNumber);

		MemoryContextResetAndDeleteChildren(ColumnarWritePerTupleContext(writeState));
	}

	MemoryContextSwitchTo(oldContext);

	pgstat_count_heap_insert(relation, ntuples);
}


static TM_Result
columnar_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck, bool wait,
					  TM_FailureData *tmfd, bool changingPart)
{
	uint64 rowNumber = tid_to_row_number(*tid);
#if PG_VERSION_NUM >= PG_VERSION_16
	uint64 storageId = LookupStorageId(relation->rd_locator);
#else
	uint64 storageId = LookupStorageId(relation->rd_node);
#endif
	/* Set lock for relation until transaction ends */
	DirectFunctionCall1(pg_advisory_xact_lock_int8,
						Int64GetDatum((int64) storageId));

#if PG_VERSION_NUM >= PG_VERSION_16
	if (!UpdateRowMask(relation->rd_locator, storageId,  snapshot, rowNumber))
#else
	if (!UpdateRowMask(relation->rd_node, storageId,  snapshot, rowNumber))
#endif
		return TM_Deleted;

	pgstat_count_heap_delete(relation);

	return TM_Ok;
}


#if PG_VERSION_NUM >= PG_VERSION_16
static TM_Result
columnar_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
#else
static TM_Result
columnar_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode, bool *update_indexes)
#endif
{

	
	uint64 rowNumber = tid_to_row_number(*otid);
#if PG_VERSION_NUM >= PG_VERSION_16
	uint64 storageId = LookupStorageId(relation->rd_locator);
#else
	uint64 storageId = LookupStorageId(relation->rd_node);
#endif

	/* Set lock for relation until transaction ends */
	DirectFunctionCall1(pg_advisory_xact_lock_int8,
						Int64GetDatum((int64) storageId));

#if PG_VERSION_NUM >= PG_VERSION_16
	if (!UpdateRowMask(relation->rd_locator, storageId, snapshot, rowNumber))
#else
	if (!UpdateRowMask(relation->rd_node, storageId, snapshot, rowNumber))
#endif
		return TM_Deleted;

	columnar_tuple_insert(relation, slot, cid, 0, NULL);

	*update_indexes = true;

#if PG_VERSION_NUM >= PG_VERSION_16
	pgstat_count_heap_update(relation, false, false);
#else
	pgstat_count_heap_update(relation, false);
#endif
	return TM_Ok;
}


static TM_Result
columnar_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					TM_FailureData *tmfd)
{
	uint64 rowNumber = tid_to_row_number(*tid);
	ColumnarReadState *readState = NULL;

	int natts = relation->rd_att->natts;
	Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

	List *scanQual = NIL;

	bool randomAccess = true;

	readState = init_columnar_read_state(relation,
										slot->tts_tupleDescriptor,
										attr_needed, scanQual,
										CurrentMemoryContext, // to be checked
										GetTransactionSnapshot(), randomAccess,
										NULL);

	ColumnarReadRowByRowNumber(readState, rowNumber,
							   slot->tts_values, slot->tts_isnull);

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *tid;

	if (TTS_EMPTY(slot))
	{
		ExecStoreVirtualTuple(slot);
	}

	return TM_Ok;
}


static void
columnar_finish_bulk_insert(Relation relation, int options)
{
	/*
	 * Nothing to do here. We keep write states live until transaction end.
	 */
}


static void
columnar_relation_set_new_filenode(Relation rel,
								   const RelFileLocator *newrnode,
								   char persistence,
								   TransactionId *freezeXid,
								   MultiXactId *minmulti)
{
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unlogged columnar tables are not supported")));
	}

	/*
	 * If existing and new relfilelocator are different, that means the existing
	 * storage was dropped and we also need to clean up the metadata and
	 * state. If they are equal, this is a new relation object and we don't
	 * need to clean anything.
	 */
#if PG_VERSION_NUM >= PG_VERSION_16
	if (rel->rd_locator.relNumber != newrnode->relNumber)
	{
		MarkRelfilenodeDropped(rel->rd_locator.relNumber, GetCurrentSubTransactionId());

		DeleteMetadataRows(rel->rd_locator);
	}
#else
	if (rel->rd_node.relNode != newrnode->relNode)
	{
		MarkRelfilenodeDropped(rel->rd_node.relNode, GetCurrentSubTransactionId());

		DeleteMetadataRows(rel->rd_node);
	}
#endif
	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();
#if PG_VERSION_NUM >= PG_VERSION_15
	SMgrRelation srel = RelationCreateStorage(*newrnode, persistence, true);
#else
	SMgrRelation srel = RelationCreateStorage(*newrnode, persistence);
#endif

	ColumnarStorageInit(srel, ColumnarMetadataNewStorageId());
	InitColumnarOptions(rel->rd_id);

	smgrclose(srel);

	/* we will lazily initialize metadata in first stripe reservation */
}


static void
columnar_relation_nontransactional_truncate(Relation rel)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	RelFileLocator relfilelocator = rel->rd_locator;

	NonTransactionDropWriteState(relfilelocator.relNumber);
#else
	RelFileLocator relfilelocator = rel->rd_node;

	NonTransactionDropWriteState(relfilelocator.relNode);
#endif
	/* Delete old relfilelocator metadata */
	DeleteMetadataRows(relfilelocator);

	/*
	 * No need to set new relfilelocator, since the table was created in this
	 * transaction and no other transaction can see this relation yet. We
	 * can just truncate the relation.
	 *
	 * This is similar to what is done in heapam_relation_nontransactional_truncate.
	 */
	RelationTruncate(rel, 0);

	uint64 storageId = ColumnarMetadataNewStorageId();

	if (unlikely(rel->rd_smgr == NULL))
	{
#if PG_VERSION_NUM >= PG_VERSION_16
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_locator, rel->rd_backend));
#else
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
#endif
	}

	ColumnarStorageInit(rel->rd_smgr, storageId);
}


static void
columnar_relation_copy_data(Relation rel, const RelFileLocator *newrnode)
{
	elog(ERROR, "columnar_relation_copy_data not implemented");
}


/*
 * columnar_relation_copy_for_cluster is called on VACUUM FULL, at which
 * we should copy data from OldHeap to NewHeap.
 *
 * In general TableAM case this can also be called for the CLUSTER command
 * which is not applicable for columnar since it doesn't support indexes.
 */
static void
columnar_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								   Relation OldIndex, bool use_sort,
								   TransactionId OldestXmin,
								   TransactionId *xid_cutoff,
								   MultiXactId *multi_cutoff,
								   double *num_tuples,
								   double *tups_vacuumed,
								   double *tups_recently_dead)
{
	TupleDesc sourceDesc = RelationGetDescr(OldHeap);
	TupleDesc targetDesc = RelationGetDescr(NewHeap);

	if (OldIndex != NULL || use_sort)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("clustering columnar tables using indexes is "
							   "not supported")));
	}

	/*
	 * copy_table_data in cluster.c assumes tuple descriptors are exactly
	 * the same. Even dropped columns exist and are marked as attisdropped
	 * in the target relation.
	 */
	Assert(sourceDesc->natts == targetDesc->natts);

	/* read settings from old heap, relfilelocator will be swapped at the end */
	ColumnarOptions columnarOptions = { 0 };
	ReadColumnarOptions(OldHeap->rd_id, &columnarOptions);

#if PG_VERSION_NUM >= PG_VERSION_16
	ColumnarWriteState *writeState = ColumnarBeginWrite(NewHeap->rd_locator,
														columnarOptions,
														targetDesc);
#else
	ColumnarWriteState *writeState = ColumnarBeginWrite(NewHeap->rd_node,
														columnarOptions,
														targetDesc);
#endif
	/* we need all columns */
	int natts = OldHeap->rd_att->natts;
	Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

	/* no quals for table rewrite */
	List *scanQual = NIL;

	/* use SnapshotAny when re-writing table as heapAM does */
	Snapshot snapshot = SnapshotAny;

	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	bool randomAccess = false;
	ColumnarReadState *readState = init_columnar_read_state(OldHeap, sourceDesc,
															attr_needed, scanQual,
															scanContext, snapshot,
															randomAccess,
															NULL);

	Datum *values = palloc0(sourceDesc->natts * sizeof(Datum));
	bool *nulls = palloc0(sourceDesc->natts * sizeof(bool));

	*num_tuples = 0;

	/* we don't need to know rowNumber here */
	while (ColumnarReadNextRow(readState, values, nulls, NULL))
	{
		ColumnarWriteRow(writeState, values, nulls);
		(*num_tuples)++;
	}

	*tups_vacuumed = 0;

	ColumnarEndWrite(writeState);
	ColumnarEndRead(readState);
	MemoryContextDelete(scanContext);
}


/*
 * NeededColumnsList returns a list of AttrNumber's for the columns that
 * are not dropped and specified by attr_needed.
 */
static List *
NeededColumnsList(TupleDesc tupdesc, Bitmapset *attr_needed)
{
	List *columnList = NIL;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		if (tupdesc->attrs[i].attisdropped)
		{
			continue;
		}

		/* attr_needed is 0-indexed but columnList is 1-indexed */
		if (bms_is_member(i, attr_needed))
		{
			AttrNumber varattno = i + 1;
			columnList = lappend_int(columnList, varattno);
		}
	}

	return columnList;
}

/*
 * TruncateAndCombineColumnarStripes will combine last n stripes so they can fit
 * maximum number of rows per stripe. Stripes that are going to be combined are deleted
 * and new stripe will be written at the end of untouched last stripe. We also include
 * information about deleted rows for this implementation.
 */
static bool
TruncateAndCombineColumnarStripes(Relation rel, int elevel)
{
	uint64 totalRowNumberCount = 0;
	uint32 startingStripeListPosition = 0;

	TupleDesc tupleDesc = RelationGetDescr(rel);

	if (tupleDesc->natts == 0)
	{
		ereport(elevel,
				(errmsg("\"%s\": stopping vacuum due to zero column table",
						RelationGetRelationName(rel))));
		return false;
	}

	/* Get current columnar options */
	ColumnarOptions columnarOptions = { 0 };
	ReadColumnarOptions(rel->rd_id, &columnarOptions);

	/* Get all stripes in reverse order */
#if PG_VERSION_NUM >= PG_VERSION_16
	List *stripeMetadataList = StripesForRelfilenode(rel->rd_locator, BackwardScanDirection);
#else
	List *stripeMetadataList = StripesForRelfilenode(rel->rd_node, BackwardScanDirection);
#endif
	/* Empty table nothing to do */
	if (stripeMetadataList == NIL)
	{
		ereport(elevel,
				(errmsg("\"%s\": stopping vacuum due to empty table",
						RelationGetRelationName(rel))));
		return false;
	}

	ListCell *lc = NULL;

	uint32 lastStripeDeletedRows = 0;

	Size totalDecompressedStripeLength = 0;

	foreach(lc, stripeMetadataList)
	{
		StripeMetadata * stripeMetadata = lfirst(lc);
#if PG_VERSION_NUM >= PG_VERSION_16
		lastStripeDeletedRows = DeletedRowsForStripe(rel->rd_locator,
													 stripeMetadata->chunkCount,
													 stripeMetadata->id);
		totalDecompressedStripeLength += 
			DecompressedLengthForStripe(rel->rd_locator, stripeMetadata->id);
#else
		lastStripeDeletedRows = DeletedRowsForStripe(rel->rd_node,
													 stripeMetadata->chunkCount,
													 stripeMetadata->id);
		totalDecompressedStripeLength += 
			DecompressedLengthForStripe(rel->rd_node, stripeMetadata->id);
#endif

		if (totalDecompressedStripeLength >= 1024000000)
		{
			break;
		}

		uint64 stripeRowCount = stripeMetadata->rowCount - lastStripeDeletedRows;

		if ((totalRowNumberCount + stripeRowCount >= columnarOptions.stripeRowCount))
		{
			break;
		}

		totalRowNumberCount += stripeRowCount;
		startingStripeListPosition++;
	}

	/*
	 * One stripe that is "full" so do nothing.
	 */
	if (startingStripeListPosition == 0)
	{
		return false;
	}

	/*
	 * There is only one stripe that is candidate. Maybe we should vacuum
	 * it if condition is met.
	 */
	else if (startingStripeListPosition == 1)
	{
		/* Maybe we should vacuum only one stripe if count of
		 * deleted rows is higher than 20 percent.
		 */
		float percentageOfDeleteRows =
			(float)lastStripeDeletedRows / (float)(totalRowNumberCount + lastStripeDeletedRows);

		bool shouldVacuumOnlyStripe = percentageOfDeleteRows > 0.2f;

		if (!shouldVacuumOnlyStripe)
		{
			return false;
		}
	}

	/*
	 * We need to clear current proces (VACUUUM) status flag here. Why?
	 * Current process has flag `PROC_IN_VACUUM` which is problematic because
	 * we write here into metadata heap tables. If concurrent process
	 * read page in which we inserted metadata tuples these tuples will be considered
	 * DEAD and will be removed (in problematic scenarion).
	 * Concurrent process, when assigning RecentXmin will scan all active processes in
	 * system but will NOT consider this process because of
	 * `PROC_IN_VACUUM` flag set. It looks that invalidating status flag here doesn't affect
	 * further execution.
	 */

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
#if PG_VERSION_NUM >= PG_VERSION_14
	MyProc->statusFlags = 0;
	ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
#else
	MyPgXact->vacuumFlags = 0;
#endif
	LWLockRelease(ProcArrayLock);

	/* We need to re-assing RecentXmin here */
	PushActiveSnapshot(GetTransactionSnapshot());
#if PG_VERSION_NUM >= PG_VERSION_16
	ColumnarWriteState *writeState = ColumnarBeginWrite(rel->rd_locator,
														columnarOptions,
														tupleDesc);
#else
	ColumnarWriteState *writeState = ColumnarBeginWrite(rel->rd_node,
														columnarOptions,
														tupleDesc);
#endif

	/* we need all columns */
	int natts = rel->rd_att->natts;
	Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

	/* no quals for table rewrite */
	List *scanQual = NIL;

	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	bool randomAccess = true;
	ColumnarReadState *readState = init_columnar_read_state(rel, tupleDesc,
															attr_needed, scanQual,
															scanContext, SnapshotAny,
															randomAccess,
															NULL);

	ColumnarSetStripeReadState(readState,
							   list_nth(stripeMetadataList, startingStripeListPosition - 1));

	Datum *values = palloc0(tupleDesc->natts * sizeof(Datum));
	bool *nulls = palloc0(tupleDesc->natts * sizeof(bool));

	/* we don't need to know rowNumber here */
	while (ColumnarReadNextRow(readState, values, nulls, NULL))
	{
		ColumnarWriteRow(writeState, values, nulls);
	}

	uint64 newDataReservation;

	if (list_length(stripeMetadataList) > startingStripeListPosition)
	{
		StripeMetadata *mtd = list_nth(stripeMetadataList, startingStripeListPosition);
		newDataReservation = mtd->fileOffset + mtd->dataLength - 1;
	}
	else
	{
		StripeMetadata *mtd = list_nth(stripeMetadataList, startingStripeListPosition - 1);
		newDataReservation = mtd->fileOffset;
	}

	ColumnarStorageTruncate(rel, newDataReservation);

	ColumnarEndWrite(writeState);
	ColumnarEndRead(readState);
	MemoryContextDelete(scanContext);

	for (int i = 0; i < startingStripeListPosition; i++)
	{
		StripeMetadata *metadata = list_nth(stripeMetadataList, i);
#if PG_VERSION_NUM >= PG_VERSION_16
		DeleteMetadataRowsForStripeId(rel->rd_locator, metadata->id);
#else
		DeleteMetadataRowsForStripeId(rel->rd_node, metadata->id);
#endif
	}

	PopActiveSnapshot();

	return true;
}

/*
 * ColumnarTableTupleCount returns the number of tuples that columnar
 * table with relationId has by using stripe metadata.
 */
static uint64
ColumnarTableTupleCount(Relation relation)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	List *stripeList = StripesForRelfilenode(relation->rd_locator, ForwardScanDirection);
#else
	List *stripeList = StripesForRelfilenode(relation->rd_node, ForwardScanDirection);
#endif
	uint64 tupleCount = 0;

	ListCell *lc = NULL;
	foreach(lc, stripeList)
	{
		StripeMetadata *stripe = lfirst(lc);
		tupleCount += stripe->rowCount;
	}

	return tupleCount;
}

/*
 * columnar_vacuum_rel implements VACUUM without FULL option.
 */
static void
columnar_vacuum_rel(Relation rel, VacuumParams *params,
					BufferAccessStrategy bstrategy)
{
	/* Capture the cache state and disable it for a vacuum. */
	bool old_cache_mode = columnar_enable_page_cache;
	columnar_enable_page_cache = false;

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
 									RelationGetRelid(rel));

	/*
	 * If metapage version of relation is older, then we hint users to VACUUM
	 * the relation in ColumnarMetapageCheckVersion. So if needed, upgrade
	 * the metapage before doing anything.
	 */
	bool isUpgrade = true;
	ColumnarStorageUpdateIfNeeded(rel, isUpgrade);

	int elevel = (params->options & VACOPT_VERBOSE) ? INFO : DEBUG2;

	/* this should have been resolved by vacuum.c until now */
	Assert(params->truncate != VACOPTVALUE_UNSPECIFIED);

	if (params->options & VACOPT_VERBOSE)
	{
		LogRelationStats(rel, elevel);
	}

	/*
	 * We don't have updates, deletes, or concurrent updates, so all we
	 * care for now is truncating the unused space at the end of storage.
	 */
	if (params->truncate == VACOPTVALUE_ENABLED)
	{
		TruncateColumnar(rel, elevel);
	}

	BlockNumber new_rel_pages = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);

	/* get the number of indexes */
	List *indexList = RelationGetIndexList(rel);
	int nindexes = list_length(indexList);

#if PG_VERSION_NUM >= PG_VERSION_16
	struct VacuumCutoffs cutoffs;
	vacuum_get_cutoffs(rel, params, &cutoffs);

	Assert(MultiXactIdPrecedesOrEquals(cutoffs.MultiXactCutoff, cutoffs.OldestMxact));
	Assert(TransactionIdPrecedesOrEquals(cutoffs.FreezeLimit, cutoffs.OldestXmin));

	/*
	 * Columnar storage doesn't hold any transaction IDs, so we can always
	 * just advance to the most aggressive value.
	 */
	TransactionId newRelFrozenXid = cutoffs.OldestXmin;
	MultiXactId newRelminMxid = cutoffs.OldestMxact;
	double new_live_tuples = ColumnarTableTupleCount(rel);

	/* all visible pages are always 0 */
	BlockNumber new_rel_allvisible = 0;

	bool frozenxid_updated;
	bool minmulti_updated;

	vac_update_relstats(rel, new_rel_pages, new_live_tuples,
						new_rel_allvisible, nindexes > 0,
						newRelFrozenXid, newRelminMxid,
						&frozenxid_updated, &minmulti_updated, false);

#else
	TransactionId oldestXmin;
	TransactionId freezeLimit;
	MultiXactId multiXactCutoff;

	/* initialize xids */
#if (PG_VERSION_NUM >= PG_VERSION_15) && (PG_VERSION_NUM < PG_VERSION_16)
	MultiXactId oldestMxact;
	vacuum_set_xid_limits(rel,
						params->freeze_min_age,
						params->freeze_table_age,
						params->multixact_freeze_min_age,
						params->multixact_freeze_table_age,
						&oldestXmin, &oldestMxact,
						&freezeLimit, &multiXactCutoff);

	Assert(MultiXactIdPrecedesOrEquals(multiXactCutoff, oldestMxact));
#else
	TransactionId xidFullScanLimit;
	MultiXactId mxactFullScanLimit;
	vacuum_set_xid_limits(rel,
							params->freeze_min_age,
							params->freeze_table_age,
							params->multixact_freeze_min_age,
							params->multixact_freeze_table_age,
							&oldestXmin, &freezeLimit, &xidFullScanLimit,
							&multiXactCutoff, &mxactFullScanLimit);
#endif

	Assert(TransactionIdPrecedesOrEquals(freezeLimit, oldestXmin));

	/*
	 * Columnar storage doesn't hold any transaction IDs, so we can always
	 * just advance to the most aggressive value.
	 */
	TransactionId newRelFrozenXid = oldestXmin;
#if (PG_VERSION_NUM >= PG_VERSION_15) && (PG_VERSION_NUM < PG_VERSION_16)
	MultiXactId newRelminMxid = oldestMxact;
#else
	MultiXactId newRelminMxid = multiXactCutoff;
#endif

	double new_live_tuples = ColumnarTableTupleCount(rel);

	/* all visible pages are always 0 */
	BlockNumber new_rel_allvisible = 0;

#if (PG_VERSION_NUM >= PG_VERSION_15) && (PG_VERSION_NUM < PG_VERSION_16)
	bool frozenxid_updated;
	bool minmulti_updated;

	vac_update_relstats(rel, new_rel_pages, new_live_tuples,
						new_rel_allvisible, nindexes > 0,
						newRelFrozenXid, newRelminMxid,
						&frozenxid_updated, &minmulti_updated, false);
#else
	vac_update_relstats(rel, new_rel_pages, new_live_tuples,
						new_rel_allvisible, nindexes > 0,
						newRelFrozenXid, newRelminMxid, false);
#endif
#endif

	pgstat_report_vacuum(RelationGetRelid(rel),
							rel->rd_rel->relisshared,
							Max(new_live_tuples, 0),
							0);
	pgstat_progress_end_command();

	/* Reenable the cache state. */
	columnar_enable_page_cache = old_cache_mode;
}


/*
 * LogRelationStats logs statistics as the output of the VACUUM VERBOSE.
 */
static void
LogRelationStats(Relation rel, int elevel)
{
	ListCell *stripeMetadataCell = NULL;
#if PG_VERSION_NUM >= PG_VERSION_16
	RelFileLocator relfilelocator = rel->rd_locator;
#else
	RelFileLocator relfilelocator = rel->rd_node;
#endif
	StringInfo infoBuf = makeStringInfo();

	int compressionStats[COMPRESSION_COUNT] = { 0 };
	uint64 totalStripeLength = 0;
	uint64 tupleCount = 0;
	uint64 chunkCount = 0;
	TupleDesc tupdesc = RelationGetDescr(rel);
	uint64 droppedChunksWithData = 0;
	uint64 totalDecompressedLength = 0;

	List *stripeList = StripesForRelfilenode(relfilelocator, ForwardScanDirection);
	int stripeCount = list_length(stripeList);

	MemoryContext relation_stats_ctx =
		AllocSetContextCreate(CurrentMemoryContext, "Vacuum Relation Stats Context",
							  ALLOCSET_SMALL_SIZES);

	MemoryContext oldcontext = MemoryContextSwitchTo(relation_stats_ctx);

	foreach(stripeMetadataCell, stripeList)
	{
		StripeMetadata *stripe = lfirst(stripeMetadataCell);
		StripeSkipList *skiplist = ReadStripeSkipList(relfilelocator, stripe->id,
													  RelationGetDescr(rel),
													  stripe->chunkCount,
													  GetTransactionSnapshot());
		for (uint32 column = 0; column < skiplist->columnCount; column++)
		{
			bool attrDropped = tupdesc->attrs[column].attisdropped;
			for (uint32 chunk = 0; chunk < skiplist->chunkCount; chunk++)
			{
				ColumnChunkSkipNode *skipnode =
					&skiplist->chunkSkipNodeArray[column][chunk];

				/* ignore zero length chunks for dropped attributes */
				if (skipnode->valueLength > 0)
				{
					compressionStats[skipnode->valueCompressionType]++;
					chunkCount++;

					if (attrDropped)
					{
						droppedChunksWithData++;
					}
				}

				/*
				 * We don't compress exists buffer, so its compressed & decompressed
				 * lengths are the same.
				 */
				totalDecompressedLength += skipnode->existsLength;
				totalDecompressedLength += skipnode->decompressedValueSize;
			}
		}

		tupleCount += stripe->rowCount;
		totalStripeLength += stripe->dataLength;

		MemoryContextReset(relation_stats_ctx);
	}

	MemoryContextSwitchTo(oldcontext);

	if (unlikely(rel->rd_smgr == NULL))
	{
#if PG_VERSION_NUM >= PG_VERSION_16
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_locator, rel->rd_backend));
#else
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
#endif
	}

	uint64 relPages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	RelationCloseSmgr(rel);

	Datum storageId = DirectFunctionCall1(columnar_relation_storageid,
										  ObjectIdGetDatum(RelationGetRelid(rel)));

	double compressionRate = totalStripeLength ?
							 (double) totalDecompressedLength / totalStripeLength :
							 1.0;

	appendStringInfo(infoBuf, "storage id: %ld\n", DatumGetInt64(storageId));
	appendStringInfo(infoBuf, "total file size: %ld, total data size: %ld\n",
					 relPages * BLCKSZ, totalStripeLength);
	appendStringInfo(infoBuf, "compression rate: %.2fx\n", compressionRate);
	appendStringInfo(infoBuf,
					 "total row count: %ld, stripe count: %d, "
					 "average rows per stripe: %ld\n",
					 tupleCount, stripeCount,
					 stripeCount ? tupleCount / stripeCount : 0);
	appendStringInfo(infoBuf,
					 "chunk count: %ld"
					 ", containing data for dropped columns: %ld",
					 chunkCount, droppedChunksWithData);
	for (int compressionType = 0; compressionType < COMPRESSION_COUNT; compressionType++)
	{
		const char *compressionName = CompressionTypeStr(compressionType);

		/* skip if this compression algorithm has not been compiled */
		if (compressionName == NULL)
		{
			continue;
		}

		/* skip if no chunks use this compression type */
		if (compressionStats[compressionType] == 0)
		{
			continue;
		}

		appendStringInfo(infoBuf,
						 ", %s compressed: %d",
						 compressionName,
						 compressionStats[compressionType]);
	}
	appendStringInfoString(infoBuf, "\n");

	ereport(elevel, (errmsg("statistics for \"%s\":\n%s", RelationGetRelationName(rel),
							infoBuf->data)));
}


/*
 * TruncateColumnar truncates the unused space at the end of main fork for
 * a columnar table. This unused space can be created by aborted transactions.
 *
 * This implementation is based on heap_vacuum_rel in vacuumlazy.c with some
 * changes so it suits columnar store relations.
 */
static void
TruncateColumnar(Relation rel, int elevel)
{
	PGRUsage ru0;

	pg_rusage_init(&ru0);

	/* Report that we are now truncating */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_TRUNCATE);


	/*
	 * We need access exclusive lock on the relation in order to do
	 * truncation. If we can't get it, give up rather than waiting --- we
	 * don't want to block other backends, and we don't want to deadlock
	 * (which is quite possible considering we already hold a lower-grade
	 * lock).
	 *
	 * The decisions for AccessExclusiveLock and conditional lock with
	 * a timeout is based on lazy_truncate_heap in vacuumlazy.c.
	 */
	if (!ConditionalLockRelationWithTimeout(rel, AccessExclusiveLock,
											VACUUM_TRUNCATE_LOCK_TIMEOUT,
											VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL,
											false))
	{
		/*
		 * We failed to establish the lock in the specified number of
		 * retries. This means we give up truncating.
		 */
		ereport(elevel,
				(errmsg("\"%s\": stopping truncate due to conflicting lock request",
						RelationGetRelationName(rel))));
		return;
	}

	bool stripesTruncated = TruncateAndCombineColumnarStripes(rel, elevel);

	/*
	 * If we didn't truncate and combine tail stripes we could have
	 * stituation where we need to truncate storage at the end.
	 */
	if (!stripesTruncated)
	{

		/*
		* Due to the AccessExclusive lock there's no danger that
		* new stripes be added beyond highestPhysicalAddress while
		* we're truncating.
		*/
#if PG_VERSION_NUM >= PG_VERSION_16
		uint64 newDataReservation = Max(GetHighestUsedAddress(rel->rd_locator) + 1,
										ColumnarFirstLogicalOffset);
#else
		uint64 newDataReservation = Max(GetHighestUsedAddress(rel->rd_node) + 1,
										ColumnarFirstLogicalOffset);
#endif
		if (unlikely(rel->rd_smgr == NULL))
		{
#if PG_VERSION_NUM >= PG_VERSION_16
			smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_locator, rel->rd_backend));
#else
			smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
#endif
		}

		BlockNumber old_rel_pages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);

		if (!ColumnarStorageTruncate(rel, newDataReservation))
		{
			UnlockRelation(rel, AccessExclusiveLock);
			return;
		}

		BlockNumber new_rel_pages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);

		ereport(elevel,
		(errmsg("\"%s\": truncated %u to %u pages",
				RelationGetRelationName(rel),
				old_rel_pages, new_rel_pages),
			errdetail_internal("%s", pg_rusage_show(&ru0))));
	}

	/*
	 * We can release the exclusive lock as soon as we have truncated.
	 * Other backends can't safely access the relation until they have
	 * processed the smgr invalidation that smgrtruncate sent out ... but
	 * that should happen as part of standard invalidation processing once
	 * they acquire lock on the relation.
	 */
	UnlockRelation(rel, AccessExclusiveLock);
}

/*
 * ConditionalLockRelationWithTimeout tries to acquire a relation lock until
 * it either succeeds or timesout. It doesn't enter wait queue and instead it
 * sleeps between lock tries.
 *
 * This is based on the lock loop in lazy_truncate_heap().
 */
static bool
ConditionalLockRelationWithTimeout(Relation rel, LOCKMODE lockMode, int timeout,
								   int retryInterval, bool acquire)
{
	int lock_retry = 0;

	while (true)
	{
		if (ConditionalLockRelation(rel, lockMode))
		{
			break;
		}

		/*
		 * Check for interrupts while trying to (re-)acquire the lock
		 */
		CHECK_FOR_INTERRUPTS();

		if (!acquire && (++lock_retry > (timeout / retryInterval)))
		{
			return false;
		}

		pg_usleep(retryInterval * 1000L);
	}

	return true;
}


static bool
columnar_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								 BufferAccessStrategy bstrategy)
{
	/*
	 * Our access method is not pages based, i.e. tuples are not confined
	 * to pages boundaries. So not much to do here. We return true anyway
	 * so acquire_sample_rows() in analyze.c would call our
	 * columnar_scan_analyze_next_tuple() callback.
	 */
	return true;
}


static bool
columnar_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								 double *liverows, double *deadrows,
								 TupleTableSlot *slot)
{
	/* Capture the cache state and disable it for a vacuum. */
	bool old_cache_mode = columnar_enable_page_cache;
	columnar_enable_page_cache = false;

	/*
	 * Currently we don't do anything smart to reduce number of rows returned
	 * for ANALYZE. The TableAM API's ANALYZE functions are designed for page
	 * based access methods where it chooses random pages, and then reads
	 * tuples from those pages.
	 *
	 * We could do something like that here by choosing sample stripes or chunks,
	 * but getting that correct might need quite some work. Since columnar_fdw's
	 * ANALYZE scanned all rows, as a starter we do the same here and scan all
	 * rows.
	 */
	if (columnar_getnextslot(scan, ForwardScanDirection, slot))
	{
		(*liverows)++;

		/* Reset cache to previous state. */
		columnar_enable_page_cache = old_cache_mode;

		return true;
	}

	/* Reset cache to previous state. */
	columnar_enable_page_cache = old_cache_mode;

	return false;
}


static double
columnar_index_build_range_scan(Relation columnarRelation,
								Relation indexRelation,
								IndexInfo *indexInfo,
								bool allow_sync,
								bool anyvisible,
								bool progress,
								BlockNumber start_blockno,
								BlockNumber numblocks,
								IndexBuildCallback callback,
								void *callback_state,
								TableScanDesc scan)
{
	if (start_blockno != 0 || numblocks != InvalidBlockNumber)
	{
		/*
		 * Columnar utility hook already errors out for BRIN indexes on columnar
		 * tables, but be on the safe side.
		 */
		ereport(ERROR, (errmsg("BRIN indexes on columnar tables are not supported")));
	}

	if (scan)
	{
		/*
		 * Parallel scans on columnar tables are already discardad by
		 * ColumnarGetRelationInfoHook but be on the safe side.
		 */
		elog(ERROR, "parallel scans on columnar are not supported");
	}

	/* Disable the page cache for the index build. */
	bool old_cache_mode = columnar_enable_page_cache;
	columnar_enable_page_cache = false;

	/*
	 * In a normal index build, we use SnapshotAny to retrieve all tuples. In
	 * a concurrent build or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 */
	TransactionId OldestXmin = InvalidTransactionId;
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
	{
		/* ignore lazy VACUUM's */
		OldestXmin = GetOldestNonRemovableTransactionId_compat(columnarRelation,
															   PROCARRAY_FLAGS_VACUUM);
	}

	Snapshot snapshot = { 0 };
	bool snapshotRegisteredByUs = false;

	/*
	 * For serial index build, we begin our own scan. We may also need to
	 * register a snapshot whose lifetime is under our direct control.
	 */
	if (!TransactionIdIsValid(OldestXmin))
	{
		snapshot = RegisterSnapshot(GetTransactionSnapshot());
		snapshotRegisteredByUs = true;
	}
	else
	{
		snapshot = SnapshotAny;
	}

	int nkeys = 0;
	ScanKeyData *scanKey = NULL;
	bool allowAccessStrategy = true;
	scan = table_beginscan_strat(columnarRelation, snapshot, nkeys, scanKey,
								 allowAccessStrategy, allow_sync);

	if (progress)
	{
		ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
										 PROGRESS_SCAN_BLOCKS_TOTAL);
	}

	/*
	 * Set up execution state for predicate, if any.
	 * Note that this is only useful for partial indexes.
	 */
	EState *estate = CreateExecutorState();
	ExprContext *econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = table_slot_create(columnarRelation, NULL);
	ExprState *predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	double reltuples = ColumnarReadRowsIntoIndex(scan, indexRelation, indexInfo,
												 progress, callback, callback_state,
												 estate, predicate);
	table_endscan(scan);

	if (progress)
	{
		/* report the last "virtual" block as "done" */
		ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
										 PROGRESS_SCAN_BLOCKS_DONE);
	}

	if (snapshotRegisteredByUs)
	{
		UnregisterSnapshot(snapshot);
	}

	ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
	FreeExecutorState(estate);
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	/* Reset the cache mode. */
	columnar_enable_page_cache = old_cache_mode;

	return reltuples;
}


/*
 * ColumnarReportTotalVirtualBlocks reports progress for index build based on
 * number of "virtual" blocks that given relation has.
 * "progressArrIndex" argument determines which entry in st_progress_param
 * array should be updated. In this case, we only expect PROGRESS_SCAN_BLOCKS_TOTAL
 * or PROGRESS_SCAN_BLOCKS_DONE to specify whether we want to report calculated
 * number of blocks as "done" or as "total" number of "virtual" blocks to scan.
 */
static void
ColumnarReportTotalVirtualBlocks(Relation relation, Snapshot snapshot,
								 int progressArrIndex)
{
	/*
	 * Indeed, columnar tables might have gaps between row numbers, e.g
	 * due to aborted transactions etc. Also, ItemPointer BlockNumber's
	 * for columnar tables don't actually correspond to actual disk blocks
	 * as in heapAM. For this reason, we call them as "virtual" blocks. At
	 * the moment, we believe it is better to report our progress based on
	 * this "virtual" block concept instead of doing nothing.
	 */
	Assert(progressArrIndex == PROGRESS_SCAN_BLOCKS_TOTAL ||
		   progressArrIndex == PROGRESS_SCAN_BLOCKS_DONE);
	BlockNumber nvirtualBlocks =
		ColumnarGetNumberOfVirtualBlocks(relation, snapshot);
	pgstat_progress_update_param(progressArrIndex, nvirtualBlocks);
}


/*
 * ColumnarGetNumberOfVirtualBlocks returns total number of "virtual" blocks
 * that given columnar table has based on based on ItemPointer BlockNumber's.
 */
static BlockNumber
ColumnarGetNumberOfVirtualBlocks(Relation relation, Snapshot snapshot)
{
	ItemPointerData highestItemPointer =
		ColumnarGetHighestItemPointer(relation, snapshot);
	if (!ItemPointerIsValid(&highestItemPointer))
	{
		/* table is empty according to our snapshot */
		return 0;
	}

	/*
	 * Since BlockNumber is 0-based, increment it by 1 to find the total
	 * number of "virtual" blocks.
	 */
	return ItemPointerGetBlockNumber(&highestItemPointer) + 1;
}


/*
 * ColumnarGetHighestItemPointer returns ItemPointerData for the tuple with
 * highest tid for given relation.
 * If given relation is empty, then returns invalid item pointer.
 */
static ItemPointerData
ColumnarGetHighestItemPointer(Relation relation, Snapshot snapshot)
{
	StripeMetadata *stripeWithHighestRowNumber =
		FindStripeWithHighestRowNumber(relation, snapshot);
	if (stripeWithHighestRowNumber == NULL ||
		StripeGetHighestRowNumber(stripeWithHighestRowNumber) == 0)
	{
		/* table is empty according to our snapshot */
		ItemPointerData invalidItemPtr;
		ItemPointerSetInvalid(&invalidItemPtr);
		return invalidItemPtr;
	}

	uint64 highestRowNumber = StripeGetHighestRowNumber(stripeWithHighestRowNumber);
	return row_number_to_tid(highestRowNumber);
}


/*
 * ColumnarReadRowsIntoIndex builds indexRelation tuples by reading the
 * actual relation based on given "scan" and returns number of tuples
 * scanned to build the indexRelation.
 */
static double
ColumnarReadRowsIntoIndex(TableScanDesc scan, Relation indexRelation,
						  IndexInfo *indexInfo, bool progress,
						  IndexBuildCallback indexCallback,
						  void *indexCallbackState, EState *estate,
						  ExprState *predicate)
{
	double reltuples = 0;

	BlockNumber lastReportedBlockNumber = InvalidBlockNumber;

	ExprContext *econtext = GetPerTupleExprContext(estate);
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	while (columnar_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();

		BlockNumber currentBlockNumber = ItemPointerGetBlockNumber(&slot->tts_tid);
		if (progress && lastReportedBlockNumber != currentBlockNumber)
		{
			/*
			 * columnar_getnextslot guarantees that returned tuple will
			 * always have a greater ItemPointer than the ones we fetched
			 * before, so we directly use BlockNumber to report our progress.
			 */
			Assert(lastReportedBlockNumber == InvalidBlockNumber ||
				   currentBlockNumber >= lastReportedBlockNumber);
			pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
										 currentBlockNumber);
			lastReportedBlockNumber = currentBlockNumber;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (predicate != NULL && !ExecQual(predicate, econtext))
		{
			/* for partial indexes, discard tuples that don't satisfy the predicate */
			continue;
		}

		Datum indexValues[INDEX_MAX_KEYS];
		bool indexNulls[INDEX_MAX_KEYS];
		FormIndexDatum(indexInfo, slot, estate, indexValues, indexNulls);

		ItemPointerData itemPointerData = slot->tts_tid;

		/* currently, columnar tables can't have dead tuples */
		bool tupleIsAlive = true;
#if PG_VERSION_NUM >= PG_VERSION_13
		indexCallback(indexRelation, &itemPointerData, indexValues, indexNulls,
					  tupleIsAlive, indexCallbackState);
#else
		HeapTuple scanTuple = ExecCopySlotHeapTuple(slot);
		scanTuple->t_self = itemPointerData;
		indexCallback(indexRelation, scanTuple, indexValues, indexNulls,
					  tupleIsAlive, indexCallbackState);
#endif

		reltuples++;
	}

	return reltuples;
}


static void
columnar_index_validate_scan(Relation columnarRelation,
							 Relation indexRelation,
							 IndexInfo *indexInfo,
							 Snapshot snapshot,
							 ValidateIndexState *
							 validateIndexState)
{
	ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
									 PROGRESS_SCAN_BLOCKS_TOTAL);

	/*
	 * Set up execution state for predicate, if any.
	 * Note that this is only useful for partial indexes.
	 */
	EState *estate = CreateExecutorState();
	ExprContext *econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = table_slot_create(columnarRelation, NULL);
	ExprState *predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	int nkeys = 0;
	ScanKeyData *scanKey = NULL;
	bool allowAccessStrategy = true;
	bool allowSyncScan = false;
	TableScanDesc scan = table_beginscan_strat(columnarRelation, snapshot, nkeys, scanKey,
											   allowAccessStrategy, allowSyncScan);

	ColumnarReadMissingRowsIntoIndex(scan, indexRelation, indexInfo, estate,
									 predicate, validateIndexState);

	table_endscan(scan);

	/* report the last "virtual" block as "done" */
	ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
									 PROGRESS_SCAN_BLOCKS_DONE);

	ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
	FreeExecutorState(estate);
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}


/*
 * ColumnarReadMissingRowsIntoIndex inserts the tuples that are not in
 * the index yet by reading the actual relation based on given "scan".
 */
static void
ColumnarReadMissingRowsIntoIndex(TableScanDesc scan, Relation indexRelation,
								 IndexInfo *indexInfo, EState *estate,
								 ExprState *predicate,
								 ValidateIndexState *validateIndexState)
{
	BlockNumber lastReportedBlockNumber = InvalidBlockNumber;

	bool indexTupleSortEmpty = false;
	ItemPointerData indexedItemPointerData;
	ItemPointerSetInvalid(&indexedItemPointerData);

	ExprContext *econtext = GetPerTupleExprContext(estate);
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	while (columnar_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();

		ItemPointer columnarItemPointer = &slot->tts_tid;
		BlockNumber currentBlockNumber = ItemPointerGetBlockNumber(columnarItemPointer);
		if (lastReportedBlockNumber != currentBlockNumber)
		{
			/*
			 * columnar_getnextslot guarantees that returned tuple will
			 * always have a greater ItemPointer than the ones we fetched
			 * before, so we directly use BlockNumber to report our progress.
			 */
			Assert(lastReportedBlockNumber == InvalidBlockNumber ||
				   currentBlockNumber >= lastReportedBlockNumber);
			pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
										 currentBlockNumber);
			lastReportedBlockNumber = currentBlockNumber;
		}

		validateIndexState->htups += 1;

		if (!indexTupleSortEmpty &&
			(!ItemPointerIsValid(&indexedItemPointerData) ||
			 ItemPointerCompare(&indexedItemPointerData, columnarItemPointer) < 0))
		{
			/*
			 * Skip indexed item pointers until we find or pass the current
			 * columnar relation item pointer.
			 */
			indexedItemPointerData =
				TupleSortSkipSmallerItemPointers(validateIndexState->tuplesort,
												 columnarItemPointer);
			indexTupleSortEmpty = !ItemPointerIsValid(&indexedItemPointerData);
		}

		if (!indexTupleSortEmpty &&
			ItemPointerCompare(&indexedItemPointerData, columnarItemPointer) == 0)
		{
			/* tuple is already covered by the index, skip */
			continue;
		}
		Assert(indexTupleSortEmpty ||
			   ItemPointerCompare(&indexedItemPointerData, columnarItemPointer) > 0);

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (predicate != NULL && !ExecQual(predicate, econtext))
		{
			/* for partial indexes, discard tuples that don't satisfy the predicate */
			continue;
		}

		Datum indexValues[INDEX_MAX_KEYS];
		bool indexNulls[INDEX_MAX_KEYS];
		FormIndexDatum(indexInfo, slot, estate, indexValues, indexNulls);

		Relation columnarRelation = scan->rs_rd;
		IndexUniqueCheck indexUniqueCheck =
			indexInfo->ii_Unique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO;
		index_insert_compat(indexRelation, indexValues, indexNulls, columnarItemPointer,
							columnarRelation, indexUniqueCheck, false, indexInfo);

		validateIndexState->tups_inserted += 1;
	}
}


/*
 * TupleSortSkipSmallerItemPointers iterates given tupleSort until finding an
 * ItemPointer that is greater than or equal to given targetItemPointer and
 * returns that ItemPointer.
 * If such an ItemPointer does not exist, then returns invalid ItemPointer.
 *
 * Note that this function assumes given tupleSort doesn't have any NULL
 * Datum's.
 */
static ItemPointerData
TupleSortSkipSmallerItemPointers(Tuplesortstate *tupleSort, ItemPointer targetItemPointer)
{
	ItemPointerData tsItemPointerData;
	ItemPointerSetInvalid(&tsItemPointerData);

	while (!ItemPointerIsValid(&tsItemPointerData) ||
		   ItemPointerCompare(&tsItemPointerData, targetItemPointer) < 0)
	{
		bool forwardDirection = true;
		Datum *abbrev = NULL;
		Datum tsDatum;
		bool tsDatumIsNull;
		if (!tuplesort_getdatum_compat(tupleSort, forwardDirection, false,
									   &tsDatum, &tsDatumIsNull, abbrev))
		{
			ItemPointerSetInvalid(&tsItemPointerData);
			break;
		}

		Assert(!tsDatumIsNull);
		itemptr_decode(&tsItemPointerData, DatumGetInt64(tsDatum));

#ifndef USE_FLOAT8_BYVAL

		/*
		 * If int8 is pass-by-ref, we need to free Datum memory.
		 * See tuplesort_getdatum function's comment.
		 */
		pfree(DatumGetPointer(tsDatum));
#endif
	}

	return tsItemPointerData;
}


static uint64
columnar_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 nblocks = 0;

	if (unlikely(rel->rd_smgr == NULL))
	{
#if PG_VERSION_NUM >= PG_VERSION_16
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_locator, rel->rd_backend));
#else
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
#endif
	}

	/* InvalidForkNumber indicates returning the size for all forks */
	if (forkNumber == InvalidForkNumber)
	{
		for (int i = 0; i < MAX_FORKNUM; i++)
		{
			nblocks += smgrnblocks(rel->rd_smgr, i);
		}
	}
	else
	{
		nblocks = smgrnblocks(rel->rd_smgr, forkNumber);
	}

	return nblocks * BLCKSZ;
}


static bool
columnar_relation_needs_toast_table(Relation rel)
{
	return false;
}


static void
columnar_estimate_rel_size(Relation rel, int32 *attr_widths,
						   BlockNumber *pages, double *tuples,
						   double *allvisfrac)
{
	if (unlikely(rel->rd_smgr == NULL))
	{
#if PG_VERSION_NUM >= PG_VERSION_16
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_locator, rel->rd_backend));
#else
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
#endif
	}

	*pages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	*tuples = ColumnarTableRowCount(rel);

	/*
	 * Append-only, so everything is visible except in-progress or rolled-back
	 * transactions.
	 */
	*allvisfrac = 1.0;

	get_rel_data_width(rel, attr_widths);
}


static bool
columnar_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "columnar_scan_sample_next_block not implemented");
}


static bool
columnar_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								TupleTableSlot *slot)
{
	elog(ERROR, "columnar_scan_sample_next_tuple not implemented");
}


static void
ColumnarXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
		{
			/* nothing to do */
			break;
		}

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		{
			DiscardWriteStateForAllRels(GetCurrentSubTransactionId(), 0);
			CleanupReadStateCache(GetCurrentSubTransactionId());
			break;
		}

		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		{
			FlushWriteStateForAllRels(GetCurrentSubTransactionId(), 0);
			CleanupReadStateCache(GetCurrentSubTransactionId());
			break;
		}
	}
}


static void
ColumnarSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
						SubTransactionId parentSubid, void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
		case SUBXACT_EVENT_COMMIT_SUB:
		{
			/* nothing to do */
			break;
		}

		case SUBXACT_EVENT_ABORT_SUB:
		{
			DiscardWriteStateForAllRels(mySubid, parentSubid);
			CleanupReadStateCache(mySubid);
			break;
		}

		case SUBXACT_EVENT_PRE_COMMIT_SUB:
		{
			FlushWriteStateForAllRels(mySubid, parentSubid);
			CleanupReadStateCache(mySubid);
			break;
		}
	}
}


void
columnar_tableam_init()
{
	ColumnarTableSetOptions_hook_type **ColumnarTableSetOptions_hook_ptr =
		(ColumnarTableSetOptions_hook_type **) find_rendezvous_variable(
			COLUMNAR_SETOPTIONS_HOOK_SYM);
	*ColumnarTableSetOptions_hook_ptr = &ColumnarTableSetOptions_hook;

	RegisterXactCallback(ColumnarXactCallback, NULL);
	RegisterSubXactCallback(ColumnarSubXactCallback, NULL);

	PrevObjectAccessHook = object_access_hook;
	object_access_hook = ColumnarTableAMObjectAccessHook;

	PrevProcessUtilityHook = ProcessUtility_hook ?
							 ProcessUtility_hook :
							 standard_ProcessUtility;
	ProcessUtility_hook = ColumnarProcessUtility;

	columnar_customscan_init();

	TTSOpsColumnar = TTSOpsVirtual;
	TTSOpsColumnar.copy_heap_tuple = ColumnarSlotCopyHeapTuple;
}


/*
 * Get the number of chunks filtered out during the given scan.
 */
int64
ColumnarScanChunkGroupsFiltered(ColumnarScanDesc columnarScanDesc)
{
	ColumnarReadState *readState = columnarScanDesc->cs_readState;

	/* readState is initialized lazily */
	if (readState != NULL)
	{
		return ColumnarReadChunkGroupsFiltered(readState);
	}
	else
	{
		return 0;
	}
}


/*
 * Implementation of TupleTableSlotOps.copy_heap_tuple for TTSOpsColumnar.
 */
static HeapTuple
ColumnarSlotCopyHeapTuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	HeapTuple tuple = heap_form_tuple(slot->tts_tupleDescriptor,
									  slot->tts_values,
									  slot->tts_isnull);

	/* slot->tts_tid is filled in columnar_getnextslot */
	tuple->t_self = slot->tts_tid;

	return tuple;
}


/*
 * ColumnarTableDropHook
 *
 * Clean-up resources for columnar tables.
 */
static void
ColumnarTableDropHook(Oid relid)
{
	/*
	 * Lock relation to prevent it from being dropped and to avoid
	 * race conditions in the next if block.
	 */
	LockRelationOid(relid, AccessShareLock);

	if (IsColumnarTableAmTable(relid))
	{
		/*
		 * Drop metadata. No need to drop storage here since for
		 * tableam tables storage is managed by postgres.
		 */
		Relation rel = table_open(relid, AccessExclusiveLock);
#if PG_VERSION_NUM >= PG_VERSION_16
		RelFileLocator relfilelocator = rel->rd_locator;
#else
		RelFileLocator relfilelocator = rel->rd_node;
#endif
		DeleteMetadataRows(relfilelocator);
		DeleteColumnarTableOptions(rel->rd_id, true);
#if PG_VERSION_NUM >= PG_VERSION_16
		MarkRelfilenodeDropped(relfilelocator.relNumber, GetCurrentSubTransactionId());
#else
		MarkRelfilenodeDropped(relfilelocator.relNode, GetCurrentSubTransactionId());
#endif

		/* keep the lock since we did physical changes to the relation */
		table_close(rel, NoLock);
	}
}


/*
 * Reject AFTER ... FOR EACH ROW triggers on columnar tables.
 */
static void
ColumnarTriggerCreateHook(Oid tgid)
{
	/*
	 * Fetch the pg_trigger tuple by the Oid of the trigger
	 */
	ScanKeyData skey[1];
	Relation tgrel = table_open(TriggerRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_trigger_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(tgid));

	SysScanDesc tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true,
											SnapshotSelf, 1, skey);

	HeapTuple tgtup = systable_getnext(tgscan);

	if (!HeapTupleIsValid(tgtup))
	{
		table_close(tgrel, AccessShareLock);
		return;
	}

	Form_pg_trigger tgrec = (Form_pg_trigger) GETSTRUCT(tgtup);

	Oid tgrelid = tgrec->tgrelid;
	int16 tgtype = tgrec->tgtype;

	systable_endscan(tgscan);
	table_close(tgrel, AccessShareLock);

	if (TRIGGER_FOR_ROW(tgtype) && TRIGGER_FOR_AFTER(tgtype) &&
		IsColumnarTableAmTable(tgrelid))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"Foreign keys and AFTER ROW triggers are not supported for columnar tables"),
						errhint("Consider an AFTER STATEMENT trigger instead.")));
	}
}


/*
 * Capture create/drop events and dispatch to the proper action.
 */
static void
ColumnarTableAMObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
								int subId, void *arg)
{
	if (PrevObjectAccessHook)
	{
		PrevObjectAccessHook(access, classId, objectId, subId, arg);
	}

	/* dispatch to the proper action */
	if (access == OAT_DROP && classId == RelationRelationId && !OidIsValid(subId))
	{
		ColumnarTableDropHook(objectId);
	}
	else if (access == OAT_POST_CREATE && classId == TriggerRelationId)
	{
		ColumnarTriggerCreateHook(objectId);
	}
}


/*
 * Utility hook for columnar tables.
 */
static void
ColumnarProcessUtility(PlannedStmt *pstmt,
					   const char *queryString,
#if PG_VERSION_NUM >= PG_VERSION_14
					   bool readOnlyTree,
#endif
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   struct QueryEnvironment *queryEnv,
					   DestReceiver *dest,
					   QueryCompletionCompat *completionTag)
{
#if PG_VERSION_NUM >= PG_VERSION_14
	if (readOnlyTree)
	{
		pstmt = copyObject(pstmt);
	}
#endif

	Node *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, IndexStmt))
	{
		IndexStmt *indexStmt = (IndexStmt *) parsetree;

		Relation rel = relation_openrv(indexStmt->relation,
									   indexStmt->concurrent ? ShareUpdateExclusiveLock :
									   ShareLock);

		if (rel->rd_tableam == GetColumnarTableAmRoutine())
		{
			if (!ColumnarSupportsIndexAM(indexStmt->accessMethod))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("unsupported access method for the "
									   "index on columnar table %s (%s)",
									   RelationGetRelationName(rel), indexStmt->accessMethod)));
			}
		}

		RelationClose(rel);
	}

	PrevProcessUtilityHook_compat(pstmt, queryString, false, context,
								  params, queryEnv, dest, completionTag);
}


/*
 * ColumnarSupportsIndexAM returns true if indexAM with given name is
 * supported by columnar tables.
 */
bool
ColumnarSupportsIndexAM(char *indexAMName)
{
	return strncmp(indexAMName, "btree", NAMEDATALEN) == 0 ||
				strncmp(indexAMName, "hash", NAMEDATALEN) == 0 ||
				strncmp(indexAMName, "gin", NAMEDATALEN) == 0 ||
				strncmp(indexAMName, "gist", NAMEDATALEN) == 0 ||
				strncmp(indexAMName, "spgist", NAMEDATALEN) == 0 ||
				strncmp(indexAMName, "rum", NAMEDATALEN) == 0;
}


/*
 * IsColumnarTableAmTable returns true if relation has columnar_tableam
 * access method. This can be called before extension creation.
 */
bool
IsColumnarTableAmTable(Oid relationId)
{
	if (!OidIsValid(relationId))
	{
		return false;
	}

	/*
	 * Lock relation to prevent it from being dropped &
	 * avoid race conditions.
	 */
	Relation rel = relation_open(relationId, AccessShareLock);
	bool result = rel->rd_tableam == GetColumnarTableAmRoutine();
	relation_close(rel, NoLock);

	return result;
}


static const TableAmRoutine columnar_am_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = columnar_slot_callbacks,

	.scan_begin = columnar_beginscan,
	.scan_end = columnar_endscan,
	.scan_rescan = columnar_rescan,
	.scan_getnextslot = columnar_getnextslot,

	.parallelscan_estimate = columnar_parallelscan_estimate,
	.parallelscan_initialize = columnar_parallelscan_initialize,
	.parallelscan_reinitialize = columnar_parallelscan_reinitialize,

	.index_fetch_begin = columnar_index_fetch_begin,
	.index_fetch_reset = columnar_index_fetch_reset,
	.index_fetch_end = columnar_index_fetch_end,
	.index_fetch_tuple = columnar_index_fetch_tuple,

	.tuple_fetch_row_version = columnar_fetch_row_version,
	.tuple_get_latest_tid = columnar_get_latest_tid,
	.tuple_tid_valid = columnar_tuple_tid_valid,
	.tuple_satisfies_snapshot = columnar_tuple_satisfies_snapshot,
#if PG_VERSION_NUM >= PG_VERSION_14
	.index_delete_tuples = columnar_index_delete_tuples,
#else
	.compute_xid_horizon_for_tuples = columnar_compute_xid_horizon_for_tuples,
#endif

	.tuple_insert = columnar_tuple_insert,
	.tuple_insert_speculative = columnar_tuple_insert_speculative,
	.tuple_complete_speculative = columnar_tuple_complete_speculative,
	.multi_insert = columnar_multi_insert,
	.tuple_delete = columnar_tuple_delete,
	.tuple_update = columnar_tuple_update,
	.tuple_lock = columnar_tuple_lock,
	.finish_bulk_insert = columnar_finish_bulk_insert,

#if PG_VERSION_NUM >= PG_VERSION_16
	.relation_set_new_filelocator = columnar_relation_set_new_filenode,
#else
	.relation_set_new_filenode = columnar_relation_set_new_filenode,
#endif
	.relation_nontransactional_truncate = columnar_relation_nontransactional_truncate,
	.relation_copy_data = columnar_relation_copy_data,
	.relation_copy_for_cluster = columnar_relation_copy_for_cluster,
	.relation_vacuum = columnar_vacuum_rel,
	.scan_analyze_next_block = columnar_scan_analyze_next_block,
	.scan_analyze_next_tuple = columnar_scan_analyze_next_tuple,
	.index_build_range_scan = columnar_index_build_range_scan,
	.index_validate_scan = columnar_index_validate_scan,

	.relation_size = columnar_relation_size,
	.relation_needs_toast_table = columnar_relation_needs_toast_table,

	.relation_estimate_size = columnar_estimate_rel_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = columnar_scan_sample_next_block,
	.scan_sample_next_tuple = columnar_scan_sample_next_tuple
};


const TableAmRoutine *
GetColumnarTableAmRoutine(void)
{
	return &columnar_am_methods;
}


PG_FUNCTION_INFO_V1(columnar_handler);
Datum
columnar_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&columnar_am_methods);
}


/*
 * detoast_values
 *
 * Detoast and decompress all values. If there's no work to do, return
 * original pointer; otherwise return a newly-allocated values array. Should
 * be called in per-tuple context.
 */
static Datum *
detoast_values(TupleDesc tupleDesc, Datum *orig_values, bool *isnull)
{
	int natts = tupleDesc->natts;

	/* copy on write to optimize for case where nothing is toasted */
	Datum *values = orig_values;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		if (!isnull[i] && tupleDesc->attrs[i].attlen == -1 &&
			VARATT_IS_EXTENDED(values[i]))
		{
			/* make a copy */
			if (values == orig_values)
			{
				values = palloc(sizeof(Datum) * natts);
				memcpy(values, orig_values, sizeof(Datum) * natts);
			}

			/* will be freed when per-tuple context is reset */
			struct varlena *new_value = (struct varlena *) DatumGetPointer(values[i]);
			new_value = detoast_attr(new_value);
			values[i] = PointerGetDatum(new_value);
		}
	}

	return values;
}


/*
 * ColumnarCheckLogicalReplication throws an error if the relation is
 * part of any publication. This should be called before any write to
 * a columnar table, because columnar changes are not replicated with
 * logical replication (similar to a row table without a replica
 * identity).
 */
static void
ColumnarCheckLogicalReplication(Relation rel)
{
	bool pubActionInsert = false;

	if (!is_publishable_relation(rel))
	{
		return;
	}

#if PG_VERSION_NUM >= PG_VERSION_15
	{
		PublicationDesc pubdesc;

		RelationBuildPublicationDesc(rel, &pubdesc);
		pubActionInsert = pubdesc.pubactions.pubinsert;
	}
#else
	if (rel->rd_pubactions == NULL)
	{
		GetRelationPublicationActions(rel);
		Assert(rel->rd_pubactions != NULL);
	}

	pubActionInsert = rel->rd_pubactions->pubinsert;
#endif

	if (pubActionInsert)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"cannot insert into columnar table that is a part of a publication")));
	}
}


/*
 * alter_columnar_table_set is a UDF exposed in postgres to change settings on a columnar
 * table. Calling this function on a non-columnar table gives an error.
 *
 * sql syntax:
 *   pg_catalog.alter_columnar_table_set(
 *        table_name regclass,
 *        chunk_group_row_limit int DEFAULT NULL,
 *        stripe_row_limit int DEFAULT NULL,
 *        compression name DEFAULT null)
 *
 * All arguments except the table name are optional. The UDF is supposed to be called
 * like:
 *   SELECT alter_columnar_table_set('table', compression => 'pglz');
 *
 * This will only update the compression of the table, keeping all other settings the
 * same. Multiple settings can be changed at the same time by providing multiple
 * arguments. Calling the argument with the NULL value will be interperted as not having
 * provided the argument.
 */
PG_FUNCTION_INFO_V1(alter_columnar_table_set);
Datum
alter_columnar_table_set(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation rel = table_open(relationId, AccessExclusiveLock); /* ALTER TABLE LOCK */
	if (!IsColumnarTableAmTable(relationId))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}
#if PG_VERSION_NUM >= PG_VERSION_16
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
#else
	if (!pg_class_ownercheck(relationId, GetUserId()))
#endif
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}

	ColumnarOptions options = { 0 };
	if (!ReadColumnarOptions(relationId, &options))
	{
		ereport(ERROR, (errmsg("unable to read current options for table")));
	}

	/* chunk_group_row_limit => not null */
	if (!PG_ARGISNULL(1))
	{
		options.chunkRowCount = PG_GETARG_INT32(1);
		if (options.chunkRowCount < CHUNK_ROW_COUNT_MINIMUM ||
			options.chunkRowCount > CHUNK_ROW_COUNT_MAXIMUM)
		{
			ereport(ERROR, (errmsg("chunk group row count limit out of range"),
							errhint("chunk group row count limit must be between "
									UINT64_FORMAT " and " UINT64_FORMAT,
									(uint64) CHUNK_ROW_COUNT_MINIMUM,
									(uint64) CHUNK_ROW_COUNT_MAXIMUM)));
		}
		ereport(DEBUG1,
				(errmsg("updating chunk row count to %d", options.chunkRowCount)));
	}

	/* stripe_row_limit => not null */
	if (!PG_ARGISNULL(2))
	{
		options.stripeRowCount = PG_GETARG_INT32(2);
		if (options.stripeRowCount < STRIPE_ROW_COUNT_MINIMUM ||
			options.stripeRowCount > STRIPE_ROW_COUNT_MAXIMUM)
		{
			ereport(ERROR, (errmsg("stripe row count limit out of range"),
							errhint("stripe row count limit must be between "
									UINT64_FORMAT " and " UINT64_FORMAT,
									(uint64) STRIPE_ROW_COUNT_MINIMUM,
									(uint64) STRIPE_ROW_COUNT_MAXIMUM)));
		}
		ereport(DEBUG1, (errmsg(
							 "updating stripe row count to " UINT64_FORMAT,
							 options.stripeRowCount)));
	}

	/* compression => not null */
	if (!PG_ARGISNULL(3))
	{
		Name compressionName = PG_GETARG_NAME(3);
		options.compressionType = ParseCompressionType(NameStr(*compressionName));
		if (options.compressionType == COMPRESSION_TYPE_INVALID)
		{
			ereport(ERROR, (errmsg("unknown compression type for columnar table: %s",
								   quote_identifier(NameStr(*compressionName)))));
		}
		ereport(DEBUG1, (errmsg("updating compression to %s",
								CompressionTypeStr(options.compressionType))));
	}

	/* compression_level => not null */
	if (!PG_ARGISNULL(4))
	{
		options.compressionLevel = PG_GETARG_INT32(4);
		if (options.compressionLevel < COMPRESSION_LEVEL_MIN ||
			options.compressionLevel > COMPRESSION_LEVEL_MAX)
		{
			ereport(ERROR, (errmsg("compression level out of range"),
							errhint("compression level must be between %d and %d",
									COMPRESSION_LEVEL_MIN,
									COMPRESSION_LEVEL_MAX)));
		}

		ereport(DEBUG1, (errmsg("updating compression level to %d",
								options.compressionLevel)));
	}

	if (ColumnarTableSetOptions_hook != NULL)
	{
		ColumnarTableSetOptions_hook(relationId, options);
	}

	SetColumnarOptions(relationId, &options);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}


/*
 * alter_columnar_table_reset is a UDF exposed in postgres to reset the settings on a
 * columnar table. Calling this function on a non-columnar table gives an error.
 *
 * sql syntax:
 *   pg_catalog.alter_columnar_table_re
 *   teset(
 *        table_name regclass,
 *        chunk_group_row_limit bool DEFAULT FALSE,
 *        stripe_row_limit bool DEFAULT FALSE,
 *        compression bool DEFAULT FALSE)
 *
 * All arguments except the table name are optional. The UDF is supposed to be called
 * like:
 *   SELECT alter_columnar_table_set('table', compression => true);
 *
 * All options set to true will be reset to the default system value.
 */
PG_FUNCTION_INFO_V1(alter_columnar_table_reset);
Datum
alter_columnar_table_reset(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation rel = table_open(relationId, AccessExclusiveLock); /* ALTER TABLE LOCK */
	if (!IsColumnarTableAmTable(relationId))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

#if PG_VERSION_NUM >= PG_VERSION_16
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
#else
	if (!pg_class_ownercheck(relationId, GetUserId()))
#endif
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}

	ColumnarOptions options = { 0 };
	if (!ReadColumnarOptions(relationId, &options))
	{
		ereport(ERROR, (errmsg("unable to read current options for table")));
	}

	/* chunk_group_row_limit => true */
	if (!PG_ARGISNULL(1) && PG_GETARG_BOOL(1))
	{
		options.chunkRowCount = columnar_chunk_group_row_limit;
		ereport(DEBUG1,
				(errmsg("resetting chunk row count to %d", options.chunkRowCount)));
	}

	/* stripe_row_limit => true */
	if (!PG_ARGISNULL(2) && PG_GETARG_BOOL(2))
	{
		options.stripeRowCount = columnar_stripe_row_limit;
		ereport(DEBUG1,
				(errmsg("resetting stripe row count to " UINT64_FORMAT,
						options.stripeRowCount)));
	}

	/* compression => true */
	if (!PG_ARGISNULL(3) && PG_GETARG_BOOL(3))
	{
		options.compressionType = columnar_compression;
		ereport(DEBUG1, (errmsg("resetting compression to %s",
								CompressionTypeStr(options.compressionType))));
	}

	/* compression_level => true */
	if (!PG_ARGISNULL(4) && PG_GETARG_BOOL(4))
	{
		options.compressionLevel = columnar_compression_level;
		ereport(DEBUG1, (errmsg("reseting compression level to %d",
								columnar_compression_level)));
	}

	if (ColumnarTableSetOptions_hook != NULL)
	{
		ColumnarTableSetOptions_hook(relationId, options);
	}

	SetColumnarOptions(relationId, &options);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}


/*
 * upgrade_columnar_storage - upgrade columnar storage to the current
 * version.
 *
 * DDL:
 *   CREATE OR REPLACE FUNCTION upgrade_columnar_storage(rel regclass)
 *     RETURNS VOID
 *     STRICT
 *     LANGUAGE c AS 'MODULE_PATHNAME', 'upgrade_columnar_storage';
 */
PG_FUNCTION_INFO_V1(upgrade_columnar_storage);
Datum
upgrade_columnar_storage(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	/*
	 * ACCESS EXCLUSIVE LOCK is not required by the low-level routines, so we
	 * can take only an ACCESS SHARE LOCK. But all access to non-current
	 * columnar tables will fail anyway, so it's better to take ACCESS
	 * EXCLUSIVE LOCK now.
	 */
	Relation rel = table_open(relid, AccessExclusiveLock);
	if (!IsColumnarTableAmTable(relid))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	ColumnarStorageUpdateIfNeeded(rel, true);

	table_close(rel, AccessExclusiveLock);
	PG_RETURN_VOID();
}


/*
 * downgrade_columnar_storage - downgrade columnar storage to the
 * current version.
 *
 * DDL:
 *   CREATE OR REPLACE FUNCTION downgrade_columnar_storage(rel regclass)
 *     RETURNS VOID
 *     STRICT
 *     LANGUAGE c AS 'MODULE_PATHNAME', 'downgrade_columnar_storage';
 */
PG_FUNCTION_INFO_V1(downgrade_columnar_storage);
Datum
downgrade_columnar_storage(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	/*
	 * ACCESS EXCLUSIVE LOCK is not required by the low-level routines, so we
	 * can take only an ACCESS SHARE LOCK. But all access to non-current
	 * columnar tables will fail anyway, so it's better to take ACCESS
	 * EXCLUSIVE LOCK now.
	 */
	Relation rel = table_open(relid, AccessExclusiveLock);
	if (!IsColumnarTableAmTable(relid))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	ColumnarStorageUpdateIfNeeded(rel, false);

	table_close(rel, AccessExclusiveLock);
	PG_RETURN_VOID();
}

typedef struct StripeHole
{
	uint64 fileOffset;
	uint64 dataLength;
} StripeHole;

/*
 * HolesForRelation returns a list of holes in the Relation id in the current
 * MemoryContext.
 */
static List *HolesForRelation(Relation rel)
{
	List *holes = NIL;

	/* Get current columnar options */
	ColumnarOptions columnarOptions = { 0 };
	ReadColumnarOptions(rel->rd_id, &columnarOptions);

	ListCell *lc = NULL;

#if PG_VERSION_NUM >= PG_VERSION_16
	List *stripeMetadataList = StripesForRelfilenode(rel->rd_locator, ForwardScanDirection);
#else
	List *stripeMetadataList = StripesForRelfilenode(rel->rd_node, ForwardScanDirection);
#endif
	uint64 lastMinimalOffset = ColumnarFirstLogicalOffset;

	foreach(lc, stripeMetadataList)
	{
		StripeMetadata * stripeMetadata = lfirst(lc);

		/* Arbitrary check to see if the offset will be large enough, 10000 bytes is what was chosen. */
		if (stripeMetadata->fileOffset == lastMinimalOffset || stripeMetadata->fileOffset - lastMinimalOffset < 10000) {
			lastMinimalOffset = stripeMetadata->fileOffset + stripeMetadata->dataLength;
			continue;
		} else {
			StripeHole * hole = palloc(sizeof(StripeHole));
			hole->fileOffset = lastMinimalOffset;
			hole->dataLength = stripeMetadata->fileOffset + stripeMetadata->dataLength - lastMinimalOffset;
			lastMinimalOffset = stripeMetadata->fileOffset + stripeMetadata->dataLength;

			holes = lappend(holes, hole);
		}
	}

	return holes;
}

/*
 * This is a check whether we need to bail out of a vacuum, it is set by the
 * signal handler, and checked by the vacuum UDF process.
 */
static bool need_to_bail = false;
static int last_signal = 0;
static struct sigaction abt_action;
static struct sigaction int_action;
static struct sigaction trm_action;
static struct sigaction kil_action;

/*
 * vacuum_signal_handler - catch any signals sent during the UDF vacuum, and
 * attempt to clean up as much as we can and bail as cleanly as possible.
 */
static void vacuum_signal_handler(int signal)
{
	elog(DEBUG3, "Received signal %d during a vacuum request", signal);

	/* Set up the ability to bail from the vacuum, with needed information. */
	need_to_bail = true;
	last_signal = signal;
}

/*
 * vacuum_columnar_table
 */
typedef struct StripeVacuumCandidate
{
	uint64 stripeId;
	int32 stripeMetadataIndex;
	uint32 candidateTotalSize;
	uint32 activeRows;
	StripeMetadata *stripeMetadata;
} StripeVacuumCandidate;

PG_FUNCTION_INFO_V1(vacuum_columnar_table);
Datum
vacuum_columnar_table(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Relation rel = RelationIdGetRelation(relid);
	TupleDesc tupleDesc = RelationGetDescr(rel);
	MemoryContext oldcontext = CurrentMemoryContext;
	uint32 stripeCount = PG_GETARG_UINT32(1);
	uint32 progress = 0;
	bool completelyDone = false;
	struct sigaction action;

	/* Capture the cache state and disable it for a vacuum. */
	bool old_cache_mode = columnar_enable_page_cache;
	columnar_enable_page_cache = false;

	/*
	 * Set up signal handlers for any incoming signals during the vacuum,
	 * killing during a write could cause corruption.  Give us time to
	 * clean as much as we can up before letting the signal pass on.
	 */
	need_to_bail = false;
	last_signal = 0;

	action.sa_handler = vacuum_signal_handler;
	sigemptyset (&action.sa_mask);
	action.sa_flags = 0;

	sigaction(SIGINT, &action, &int_action);
	sigaction(SIGTERM, &action, &trm_action);
	sigaction(SIGABRT, &action, &abt_action);
	sigaction(SIGKILL, &action, &kil_action);

	MemoryContext vacuum_context = AllocSetContextCreate(CurrentMemoryContext,
						"Columnar Vacuum Context",
						ALLOCSET_SMALL_SIZES);

	oldcontext = MemoryContextSwitchTo(vacuum_context);

	if (tupleDesc->natts == 0)
	{
		ereport(INFO,
				(errmsg("\"%s\": stopping vacuum due to zero column table",
						RelationGetRelationName(rel))));

		MemoryContextSwitchTo(oldcontext);

		/* Reset cache to previous state. */
		columnar_enable_page_cache = old_cache_mode;

		PG_RETURN_VOID();
	}

	LockRelation(rel, ExclusiveLock);

	/* Get current columnar options */
	ColumnarOptions columnarOptions = { 0 };
	ReadColumnarOptions(rel->rd_id, &columnarOptions);

	/* Get all stripes in order. */
#if PG_VERSION_NUM >= PG_VERSION_16
	List *stripeMetadataList = StripesForRelfilenode(rel->rd_locator, ForwardScanDirection);
#else
	List *stripeMetadataList = StripesForRelfilenode(rel->rd_node, ForwardScanDirection);
#endif
	List *vacuumCandidatesStripeList = NIL;

	/* Empty table nothing to do */
	if (stripeMetadataList == NIL)
	{
		ereport(INFO,
				(errmsg("\"%s\": stopping vacuum due to empty table",
						RelationGetRelationName(rel))));

		/* Close the relation as we are done with it */
		RelationClose(rel);

		MemoryContextSwitchTo(oldcontext);

		/* Reset cache to previous state. */
		columnar_enable_page_cache = old_cache_mode;

		PG_RETURN_VOID();
	}

	ListCell *lc = NULL;

	int stripeMetadataIndex = 0;

	elog(DEBUG3, "Beginning combination of stripes");

	/*
	 * Get a list of all stripes that can be combined into larger stripes.
	 */
	foreach(lc, stripeMetadataList)
	{
		/* We should not consider the last stripe. */
		if (lfirst(lc) == llast(stripeMetadataList))
		{
			break;
		}

		StripeMetadata * stripeMetadata = lfirst(lc);
#if PG_VERSION_NUM >= PG_VERSION_16
		uint32 stripeDeletedRows = DeletedRowsForStripe(rel->rd_locator,
												 		stripeMetadata->chunkCount,
														stripeMetadata->id);
#else
		uint32 stripeDeletedRows = DeletedRowsForStripe(rel->rd_node,
												 		stripeMetadata->chunkCount,
														stripeMetadata->id);
#endif
		float percentageOfDeleteRows =
			(float)stripeDeletedRows / (float)(stripeMetadata->rowCount);

		/*
		 * If inspected stripe has less than 0.5 percent of maximum strip row size
		 * or percentage of deleted rows is less than 20% we will skip this stripe
		 * for vacuum.
		*/
		if ((stripeMetadata->rowCount > columnarOptions.stripeRowCount * 0.5) &&
			percentageOfDeleteRows <= 0.2f)
		{
			continue;
		}

		StripeVacuumCandidate *vacuumCandidate = palloc(sizeof(StripeVacuumCandidate));
		vacuumCandidate->stripeMetadataIndex = stripeMetadataIndex;
		vacuumCandidate->candidateTotalSize = stripeMetadata->rowCount - stripeDeletedRows;
		vacuumCandidate->stripeMetadata = stripeMetadata;
		vacuumCandidate->activeRows = stripeMetadata->rowCount - stripeDeletedRows;

		vacuumCandidatesStripeList = lappend(vacuumCandidatesStripeList, vacuumCandidate);
	}

	/* We need all columns. */
	int natts = rel->rd_att->natts;
	Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

	/* No quals for table rewrite */
	List *scanQual = NIL;

	/* Use SnapshotAny when re-writing table as heapAM does. */
	Snapshot snapshot = SnapshotAny;

	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	bool randomAccess = true;

#if PG_VERSION_NUM >= PG_VERSION_16
	ColumnarWriteState *writeState = ColumnarBeginWrite(rel->rd_locator,
											columnarOptions,
											tupleDesc);
#else
	ColumnarWriteState *writeState = ColumnarBeginWrite(rel->rd_node,
											columnarOptions,
											tupleDesc);
#endif

	/*
	 * Combine the vacuum candidates into their own stripes appended to the rel, this
	 * should clear out any space from partial stripes to make space to move stripes into.
	 */
	foreach(lc, vacuumCandidatesStripeList)
	{
		StripeVacuumCandidate *vacuumCandidate = lfirst(lc);

		MemoryContext combineContext = AllocSetContextCreate(CurrentMemoryContext,
						"Stripe Combine Context", ALLOCSET_DEFAULT_SIZES);
		MemoryContext previousContext = MemoryContextSwitchTo(combineContext);

		ColumnarReadState *readState = init_columnar_read_state(rel, tupleDesc,
															attr_needed, scanQual,
															scanContext, snapshot,
															randomAccess,
															NULL);


		ColumnarSetStripeReadState(readState,
								vacuumCandidate->stripeMetadata);

		Datum *values = palloc0(tupleDesc->natts * sizeof(Datum));
		bool *nulls = palloc0(tupleDesc->natts * sizeof(bool));

		int32 rowCount = 0;

		while (rowCount < vacuumCandidate->activeRows && ColumnarReadNextRow(readState, values, nulls, NULL))
		{
			ColumnarWriteRow(writeState, values, nulls);
			rowCount++;
		}

#if PG_VERSION_NUM >= PG_VERSION_16
		DeleteMetadataRowsForStripeId(rel->rd_locator, vacuumCandidate->stripeMetadata->id);
#else
		DeleteMetadataRowsForStripeId(rel->rd_node, vacuumCandidate->stripeMetadata->id);
#endif
		ColumnarEndRead(readState);

		pfree(values);
		pfree(nulls);

		progress++;

		/* Check if a signal has been sent, if so close out and deal with it. */
		if (need_to_bail)
		{
			ColumnarEndWrite(writeState);
			UnlockRelation(rel, ExclusiveLock);
			relation_close(rel, NoLock);

			need_to_bail = 0;
			/* Reset the signal handlers back to their original state. */
			sigaction(SIGINT, &int_action, NULL);
			sigaction(SIGTERM, &trm_action, NULL);
			sigaction(SIGABRT, &abt_action, NULL);
			sigaction(SIGKILL, &kil_action, NULL);

			/* Call any missed signal handlers. */
			if (last_signal == SIGABRT && abt_action.sa_handler)
			{
				abt_action.sa_handler(SIGABRT);
			} else if (last_signal == SIGTERM && trm_action.sa_handler)
			{
				trm_action.sa_handler(SIGTERM);
			} else if (last_signal == SIGINT && int_action.sa_handler)
			{
				int_action.sa_handler(SIGINT);
			} else if (last_signal == SIGKILL && kil_action.sa_handler)
			{
				kil_action.sa_handler(SIGKILL);
			}

			/* Reset cache to previous state. */
			columnar_enable_page_cache = old_cache_mode;

			PG_RETURN_NULL();
		}

		if (stripeCount && progress >= stripeCount)
		{
			completelyDone = true;
			break;
		}

		MemoryContextSwitchTo(previousContext);
		MemoryContextDelete(combineContext);
	}
	/*
	 * We have finished the first route of writes, let's drop our lock until we are
	 * ready for the next stage: compaction.
	 */
	ColumnarEndWrite(writeState);

	elog(DEBUG3, "Combined %d stripes", progress);

	if (completelyDone)
	{
		relation_close(rel, NoLock);
		TruncateColumnar(rel, DEBUG3);
		UnlockRelation(rel, ExclusiveLock);

		/* Reset cache to previous state. */
		columnar_enable_page_cache = old_cache_mode;

		PG_RETURN_UINT32(progress);
	}

	elog(DEBUG3, "Beginning reorganization");
	/*
	 * Continually iterate through the holes, finding where we can place
	 * old stripes.
	 */
	bool done = false;

	uint32 relocationProgress = 0;

	while (!done)
	{
		MemoryContext rewriteContext = AllocSetContextCreate(CurrentMemoryContext,
					"Stripe Rewrite Context", ALLOCSET_DEFAULT_SIZES);
		MemoryContext previousContext = MemoryContextSwitchTo(rewriteContext);

		/* Check if a signal has been sent, if so close out and deal with it. */
		if (need_to_bail)
		{
			relation_close(rel, NoLock);
			TruncateColumnar(rel, DEBUG3);
			UnlockRelation(rel, ExclusiveLock);
			ForceSyncCommit();

			need_to_bail = 0;
			/* Reset the signal handlers back to their original state. */
			sigaction(SIGINT, &int_action, NULL);
			sigaction(SIGTERM, &trm_action, NULL);
			sigaction(SIGABRT, &abt_action, NULL);
			sigaction(SIGKILL, &kil_action, NULL);

			/* Call any missed signal handlers. */
			if (last_signal == SIGABRT && abt_action.sa_handler)
			{
				abt_action.sa_handler(SIGABRT);
			} else if (last_signal == SIGTERM && trm_action.sa_handler)
			{
				trm_action.sa_handler(SIGTERM);
			} else if (last_signal == SIGINT && int_action.sa_handler)
			{
				int_action.sa_handler(SIGINT);
			} else if (last_signal == SIGKILL && kil_action.sa_handler)
			{
				kil_action.sa_handler(SIGKILL);
			}

			/* Reset cache to previous state. */
			columnar_enable_page_cache = old_cache_mode;

			PG_RETURN_NULL();
		}

		/*
		 * Get a List of empty spaces to fill with later stripes.
		 */
		List *holes = HolesForRelation(rel);

		if (list_length(holes) == 0)
		{
			done = true;
			continue;
		}

		/*
		 * Iterate through the holes, moving later slices into the holes.
		 */
		foreach(lc, holes)
		{
			StripeHole *hole = lfirst(lc);

			ListCell *stripeLc = NULL;

			/* Reload the metadata. */
#if PG_VERSION_NUM >= PG_VERSION_16
			stripeMetadataList = StripesForRelfilenode(rel->rd_locator, ForwardScanDirection);
#else
			stripeMetadataList = StripesForRelfilenode(rel->rd_node, ForwardScanDirection);
#endif
			foreach(stripeLc, stripeMetadataList)
			{
				StripeMetadata *stripe = lfirst(stripeLc);

				/* If we are marked done, we should simply drop out. */
				if (done)
				{
					break;
				}

				/* Find one that will fit, and move it. */
				if (hole->fileOffset && stripe->dataLength < hole->dataLength && stripe->fileOffset > hole->fileOffset)
				{
					/* Read a copy of the old row. */
					char * data = palloc(stripe->dataLength);
					ColumnarStorageRead(rel, stripe->fileOffset, data, stripe->dataLength);

					/* Write the data to the new offset. */
					ColumnarStorageWrite(rel, hole->fileOffset, data, stripe->dataLength);

					/* Update the stripe metadata for the moved stripe. */
					RewriteStripeMetadataRowWithNewValues(rel, stripe->id, stripe->dataLength, hole->fileOffset, stripe->rowCount, stripe->chunkCount);

					relocationProgress++;

					pfree(data);

					if (relocationProgress >= 1)
					{
						done = true;
					}

					break;
				}
			}


			if (relocationProgress >= 1)
			{
				done = true;
			}

			holes = HolesForRelation(rel);
		}

		MemoryContextSwitchTo(previousContext);
		MemoryContextDelete(rewriteContext);
	}

	elog(DEBUG3, "Ending reorganization");

	relation_close(rel, NoLock);

	TruncateColumnar(rel, DEBUG3);
	UnlockRelation(rel, ExclusiveLock);

	MemoryContextSwitchTo(oldcontext);

	/* Reset the signal handlers back to their original state. */
	sigaction(SIGINT, &int_action, NULL);
	sigaction(SIGTERM, &trm_action, NULL);
	sigaction(SIGABRT, &abt_action, NULL);
	sigaction(SIGKILL, &kil_action, NULL);

	/* Reset cache to previous state. */
	columnar_enable_page_cache = old_cache_mode;

	PG_RETURN_UINT32(progress + relocationProgress);
}

/*
 * Data storage for columnar stats.
 */
typedef struct ColumnarStats
{
	uint64 stripeId;
	uint64 fileOffset;
	uint32 rowCount;
	uint32 deletedRows;
	uint32 chunkCount;
	uint32 dataLength;
} ColumnarStats;

/* We return 6 columns. */
#define STATS_INFO_NATTS 6

PG_FUNCTION_INFO_V1(columnar_stats);
Datum
columnar_stats(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int call_cntr;
	int max_calls;
	TupleDesc tupdesc;
	AttInMetadata *attinmeta;


	/* If this is the first call in, set up the data. */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		Oid relid = PG_GETARG_OID(0);
		Relation rel = RelationIdGetRelation(relid);

		/* Function context for persistance. */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Use the SRF memory context */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Retrieve the stripe metadata. */
#if PG_VERSION_NUM >= PG_VERSION_16
		List *stripeMetadataList = StripesForRelfilenode(rel->rd_locator, ForwardScanDirection);
#else
		List *stripeMetadataList = StripesForRelfilenode(rel->rd_node, ForwardScanDirection);
#endif
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
				ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("function returning record called in context "
												"that cannot accept type record")));

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		/* Set up the stats. */
		ColumnarStats *stats = palloc(sizeof(ColumnarStats) * list_length(stripeMetadataList));
		funcctx->max_calls = list_length(stripeMetadataList);

		/* Iterate through the stripes to get a copy of the important data. */
		for (int i = 0; i < funcctx->max_calls; i++)
		{
			StripeMetadata *data = list_nth(stripeMetadataList, i);

			stats[i].stripeId = data->id;
			stats[i].fileOffset = data->fileOffset;
			stats[i].rowCount = data->rowCount;
			stats[i].chunkCount = data->chunkCount;
			stats[i].dataLength = data->dataLength;
#if PG_VERSION_NUM >= PG_VERSION_16
			stats[i].deletedRows = DeletedRowsForStripe(rel->rd_locator,
												 		data->chunkCount,
														data->id);
#else
			stats[i].deletedRows = DeletedRowsForStripe(rel->rd_node,
												 		data->chunkCount,
														data->id);
#endif
		}

		funcctx->user_fctx = stats;

		table_close(rel, NoLock);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	ColumnarStats *stats = funcctx->user_fctx;

	if (call_cntr < max_calls)
	{
		Datum result;

		Datum values[STATS_INFO_NATTS] = { 0 };
		bool nulls[STATS_INFO_NATTS] = { 0 };

		get_call_result_type(fcinfo, NULL, &tupdesc);

		values[0] = Int64GetDatum(stats[call_cntr].stripeId);
		values[1] = Int64GetDatum(stats[call_cntr].fileOffset);
		values[2] = Int32GetDatum(stats[call_cntr].rowCount);
		values[3] = Int32GetDatum(stats[call_cntr].deletedRows);
		values[4] = Int32GetDatum(stats[call_cntr].chunkCount);
		values[5] = Int32GetDatum(stats[call_cntr].dataLength);

		HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}
