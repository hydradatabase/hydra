/*-------------------------------------------------------------------------
 *
 * columnar_read_state_cache.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * ColumnarReadState cache is used to have active read state in transaction when
 * update is used. We need this sort of cacheing because `tuple_fetch_row_version`
 * need to fetch row and state variable is not shared / supported between calls.
 *
 * Note: Probably we need to rethink this again and provide more general cache for
 * already read stripes / chunks but on global level.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM >= 130000
#include "access/heaptoast.h"
#include "common/hashfn.h"
#else
#include "access/tuptoaster.h"
#endif
#include "executor/executor.h"

#include "columnar/columnar_tableam.h"

/*
 * Mapping from relfilelocator to RowMaskCacheMapEntry. This keeps deleted rows for
 * each relation.
 */
static HTAB *ColumnarReadStateMap = NULL;

/* memory context for allocating RowMaskCache & all write states */
static MemoryContext ColumnarReadStateContext = NULL;

/*
 * Each member of the writeStateStack in WriteStateMapEntry. This means that
 * we did some inserts in the subtransaction subXid, and the state of those
 * inserts is stored at writeState. Those writes can be flushed or unflushed.
*/
typedef struct SubXidWriteState
{
	SubTransactionId subXid;
    ColumnarReadState *readState;
	struct SubXidWriteState *next;
} SubXidWriteState;

/*
 * An entry in RowMaskCacheMap.
 */
typedef struct ColumnarReadStateMapEntry
{
	/* key of the entry */
	Oid relfilelocator;

	/*
	* Stack of SubXidWriteState where first element is top of the stack. When
	* inserts happen, we look at top of the stack. If top of stack belongs to
	* current subtransaction, we forward writes to its writeState. Otherwise,
	* we create a new stack entry for current subtransaction and push it to
	* the stack, and forward writes to that.
	*/
	SubXidWriteState *writeStateStack;
} ColumnarReadStateMapEntry;

/*
 * Memory context reset callback so we reset RowMaskCacheMap to NULL at the end
 * of transaction. RowMaskCacheMap is allocated in & RowMaskCacheMap, so its
 * leaked reference can cause memory issues.
 */
static MemoryContextCallback cleanupCallback;
static void CleanupColumnarReadStateMap(void *arg)
{
	ColumnarReadStateMap = NULL;
	ColumnarReadStateContext = NULL;
}

ColumnarReadState **
InitColumnarReadStateCache(Relation relation, SubTransactionId currentSubXid)
{
	bool found;

	/*
	* If this is the first call in current transaction, allocate the hash
	* table.
	*/
	if (ColumnarReadStateMap == NULL) 
	{
		ColumnarReadStateContext = AllocSetContextCreate(
			TopTransactionContext, "Columnar Read State context",
			ALLOCSET_DEFAULT_SIZES);
		HASHCTL info;
		uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(Oid);
		info.hash = oid_hash;
		info.entrysize = sizeof(ColumnarReadStateMapEntry);
		info.hcxt = ColumnarReadStateContext;

		ColumnarReadStateMap =
			hash_create("columnar read staate map", 64, &info, hashFlags);

		cleanupCallback.arg = NULL;
		cleanupCallback.func = &CleanupColumnarReadStateMap;
		cleanupCallback.next = NULL;
		MemoryContextRegisterResetCallback(ColumnarReadStateContext,
										   &cleanupCallback);
	}

#if PG_VERSION_NUM >= PG_VERSION_16
	ColumnarReadStateMapEntry *hashEntry =
		hash_search(ColumnarReadStateMap, &relation->rd_locator.relNumber, HASH_ENTER, &found);
#else
	ColumnarReadStateMapEntry *hashEntry =
		hash_search(ColumnarReadStateMap, &relation->rd_node.relNode, HASH_ENTER, &found);
#endif

	if (!found)
	{
		hashEntry->writeStateStack = NULL;
	}

	SubXidWriteState *stackEntry = NULL;
	if (hashEntry->writeStateStack != NULL)
	{
		SubXidWriteState *stackHead = hashEntry->writeStateStack;

		if (stackHead->subXid == currentSubXid)
		{
			stackEntry = stackHead;
		}
	}

	MemoryContext oldContext = MemoryContextSwitchTo(ColumnarReadStateContext);

	if (stackEntry == NULL)
	{
		stackEntry = palloc0(sizeof(SubXidWriteState));
		stackEntry->subXid = currentSubXid;
		stackEntry->next = hashEntry->writeStateStack;
		hashEntry->writeStateStack = stackEntry;
	}

	MemoryContextSwitchTo(oldContext);

	return &stackEntry->readState;
}


ColumnarReadState **
FindReadStateCache(Relation relation, SubTransactionId currentSubXid)
{
	/* Empty Cache */
	if (ColumnarReadStateMap == NULL)
	{
		return NULL;
	}

	bool found;

#if PG_VERSION_NUM >= PG_VERSION_16
	ColumnarReadStateMapEntry *hashEntry =
		hash_search(ColumnarReadStateMap, &relation->rd_locator.relNumber, HASH_FIND, &found);
#else
	ColumnarReadStateMapEntry *hashEntry =
		hash_search(ColumnarReadStateMap, &relation->rd_node.relNode, HASH_FIND, &found);
#endif

	/* No cache for table found*/
	if (!found)
	{
		return NULL;
	}

	SubXidWriteState *stackHead = hashEntry->writeStateStack;

	while (stackHead)
	{
		/* Found SubTransactionId */
		if (stackHead->subXid == currentSubXid) 
		{
			return &stackHead->readState;
		}

		stackHead = stackHead->next;
	}

	return NULL;
}


void 
CleanupReadStateCache(SubTransactionId currentSubXid)
{
	HASH_SEQ_STATUS status;
	ColumnarReadStateMapEntry *entry;

	if (ColumnarReadStateMap == NULL)
	{
		return;
	}

	hash_seq_init(&status, ColumnarReadStateMap);

	while ((entry = hash_seq_search(&status)) != 0)
	{
		if (entry->writeStateStack == NULL) 
		{
			continue;
		}

		SubXidWriteState *stackHead = entry->writeStateStack;
		if (stackHead->subXid == currentSubXid)
		{
			ColumnarEndRead(stackHead->readState);
			entry->writeStateStack = stackHead->next;
		}
	}
}


MemoryContext GetColumnarReadStateCache(void)
{
	return ColumnarReadStateContext;
}