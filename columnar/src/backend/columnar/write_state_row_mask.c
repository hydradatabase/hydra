/*-------------------------------------------------------------------------
 *
 * write_state_row_mask.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * Write state for columnar.row_mask table. It is used to aggregate updates
 * for rows and flush them when needed (commit / scan).
 *
 * Following code is done in similiar way as write_state_management module.
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

static RowMaskWriteStateEntry *InitRowMaskEntry(uint64 storageId,
												bytea *rowMask);
static void RowMaskFlushPendingWriteState(List *rowMaskWriteStateList);

/*
 * Mapping from relfilelocator to RowMaskCacheMapEntry. This keeps deleted rows for
 * each relation.
 */
static HTAB *RowMaskWriteStateMap = NULL;

/* memory context for allocating RowMaskCache & all write states */
static MemoryContext RowMaskWriteStateContext = NULL;

/*
 * Each member of the writeStateStack in WriteStateMapEntry. This means that
 * we did some inserts in the subtransaction subXid, and the state of those
 * inserts is stored at writeState. Those writes can be flushed or unflushed.
*/
typedef struct SubXidWriteState
{
	SubTransactionId subXid;
	List *rowMaskWriteStateEntryList;
	RowMaskWriteStateEntry *lastRowMaskWriteStateEntry;
	struct SubXidWriteState *next;
} SubXidWriteState;

/*
 * An entry in RowMaskCacheMap.
 */
typedef struct RowMaskWriteStateMapEntry
{
	/* key of the entry */
	Oid relfilelocator;

	/*
	* If a table is dropped, we set dropped to true and set dropSubXid to the
	* id of the subtransaction in which the drop happened.
	*/
	bool dropped;
	SubTransactionId dropSubXid;

	/*
	* Stack of SubXidWriteState where first element is top of the stack. When
	* inserts happen, we look at top of the stack. If top of stack belongs to
	* current subtransaction, we forward writes to its writeState. Otherwise,
	* we create a new stack entry for current subtransaction and push it to
	* the stack, and forward writes to that.
	*/
	SubXidWriteState *writeStateStack;
} RowMaskWriteStateMapEntry;

/*
 * Memory context reset callback so we reset RowMaskCacheMap to NULL at the end
 * of transaction. RowMaskCacheMap is allocated in & RowMaskCacheMap, so its
 * leaked reference can cause memory issues.
 */
static MemoryContextCallback cleanupCallback;
static void 
CleanupWriteStateMap(void *arg)
{
	RowMaskWriteStateMap = NULL;
	RowMaskWriteStateContext = NULL;
}

RowMaskWriteStateEntry *
RowMaskInitWriteState(Oid relfilelocator,
					  uint64 storageId,
					  SubTransactionId currentSubXid,
					  bytea *rowMask)
{
	bool found;

	/*
	* If this is the first call in current transaction, allocate the hash
	* table.
	*/
	if (RowMaskWriteStateMap == NULL) 
	{
		RowMaskWriteStateContext = AllocSetContextCreate(
			TopTransactionContext, "Row Mask Write State context",
			ALLOCSET_DEFAULT_SIZES);
		HASHCTL info;
		uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(Oid);
		info.hash = oid_hash;
		info.entrysize = sizeof(RowMaskWriteStateMapEntry);
		info.hcxt = RowMaskWriteStateContext;

		RowMaskWriteStateMap =
			hash_create("row mask cache map", 64, &info, hashFlags);

		cleanupCallback.arg = NULL;
		cleanupCallback.func = &CleanupWriteStateMap;
		cleanupCallback.next = NULL;
		MemoryContextRegisterResetCallback(RowMaskWriteStateContext,
										&cleanupCallback);
	}

	RowMaskWriteStateMapEntry *hashEntry =
		hash_search(RowMaskWriteStateMap, &relfilelocator, HASH_ENTER, &found);

	if (!found)
	{
		hashEntry->writeStateStack = NULL;
		hashEntry->dropped = false;
	}

	Assert(!hashEntry->dropped);

	SubXidWriteState *stackEntry = NULL;
	if (hashEntry->writeStateStack != NULL)
	{
		SubXidWriteState *stackHead = hashEntry->writeStateStack;

		if (stackHead->subXid == currentSubXid)
		{
			stackEntry = stackHead;
		}
	}

	MemoryContext oldContext = MemoryContextSwitchTo(RowMaskWriteStateContext);

	if (stackEntry == NULL)
	{
		stackEntry = palloc0(sizeof(SubXidWriteState));
		stackEntry->subXid = currentSubXid;
		stackEntry->next = hashEntry->writeStateStack;
		stackEntry->lastRowMaskWriteStateEntry = NULL;
		hashEntry->writeStateStack = stackEntry;
	}

	RowMaskWriteStateEntry *RowMaskWriteState = InitRowMaskEntry(storageId, rowMask);

	stackEntry->rowMaskWriteStateEntryList =
		lappend(stackEntry->rowMaskWriteStateEntryList, RowMaskWriteState);

	MemoryContextSwitchTo(oldContext);

	return RowMaskWriteState;
}

static RowMaskWriteStateEntry *
InitRowMaskEntry(uint64 storageId, bytea *mask)
{
	RowMaskWriteStateEntry *rowMask = palloc0(sizeof(RowMaskWriteStateEntry));

	rowMask->storageId = storageId;
	rowMask->mask = (bytea *)palloc0(VARSIZE(mask) + VARHDRSZ);
	memcpy(rowMask->mask, mask, VARSIZE(mask) + VARHDRSZ);
	// rest of structure members needs be populated where RowMaskState was created

	return rowMask;
}

static void
RowMaskFlushPendingWriteState(List *rowMaskWriteStateList)
{
	ListCell *lc;
	foreach (lc, rowMaskWriteStateList)
	{
		RowMaskWriteStateEntry *rowMaskCache = (RowMaskWriteStateEntry *)lfirst(lc);
		FlushRowMaskCache(rowMaskCache);
		UpdateChunkGroupDeletedRows(rowMaskCache->storageId, rowMaskCache->stripeId,
									rowMaskCache->chunkId, rowMaskCache->deletedRows);
		pfree(rowMaskCache->mask);
	}
}


/*
 * Flushes pending writes for given relfilelocator in the given subtransaction.
 */
void RowMaskFlushWriteStateForRelfilenode(Oid relfilelocator,
										  SubTransactionId currentSubXid) {
	if (RowMaskWriteStateMap == NULL)
	{
		return;
	}

	RowMaskWriteStateMapEntry *entry =
		hash_search(RowMaskWriteStateMap, &relfilelocator, HASH_FIND, NULL);

	Assert(!entry || !entry->dropped);

	if (entry && entry->writeStateStack != NULL)
	{
		SubXidWriteState *stackEntry = entry->writeStateStack;
		if (stackEntry->subXid == currentSubXid)
		{
			RowMaskFlushPendingWriteState(stackEntry->rowMaskWriteStateEntryList);
			list_free(stackEntry->rowMaskWriteStateEntryList);
			stackEntry->rowMaskWriteStateEntryList = NIL;
		}
	}
}


RowMaskWriteStateEntry *
RowMaskFindWriteState(Oid relfilelocator, SubTransactionId currentSubXid,
					  uint64 rowId)
{
	/* Empty Cache */
	if (RowMaskWriteStateMap == NULL)
	{
		return NULL;
	}

	bool found;

	RowMaskWriteStateMapEntry *hashEntry =
		hash_search(RowMaskWriteStateMap, &relfilelocator, HASH_FIND, &found);

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
			if (stackHead->lastRowMaskWriteStateEntry != NULL &&
				stackHead->lastRowMaskWriteStateEntry->startRowNumber <= rowId &&
				stackHead->lastRowMaskWriteStateEntry->endRowNumber >= rowId) 
			{
				return stackHead->lastRowMaskWriteStateEntry;;
			}

			ListCell *lc;
			foreach (lc, stackHead->rowMaskWriteStateEntryList)
			{
				RowMaskWriteStateEntry *rowMask = (RowMaskWriteStateEntry *)lfirst(lc);

				if (rowMask->startRowNumber <= rowId && rowMask->endRowNumber >= rowId)
				{
					stackHead->lastRowMaskWriteStateEntry = rowMask;
					return rowMask;
				}
			}
		}

		stackHead = stackHead->next;
	}

	return NULL;
}


void 
RowMaskPopWriteStateForAllRels(SubTransactionId currentSubXid,
							   SubTransactionId parentSubXid,
							   bool commit)
{
	HASH_SEQ_STATUS status;
	RowMaskWriteStateMapEntry *entry;

	if (RowMaskWriteStateMap == NULL)
	{
		return;
	}

	hash_seq_init(&status, RowMaskWriteStateMap);

	while ((entry = hash_seq_search(&status)) != 0)
	{
		if (entry->writeStateStack == NULL) 
		{
			continue;
		}

		/*
		 * If the table has been dropped in current subtransaction, either
		 * commit the drop or roll it back.
		 */
		if (entry->dropped)
		{
			if (entry->dropSubXid == currentSubXid)
			{
				if (commit) {
					/* elevate drop to the upper subtransaction */
					entry->dropSubXid = parentSubXid;
				} else {
					/* abort the drop */
					entry->dropped = false;
				}
			}
		}

		/*
		 * Otherwise, commit or discard pending writes.
		 */
		else
		{
			SubXidWriteState *stackHead = entry->writeStateStack;
			if (stackHead->subXid == currentSubXid)
			{
				if (commit)
				{
					RowMaskFlushPendingWriteState(stackHead->rowMaskWriteStateEntryList);
				}
				else
				{
					// cleanup allocated memory ?!
				} 
				entry->writeStateStack = stackHead->next;
			}
		}
	}
}


void
RowMaskMarkRelfilenodeDropped(Oid relfilelocator, SubTransactionId currentSubXid)
{
	if (RowMaskWriteStateMap == NULL)
	{
		return;
	}

	RowMaskWriteStateMapEntry *entry =
		hash_search(RowMaskWriteStateMap, &relfilelocator, HASH_FIND, NULL);
	
	if (!entry || entry->dropped)
	{
		return;
	}

	entry->dropped = true;
	entry->dropSubXid = currentSubXid;
}


void
RowMaskNonTransactionDrop(Oid relfilelocator)
{
	if (RowMaskWriteStateMap)
	{
		hash_search(RowMaskWriteStateMap, &relfilelocator, HASH_REMOVE, false);
	}
}


bool
RowMaskPendingWritesInUpperTransactions(Oid relfilelocator, SubTransactionId currentSubXid)
{
	if (RowMaskWriteStateMap == NULL)
	{
		return false;
	}

	RowMaskWriteStateMapEntry *entry =
		hash_search(RowMaskWriteStateMap, &relfilelocator, HASH_FIND, NULL);

	if (entry && entry->writeStateStack != NULL)
	{
		SubXidWriteState *stackEntry = entry->writeStateStack;

		while (stackEntry != NULL)
		{
			if (stackEntry->subXid != currentSubXid &&
				list_length(stackEntry->rowMaskWriteStateEntryList))
			{
				return true;
			}

			stackEntry = stackEntry->next;
		}
	}

	return false;
}


extern MemoryContext
GetRowMaskWriteStateContextForDebug(void)
{
	return RowMaskWriteStateContext;
}
