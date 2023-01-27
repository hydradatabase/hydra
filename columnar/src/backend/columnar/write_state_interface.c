/*-------------------------------------------------------------------------
 *
 * write_state_interface.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * Write state interface is main entry function for functionalities
 * that store in-nemory write state.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/xact.h"

#include "columnar/columnar.h"

void
FlushWriteStateWithNewSnapshot(Oid relfilenode,
							   Snapshot * snapshot,
							   bool * snapshotRegisteredByUs)
{
	FlushWriteStateForRelfilenode(relfilenode, GetCurrentSubTransactionId());

	if (*snapshot == InvalidSnapshot || !IsMVCCSnapshot(*snapshot))
	{
		return;
	}

	/*
	 * If we flushed any pending writes, then we should guarantee that
	 * those writes are visible to us too. For this reason, if given
	 * snapshot is an MVCC snapshot, then we set its curcid to current
	 * command id.
	 *
	 * For simplicity, we do that even if we didn't flush any writes
	 * since we don't see any problem with that.
	 *
	 * XXX: We should either not update cid if we are executing a FETCH
	 * (from cursor) command, or we should have a better way to deal with
	 * pending writes, see the discussion in
	 * https://github.com/citusdata/citus/issues/5231.
	 */
	PushCopiedSnapshot(*snapshot);

	/* now our snapshot is the active one */
	UpdateActiveSnapshotCommandId();
	Snapshot newSnapshot = GetActiveSnapshot();
	RegisterSnapshot(newSnapshot);

	/*
	 * To be able to use UpdateActiveSnapshotCommandId, we pushed the
	 * copied snapshot to the stack. However, we don't need to keep it
	 * there since we will anyway rely on ColumnarReadState->snapshot
	 * during read operation.
	 *
	 * Note that since we registered the snapshot already, we guarantee
	 * that PopActiveSnapshot won't free it.
	 */
	PopActiveSnapshot();

	*snapshot = newSnapshot;
	*snapshotRegisteredByUs = true;
}


/*
 * Called when current subtransaction is committed.
 */
void
FlushWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId parentSubXid)
{
	ColumnarPopWriteStateForAllRels(currentSubXid, parentSubXid, true);
	RowMaskPopWriteStateForAllRels(currentSubXid, parentSubXid, true);
}


/*
 * Called when current subtransaction is aborted.
 */
void
DiscardWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId parentSubXid)
{
	ColumnarPopWriteStateForAllRels(currentSubXid, parentSubXid, false);
	RowMaskPopWriteStateForAllRels(currentSubXid, parentSubXid, false);
}


/*
 * Called when the given relfilenode is dropped. Calls both
 * write_state_management and row mask write state.
 */
void
MarkRelfilenodeDropped(Oid relfilenode, SubTransactionId currentSubXid)
{
	ColumnarMarkRelfilenodeDroppedColumnar(relfilenode, currentSubXid);
	RowMaskMarkRelfilenodeDropped(relfilenode, currentSubXid);
}


/*
 * Called when the given relfilenode is dropped in non-transactional TRUNCATE.
 */
void
NonTransactionDropWriteState(Oid relfilenode)
{
	ColumnarNonTransactionDropWriteState(relfilenode);
	RowMaskNonTransactionDrop(relfilenode);
}

/*
 * Returns true if there are any pending writes in upper transactions.
 */
bool
PendingWritesInUpperTransactions(Oid relfilenode, SubTransactionId currentSubXid)
{
	return (ColumnarPendingWritesInUpperTransactions(relfilenode, currentSubXid) ||
			RowMaskPendingWritesInUpperTransactions(relfilenode, currentSubXid));
}
