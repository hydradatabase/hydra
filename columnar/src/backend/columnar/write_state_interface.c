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

#include "columnar/columnar.h"

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
