/*-------------------------------------------------------------------------
 *
 * columnar_write_state_row_mask.h
 *
 * Declaration for Row Mask Entry
 *
 * Copyright (c) Hydra, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_WRITE_STATE_ROW_MASK_H
#define COLUMNAR_WRITE_STATE_ROW_MASK_H

struct RowMaskWriteStateEntry
{
	uint64 id;
	uint64 storageId;
	uint64 stripeId;
	uint32 chunkId;
	int64 startRowNumber;
	int64 endRowNumber;
	int32 deletedRows;
	bytea *mask;
};

#endif