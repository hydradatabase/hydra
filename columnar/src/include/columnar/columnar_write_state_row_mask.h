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
	int64 id;
	int64 storageId;
	int64 startRowNumber;
	int64 endRowNumber;
	bytea *mask;
};

#endif