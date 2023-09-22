/*-------------------------------------------------------------------------
 *
 * columnar_indexscan.h
 *	Custom scan method for index
 * 
 * IDENTIFICATION
 *	src/backend/columnar/columnar_indexscan.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef COLUMNAR_INDEXSCAN_H
#define COLUMNAR_INDEXSCAN_H

#include "postgres.h"

#include "nodes/execnodes.h"

typedef struct ColumnarIndexScanState
{
	CustomScanState css;
	IndexScanState *indexscan_state;
} ColumnarIndexScanState;

extern CustomScan * columnar_create_indexscan_node(void);
extern void columnar_register_indexscan_node(void);

#endif