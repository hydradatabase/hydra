/*-------------------------------------------------------------------------
 *
 * columnar_aggregator_node.h
 *	Custom scan method for aggregation
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/nodes/columnar_aggregator_node.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef COLUMNAR_AGGEREGATOR_NODE_H
#define COLUMNAR_AGGEREGATOR_NODE_H

#include "postgres.h"

#include "nodes/execnodes.h"

typedef struct VectorAggState
{
	CustomScanState css;
	AggState *aggstate;
} VectorAggState;

extern CustomScan *columnar_create_aggregator_node(void);
extern void columnar_register_aggregator_node(void);

#endif
