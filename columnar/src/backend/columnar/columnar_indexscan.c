#include "postgres.h"

#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/index.h"
#include "executor/execdebug.h"
#include "executor/nodeIndexscan.h"
#include "lib/pairingheap.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "storage/predicate.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "columnar/columnar_tableam.h"
#include "columnar/columnar_indexscan.h"
#include "columnar/columnar_customscan.h"

/*
 * When an ordering operator is used, tuples fetched from the index that
 * need to be reordered are queued in a pairing heap, as ReorderTuples.
 */
typedef struct
{
	pairingheap_node ph_node;
	HeapTuple	htup;
	Datum	   *orderbyvals;
	bool	   *orderbynulls;
} ReorderTuple;

static TupleTableSlot *IndexNext(IndexScanState *node);
static TupleTableSlot *IndexNextWithReorder(IndexScanState *node);
static void EvalOrderByExpressions(IndexScanState *node, ExprContext *econtext);
static bool IndexRecheck(IndexScanState *node, TupleTableSlot *slot);
static int	cmp_orderbyvals(const Datum *adist, const bool *anulls,
							const Datum *bdist, const bool *bnulls,
							IndexScanState *node);
static void reorderqueue_push(IndexScanState *node, TupleTableSlot *slot,
							  Datum *orderbyvals, bool *orderbynulls);
static HeapTuple reorderqueue_pop(IndexScanState *node);


#define RELATION_CHECKS \
( \
	AssertMacro(RelationIsValid(indexRelation)), \
	AssertMacro(PointerIsValid(indexRelation->rd_indam)), \
	AssertMacro(!ReindexIsProcessingIndex(RelationGetRelid(indexRelation))) \
)

#define CHECK_REL_PROCEDURE(pname) \
do { \
	if (indexRelation->rd_indam->pname == NULL) \
		elog(ERROR, "function \"%s\" is not defined for index \"%s\"", \
			 CppAsString(pname), RelationGetRelationName(indexRelation)); \
} while(0)


static IndexScanDesc
index_beginscan_internal(Relation indexRelation,
						 int nkeys, int norderbys, Snapshot snapshot,
						 ParallelIndexScanDesc pscan, bool temp_snap)
{
	IndexScanDesc scan;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(ambeginscan);

	if (!(indexRelation->rd_indam->ampredlocks))
		PredicateLockRelation(indexRelation, snapshot);

	/*
	 * We hold a reference count to the relcache entry throughout the scan.
	 */
	RelationIncrementReferenceCount(indexRelation);

	/*
	 * Tell the AM to open a scan.
	 */
	scan = indexRelation->rd_indam->ambeginscan(indexRelation, nkeys,
												norderbys);
	/* Initialize information for parallel scan. */
	scan->parallel_scan = pscan;
	scan->xs_temp_snap = temp_snap;

	return scan;
}

/*
 * index_beginscan - start a scan of an index with amgettuple
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
static IndexScanDesc
columnar_index_beginscan(Relation heapRelation,
						 Relation indexRelation,
						 Snapshot snapshot,
						 int nkeys, int norderbys,
						 Bitmapset *attr_needed)
{
	IndexScanDesc scan;

	scan = index_beginscan_internal(indexRelation, nkeys, norderbys, snapshot, NULL, false);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->heapRelation = heapRelation;
	scan->xs_snapshot = snapshot;

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = columnar_index_fetch_begin_extended(heapRelation, attr_needed);

	return scan;
}

#include "optimizer/optimizer.h"

/* ----------------------------------------------------------------
 *		IndexNext
 *
 *		Retrieve a tuple from the IndexScan node's currentRelation
 *		using the index specified in the IndexScanState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
IndexNext(IndexScanState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	ScanDirection direction;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	/* flip direction if this is an overall backward scan */
	if (ScanDirectionIsBackward(((IndexScan *) node->ss.ps.plan)->indexorderdir))
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	scandesc = node->iss_ScanDesc;
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the index scan is not parallel, or if we're
		 * serially executing an index scan that was planned to be parallel.
		 */	

		Bitmapset *attr_needed =
			ColumnarAttrNeeded(&node->ss, ((IndexScan *) node->ss.ps.plan)->indexqualorig);

		scandesc = 
			columnar_index_beginscan(node->ss.ss_currentRelation,
									 node->iss_RelationDesc,
									 estate->es_snapshot,
									 node->iss_NumScanKeys,
									 node->iss_NumOrderByKeys,
									 attr_needed);

		bms_free(attr_needed);

		node->iss_ScanDesc = scandesc;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->iss_ScanKeys, node->iss_NumScanKeys,
						 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
	}

	/*
	 * ok, now that we have what we need, fetch the next tuple.
	 */
	while (index_getnext_slot(scandesc, direction, slot))
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * If the index was lossy, we have to recheck the index quals using
		 * the fetched tuple.
		 */
		if (scandesc->xs_recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->indexqualorig, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				continue;
			}
		}

		return slot;
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	node->iss_ReachedEnd = true;
	return ExecClearTuple(slot);
}

/* ----------------------------------------------------------------
 *		IndexNextWithReorder
 *
 *		Like IndexNext, but this version can also re-check ORDER BY
 *		expressions, and reorder the tuples as necessary.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
IndexNextWithReorder(IndexScanState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;
	ReorderTuple *topmost = NULL;
	bool		was_exact;
	Datum	   *lastfetched_vals;
	bool	   *lastfetched_nulls;
	int			cmp;

	estate = node->ss.ps.state;

	/*
	 * Only forward scan is supported with reordering.  Note: we can get away
	 * with just Asserting here because the system will not try to run the
	 * plan backwards if ExecSupportsBackwardScan() says it won't work.
	 * Currently, that is guaranteed because no index AMs support both
	 * amcanorderbyop and amcanbackward; if any ever do,
	 * ExecSupportsBackwardScan() will need to consider indexorderbys
	 * explicitly.
	 */
	Assert(!ScanDirectionIsBackward(((IndexScan *) node->ss.ps.plan)->indexorderdir));
	Assert(ScanDirectionIsForward(estate->es_direction));

	scandesc = node->iss_ScanDesc;
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the index scan is not parallel, or if we're
		 * serially executing an index scan that was planned to be parallel.
		 */

		Bitmapset *attr_needed =
			ColumnarAttrNeeded(&node->ss, ((IndexScan *) node->ss.ps.plan)->indexqualorig);

		scandesc = 
			columnar_index_beginscan(node->ss.ss_currentRelation,
									 node->iss_RelationDesc,
									 estate->es_snapshot,
									 node->iss_NumScanKeys,
									 node->iss_NumOrderByKeys,
									 attr_needed);

		bms_free(attr_needed);

		node->iss_ScanDesc = scandesc;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->iss_ScanKeys, node->iss_NumScanKeys,
						 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
	}

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Check the reorder queue first.  If the topmost tuple in the queue
		 * has an ORDER BY value smaller than (or equal to) the value last
		 * returned by the index, we can return it now.
		 */
		if (!pairingheap_is_empty(node->iss_ReorderQueue))
		{
			topmost = (ReorderTuple *) pairingheap_first(node->iss_ReorderQueue);

			if (node->iss_ReachedEnd ||
				cmp_orderbyvals(topmost->orderbyvals,
								topmost->orderbynulls,
								scandesc->xs_orderbyvals,
								scandesc->xs_orderbynulls,
								node) <= 0)
			{
				HeapTuple	tuple;

				tuple = reorderqueue_pop(node);

				/* Pass 'true', as the tuple in the queue is a palloc'd copy */
				ExecForceStoreHeapTuple(tuple, slot, true);
				return slot;
			}
		}
		else if (node->iss_ReachedEnd)
		{
			/* Queue is empty, and no more tuples from index.  We're done. */
			return ExecClearTuple(slot);
		}

		/*
		 * Fetch next tuple from the index.
		 */
next_indextuple:
		if (!index_getnext_slot(scandesc, ForwardScanDirection, slot))
		{
			/*
			 * No more tuples from the index.  But we still need to drain any
			 * remaining tuples from the queue before we're done.
			 */
			node->iss_ReachedEnd = true;
			continue;
		}

		/*
		 * If the index was lossy, we have to recheck the index quals and
		 * ORDER BY expressions using the fetched tuple.
		 */
		if (scandesc->xs_recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->indexqualorig, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				/* allow this loop to be cancellable */
				CHECK_FOR_INTERRUPTS();
				goto next_indextuple;
			}
		}

		if (scandesc->xs_recheckorderby)
		{
			econtext->ecxt_scantuple = slot;
			ResetExprContext(econtext);
			EvalOrderByExpressions(node, econtext);

			/*
			 * Was the ORDER BY value returned by the index accurate?  The
			 * recheck flag means that the index can return inaccurate values,
			 * but then again, the value returned for any particular tuple
			 * could also be exactly correct.  Compare the value returned by
			 * the index with the recalculated value.  (If the value returned
			 * by the index happened to be exact right, we can often avoid
			 * pushing the tuple to the queue, just to pop it back out again.)
			 */
			cmp = cmp_orderbyvals(node->iss_OrderByValues,
								  node->iss_OrderByNulls,
								  scandesc->xs_orderbyvals,
								  scandesc->xs_orderbynulls,
								  node);
			if (cmp < 0)
				elog(ERROR, "index returned tuples in wrong order");
			else if (cmp == 0)
				was_exact = true;
			else
				was_exact = false;
			lastfetched_vals = node->iss_OrderByValues;
			lastfetched_nulls = node->iss_OrderByNulls;
		}
		else
		{
			was_exact = true;
			lastfetched_vals = scandesc->xs_orderbyvals;
			lastfetched_nulls = scandesc->xs_orderbynulls;
		}

		/*
		 * Can we return this tuple immediately, or does it need to be pushed
		 * to the reorder queue?  If the ORDER BY expression values returned
		 * by the index were inaccurate, we can't return it yet, because the
		 * next tuple from the index might need to come before this one. Also,
		 * we can't return it yet if there are any smaller tuples in the queue
		 * already.
		 */
		if (!was_exact || (topmost && cmp_orderbyvals(lastfetched_vals,
													  lastfetched_nulls,
													  topmost->orderbyvals,
													  topmost->orderbynulls,
													  node) > 0))
		{
			/* Put this tuple to the queue */
			reorderqueue_push(node, slot, lastfetched_vals, lastfetched_nulls);
			continue;
		}
		else
		{
			/* Can return this tuple immediately. */
			return slot;
		}
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 * Calculate the expressions in the ORDER BY clause, based on the heap tuple.
 */
static void
EvalOrderByExpressions(IndexScanState *node, ExprContext *econtext)
{
	int			i;
	ListCell   *l;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	i = 0;
	foreach(l, node->indexorderbyorig)
	{
		ExprState  *orderby = (ExprState *) lfirst(l);

		node->iss_OrderByValues[i] = ExecEvalExpr(orderby,
												  econtext,
												  &node->iss_OrderByNulls[i]);
		i++;
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * IndexRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
IndexRecheck(IndexScanState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the indexqual condition? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->indexqualorig, econtext);
}


/*
 * Compare ORDER BY expression values.
 */
static int
cmp_orderbyvals(const Datum *adist, const bool *anulls,
				const Datum *bdist, const bool *bnulls,
				IndexScanState *node)
{
	int			i;
	int			result;

	for (i = 0; i < node->iss_NumOrderByKeys; i++)
	{
		SortSupport ssup = &node->iss_SortSupport[i];

		/*
		 * Handle nulls.  We only need to support NULLS LAST ordering, because
		 * match_pathkeys_to_index() doesn't consider indexorderby
		 * implementation otherwise.
		 */
		if (anulls[i] && !bnulls[i])
			return 1;
		else if (!anulls[i] && bnulls[i])
			return -1;
		else if (anulls[i] && bnulls[i])
			return 0;

		result = ssup->comparator(adist[i], bdist[i], ssup);
		if (result != 0)
			return result;
	}

	return 0;
}


/*
 * Helper function to push a tuple to the reorder queue.
 */
static void
reorderqueue_push(IndexScanState *node, TupleTableSlot *slot,
				  Datum *orderbyvals, bool *orderbynulls)
{
	IndexScanDesc scandesc = node->iss_ScanDesc;
	EState	   *estate = node->ss.ps.state;
	MemoryContext oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
	ReorderTuple *rt;
	int			i;

	rt = (ReorderTuple *) palloc(sizeof(ReorderTuple));
	rt->htup = ExecCopySlotHeapTuple(slot);
	rt->orderbyvals =
		(Datum *) palloc(sizeof(Datum) * scandesc->numberOfOrderBys);
	rt->orderbynulls =
		(bool *) palloc(sizeof(bool) * scandesc->numberOfOrderBys);
	for (i = 0; i < node->iss_NumOrderByKeys; i++)
	{
		if (!orderbynulls[i])
			rt->orderbyvals[i] = datumCopy(orderbyvals[i],
										   node->iss_OrderByTypByVals[i],
										   node->iss_OrderByTypLens[i]);
		else
			rt->orderbyvals[i] = (Datum) 0;
		rt->orderbynulls[i] = orderbynulls[i];
	}
	pairingheap_add(node->iss_ReorderQueue, &rt->ph_node);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Helper function to pop the next tuple from the reorder queue.
 */
static HeapTuple
reorderqueue_pop(IndexScanState *node)
{
	HeapTuple	result;
	ReorderTuple *topmost;
	int			i;

	topmost = (ReorderTuple *) pairingheap_remove_first(node->iss_ReorderQueue);

	result = topmost->htup;
	for (i = 0; i < node->iss_NumOrderByKeys; i++)
	{
		if (!node->iss_OrderByTypByVals[i] && !topmost->orderbynulls[i])
			pfree(DatumGetPointer(topmost->orderbyvals[i]));
	}
	pfree(topmost->orderbyvals);
	pfree(topmost->orderbynulls);
	pfree(topmost);

	return result;
}


/* ----------------------------------------------------------------
 *		ExecIndexScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecIndexScan(PlanState *pstate)
{
	ColumnarIndexScanState *ciis = (ColumnarIndexScanState *) pstate;
	IndexScanState *node = castNode(IndexScanState, ciis->indexscan_state);

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 */
	if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady)
		ExecReScan((PlanState *) node);

	if (node->iss_NumOrderByKeys > 0)
		return ExecScan(&node->ss,
						(ExecScanAccessMtd) IndexNextWithReorder,
						(ExecScanRecheckMtd) IndexRecheck);
	else
		return ExecScan(&node->ss,
						(ExecScanAccessMtd) IndexNext,
						(ExecScanRecheckMtd) IndexRecheck);
}

/* ----------------------------------------------------------------
 *		ExecIndexScanInitializeDSM
 *
 *		Set up a parallel index scan descriptor.
 * ----------------------------------------------------------------
 */
static void
ColumnarIndexScan_ExecIndexScanInitializeDSM(IndexScanState *node,
											 ParallelContext *pcxt,
											 void *coordinate)
{
	EState *estate = node->ss.ps.state;
	ParallelIndexScanDesc piscan = (ParallelIndexScanDesc) coordinate;

	index_parallelscan_initialize(node->ss.ss_currentRelation,
								  node->iss_RelationDesc,
								  estate->es_snapshot,
								  piscan);

	node->iss_ScanDesc =
		index_beginscan_parallel(node->ss.ss_currentRelation,
								 node->iss_RelationDesc,
								 node->iss_NumScanKeys,
								 node->iss_NumOrderByKeys,
								 piscan);

	/*
	 * If no run-time keys to calculate or they are ready, go ahead and pass
	 * the scankeys to the index AM.
	 */
	if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
}

/* ----------------------------------------------------------------
 *		ExecIndexScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */

static void
ColumnarIndexScan_ExecIndexScanInitializeWorker(IndexScanState *node,
												void *coordinate)
{
	ParallelIndexScanDesc piscan = (ParallelIndexScanDesc) coordinate;

	node->iss_ScanDesc =
		index_beginscan_parallel(node->ss.ss_currentRelation,
								 node->iss_RelationDesc,
								 node->iss_NumScanKeys,
								 node->iss_NumOrderByKeys,
								 piscan);

	/*
	 * If no run-time keys to calculate or they are ready, go ahead and pass
	 * the scankeys to the index AM.
	 */
	if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
}



/* CustomScanMethods */
static Node * CreateColumnarIndexScanState(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void ColumnarIndexScan_BeginCustomScan(CustomScanState *node,
											  EState *estate, int eflags);
static TupleTableSlot * ColumnarIndexScan_ExecCustomScan(CustomScanState *node);
static void ColumnarIndexScan_EndCustomScan(CustomScanState *node);
static void ColumnarIndexScan_ExplainCustomScan(CustomScanState *node,
												List *ancestors,
												ExplainState *es);
static Size ColumnarIndexScan_EstimateDSMCustomScan(CustomScanState *node,
													ParallelContext *pcxt);
static void ColumnarIndexScan_InitializeDSMCustomScan(CustomScanState *node,
								 					   ParallelContext *pcxt,
								 					   void *coordinate);
static void ColumnarIndexScan_ReinitializeDSMCustomScan(CustomScanState *node,
														ParallelContext *pcxt,
														void *coordinate);
static void ColumnarIndexScan_InitializeWorkerCustomScan(CustomScanState *node,
														 shm_toc *toc,
														 void *coordinate);
static CustomScanMethods ColumnarIndexCustomScanMethods = {
	"ColumnarIndexScan",			/* CustomName */
	CreateColumnarIndexScanState,	/* CreateCustomScanState */
};

static CustomExecMethods ColumnarIndexScanExecMethods = {
	.CustomName = "ColumnarIndexScan",

	.BeginCustomScan = ColumnarIndexScan_BeginCustomScan,
	.ExecCustomScan = ColumnarIndexScan_ExecCustomScan,
	.EndCustomScan = ColumnarIndexScan_EndCustomScan,

	.ExplainCustomScan = ColumnarIndexScan_ExplainCustomScan,

	.EstimateDSMCustomScan = ColumnarIndexScan_EstimateDSMCustomScan,
	.InitializeDSMCustomScan = ColumnarIndexScan_InitializeDSMCustomScan,
	.ReInitializeDSMCustomScan = ColumnarIndexScan_ReinitializeDSMCustomScan,
	.InitializeWorkerCustomScan = ColumnarIndexScan_InitializeWorkerCustomScan
};


static Node *
CreateColumnarIndexScanState(CustomScan *custom_plan)
{
	ColumnarIndexScanState *ciss = (ColumnarIndexScanState *) newNode(
		sizeof(ColumnarIndexScanState), T_CustomScanState);

	CustomScanState *cscanstate = &ciss->css;
	cscanstate->methods = &ColumnarIndexScanExecMethods;

	return (Node *) cscanstate;
}


static void
ColumnarIndexScan_BeginCustomScan(CustomScanState *css, EState *estate, int eflags)
{
	ColumnarIndexScanState *ciis = (ColumnarIndexScanState*) css;
	CustomScan  *cscan = (CustomScan *)css->ss.ps.plan;
	IndexScan *isNode = (IndexScan *) linitial(cscan->custom_plans);
	
	/* Free the exprcontext */
	ExecFreeExprContext(&css->ss.ps);

	/* Clean out the tuple table */
	ExecClearTuple(css->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(css->ss.ss_ScanTupleSlot);

	ciis->indexscan_state = ExecInitIndexScan(isNode, estate, eflags);

	 /*Initialize result type and projection. */
	ExecInitResultTypeTL(&ciis->css.ss.ps);
}


static TupleTableSlot *
ColumnarIndexScan_ExecCustomScan(CustomScanState *node)
{
	return ExecIndexScan((PlanState *) node);
}


static void
ColumnarIndexScan_EndCustomScan(CustomScanState *node)
{
	ExecEndIndexScan(((ColumnarIndexScanState *)node)->indexscan_state);
}

#include "utils/ruleutils.h"
#include "nodes/makefuncs.h"

/*
 * Show a generic expression
 */
static void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es)
{
	List	   *context;
	char	   *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   planstate->plan,
									   ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
static void
show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ExplainState *es)
{
	Node	   *node;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* And show it */
	show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
static void
show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ExplainState *es)
{
	bool		useprefix;

	useprefix = (IsA(planstate->plan, SubqueryScan) || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}


static void
show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ExplainState *es)
{
	double		nfiltered;
	double		nloops;

	if (!es->analyze || !planstate->instrument)
		return;

	if (which == 2)
		nfiltered = planstate->instrument->nfiltered2;
	else
		nfiltered = planstate->instrument->nfiltered1;
	nloops = planstate->instrument->nloops;

	/* In text mode, suppress zero counts; they're not interesting enough */
	if (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)
	{
		if (nloops > 0)
			ExplainPropertyFloat(qlabel, NULL, nfiltered / nloops, 0, es);
		else
			ExplainPropertyFloat(qlabel, NULL, 0.0, 0, es);
	}
}

static void
ColumnarIndexScan_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es)
{

	ColumnarIndexScanState *ciis = (ColumnarIndexScanState*) node;
	CustomScan  *cscan = (CustomScan *) node->ss.ps.plan;
	IndexScan *isNode = (IndexScan *) linitial(cscan->custom_plans);
	
	const char *indexname = get_rel_name(isNode->indexid);

	ExplainPropertyText("ColumnarIndexScan using ", indexname, es);

	show_scan_qual(((IndexScan *) isNode)->indexqualorig,
					"Index Cond", &ciis->css.ss.ps, ancestors, es);

	if (isNode->indexqualorig)
		show_instrumentation_count("Rows Removed by Index Recheck", 2,
									&ciis->css.ss.ps, es);
									
	show_scan_qual(isNode->indexorderbyorig,
					"Order By", &ciis->css.ss.ps, ancestors, es);

	show_scan_qual(node->ss.ps.plan->qual, "Filter", &ciis->css.ss.ps, ancestors, es);

	if (node->ss.ps.plan->qual)
		show_instrumentation_count("Rows Removed by Filter", 1,
									&ciis->css.ss.ps, es);
}

/* Parallel Execution */

static Size
ColumnarIndexScan_EstimateDSMCustomScan(CustomScanState *node,
										ParallelContext *pcxt)
{
	ColumnarIndexScanState *ciis = (ColumnarIndexScanState*) node;

	ExecIndexScanEstimate(ciis->indexscan_state, pcxt);

	return ciis->indexscan_state->iss_PscanLen;
}


static void
ColumnarIndexScan_InitializeDSMCustomScan(CustomScanState *node,
										  ParallelContext *pcxt,
										  void *coordinate)
{
	ColumnarIndexScanState *ciis = (ColumnarIndexScanState*) node;
	ColumnarIndexScan_ExecIndexScanInitializeDSM(ciis->indexscan_state, pcxt, coordinate);
}


static void
ColumnarIndexScan_ReinitializeDSMCustomScan(CustomScanState *node,
											ParallelContext *pcxt,
											void *coordinate)
{
	ColumnarIndexScanState *ciis = (ColumnarIndexScanState*) node;
	ExecIndexScanReInitializeDSM(ciis->indexscan_state, pcxt);
}


static void
ColumnarIndexScan_InitializeWorkerCustomScan(CustomScanState *node,
											 shm_toc *toc,
											 void *coordinate)
{
	ColumnarIndexScanState *ciis = (ColumnarIndexScanState*) node;
	ColumnarIndexScan_ExecIndexScanInitializeWorker(ciis->indexscan_state, coordinate);
}


CustomScan *
columnar_create_indexscan_node(void)
{
	CustomScan *cscan = (CustomScan *) makeNode(CustomScan);
	cscan->methods = &ColumnarIndexCustomScanMethods;
	return cscan;
}


void
columnar_register_indexscan_node(void)
{
	RegisterCustomScanMethods(&ColumnarIndexCustomScanMethods);
}
