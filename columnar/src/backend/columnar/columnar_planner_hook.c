/*-------------------------------------------------------------------------
 *
 * columnar_planner_hook.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * Modify top plan and change aggregate function to provided ones that can execute on 
 * column vector.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/amapi.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "tcop/utility.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/spccache.h"

#include "columnar/columnar.h"
#include "columnar/columnar_customscan.h"
#include "columnar/vectorization/columnar_vector_execution.h"
#include "columnar/vectorization/nodes/columnar_aggregator_node.h"

#include "columnar/utils/listutils.h"

static planner_hook_type PreviousPlannerHook = NULL;

static PlannedStmt * ColumnarPlannerHook(Query *parse,  const char *query_string,
										 int cursorOptions, ParamListInfo boundParams);

#if PG_VERSION_NUM >= PG_VERSION_14
static Plan * PlanTreeMutator(Plan *node, void *context);

typedef struct PlanTreeMutatorContext
{
	bool vectorizedAggregation;
} PlanTreeMutatorContext;


#define FLATCOPY(newnode, node, nodetype)  \
	( (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

static Node *
AggRefArgsExpressionMutator(Node *node, void *context)
{
	if (node == NULL)
		return false;

	Node *previousNode = (Node *) context;

	if (IsA(node, OpExpr) || IsA(node, DistinctExpr) || IsA(node, NullIfExpr) )
	{
		OpExpr *opExprNode = (OpExpr *) node;

		Form_pg_operator operatorForm;
		HeapTuple operatorTuple;

		if (list_length(opExprNode->args) != 2)
		{
			elog(ERROR, "Aggregation vectorizaion works only on two arguments.");
			return false;
		}

		if (CheckOpExprArgumentRules(opExprNode->args))
		{
			elog(ERROR, "Unsupported aggregate argument combination.");
			return false;
		}

		operatorTuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opExprNode->opno));
		operatorForm = (Form_pg_operator) GETSTRUCT(operatorTuple);
		Oid procedureOid = operatorForm->oprcode;
		ReleaseSysCache(operatorTuple);

		Oid vectorizedProcedureOid;
		if (!GetVectorizedProcedureOid(procedureOid, &vectorizedProcedureOid))
		{
			elog(ERROR, "Vectorized aggregate not found.");
		}

		opExprNode->opfuncid = vectorizedProcedureOid;

		return (Node *) opExprNode;
	}

	/* This should handle aggregates that have non var(column) as argument*/
	if (previousNode != NULL && IsA(previousNode, TargetEntry) && !IsA(node, Var))
	{
		elog(ERROR, "Vectorized Aggregates accepts accepts only valid column argument");
		return false;
	}

	return expression_tree_mutator(node, AggRefArgsExpressionMutator, (void *) node);
}

static Node *
ExpressionMutator(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *oldAggRefNode = (Aggref *) node;
		Aggref *newAggRefNode = copyObject(oldAggRefNode);

		if (oldAggRefNode->aggdistinct)
		{
			elog(ERROR, "Vectorized aggregate with DISTINCT not supported.");
		}

		newAggRefNode->args = (List *)
			expression_tree_mutator((Node *) oldAggRefNode->args, AggRefArgsExpressionMutator, NULL);
		
		Oid vectorizedProcedureOid = 0;
		if (!GetVectorizedProcedureOid(newAggRefNode->aggfnoid, &vectorizedProcedureOid))
		{
			elog(ERROR, "Vectorized aggregate not found.");
		}

		newAggRefNode->aggfnoid = vectorizedProcedureOid;

		return (Node *) newAggRefNode;
	}

	return expression_tree_mutator(node, ExpressionMutator, (void *) context);
}


static Plan *
PlanTreeMutator(Plan *node, void *context)
{
	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_CustomScan:
		{
			CustomScan *customScan = (CustomScan *) node;

			if (customScan->methods == columnar_customscan_methods())
			{
				PlanTreeMutatorContext *planTreeContext = (PlanTreeMutatorContext *) context;

				Const * vectorizedAggregateExecution = makeNode(Const);

				vectorizedAggregateExecution->constbyval = true;
				vectorizedAggregateExecution->consttype = CUSTOM_SCAN_VECTORIZED_AGGREGATE;
				vectorizedAggregateExecution->constvalue =  planTreeContext->vectorizedAggregation;
				vectorizedAggregateExecution->constlen = sizeof(bool);

				customScan->custom_private = lappend(customScan->custom_private, vectorizedAggregateExecution);
			}

			break;
		}

		case T_Agg:
		{
			Agg *aggNode = (Agg *) node;
			Agg	*newAgg;
			CustomScan *vectorizedAggNode;

			if (aggNode->plan.lefttree->type == T_CustomScan)
			{
				if (aggNode->aggstrategy == AGG_PLAIN)
				{
					vectorizedAggNode = columnar_create_aggregator_node();

					FLATCOPY(newAgg, aggNode, Agg);

					newAgg->plan.targetlist = 
						(List *) expression_tree_mutator((Node *) newAgg->plan.targetlist, ExpressionMutator, NULL);


					vectorizedAggNode->custom_plans = 
						lappend(vectorizedAggNode->custom_plans, newAgg);
					vectorizedAggNode->scan.plan.targetlist = 
						CustomBuildTargetList(aggNode->plan.targetlist, INDEX_VAR);
					vectorizedAggNode->custom_scan_tlist = newAgg->plan.targetlist;

					// Parallel agg node
					Plan *vectorizedAggNodePlan = (Plan *) vectorizedAggNode;
					vectorizedAggNodePlan->parallel_aware = aggNode->plan.lefttree->parallel_aware;
					vectorizedAggNodePlan->startup_cost = aggNode->plan.startup_cost;
					vectorizedAggNodePlan->total_cost = aggNode->plan.total_cost;
					vectorizedAggNodePlan->plan_rows = aggNode->plan.plan_rows;
					vectorizedAggNodePlan->plan_width = aggNode->plan.plan_width;


					PlanTreeMutatorContext *planTreeContext = (PlanTreeMutatorContext *) context;
					planTreeContext->vectorizedAggregation = true;

					PlanTreeMutator(node->lefttree, context);
					PlanTreeMutator(node->righttree, context);

					vectorizedAggNode->scan.plan.lefttree = node->lefttree;
					vectorizedAggNode->scan.plan.righttree = node->righttree;

					return (Plan *) vectorizedAggNode;
				}

				return node;
			}

			break;
		}
		default:
		{
			
			break;
		}
	}

	node->lefttree = PlanTreeMutator(node->lefttree, context);
	node->righttree = PlanTreeMutator(node->righttree, context);

	return node;
}
#endif

static PlannedStmt *
ColumnarPlannerHook(Query *parse,
					const char *query_string,
					int cursorOptions,
					ParamListInfo boundParams)
{
	PlannedStmt	*stmt;
#if PG_VERSION_NUM >= PG_VERSION_14
	Plan *savedPlanTree;
	List *savedSubplan;
	MemoryContext saved_context;
#endif

	if (PreviousPlannerHook)
		stmt = PreviousPlannerHook(parse, query_string, cursorOptions, boundParams);
	else
		stmt = standard_planner(parse, query_string, cursorOptions, boundParams);

#if PG_VERSION_NUM >= PG_VERSION_14
	if (!columnar_enable_vectorization			/* Vectorization should be enabled */
		|| stmt->commandType != CMD_SELECT		 /* only SELECTS are supported  */
		|| list_length(stmt->rtable) != 1)		 /* JOINs are not yet supported */
		return stmt;


	savedPlanTree = stmt->planTree;
	savedSubplan = stmt->subplans;

	saved_context = CurrentMemoryContext;

	PG_TRY();
	{
		List		*subplans = NULL;
		ListCell	*cell;

		PlanTreeMutatorContext plainTreeContext;
		plainTreeContext.vectorizedAggregation = 0;

		stmt->planTree = (Plan *) PlanTreeMutator(stmt->planTree, (void *) &plainTreeContext);

		foreach(cell, stmt->subplans)
		{
			PlanTreeMutatorContext subPlainTreeContext;
			plainTreeContext.vectorizedAggregation = 0;
			Plan *subplan = (Plan *) PlanTreeMutator(lfirst(cell), (void *) &subPlainTreeContext);
			subplans = lappend(subplans, subplan);
		}

		stmt->subplans = subplans;
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContextSwitchTo(saved_context);

		edata = CopyErrorData();
		FlushErrorState();
		ereport(DEBUG1,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Query can't be vectorized. Falling back to original execution."),
					errdetail("%s", edata->message)));
		stmt->planTree = savedPlanTree;
		stmt->subplans = savedSubplan;
	}

	PG_END_TRY();
#endif

	return stmt;
}


void columnar_planner_init(void)
{
	PreviousPlannerHook = planner_hook;
	planner_hook = ColumnarPlannerHook;
#if  PG_VERSION_NUM >= PG_VERSION_14
	columnar_register_aggregator_node();
#endif
}