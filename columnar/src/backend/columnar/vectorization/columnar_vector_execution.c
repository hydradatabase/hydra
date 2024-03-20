/*-------------------------------------------------------------------------
 *
 * columnar_vector_execution.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"

#include "utils/syscache.h"

#include "pg_version_constants.h"
#include "columnar/vectorization/columnar_vector_execution.h"
#include "columnar/vectorization/columnar_vector_types.h"

/*
 * Check OpExpr argument so they can be vectorized.
 * For now, vectorization only support "normal" clauses where
 * we compare tuple column against constant value.
 */
bool
CheckOpExprArgumentRules(List *args)
{
	ListCell *lcOpExprArgs;
	bool invalidArgument = false;

	/* We accept only one CONST argument */
	bool singleConstArgument = false;
	/* We accept only one VAR argument*/
	bool singleVarArgument = false;

	foreach(lcOpExprArgs, args)
	{
		if (invalidArgument)
			break;
	
		Expr *arg = (Expr *) lfirst(lcOpExprArgs);
		if (IsA(arg, Const))
		{
			if (singleConstArgument)
			{
				invalidArgument = true;
				break;
			}
			singleConstArgument = true;
		}
		else if (IsA(arg, Var))
		{
			if (singleVarArgument)
			{
				invalidArgument = true;
				break;
			}
			singleVarArgument = true;
		}
		else
		{
			invalidArgument = true;
			break;
		}
	}

	return invalidArgument;
}

/*
 * Get vectorized procedure OID.
 */
bool
GetVectorizedProcedureOid(Oid procedureOid, Oid *vectorizedProcedureOid)
{
	Form_pg_proc procedureForm;
	HeapTuple procedureTuple;

	List *funcNames = NIL;
	Oid *argtypes;
	FuncDetailCode fdResult;
	int i;

	bool retset;
	Oid retype;
	int nvargs;
	Oid vatype;
	Oid *true_oid_array;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(procedureOid));
	procedureForm = (Form_pg_proc) GETSTRUCT(procedureTuple);

	int originalProcedureNameLen = strlen(NameStr(procedureForm->proname));

	char * vectorizedProcedureName =
		palloc0(sizeof(char) * originalProcedureNameLen + 2);
	
	vectorizedProcedureName[0] = 'v';

	memcpy(vectorizedProcedureName + 1,
		NameStr(procedureForm->proname),
		originalProcedureNameLen);
	
	ReleaseSysCache(procedureTuple);

	funcNames = lappend(funcNames, makeString(vectorizedProcedureName));

	argtypes = palloc(sizeof(Oid) * procedureForm->pronargs);

	for (i = 0; i < procedureForm->pronargs; i++)
		argtypes[i] = procedureForm->proargtypes.values[i];
	
#if PG_VERSION_NUM >= PG_VERSION_14
	fdResult = func_get_detail(funcNames, NIL, NIL,
								procedureForm->pronargs, argtypes,
								false, true, false,
								vectorizedProcedureOid,
								&retype, &retset,
								&nvargs, &vatype,
								&true_oid_array, NULL);
#else
	fdResult = func_get_detail(funcNames,
								NIL, NIL,
								procedureForm->pronargs, argtypes,
								false, false,
								vectorizedProcedureOid, &retype,
								&retset, &nvargs, &vatype,
								&true_oid_array, NULL);
#endif

	if ((fdResult == FUNCDETAIL_NOTFOUND || fdResult == FUNCDETAIL_MULTIPLE) || 
		!OidIsValid(*vectorizedProcedureOid) ||
		(procedureForm->pronargs != 0 && 
		 memcmp(argtypes, true_oid_array, procedureForm->pronargs * sizeof(Oid)) != 0))
	{
		return false;
	}

	return true;

}

List *
CreateVectorizedExprList(List *exprList)
{
	if (exprList == NULL)
		return exprList;

	check_stack_depth();

	List *newQualList = NIL;
	ListCell *lc;

	foreach(lc, exprList)
	{
		Node *node = lfirst(lc);

		if (node == NULL)
			return NULL;

		switch(nodeTag(node))
		{
			case T_OpExpr:
			case T_DistinctExpr:	/* struct-equivalent to OpExpr */
			case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr *opExprNode = (OpExpr *) node;

				Form_pg_operator operatorForm;
				HeapTuple operatorTuple;

				if (list_length(opExprNode->args) != 2)
				{
					newQualList = lappend(newQualList, opExprNode);
					break;
				}

				/*
				 * Let's inspect argument rules and break if they
				 * don't match rules for vectorized execution.
				 */
				if (CheckOpExprArgumentRules(opExprNode->args))
				{
					newQualList = lappend(newQualList, opExprNode);
					break;
				}

				operatorTuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opExprNode->opno));
				operatorForm = (Form_pg_operator) GETSTRUCT(operatorTuple);
				Oid procedureOid = operatorForm->oprcode;
				ReleaseSysCache(operatorTuple);

				Oid vectorizedOid;
				if (!GetVectorizedProcedureOid(procedureOid, &vectorizedOid))
				{
					newQualList = lappend(newQualList, opExprNode);
					break;
				}

				OpExpr *opExprNodeVector = copyObject(opExprNode);
				opExprNodeVector->opfuncid = vectorizedOid;
				newQualList = lappend(newQualList, opExprNodeVector);
				
				break;
			}

			case T_BoolExpr:
			{
				BoolExpr *boolExpr = castNode(BoolExpr, node);

				List *newBoolExprArgList = NIL;

				newBoolExprArgList = 
					CreateVectorizedExprList(boolExpr->args);

				if (list_length(list_intersection(newBoolExprArgList, boolExpr->args)) == 0)
				{
					Expr *booleanClause = NULL;

					if (boolExpr->boolop == AND_EXPR)
					{
						booleanClause = make_andclause(newBoolExprArgList);
					}
					else if (boolExpr->boolop == OR_EXPR)
					{
						booleanClause = make_orclause(newBoolExprArgList);
					}

					newQualList = lappend(newQualList, booleanClause);
				}
				else
				{
					newQualList = lappend(newQualList, boolExpr);
				}

				break;
			}
			default:
			{
				newQualList = lappend(newQualList, node);
				break;
			}
		}
	}

	return newQualList;
}


List *
ConstructVectorizedQualList(TupleTableSlot *slot, List *vectorizedQual)
{
	VectorTupleTableSlot *vectorSlot = (VectorTupleTableSlot *) slot;

	List *vectorQualList = NIL;
	ListCell *lc;

	foreach(lc, vectorizedQual)
	{
		Node *node = lfirst(lc);

		switch(nodeTag(node))
		{
			case T_OpExpr:
			case T_DistinctExpr:	/* struct-equivalent to OpExpr */
			case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr *opExprNode = (OpExpr *) node;

				int argno = 0;
				int nargs = list_length(opExprNode->args);

				VectorQual *newVectorQual = palloc(sizeof(VectorQual));
				newVectorQual->vectorQualType = VECTOR_QUAL_EXPR;

				newVectorQual->u.expr.fmgrInfo = palloc0(sizeof(FmgrInfo));
				newVectorQual->u.expr.fcInfo  = palloc0(SizeForFunctionCallInfo(nargs));
				newVectorQual->u.expr.vectorFnArguments = 
					(VectorFnArgument *) palloc0(sizeof(VectorFnArgument) * nargs);


				fmgr_info(opExprNode->opfuncid, newVectorQual->u.expr.fmgrInfo);
				fmgr_info_set_expr((Node *) node, newVectorQual->u.expr.fmgrInfo);

				/* Initialize function call parameter structure too */
				InitFunctionCallInfoData(*(newVectorQual->u.expr.fcInfo), 
										 newVectorQual->u.expr.fmgrInfo,
										 nargs, opExprNode->inputcollid, NULL, NULL);

				ListCell *lcOpExprArgs;
				foreach(lcOpExprArgs, opExprNode->args)
				{
					Expr *arg = (Expr *) lfirst(lcOpExprArgs);

					VectorFnArgument *vectorFnArgument = 
						newVectorQual->u.expr.vectorFnArguments + argno;

					if (IsA(arg, Const))
					{
						Const *con = (Const *) arg;

						vectorFnArgument->type = VECTOR_FN_ARG_CONSTANT;
						vectorFnArgument->arg = con->constvalue;
						
						newVectorQual->u.expr.fcInfo->args[argno].value = (Datum) vectorFnArgument;
						newVectorQual->u.expr.fcInfo->args[argno].isnull = con->constisnull;
					}
					else if (IsA(arg, Var))
					{
						Var *variable = (Var *) arg;
						int columnIdx = variable->varattno - 1;

						vectorFnArgument->type = VECTOR_FN_ARG_VAR;
						vectorFnArgument->arg = vectorSlot->tts.tts_values[columnIdx];

						newVectorQual->u.expr.fcInfo->args[argno].value = (Datum) vectorFnArgument;
						newVectorQual->u.expr.fcInfo->args[argno].isnull = false;
					}
					
					argno++;
				}

				vectorQualList = lappend(vectorQualList, newVectorQual);
				break;
			}

			case T_BoolExpr:
			{
				BoolExpr *boolExpr = castNode(BoolExpr, node);

				VectorQual *newVectorQual = palloc0(sizeof(VectorQual));
				newVectorQual->vectorQualType = VECTOR_QUAL_BOOL_EXPR;
				
				List *newQualExprArgList = 
					ConstructVectorizedQualList(slot, boolExpr->args);

				newVectorQual->u.boolExpr.boolExprType = boolExpr->boolop;

				ListCell *lcExprArgList;
				foreach(lcExprArgList, newQualExprArgList)
				{
					newVectorQual->u.boolExpr.vectorQualExprList = 
						lappend(newVectorQual->u.boolExpr.vectorQualExprList, 
								(VectorQual*)lfirst(lcExprArgList));
				}

				vectorQualList = lappend(vectorQualList, newVectorQual);

				break;
			}

			default:
				break;
		}
	}

	return vectorQualList;
}

/*
 * vectorizedOr / vectorizedAnd
 */

static void
vectorizedAnd(bool *left, bool *right, int dimension)
{
	for (int n = 0; n < dimension; n++)
	{
		left[n] &= right[n];
	}
}

static void
vectorizedOr(bool *left, bool *right, int dimension)
{
	for (int n = 0; n < dimension; n++)
	{
		left[n] |= right[n];
	} 
}

static bool *
executeVectorizedExpr(VectorQual *vectorQual, ExprContext *econtext)
{
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	VectorColumn *res = 
		(VectorColumn *) vectorQual->u.expr.fmgrInfo->fn_addr(vectorQual->u.expr.fcInfo);
	MemoryContextSwitchTo(oldContext);

	return (bool *) res->value;
}

bool * 
ExecuteVectorizedQual(TupleTableSlot *slot, List *vectorizedQualList,
			BoolExprType boolType, ExprContext *econtext)
{
	VectorTupleTableSlot *vectorSlot = (VectorTupleTableSlot *) slot;
	ListCell *lc;

	bool *result = NULL;

	foreach(lc, vectorizedQualList)
	{
		VectorQual *vectorQual = (VectorQual *) lfirst(lc);

		bool *qualResult = NULL;

		switch(vectorQual->vectorQualType)
		{
			case VECTOR_QUAL_EXPR:
			{
				qualResult = executeVectorizedExpr(vectorQual, econtext);
				break;
			}
			case VECTOR_QUAL_BOOL_EXPR:
			{
				if (vectorQual->u.boolExpr.boolExprType == AND_EXPR)
				{
					qualResult = ExecuteVectorizedQual(slot,
										vectorQual->u.boolExpr.vectorQualExprList,
										AND_EXPR, econtext);
				}
				else if (vectorQual->u.boolExpr.boolExprType == OR_EXPR)
				{
					qualResult = ExecuteVectorizedQual(slot,
										vectorQual->u.boolExpr.vectorQualExprList,
										OR_EXPR, econtext);
				}
				break;
			}
		}

		if (result == NULL)
		{
			result = qualResult;
		}
		else
		{
		
			if (boolType == AND_EXPR)
				vectorizedAnd(result, qualResult, vectorSlot->dimension);
			else if (boolType == OR_EXPR)
				vectorizedOr(result, qualResult, vectorSlot->dimension);
		}
	}

	return result;
}
