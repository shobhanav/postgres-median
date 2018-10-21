
	elog(LOG, "median_finalfn: Entering");
	MemoryContext agg_context;
	bytea *state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	elog(LOG, "median_finalfn: 2");
	Oid element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	elog(LOG, "median_finalfn: 3");
	Datum *val_arr;
	Datum ret;
	int16 typlen;
	bool typbyval;
	char typalign;
	STATE_STR *istate;
	NODE *nodep;
	ArrayType *arr;
	TupleDesc *tupdesc;
	HeapTuple heapTuple;
	bool isnull;

	Aggref *aggref;
	List *sortlist;
	/* Sort single datums */
	SortGroupClause *sortcl;
	TargetEntry *tle;
	Oid sortOperator;
	Oid sortCollation;
	bool sortNullsFirst;
	Oid sortColType;
	bool rescan_needed;
	Oid eqOperator;
	Tuplesortstate *tuplestate;

	int mid;

	elog(LOG, "median_finalfn: 4");

	if (!state)
		PG_RETURN_NULL();

	elog(LOG, "median_finalfn: 4.1");

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	elog(LOG, "median_finalfn: 4.2");

	if (!OidIsValid(element_type))
		elog(ERROR, "could not determine data type of input");

	elog(LOG, "median_finalfn: 4.3");

	istate = (STATE_STR*) VARDATA(state);
	/*return null if no elements in the list*/
	if (istate->length == 0)
		PG_RETURN_NULL();

	elog(LOG, "median_finalfn: 4.8");

	nodep = istate->start;

	elog(LOG, "median_finalfn: 4.9");

	/* get required info about the element type */
	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	elog(LOG, "median_finalfn: 5");

	/* Get the Aggref so we can examine aggregate's arguments */
	aggref = AggGetAggref(fcinfo);

	elog(LOG, "median_finalfn: 5.1");

	/* Extract the sort information */
	sortlist = aggref->aggorder;
	elog(LOG, "median_finalfn: 5.2");
	sortcl = (SortGroupClause *) linitial(sortlist);
	elog(LOG, "median_finalfn: 5.3");
	tle = get_sortgroupclause_tle(sortcl, aggref->args);
	elog(LOG, "median_finalfn: 5.4");

	/* Save sort ordering info */
	sortColType = exprType((Node *) tle->expr);
	elog(LOG, "median_finalfn: 5.5");
	sortOperator = sortcl->sortop;
	eqOperator = sortcl->eqop;
	elog(LOG, "median_finalfn: 5.6");
	sortCollation = exprCollation((Node *) tle->expr);
	elog(LOG, "median_finalfn: 5.7");
	sortNullsFirst = sortcl->nulls_first;

	elog(LOG, "median_finalfn: 6");

	tuplestate = tuplesort_begin_datum(element_type, sortOperator, sortCollation, 0, work_mem, 0);



	//convert value list to array for efficient sorting
	val_arr = (Datum*) palloc(sizeof(Datum) * istate->length);
	for (int i = 0; i < istate->length; i++) {
		val_arr[i] = nodep->val;
		tuplesort_putdatum(tuplestate, nodep->val, false);
		nodep = nodep->next;
	}

	get_call_result_type(fcinfo, &element_type, tupdesc);
	heapTuple = heap_form_tuple(tupdesc, val_arr, &isnull);
	tuplesort_putheaptuple(tuplestate, heapTuple);
	elog(LOG, "median_finalfn: 7");
	//sort
	tuplesort_performsort(tuplestate);

	elog(LOG, "median_finalfn: 8");


	/* now build the array */
	//arr = construct_array (val_arr, istate->length, element_type, istate->length, typbyval, typalign);
	//qsort(val_arr, istate->length, sizeof(Datum), cmp_dimension_id);
	mid = istate->length / 2;
	Datum val;
	Datum middle;
	Datum middle_prev;
	bool null;
	Datum abbrev_val = (Datum) 0;
	int i = 0;
	while(tuplesort_getdatum(tuplestate, true, &val, &null, &abbrev_val)){
		i++;
		if(i == mid -1){
			middle_prev = val;
		} else if(i == mid){
			middle = val;
		}
	}

	if (istate->length % 2 != 0)
		ret = middle;
	else
		ret = (middle + middle_prev -1) / 2;

//	if (istate->length % 2 != 0)
//		ret = val_arr[mid];
//	else
//		ret = (val_arr[mid] + val_arr[mid - 1]) / 2;

	elog(LOG, "median_finalfn: returning");

	PG_RETURN_DATUM(ret);

