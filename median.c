#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <catalog/pg_type.h>
#include "utils/datum.h"
#include "datum_comparator.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define MAX_LEN 200

typedef struct state {
	int length;
	Datum values[MAX_LEN];
} State;

PG_FUNCTION_INFO_V1( median_transfn);
PG_FUNCTION_INFO_V1( median_finalfn);



static comparison_fn_t get_compare_function(Oid oid) {

	comparison_fn_t cmp;

	switch (oid) {
	case INT2OID:
		cmp = cmp_dimension_id_int16;
		break;
	case INT4OID:
		cmp = cmp_dimension_id_int32;
		break;
	case INT8OID:
		cmp = cmp_dimension_id_int64;
		break;
	case FLOAT4OID:
		cmp = cmp_dimension_id_float4;
		break;
	case FLOAT8OID:
		cmp = cmp_dimension_id_float8;
		break;
	case TIMESTAMPTZOID:
		cmp = cmp_dimension_id_timestamptz;
		break;
	case TEXTOID:
		cmp = cmp_dimension_id_varchar;
		break;
	default:
		elog(ERROR, "unsupported data type for comparison");
	}
	return cmp;
}

static Datum get_mean(Oid oid, Datum val1, Datum val2) {
	Datum res;
	switch (oid) {
	case INT2OID:
		res = Int16GetDatum(
				DatumGetInt16(val1)
						+ (DatumGetInt16(val2) - DatumGetInt16(val1)) / 2);
		break;
	case INT4OID:
		res = Int32GetDatum(
				DatumGetInt32(val1)
						+ (DatumGetInt32(val2) - DatumGetInt32(val1)) / 2);
		break;
	case INT8OID:
		res = Int64GetDatum(
				DatumGetInt64(val1)
						+ (DatumGetInt64(val2) - DatumGetInt64(val1)) / 2);
		break;
	case FLOAT4OID:
		res = Float4GetDatum(
				DatumGetFloat4(val1)
						+ (DatumGetFloat4(val2) - DatumGetFloat4(val1)) / 2.0);
		break;
	case FLOAT8OID:
		res = Float8GetDatum(
				DatumGetFloat8(val1)
						+ (DatumGetFloat8(val2) - DatumGetFloat8(val1)) / 2.0);
		break;
	case TIMESTAMPTZOID:
		res =
				TimestampTzGetDatum(
						DatumGetTimestampTz(val1)
								+ (DatumGetTimestampTz(val2)
										- DatumGetTimestampTz(val1)) / 2.0);
		break;
	case TEXTOID:
		/*Not possible to evaluate mean of two text values*/
		res = (Datum) 0;
		break;
	default:
		elog(ERROR, "unknown input datatype");
		res = (Datum) 0;
	}

	return res;
}

static void swap(Datum *arr, int i, int j) {

	Datum temp;
	temp = arr[i];
	arr[i] = arr[j];
	arr[j] = temp;
}

/*
 * Performs quick sort partitioning. Treats rightmost element as the pivot and returns
 * its index in a sorted array.
 */

static int partition(Datum *arr, int l, int r, comparison_fn_t cmp)
{
    int i;
    i = l;
    for (int j = l; j <= r - 1; j++) {

        if (cmp(&arr[j],&arr[r]) <= 0) {
            swap(arr, i, j);
            i++;
        }
    }
    swap(arr, i, r);
    return i;
}

/**
 * Implements 'quickselect' algorithm.
 * Returns the kth smallest element in the given array with average O(n) time complexity.
 */

static Datum get_k_smallest(Datum *arr, int l, int r, int k, comparison_fn_t cmp)
{
	int index;

    if (k > 0 && k <= r - l + 1) {

        index = partition(arr, l, r, cmp);


        // If kth position, return element
        if (index - l == k - 1){
        	return arr[index];
        }

        // If position is more, look in the left partition
        if (index - l > k - 1){
            return get_k_smallest(arr, l, index - 1, k, cmp);

        }
        // Else look in the right partition
        return get_k_smallest(arr, index + 1, r, k - index + l - 1, cmp);
    }

    //k should not be less than zero or greater the array size
    return (Datum)0;
}

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */

Datum median_transfn( PG_FUNCTION_ARGS) {
	MemoryContext agg_context;
	bytea *pg_state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	Oid element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	Datum element;
	State *state;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	if (!OidIsValid(element_type))
		elog(ERROR, "could not determine data type of input");

	if (PG_ARGISNULL(1))
		PG_RETURN_BYTEA_P(pg_state); //ignore null values, and return current state without change
	else
		element = PG_GETARG_DATUM(1);

	if (!pg_state) {
		/* Allocate memory for the state and add the first node*/
		int size = sizeof(State);
		pg_state = (bytea*) palloc(VARHDRSZ + size);
		SET_VARSIZE(pg_state, size);

		state = (State*) VARDATA(pg_state);
		state->length = 1;
		state->values[0] = element;
	} else {
		state = (State*) VARDATA(pg_state);
		/* Add the new datum node to the list*/
		state->values[state->length] = element;
		state->length += 1;
	}
	PG_RETURN_BYTEA_P(pg_state);
}

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum median_finalfn( PG_FUNCTION_ARGS) {
	MemoryContext agg_context;
	bytea *pg_state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	Oid element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	Datum ret;
	State *state;
	comparison_fn_t cmp;
	int mid;

	if (pg_state == NULL)
		PG_RETURN_NULL();

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	if (!OidIsValid(element_type))
		elog(ERROR, "could not determine data type of input");

	if (element_type != INT2OID && element_type != INT4OID
			&& element_type != INT8OID && element_type != FLOAT4OID
			&& element_type != FLOAT8OID && element_type != TIMESTAMPTZOID
			&& element_type != TEXTOID) {
		elog(ERROR, "input data type not supported for median determination");
		PG_RETURN_NULL();
	}

	state = (State*) VARDATA(pg_state);
	/*return null if no elements in the list*/
	if (state->length == 0)
		PG_RETURN_NULL();

	if (element_type == TEXTOID && state->length % 2 == 0) {
		elog(ERROR, "median for even number of text inputs not supported");
		PG_RETURN_NULL();
	}

	/* find compare function for the input data type */
	cmp = get_compare_function(element_type);

	mid = state->length / 2;

	/*get the kth smallest element without actually sorting the array*/
	ret = get_k_smallest(state->values, 0, state->length -1, mid+1, cmp);
	if(state->length % 2 != 0){
		//odd entries,return the median
	} else{
		Datum median2;
		//get (mid-1)th smallest element
		median2 = get_k_smallest(state->values, 0, state->length -1, mid, cmp);
		ret = get_mean(element_type, ret, median2);
	}


	PG_RETURN_DATUM(ret);
}
