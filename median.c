#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <catalog/pg_type.h>
#include <string.h>
#include "utils/datum.h"

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

static int cmp_dimension_id_int16(const void *left, const void *right) {
	const Datum datuml = *(Datum *) left;
	const Datum datumr = *(Datum *) right;

	int16 vall = DatumGetInt16(datuml);
	int16 valr = DatumGetInt16(datumr);

	if (vall < valr)
		return -1;

	if (vall > valr)
		return 1;

	return 0;
}

static int cmp_dimension_id_int32(const void *left, const void *right) {
	const Datum datuml = *(Datum *) left;
	const Datum datumr = *(Datum *) right;

	int32 vall = DatumGetInt32(datuml);
	int32 valr = DatumGetInt32(datumr);

	if (vall < valr)
		return -1;

	if (vall > valr)
		return 1;

	return 0;
}

static int cmp_dimension_id_int64(const void *left, const void *right) {
	const Datum datuml = *(Datum *) left;
	const Datum datumr = *(Datum *) right;

	int64 vall = DatumGetInt64(datuml);
	int64 valr = DatumGetInt64(datumr);

	if (vall < valr)
		return -1;

	if (vall > valr)
		return 1;

	return 0;
}

static int cmp_dimension_id_float4(const void *left, const void *right) {
	const Datum datuml = *(Datum *) left;
	const Datum datumr = *(Datum *) right;

	float4 vall = DatumGetFloat4(datuml);
	float4 valr = DatumGetFloat4(datumr);

	if (vall < valr)
		return -1;

	if (vall > valr)
		return 1;

	return 0;
}

static int cmp_dimension_id_float8(const void *left, const void *right) {
	const Datum datuml = *(Datum *) left;
	const Datum datumr = *(Datum *) right;

	float8 vall = DatumGetFloat8(datuml);
	float8 valr = DatumGetFloat8(datumr);

	if (vall < valr)
		return -1;

	if (vall > valr)
		return 1;

	return 0;
}

static int cmp_dimension_id_timestamptzcmp(const void *left, const void *right) {
	const Datum datuml = *(Datum *) left;
	const Datum datumr = *(Datum *) right;

	TimestampTz ts1 = DatumGetTimestampTz(datuml);
	TimestampTz ts2 = DatumGetTimestampTz(datumr);

	if (ts1 < ts2)
		return -1;

	if (ts1 > ts2)
		return 1;

	return 0;
}

static int cmp_dimension_id_varchar(const void *left, const void *right) {
	const Datum datuml = *(Datum *) left;
	const Datum datumr = *(Datum *) right;

	bytea *vall = (bytea *) DatumGetPointer(datuml);
	bytea *valr = (bytea *) DatumGetPointer(datumr);

	char *str1 = VARDATA_ANY(vall);
	char *str2 = VARDATA_ANY(valr);

	/*Null terminate C strings*/
	str1[VARSIZE_ANY_EXHDR(vall)] = '\0';
	str2[VARSIZE_ANY_EXHDR(valr)] = '\0';

	return strcmp(str1, str2);
}

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
		cmp = cmp_dimension_id_timestamptzcmp;
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

	qsort(state->values, state->length, sizeof(Datum), cmp);

	mid = state->length / 2;

	if (state->length % 2 != 0)
		ret = state->values[mid];
	else
		ret = get_mean(element_type, state->values[mid],
				state->values[mid - 1]);

	PG_RETURN_DATUM(ret);
}
