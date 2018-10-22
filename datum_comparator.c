#include <postgres.h>
#include <utils/datum.h>
#include <string.h>
#include <utils/timestamp.h>

#include "datum_comparator.h"

int cmp_dimension_id_int16(const void *left, const void *right) {
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

int cmp_dimension_id_int32(const void *left, const void *right) {
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

int cmp_dimension_id_int64(const void *left, const void *right) {
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

int cmp_dimension_id_float4(const void *left, const void *right) {
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

int cmp_dimension_id_float8(const void *left, const void *right) {
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

int cmp_dimension_id_timestamptz(const void *left, const void *right) {
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

int cmp_dimension_id_varchar(const void *left, const void *right) {
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



