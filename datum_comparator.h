#ifndef DATUM_COMPARATOR_H_
#define DATUM_COMPARATOR_H_

int cmp_dimension_id_int16(const void *left, const void *right);
int cmp_dimension_id_int32(const void *left, const void *right);
int cmp_dimension_id_int64(const void *left, const void *right);
int cmp_dimension_id_float4(const void *left, const void *right);
int cmp_dimension_id_float8(const void *left, const void *right);
int cmp_dimension_id_timestamptz(const void *left, const void *right);
int cmp_dimension_id_varchar(const void *left, const void *right);


#endif /* DATUM_COMPARATOR_H_ */
