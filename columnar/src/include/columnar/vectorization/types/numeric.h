/*-------------------------------------------------------------------------
 *
 * numeric.h
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/types/numeric.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTORIZATION_TYPE_INT128_H
#define VECTORIZATION_TYPE_INT128_H

#include "postgres.h"

#include "utils/numeric.h"
extern Numeric int128_to_numeric(int128 val);

#endif