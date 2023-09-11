
#include "postgres.h"
#include "common/int.h"

#include "fmgr.h"
#include "nodes/execnodes.h"

#include "columnar/vectorization/types/types.h"

// char
BUILD_CMP_OPERATOR_INT( char,  char,  char)

// int2
BUILD_CMP_OPERATOR_INT( int2, int16, int16)
BUILD_CMP_OPERATOR_INT(int24, int16, int32)
BUILD_CMP_OPERATOR_INT(int28, int16, int64)

// int4
BUILD_CMP_OPERATOR_INT( int4, int32, int32)
BUILD_CMP_OPERATOR_INT(int42, int32, int16)
BUILD_CMP_OPERATOR_INT(int48, int32, int64)

// int8
BUILD_CMP_OPERATOR_INT( int8, int64, int64)
BUILD_CMP_OPERATOR_INT(int82, int64, int16)
BUILD_CMP_OPERATOR_INT(int84, int64, int32)
