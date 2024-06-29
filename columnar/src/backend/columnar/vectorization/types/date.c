
#include "postgres.h"

#include "nodes/execnodes.h"
#include "utils/date.h"

#include "columnar/vectorization/types/types.h"

// date (int32)
BUILD_CMP_OPERATOR_INT(date_, DateADT, DateADT)

// time (int64)
BUILD_CMP_OPERATOR_INT(time_, TimeADT, TimeADT)

// timestamp (int64)
BUILD_CMP_OPERATOR_INT(timestamp_, Timestamp, Timestamp)

// timestamptz (int64)
BUILD_CMP_OPERATOR_INT(timestamptz_, TimestampTz, TimestampTz)
