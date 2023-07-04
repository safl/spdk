#!/usr/bin/env bash
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2023 Intel Corporation.
#  All rights reserved.
#
testdir=$(readlink -f "$(dirname $0)")
rootdir=$(readlink -f "$testdir/../..")
source "$rootdir/test/common/autotest_common.sh"

# set up reference collector environment variables
export INTEL_LIBITTNOTIFY64=$VTUNE_ITTAPI_DIR/src/ittnotify_refcol/libittrefcol.so
export INTEL_LIBITTNOTIFY_LOG_DIR=$SPDK_TEST_STORAGE

cleanup() {
	rm -f "$INTEL_LIBITTNOTIFY_LOG_DIR"/libittnotify_refcol_*.log
}

parse_itt_bytes_out() {
	local itt_refcol bytes=0 m1 m4

	mapfile -t itt_refcol < <(grep "domain=$1" "$2")
	itt_refcol=("${itt_refcol[@]##*=}")

	while IFS=";" read -r _ m1 _ m4 _; do
		((bytes += m1 + m4))
	done < <(printf '%s\n' "${itt_refcol[@]}")

	echo "$bytes"
}

bdevperf_workload() {
	local itt_refcol itt_refcol_log bdevperf bdevperf_total diff
	local bytes_transferred_bdevperf=0 bytes_captured_ittrefcol=0

	mapfile -t bdevperf < <(
		"$rootdir/build/examples/bdevperf" \
			-c "$rootdir/examples/bdev/fio_plugin/bdev.json" \
			-t 10 -w randrw -M 50 -o 4096 -q 32 -S 1 2>&1
	)

	itt_refcol_log=("$INTEL_LIBITTNOTIFY_LOG_DIR/"libittnotify_refcol_*.log)

	((${#itt_refcol_log[@]} == 1))
	[[ -s ${itt_refcol_log[0]} ]]

	read -r _ _ _ bytes_transferred_bdevperf _ <<< "${bdevperf[-1]}"
	bytes_transferred_bdevperf=${bytes_transferred_bdevperf%.*}
	((bytes_transferred_bdevperf *= 4096 * 10, bytes_transferred_bdevperf > 0))

	bytes_captured_ittrefcol=$(xtrace_disable_per_cmd parse_itt_bytes_out spdk_bdev "${itt_refcol_log[0]}")
	((bytes_captured_ittrefcol > 0))

	diff=$((100 - (bytes_captured_ittrefcol * 100 / bytes_transferred_bdevperf))) diff=${diff#-}

	cat <<- RESULTS >&2
		bdevperf: $bytes_transferred_bdevperf rx/tx bytes
		ITT Reference collector: $bytes_captured_ittrefcol bytes
		Difference: $diff%
	RESULTS

	((diff < 5))
}

trap 'cleanup' EXIT

run_test "bdevperf_workload" bdevperf_workload
