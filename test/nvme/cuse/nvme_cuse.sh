#!/usr/bin/env bash
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2020 Intel Corporation
#  All rights reserved.
#
testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../../..)
source $rootdir/scripts/common.sh
source $rootdir/test/common/autotest_common.sh

if [[ $(uname) == "Linux" ]]; then
	modprobe cuse
elif [[ $(uname) == "FreeBSD" ]]; then
	kldload -n cuse
else
	echo "NVMe cuse tests not supported"
	exit 1
fi

run_test "nvme_cuse_app" $testdir/cuse
run_test "nvme_cuse_rpc" $testdir/nvme_cuse_rpc.sh
run_test "nvme_cli_cuse" $testdir/spdk_nvme_cli_cuse.sh

# The smartmontools smartctl utility on FreeBSD does not work with nvme devices
# other then with /dev/nvme preifx.
# TODO: Skip it, until smartmontools will be fixed appropriately.
if [[ $(uname) == "Linux" ]]; then
	run_test "nvme_smartctl_cuse" $testdir/spdk_smartctl_cuse.sh
fi

run_test "nvme_ns_manage_cuse" $testdir/nvme_ns_manage_cuse.sh

if [[ $(uname) == "Linux" ]]; then
	rmmod cuse
elif [[ $(uname) == "FreeBSD" ]]; then
	kldunload cuse
fi

"$rootdir/scripts/setup.sh"
