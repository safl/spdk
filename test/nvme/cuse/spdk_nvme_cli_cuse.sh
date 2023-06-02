#!/usr/bin/env bash
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2019 Intel Corporation
#  All rights reserved.
#
testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../../..)
source "$testdir/common.sh"

rm -Rf $testdir/match_files
mkdir $testdir/match_files

KERNEL_OUT=$testdir/match_files/kernel.out
CUSE_OUT=$testdir/match_files/cuse.out

if [[ $(uname) == "Linux" ]]; then
	NVME_CMD=/usr/local/src/nvme-cli/nvme
elif [[ $(uname) == "FreeBSD" ]]; then
	export LC_ALL=C
	NVME_CMD=/sbin/nvmecontrol
fi

rpc_py=$rootdir/scripts/rpc.py

"$rootdir/scripts/setup.sh" reset
scan_nvme_ctrls

if ! nvme_name=$(get_nvme_with_ns_management); then
	echo "Failed to find suitable nvme for the test" >&2
	return 1
fi

function verify_nvme() {
	ctrlr=${1}
	ns=${2}
	out=${3}

	if [[ $(uname) == "Linux" ]]; then
		oacs=$(${NVME_CMD} id-ctrl $ctrlr | grep oacs | cut -d: -f2)
		oacs_firmware=$((oacs & 0x4))

		${NVME_CMD} get-ns-id $ns > ${out}.1
		${NVME_CMD} id-ns $ns > ${out}.2
		${NVME_CMD} list-ns $ns > ${out}.3

		${NVME_CMD} id-ctrl $ctrlr > ${out}.4
		${NVME_CMD} list-ctrl $ctrlr > ${out}.5
		if [ "$oacs_firmware" -ne "0" ]; then
			${NVME_CMD} fw-log $ctrlr > ${out}.6
		fi
		${NVME_CMD} smart-log $ctrlr
		${NVME_CMD} error-log $ctrlr > ${out}.7
		${NVME_CMD} get-feature $ctrlr -f 1 -s 1 -l 100 > ${out}.8
		${NVME_CMD} get-log $ctrlr -i 1 -l 100 > ${out}.9
		${NVME_CMD} reset $ctrlr > ${out}.10
		# Negative test to make sure status message is the same on failures
		# FID 2 is power management. It should be constrained to the whole system
		# Attempting to apply it to a namespace should result in a failure
		${NVME_CMD} set-feature $ctrlr -n 1 -f 2 -v 0 2> ${out}.11 || true
	elif [[ $(uname) == "FreeBSD" ]]; then
		oacs_firmware=$(${NVME_CMD} identify $ctrlr | grep 'Firmware Activate/Download' | grep 'Not')
		${NVME_CMD} nsid $ns > ${out}.1
		${NVME_CMD} identify $ns > ${out}.2
		${NVME_CMD} identify $ctrlr > ${out}.3

		if [ -z "$oacs_firmware" ]; then
			${NVME_CMD} logpage -p 3 $ctrlr > ${out}.6
		fi

		${NVME_CMD} logpage -p 2 $ctrlr
		${NVME_CMD} logpage -p 1 $ctrlr > ${out}.7
		${NVME_CMD} admin-passthru $ctrlr -o 10 -l 64 -4 1 -r > ${out}.8
		${NVME_CMD} admin-passthru $ctrlr -o 02 -l 64 -4 1 -r > ${out}.9
		${NVME_CMD} reset $ctrlr > ${out}.10
	fi
}

function verify_read_write() {
	if [[ $(uname) == "Linux" ]]; then
		tr < /dev/urandom -dc "a-zA-Z0-9" | fold -w 512 | head -n 1 > $testdir/write_file
		${NVME_CMD} write $ns --data-size=512 --data=$testdir/write_file
		${NVME_CMD} read $ns --data-size=512 --data=$testdir/read_file
		diff --ignore-trailing-space $testdir/write_file $testdir/read_file
		rm -f $testdir/write_file $testdir/read_file
	elif [[ $(uname) == "FreeBSD" ]]; then
		tr < /dev/urandom -dc "a-zA-Z0-9" | fold -w 511 | head -n 1 > $testdir/write_file
		truncate -s 512B $testdir/read_file
		${NVME_CMD} io-passthru $ns -o 1 -n 1 -w -l 512 -i $testdir/write_file
		${NVME_CMD} io-passthru $ns -o 2 -n 1 -r -l 512 -b > $testdir/read_file
		diff $testdir/write_file $testdir/read_file
		rm -f $testdir/write_file $testdir/read_file
	fi
}

function verify_admin_cmd_no_data_transferred() {
	${NVME_CMD} admin-passthru $ctrlr -o 5 --cdw10=0x3ff0003 --cdw11=0x1 -r
	${NVME_CMD} admin-passthru $ctrlr -o 4 --cdw10=0x3
}

if [[ $(uname) == "Linux" ]]; then
	ctrlr="/dev/${nvme_name}"
	ns="/dev/${nvme_name}n1"
	waitforblk "${nvme_name}n1"
elif [[ $(uname) == "FreeBSD" ]]; then
	ctrlr="${nvme_name}"
	ns="${nvme_name}ns1"
	waitforfile "/dev/${nvme_name}ns1"
fi

bdf=${bdfs["$nvme_name"]}

verify_nvme ${ctrlr} ${ns} ${KERNEL_OUT}

$rootdir/scripts/setup.sh

$SPDK_BIN_DIR/spdk_tgt -m 0x3 &
spdk_tgt_pid=$!
trap 'kill -9 ${spdk_tgt_pid}; exit 1' SIGINT SIGTERM EXIT

waitforlisten $spdk_tgt_pid

$rpc_py bdev_nvme_attach_controller -b Nvme0 -t PCIe -a ${bdf}
$rpc_py bdev_nvme_cuse_register -n Nvme0

if [[ $(uname) == "Linux" ]]; then
	ctrlr="/dev/spdk/nvme0"
	ns="${ctrlr}n1"
	waitforfile "${ns}"
elif [[ $(uname) == "FreeBSD" ]]; then
	ctrlr="spdk/nvme0"
	ns="${ctrlr}ns1"
	waitforfile "/dev/${ns}"
fi

$rpc_py bdev_get_bdevs
$rpc_py bdev_nvme_get_controllers

verify_nvme ${ctrlr} ${ns} ${CUSE_OUT}

if [[ $(uname) == "Linux" ]]; then
	for i in {1..11}; do
		if [ -f "${KERNEL_OUT}.${i}" ] && [ -f "${CUSE_OUT}.${i}" ]; then
			sed -i "s/${nvme_name}/nvme0/g" ${KERNEL_OUT}.${i}
			diff --suppress-common-lines ${KERNEL_OUT}.${i} ${CUSE_OUT}.${i}
		fi
	done
elif [[ $(uname) == "FreeBSD" ]]; then
	kernel_ns=$(cat ${KERNEL_OUT}.1 | awk -F ' ' '{print $2}')
	cuse_ns=$(cat ${CUSE_OUT}.1 | awk -F ' ' '{print $2}')
	[[ "$kernel_ns" == "$cuse_ns" ]]

	for i in {2..11}; do
		if [ -f "${KERNEL_OUT}.${i}" ] && [ -f "${CUSE_OUT}.${i}" ]; then
			diff --suppress-common-lines ${KERNEL_OUT}.${i} ${CUSE_OUT}.${i}
		fi
	done
fi

rm -Rf $testdir/match_files

verify_read_write
verify_admin_cmd_no_data_transferred

if [[ $(uname) == "Linux" ]]; then
	[[ -c "$ctrlr" ]]
	[[ -c "$ns" ]]
elif [[ $(uname) == "FreeBSD" ]]; then
	[[ -c "/dev/$ctrlr" ]]
	[[ -c "/dev/$ns" ]]
fi

trap - SIGINT SIGTERM EXIT
killprocess $spdk_tgt_pid
