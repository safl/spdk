#!/usr/bin/env bash
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2024 Intel Corporation
#  All rights reserved.
#

testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../..)

source $rootdir/test/common/autotest_common.sh
source $rootdir/test/nvmf/common.sh

fio_py="$rootdir/scripts/fio-wrapper"
bdevperf_py="$rootdir/examples/bdev/bdevperf/bdevperf.py"

SPEC_KEY="NVMeTLSkey-1:01:VRLbtnN9AQb2WXW3c9+wEf/DRLz0QuLdbYvEhwtdWwNf9LrZ:"
# SUBNQN and HOSTNQN may produce a key with NULL byte inside, which might be problematic.
# Use values from NVMe TCP spec, as they are confirmed to work.
SPEC_SUBSYSNQN="nqn.2014-08.org.nvmexpress:uuid:36ebf5a9-1df9-47b3-a6d0-e9ba32e428a2"
SPEC_HOSTID="f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
SPEC_HOSTNQN="nqn.2014-08.org.nvmexpress:uuid:${SPEC_HOSTID}"
PSK_IDENTITY="NVMe0R01 ${SPEC_HOSTNQN} ${SPEC_SUBSYSNQN}"
TLSHD_CONF="$testdir/tlshd.conf"
SPDK_PSK_PATH="$testdir/key.txt"
PSK_NAME="psk0"
CONTROLLER_NAME="TLSTEST"
nvmet=/sys/kernel/config/nvmet
nvmet_subsys="$nvmet/subsystems/$SPEC_SUBSYSNQN"
nvmet_host="$nvmet/hosts/$SPEC_HOSTNQN"
kernel_port="/sys/kernel/config/nvmet/ports/1"
kernel_subsystem="$nvmet/subsystems/$SPEC_SUBSYSNQN"

cleanup() {
	killprocess $tlshdpid || :
	killprocess $bdevperfpid || :
	nvmftestfini || :
	rm -rf "$nvmet_subsys/allowed_hosts/$SPEC_HOSTNQN" || :
	rmdir "$nvmet_host" || :
	clean_kernel_target || :
	rm "$SPDK_PSK_PATH" || :
	rm "$TLSHD_CONF" || :
	# configure_kernel_target() binds the SSDs to the kernel driver, so move them back to
	# userspace, as this is what the tests running after this one expect
	"$rootdir/scripts/setup.sh"
}

construct_tlshd_conf() {
	local keyring_name=$1
	cat << EOF > $TLSHD_CONF
[debug]
loglevel=0
tls=0
nl=0

[authentication]
keyrings= ${keyring_name}
EOF
}

post_configure_kernel_target() {
	echo 0 > "$nvmet_subsys/attr_allow_any_host"
	mkdir "$nvmet_host"
	ln -s "$nvmet_host" "$nvmet_subsys/allowed_hosts/$SPEC_HOSTNQN"

	# Disable the listener to set tsas
	rm "$kernel_port/subsystems/$SPEC_SUBSYSNQN"
	echo "tls1.3" > "$kernel_port/addr_tsas"
	ln -s "$kernel_subsystem" "$kernel_port/subsystems/"
}

nvmet_tls_init() {
	configure_kernel_target "$SPEC_SUBSYSNQN" "$(get_main_ns_ip)" "$NVMF_THIRD_PORT"
	post_configure_kernel_target
}

setup_nvmf_tgt() {
	local psk_path=$1

	$rpc_py sock_impl_set_options -i ssl --enable-ktls --tls-version 13
	$rpc_py framework_start_init
	$rpc_py nvmf_create_transport $NVMF_TRANSPORT_OPTS
	$rpc_py nvmf_create_subsystem $SPEC_SUBSYSNQN -s "$NVMF_SERIAL" -m 10
	$rpc_py nvmf_subsystem_add_listener $SPEC_SUBSYSNQN -t $TEST_TRANSPORT \
		-a $NVMF_FIRST_TARGET_IP -s $NVMF_PORT -k -c 1
	$rpc_py bdev_malloc_create 32 4096 -b malloc0
	$rpc_py nvmf_subsystem_add_ns $SPEC_SUBSYSNQN malloc0 -n 1

	$rpc_py keyring_file_add_key $PSK_NAME "$psk_path"

	$rpc_py nvmf_subsystem_add_host $SPEC_SUBSYSNQN $SPEC_HOSTNQN --psk $PSK_NAME
}

if [ "$TEST_TRANSPORT" != tcp ]; then
	echo "Unsupported transport: $TEST_TRANSPORT"
	exit 1
fi

"$rootdir/scripts/setup.sh"

nvmftestinit

timing_enter prepare_keyring_and_daemon

session_id=$(keyctl show | awk '{print $1}' | tail -1)
keyring_name="test_${session_id}"
keyring_id=$(keyctl newring $keyring_name $session_id)
keyctl setperm $keyring_id 0x3f3f0b00

key_name=test_key_${session_id}
key_id=$(keyctl add psk "$PSK_IDENTITY" $("$rootdir/build/examples/tls_psk_print" -k $SPEC_KEY \
	-s $SPEC_SUBSYSNQN -n $SPEC_HOSTNQN) "$keyring_id")

echo -n "$SPEC_KEY" > $SPDK_PSK_PATH
chmod 0600 $SPDK_PSK_PATH

construct_tlshd_conf $keyring_name
tlshd -s -c "$TLSHD_CONF" &
tlshdpid=$!

timing_exit prepare_keyring_and_daemon

trap "cleanup" SIGINT SIGTERM EXIT

#Test case 1: Kernel target + SPDK initiator (bdevperf)
nvmet_tls_init

$rootdir/build/examples/bdevperf -m 0x4 -z "${NO_HUGE[@]}" &
bdevperfpid=$!
waitforlisten $bdevperfpid

$rpc_py keyring_file_add_key $PSK_NAME "$SPDK_PSK_PATH"

$rpc_py bdev_nvme_attach_controller -b $CONTROLLER_NAME -t $TEST_TRANSPORT -a "$(get_main_ns_ip)" \
	-s $NVMF_THIRD_PORT -f ipv4 -n "$SPEC_SUBSYSNQN" -q "$SPEC_HOSTNQN" --psk $PSK_NAME

$rpc_py bdev_nvme_get_controllers -n $CONTROLLER_NAME

$bdevperf_py perform_tests -q 1 -o 4096 -t 5 -w read

$rpc_py bdev_nvme_detach_controller $CONTROLLER_NAME
killprocess $bdevperfpid

# Test case 2: SPDK target (nvmf_tgt) + Kernel initiator (via nvme-cli)
timing_enter start_nvmf_tgt

nvmfappstart -m 0x2 --wait-for-rpc
trap "cleanup" SIGINT SIGTERM EXIT
waitforlisten $nvmfpid
setup_nvmf_tgt $SPDK_PSK_PATH

timing_exit start_nvmf_tgt

nvme connect --nqn=$SPEC_SUBSYSNQN --traddr=$NVMF_FIRST_TARGET_IP --trsvcid=$NVMF_PORT \
	--transport=$TEST_TRANSPORT --hostnqn=$SPEC_HOSTNQN --hostid=$SPEC_HOSTID \
	--tls -o normal --verbose --tls_key=$key_id --keyring=$keyring_id -i 1

waitforserial $NVMF_SERIAL

$fio_py -p nvmf -i 4096 -d 1 -t read -r 5

nvme disconnect --nqn=$SPEC_SUBSYSNQN &

waitforserial_disconnect $NVMF_SERIAL
killprocess $nvmfpid

trap - SIGINT SIGTERM EXIT
cleanup
