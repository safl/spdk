#!/usr/bin/env bash
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2024 Intel Corporation
#  All rights reserved.

NVMF_BRIDGE=nvmf_br
NVMF_TARGET_NAMESPACE=nvmf_ns_spdk
NVMF_TARGET_NS_CMD=()

val_to_ip() {
	local val=$1

	printf '%u.%u.%u.%u\n' \
		$(((val >> 24) & 0xff)) \
		$(((val >> 16) & 0xff)) \
		$(((val >> 8) & 0xff)) \
		$(((val >> 0) & 0xff))
}

setup_interfaces() {
	# This is meant to allocate initiator + target pairs of local interfaces,
	# i.e. under a single host. This is not meant to support remote setups.
	# It keeps global counter to allow for subsequent, multiple calls to
	# extend the existing setup.
	local no=$1 type=${2:-veth} transport=${3:-tcp} ip_pool=0x0a000001 max

	local -gA dev_map
	local -g _dev

	# 255 IPs is definitely enough for testing
	((ip_pool += _dev * 2, (_dev + no) * 2 <= 255)) || return 1

	for ((_dev = _dev, max = _dev; _dev < max + no; _dev++, ip_pool += 2)); do
		setup_interface_pair "$_dev" "$type" "$ip_pool" "$transport"
	done

	# Verify connectivity
	ping_ips "$_dev"
}

reset_setup_interfaces() { _dev=0 dev_map=(); }

setup_interface_pair() {
	local id=$1 type=$2 ip=$3 transport=$4 ips=()
	local initiator=initiator${id} target=target${id} _ns=""

	# initiator + target
	ips=("$ip" $((++ip)))

	[[ $transport == tcp ]] && _ns=NVMF_TARGET_NS_CMD

	if [[ $type == phy ]]; then
		# This dependency comes from gather_supported_nvmf_pci_devs(). Caller should
		# guarantee accessibility of these devices.
		initiator=${net_devs[id]?} target=${net_devs[id + 1]?}
	fi

	[[ $type == veth ]] && create_veth "$initiator" "${initiator}_br"
	[[ $type == veth ]] && create_veth "$target" "${target}_br"

	[[ $transport == tcp ]] && add_to_ns "$target"

	set_ip "$initiator" "${ips[0]}"
	set_ip "$target" "${ips[1]}" $_ns

	set_up "$initiator"
	set_up "$target" $_ns

	[[ $type == veth ]] && add_to_bridge "${initiator}_br"
	[[ $type == veth ]] && add_to_bridge "${target}_br"

	if [[ $transport == tcp ]]; then
		ipts -I INPUT 1 -i "$initiator" -p tcp --dport $NVMF_PORT -j ACCEPT
	fi

	dev_map["initiator$id"]=$initiator dev_map["target$id"]=$target
}

ping_ip() {
	local ip=$1 in_ns=$2 count=${3:-1}
	[[ -n $in_ns ]] && local -n ns=$in_ns

	eval "${ns[*]} ping -c $count $ip"
}

ping_ips() {
	local pairs=$1 pair

	for ((pair = 0; pair < pairs; pair++)); do
		ping_ip "$(get_initiator_ip_address "initiator${pair}")" NVMF_TARGET_NS_CMD
		ping_ip "$(get_target_ip_address "target${pair}" NVMF_TARGET_NS_CMD)"
	done
}

get_net_dev() {
	# Return name of the actual net device which maps to given symbolic name.
	# E.g. "initiator0" -> "eth0"
	local dev=$1

	[[ -n $dev && -n ${dev_map["$dev"]} ]] || return 1
	echo "${dev_map["$dev"]}"
}

create_main_bridge() {
	delete_main_bridge

	ip link add "$NVMF_BRIDGE" type bridge
	set_up "$NVMF_BRIDGE"

	ipts -A FORWARD -i "$NVMF_BRIDGE" -o "$NVMF_BRIDGE" -j ACCEPT
}

delete_dev() {
	local dev=$1 in_ns=$2
	[[ -n $in_ns ]] && local -n ns=$in_ns

	eval "${ns[*]} ip link delete $dev"
}

delete_main_bridge() {
	[[ -e /sys/class/net/$NVMF_BRIDGE/address ]] || return 0
	delete_dev "$NVMF_BRIDGE"
}

add_to_bridge() {
	local dev=$1 bridge=${2:-$NVMF_BRIDGE}
	ip link set "$dev" master "$bridge"

	set_up "$dev"
}

create_target_ns() {
	local ns=${1:-$NVMF_TARGET_NAMESPACE}

	NVMF_TARGET_NAMESPACE=$ns
	ip netns add "$NVMF_TARGET_NAMESPACE"
	NVMF_TARGET_NS_CMD=(ip netns exec "$NVMF_TARGET_NAMESPACE")

	set_up lo NVMF_TARGET_NS_CMD
}

add_to_ns() {
	local dev=$1 ns=${2:-$NVMF_TARGET_NAMESPACE}
	ip link set "$dev" netns "$ns"
}

create_veth() {
	local dev=$1 peer=$2
	ip link add "$dev" type veth peer name "$peer"

	set_up "$dev"
	set_up "$peer"
}

get_ip_address() {
	local dev=$1 in_ns=$2 ip
	[[ -n $in_ns ]] && local -n ns=$in_ns

	if ! dev=$(get_net_dev "$dev"); then
		return 0
	fi

	ip=$(eval "${ns[*]} cat /sys/class/net/$dev/ifalias")
	[[ -n $ip ]] || return 1

	echo "$ip"
}

get_target_ip_address() {
	get_ip_address "${1:-target0}" "$2"
}

get_initiator_ip_address() {
	get_ip_address "${1:-initiator0}"
}

get_tcp_initiator_ip_address() {
	get_initiator_ip_address "$1"
}

get_rdma_initiator_ip_address() {
	get_initiator_ip_address "$1"
}

get_tcp_target_ip_address() {
	get_target_ip_address "$1" NVMF_TARGET_NS_CMD
}

get_rdma_target_ip_address() {
	get_target_ip_address "$1"
}

set_ip() {
	local dev=$1 ip=$2 in_ns=$3
	[[ -n $in_ns ]] && local -n ns=$in_ns

	ip=$(val_to_ip "$ip")
	eval "${ns[*]} ip addr add $ip/24 dev $dev"
	# Record ip in given dev's ifalias for easier lookup
	eval "echo $ip | ${ns[*]} tee /sys/class/net/$dev/ifalias"
}

set_up() {
	local dev=$1 in_ns=$2
	[[ -n $in_ns ]] && local -n ns=$in_ns

	eval "${ns[*]} ip link set $dev up"
}

flush_ip() {
	local dev=$1 in_ns=$2
	[[ -n $in_ns ]] && local -n ns=$in_ns

	eval "${ns[*]} ip addr flush dev $dev"
}

nvmf_veth_init() {
	# Configure 2 pairs of initiator + target interfaces using veth. Extra pair
	# is needed for specific set of nvmf tests which are not covered under physical
	# setup.
	local total_initiator_target_pairs=${NVMF_OVERRIDE_SETUP_NO:-2}

	create_target_ns
	create_main_bridge
	setup_interfaces "$total_initiator_target_pairs" veth

	NVMF_APP=("${NVMF_TARGET_NS_CMD[@]}" "${NVMF_APP[@]}")
}

nvmf_rdma_init() {
	# Configure single pair of initiator + target interfaces (the assumption here is that
	# we have infiniband-capable NICs present). Currently, use-cases with multiple pairs
	# are covered only under tcp + veth.
	local total_initiator_target_pairs=${NVMF_OVERRIDE_SETUP_NO:-1}

	load_ib_rdma_modules
	setup_interfaces "$total_initiator_target_pairs" phy rdma
}

nvmf_tcp_init() {
	# Configure single pair of initiator + target interfaces (the assumption here is that
	# we have at minimum two interfaces, which belong to the same NIC, connected over a
	# loopback). This requirement comes from CI's limitation. Use-cases which require more
	# interfaces are covered under virt|veth setup.
	local total_initiator_target_pairs=${NVMF_OVERRIDE_SETUP_NO:-1}

	create_target_ns
	setup_interfaces "$total_initiator_target_pairs" phy

	NVMF_APP=("${NVMF_TARGET_NS_CMD[@]}" "${NVMF_APP[@]}")
}

nvmf_fini() {
	local dev

	# This will also handle all ips|interfaces we assigned inside the target namespace
	remove_target_ns
	delete_main_bridge

	for dev in "${dev_map[@]}"; do
		[[ -e /sys/class/net/$dev/address ]] || continue
		# Check the name assign type to identify our veth devices. If there's a
		# match we know we may safely remove this device.
		# 3 == name assigned from userspace
		if (($(< "/sys/class/net/$dev/name_assign_type") == 3)); then
			delete_dev "$dev"
		else
			# simply flush the ip
			flush_ip "$dev"
		fi
	done

	reset_setup_interfaces
	iptr
}

_remove_target_ns() {
	local ns {ns,mn,an}_net_devs

	[[ -f /var/run/netns/$NVMF_TARGET_NAMESPACE ]] || return 0
	ns=$NVMF_TARGET_NAMESPACE

	# Gather all devs from the target $ns namespace. We want to differentiate
	# between veth and physical links and gather just the latter. To do so,
	# we simply compare ifindex to iflink - as per kernel docs, these should
	# be always equal for the physical links. For veth devices, since they are
	# paired, iflink should point at an actual bridge, hence being different
	# from its own ifindex.
	ns_net_devs=($(
		ip netns exec "$ns" bash <<- 'IN_NS'
			shopt -s extglob nullglob
			for dev in /sys/class/net/!(lo|bond*); do
				(($(< "$dev/ifindex") == $(< "$dev/iflink"))) || continue
				echo "${dev##*/}"
			done
		IN_NS
	))
	# Gather all the net devs from the main ns
	mn_net_devs=($(basename -a /sys/class/net/!(lo|bond*)))
	# Merge these two to have a list for comparison
	an_net_devs=($(printf '%s\n' "${ns_net_devs[@]}" "${mn_net_devs[@]}" | sort))

	ip netns delete "$ns"

	# Check if our list matches against the main ns after $ns got deleted
	while [[ ${an_net_devs[*]} != "${mn_net_devs[*]}" ]]; do
		mn_net_devs=($(basename -a /sys/class/net/!(lo|bond*)))
		sleep 1s
	done
}

remove_target_ns() {
	xtrace_disable_per_cmd _remove_target_ns
}

nvmf_legacy_env() {
	# Prep legacy environment which most of the test/nvmf/* still depend on.
	# Moving forward, each test-case should request specific IPs|interfaces
	# depending on the use-case to avoid any discrepancies which may be caused
	# by, e.g. using different transports. See the case below.
	NVMF_TARGET_INTERFACE=${dev_map[target0]}
	NVMF_TARGET_INTERFACE2=${dev_map[target1]}

	NVMF_FIRST_INITIATOR_IP=$(get_initiator_ip_address)
	NVMF_SECOND_INITIATOR_IP=$(get_initiator_ip_address initiator1)

	# *TARGET_IP is being used differently depending on the transport - for
	# tcp, all target interfaces are always part of a separate namespace while
	# for rdma they are kept in the main namespace. As per above, this should
	# be reviewed per test-case and changed accordingly.
	NVMF_FIRST_TARGET_IP=$("get_${TEST_TRANSPORT}_target_ip_address")
	NVMF_SECOND_TARGET_IP=$("get_${TEST_TRANSPORT}_target_ip_address" target1)
	# See target_disconnect.sh or multipath.sh wonkiness - rdma MUST have
	# the NVMF_SECOND_TARGET_IP in place, but tcp, under phy CANNOT (CI's
	# limitation).
	if [[ $TEST_TRANSPORT == rdma ]]; then
		NVMF_SECOND_TARGET_IP=${NVMF_SECOND_TARGET_IP:-$NVMF_FIRST_INITIATOR_IP}
	fi
}
