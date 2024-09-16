#!/usr/bin/env bash
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2020 Intel Corporation
#  All rights reserved.
#
# We don't want to tell kernel to include %e or %E since these
# can include whitespaces or other funny characters, and working
# with those on the cmdline would be a nightmare. Use procfs for
# the remaining pieces we want to gather:
# |$rootdir/scripts/core-collector.sh %P %s %t $output_dir

rootdir=$(readlink -f "$(dirname "$0")/../")

maps_to_json() {
	local _maps=("${maps[@]}")
	local mem_regions=() mem

	mem_regions=("/proc/$core_pid/map_files/"*)

	for mem in "${!mem_regions[@]}"; do
		_maps[mem]=\"${_maps[mem]}@${mem_regions[mem]##*/}\"
	done

	local IFS=","
	echo "${_maps[*]}"
}

core_meta() {
	jq . <<- CORE
		{
		  "$exe_comm": {
		    "ts": "$core_time",
		    "size": "$core_size bytes",
		    "PID": $core_pid,
		    "signal": "$core_sig ($core_sig_name)",
		    "path": "$exe_path",
		    "cwd": "$cwd_path",
		    "statm": "$statm",
		    "filter": "$filter",
		    "mapped": [ $mapped ]
		  }
		}
	CORE
}

bt() { hash gdb && gdb -batch -ex "thread apply all bt full" "$1" "$2" 2>&1; }

stderr() {
	exec 2> "$core.stderr.txt"
	set -x
}

coredump_filter() {
	local bitmap bit
	local _filter filter

	bitmap[0]=anon-priv-mappings
	bitmap[1]=anon-shared-mappings
	bitmap[2]=file-priv-mappings
	bitmap[3]=file-shared-mappings
	bitmap[4]=elf-headers
	bitmap[5]=priv-hp
	bitmap[6]=shared-hp
	bitmap[7]=priv-DAX
	bitmap[8]=shared-DAX

	_filter=0x$(< "/proc/$core_pid/coredump_filter")

	for bit in "${!bitmap[@]}"; do
		((_filter & 1 << bit)) || continue
		filter=${filter:+$filter,}${bitmap[bit]}
	done

	echo "$filter"
}

filter_process() {
	# Did the process sit in our repo?
	[[ $cwd_path == "$rootdir"* ]] && return 0

	# Did we load our fio plugins?
	[[ ${maps[*]} == *"$rootdir/build/fio/spdk_nvme"* ]] && return 0
	[[ ${maps[*]} == *"$rootdir/build/fio/spdk_bdev"* ]] && return 0

	# Do we depend on it?
	local crit_binaries=() bin

	crit_binaries+=("nvme")
	crit_binaries+=("qemu-system*")
	# Add more if needed

	for bin in "${crit_binaries[@]}"; do
		# The below SC is intentional
		# shellcheck disable=SC2053
		[[ ${exe_path##*/} == $bin ]] && return 0
	done

	return 1
}

pid_hook() {
	# Find top parent PID of the crashing PID which is still bound to the same
	# namespace. For containers, this should iterate until docker's|containerd's
	# shim is found.
	local pid=$1 _pid pid_hook ppid
	_pid=$pid

	while ppid=$(ps -p"$_pid" -oppid=) && ppid=${ppid//[[:space:]]/}; do
		[[ /proc/$pid/root -ef /proc/$ppid/root ]] || break
		pid_hook=$ppid _pid=$ppid
	done
	[[ -n $pid_hook ]] || return 1
	echo "$pid_hook"
}

args+=(core_pid)
args+=(core_sig)
args+=(core_ts)

read -r "${args[@]}" <<< "$*"

# Fetch all the info while the proper PID entry is still around
exe_path=$(readlink "/proc/$core_pid/exe")
cwd_path=$(readlink "/proc/$core_pid/cwd")
exe_comm=$(< "/proc/$core_pid/comm")
statm=$(< "/proc/$core_pid/statm")
core_time=$(date -d@"$core_ts")
core_sig_name=$(kill -l "$core_sig")
mapfile -t maps < <(readlink "/proc/$core_pid/map_files/"*)
mapped=$(maps_to_json)
filter=$(coredump_filter)

# Filter out processes that we don't care about
filter_process || exit 0

if [[ ! /proc/$core_pid/ns/mnt -ef /proc/self/ns/mnt ]]; then
	# $core_pid is in a separate namespace so it's quite likely that we are
	# catching a core triggered from within a supported container.
	if [[ -e /proc/$core_pid/root/.dockerenv ]]; then
		# Since we can't tell exactly where actual spdk $rootdir is within
		# the container we need to use some predefined path to dump the core
		# to. Also, since the proc entry for $core_pid will disappear from
		# the main namespace after the core is written out, we need to find
		# something to hook ourselves into, i.e., $core_pid's top parent.
		coredump_path=/proc/$(pid_hook "$core_pid")/root/var/spdk/coredumps
		mkdir -p "$coredump_path"
	else
		# Oh? Well, if this is not a supported container, then we are inside
		# some unknown environment. No point in dumping a core then so
		# just bail.
		exit 0
	fi
else
	coredump_path=$(< "$rootdir/.coredump_path")
fi

core=$coredump_path/${exe_path##*/}_$core_pid.core
stderr

# RLIMIT_CORE is not enforced when core is piped to us. To make
# sure we won't attempt to overload underlying storage, copy
# only the reasonable amount of bytes (systemd defaults to 2G
# so let's follow that).
rlimit=$((1024 * 1024 * 1024 * 2))

# Clear path for lz
rm -f "$core"{,.{bin,bt,gz,json}}

# Slurp the core
head -c "$rlimit" <&0 > "$core"
core_size=$(wc -c < "$core")

# Compress it
gzip -c "$core" > "$core.gz"

# Save the binary
cp "$exe_path" "$core.bin"

# Save the backtrace
bt "$exe_path" "$core" > "$core.bt.txt"

# Save the metadata of the core
core_meta > "$core.json"

# Nuke the original core
rm "$core"
