#!/usr/bin/env bash
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2024 Intel Corporation
#  All rights reserved.
#

rootdir=$(readlink -f "$(dirname "$BASH_SOURCE")/../../../")

waitforfile() {
	local i=0
	while [[ ! -e $1 ]] && ((i++ < 200)); do
		sleep 0.1
	done

	if [[ ! -e $1 ]]; then
		return 1
	fi

	return 0
}
