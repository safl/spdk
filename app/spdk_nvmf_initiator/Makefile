#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (c) 2024 Nutanix Inc.
#  All rights reserved.
#

SPDK_ROOT_DIR := $(abspath $(CURDIR)/../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

APP = nvmf_bdev_initiator

C_SRCS := nvmf_bdev_initiator.c

SPDK_LIB_LIST += $(SOCK_MODULES_LIST) bdev_nvme event event_bdev

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk

install: $(APP)
	$(INSTALL_APP)

uninstall:
	$(UNINSTALL_APP)
