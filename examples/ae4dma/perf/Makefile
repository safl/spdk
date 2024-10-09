#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2024 Advanced Micro Devices, Inc.
#  All rights reserved.
#

SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

APP = ae4dma_perf

C_SRCS := perf.c

SPDK_LIB_LIST = ae4dma util

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk
