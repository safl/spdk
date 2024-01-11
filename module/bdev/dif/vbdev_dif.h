/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 */

#ifndef SPDK_VBDEV_DIF_H
#define SPDK_VBDEV_DIF_H

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

/**
 * Create new DIF vbdev.
 *
 * \param bdev_name Bdev on which DIF vbdev will be created.
 * \param vbdev_name Name of the DIF vbdev.
 * \param uuid Optional UUID to assign to the DIF vbdev.
 * \return 0 on success, other on failure.
 */
int bdev_dif_create_disk(const char *bdev_name, const char *vbdev_name,
			 const struct spdk_uuid *uuid, enum spdk_dif_type dif_type,
			 enum spdk_dif_pi_format dif_pi_format, bool dif_is_head_of_md,
			 bool check_reftag, bool check_guard);

/**
 * Delete DIF vbdev.
 *
 * \param bdev_name Name of the DIF vbdev.
 * \param cb_fn Function to call after deletion.
 * \param cb_arg Argument to pass to cb_fn.
 */
void bdev_dif_delete_disk(const char *bdev_name, spdk_bdev_unregister_cb cb_fn,
			  void *cb_arg);

#endif /* SPDK_VBDEV_DIF_H */
