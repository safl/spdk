/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Advanced Micro Devices, Inc.
 *   All rights reserved.
 */

#include "spdk/stdinc.h"

#include "ae4dma_internal.h"

#include "spdk/env.h"
#include "spdk/util.h"
#include "spdk/memory.h"

#include "spdk/log.h"

struct ae4dma_driver {
	pthread_mutex_t			lock;
	TAILQ_HEAD(, spdk_ae4dma_chan)	attached_chans;
};

static struct ae4dma_driver g_ae4dma_driver = {
	.lock = PTHREAD_MUTEX_INITIALIZER,
	.attached_chans = TAILQ_HEAD_INITIALIZER(g_ae4dma_driver.attached_chans),
};

int
spdk_ae4dma_build_copy(struct spdk_ae4dma_chan *ae4dma, int hwq_id, void *cb_arg,
		       spdk_ae4dma_req_cb cb_fn,
		       void *dst, const void *src, uint64_t nbytes)
{
	/* Building the descriptors in the given HW queue */

	return 0;
}

int
spdk_ae4dma_probe(void *cb_ctx, spdk_ae4dma_probe_cb probe_cb, spdk_ae4dma_attach_cb attach_cb)
{
	/* Probe for ae4dma devices */

	return 0 ;
}

static int
ae4dma_process_channel_events(struct spdk_ae4dma_chan *ae4dma, int hwq_id)
{
	return 0;
}

void
spdk_ae4dma_detach(struct spdk_ae4dma_chan *ae4dma)
{
	/* Detach the attached devices */
}

void
spdk_ae4dma_flush(struct spdk_ae4dma_chan *ae4dma, int hwq_id)

{
	/* To flush the updated descs by incrementing the write_index of the queue */
}

int
spdk_ae4dma_process_events(struct spdk_ae4dma_chan *ae4dma, int hwq_id)
{
	/* To process all the submitted descriptors for the HW queue */
	return ae4dma_process_channel_events(ae4dma, hwq_id);
}
