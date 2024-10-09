/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Advanced Micro Devices, Inc.
 *   All rights reserved.
 */

#ifndef __AE4DMA_INTERNAL_H__
#define __AE4DMA_INTERNAL_H__

#include "spdk/stdinc.h"
#include "spdk/ae4dma.h"
#include "spdk/ae4dma_spec.h"
#include "spdk/queue.h"
#include "spdk/mmio.h"
/**
 * upper_32_bits - return bits 32-63 of a number
 * @n: the number we're accessing
 */
#define upper_32_bits(n) ((uint32_t)(((n) >> 16) >> 16))

/**
 * lower_32_bits - return bits 0-31 of a number
 * @n: the number we're accessing
 */
#define lower_32_bits(n) ((uint32_t)((n) & 0xffffffff))

#define AE4DMA_DESCRITPTORS_PER_CMDQ 32
#define AE4DMA_QUEUE_DESC_SIZE  sizeof(struct spdk_ae4dma_desc)
#define AE4DMA_QUEUE_SIZE(n)  (AE4DMA_DESCRITPTORS_PER_CMDQ * (n))

struct ae4dma_descriptor {
	spdk_ae4dma_req_cb	callback_fn;
	void			*callback_arg;
};

struct ae4dma_cmd_queue {
	volatile struct spdk_ae4dma_hwq_regs *regs;

	/* Queue base address */
	struct spdk_ae4dma_desc *qbase_addr;

	struct ae4dma_descriptor *ring;

	uint64_t tail;
	unsigned int queue_size;
	uint64_t qring_buffer_pa;
	uint64_t qdma_tail;

	/* Queue Statistics */
	uint32_t write_index;
	uint32_t ring_buff_count;
};

struct spdk_ae4dma_chan {
	/* Opaque handle to upper layer */
	struct    spdk_pci_device *device;
	uint64_t  max_xfer_size;

	/* I/O area used for device communication */
	void *io_regs;

	struct ae4dma_cmd_queue cmd_q[AE4DMA_MAX_HW_QUEUES];
	unsigned int cmd_q_count;
	uint32_t	dma_capabilities;

	/* tailq entry for attached_chans */
	TAILQ_ENTRY(spdk_ae4dma_chan)	tailq;
};

static inline uint32_t
ae4dma_max_descriptors_per_queue(uint8_t max_desc)
{
	if (max_desc == AE4DMA_DESCRITPTORS_PER_CMDQ) {
		return 1;
	} else {
		return 0;
	}
}

static inline uint32_t
ae4dma_valid_queues(uint8_t no_hw_queues)
{
	if (no_hw_queues) {
		if (no_hw_queues <= AE4DMA_MAX_HW_QUEUES) {
			return 1;
		} else {
			return 0;
		}
	} else {
		return 0;
	}
}

#endif /* __AE4DMA_INTERNAL_H__ */
