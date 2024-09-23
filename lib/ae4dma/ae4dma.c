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

#define ALIGN_4K 0x1000
#define MAX_RETRY 10

struct ae4dma_driver {
	pthread_mutex_t			lock;
	TAILQ_HEAD(, spdk_ae4dma_chan)	attached_chans;
};

static struct ae4dma_driver g_ae4dma_driver = {
	.lock = PTHREAD_MUTEX_INITIALIZER,
	.attached_chans = TAILQ_HEAD_INITIALIZER(g_ae4dma_driver.attached_chans),
};

/* Mapping the PCI BAR */
static int
ae4dma_map_pci_bar(struct spdk_ae4dma_chan *ae4dma)
{
	int rc;
	void *addr;
	uint64_t phys_addr, size;

	rc = spdk_pci_device_map_bar(ae4dma->device, AE4DMA_PCIE_BAR, &addr, &phys_addr, &size);
	if (rc != 0 || addr == NULL) {
		SPDK_ERRLOG("pci_device_map_range failed with error code %d\n", rc);
		return -1;
	}

	ae4dma->io_regs = addr;

	return 0;
}

static int
ae4dma_unmap_pci_bar(struct spdk_ae4dma_chan *ae4dma)
{
	int rc = 0;
	void *addr = (void *)ae4dma->io_regs;
	if (addr) {
		rc = spdk_pci_device_unmap_bar(ae4dma->device, 0, addr);
	}
	return rc;
}

void
spdk_ae4dma_flush(struct spdk_ae4dma_chan *ae4dma, int hwq_id)
{
	volatile uint32_t write_idx;

	/* To flush the updated descs by incrementing the write_index of the queue */
	write_idx = ae4dma->cmd_q[hwq_id].write_index;
	spdk_mmio_write_4(&ae4dma->cmd_q[hwq_id].regs->write_idx, write_idx);
}

static struct ae4dma_descriptor *
ae4dma_prep_copy(struct spdk_ae4dma_chan *ae4dma, uint64_t dst,
		 uint64_t src, uint32_t len, uint32_t hwq_index)
{
	struct ae4dma_descriptor *desc;
	uint32_t hwq;
	uint32_t desc_index ;

	assert(len <= ae4dma->max_xfer_size);
	hwq = hwq_index ;

	desc_index = ae4dma->cmd_q[hwq].write_index;

	desc = &ae4dma->cmd_q[hwq].ring[desc_index];
	if (desc == NULL) {
		SPDK_ERRLOG("desc at %d Q and %d ring is NULL\n", hwq, desc_index);
		return NULL;
	}

	ae4dma->cmd_q[hwq].qbase_addr[desc_index].dw0.byte0 = 0;
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].dw1.status = 0;
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].dw1.err_code = 0;
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].dw1.desc_id  = 0;
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].length = len;
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].src_hi = upper_32_bits(src);
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].src_lo = lower_32_bits(src);
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].dst_hi = upper_32_bits(dst);
	ae4dma->cmd_q[hwq].qbase_addr[desc_index].dst_lo = lower_32_bits(dst);

	ae4dma->cmd_q[hwq].ring_buff_count++;

	desc_index = (desc_index + 1) % (AE4DMA_DESCRITPTORS_PER_CMDQ);
	ae4dma->cmd_q[hwq].write_index = desc_index;

	return desc;
}

int
spdk_ae4dma_build_copy(struct spdk_ae4dma_chan *ae4dma, int hwq_id, void *cb_arg,
		       spdk_ae4dma_req_cb cb_fn,
		       void *dst, const void *src, uint64_t nbytes)
{
	struct ae4dma_descriptor        *cb_desc;
	uint64_t        vdst, vsrc;
	uint64_t        pdst_addr, psrc_addr;

	if (!ae4dma) {
		return -EINVAL;
	}

	if (ae4dma->cmd_q[hwq_id].ring_buff_count >= (AE4DMA_DESCRITPTORS_PER_CMDQ - 4)) {
		return 1;
	}

	vdst = (uint64_t)dst;
	vsrc = (uint64_t)src;

	psrc_addr = spdk_vtophys((void *) vsrc, NULL);
	if (psrc_addr == SPDK_VTOPHYS_ERROR) {
		return -EINVAL;
	}

	pdst_addr = spdk_vtophys((void *) vdst, NULL);
	if (pdst_addr == SPDK_VTOPHYS_ERROR) {
		return -EINVAL;
	}

	cb_desc = ae4dma_prep_copy(ae4dma, pdst_addr, psrc_addr, nbytes, hwq_id);

	if (cb_desc) {
		cb_desc->callback_fn = cb_fn;
		cb_desc->callback_arg = cb_arg;
	} else {
		/*
		 * Ran out of descriptors in the ring - reset head to leave things as they were
		 * in case we managed to fill out any descriptors.
		 */
		SPDK_ERRLOG("Error: Out of descriptors\n");
		return -ENOMEM;
	}

	return 0;
}

static int
ae4dma_process_channel_events(struct spdk_ae4dma_chan *ae4dma, int hwq_id)
{
	volatile struct spdk_ae4dma_desc *hw_desc;
	uint32_t events_count = 0;
	volatile uint32_t   tail;
	volatile uint32_t desc_status;
	uint64_t retry_count = MAX_RETRY;
	static int desc_failed;
	uint64_t  sub_desc_cnt;

	tail = ae4dma->cmd_q[hwq_id].tail;

	/* To process all the submitted descriptors for the HW queue */

	sub_desc_cnt = ae4dma->cmd_q[hwq_id].ring_buff_count;
	while (sub_desc_cnt) {
		desc_status = 0;
		retry_count = MAX_RETRY;
		do {
			hw_desc = &ae4dma->cmd_q[hwq_id].qbase_addr[tail];

			desc_status = hw_desc->dw1.status;
			if (desc_status) {
				if (desc_status != AE4DMA_DMA_DESC_COMPLETED) {
					desc_failed++;
					SPDK_ERRLOG("Desc error code : %d\n", hw_desc->dw1.err_code);
				}

				if (ae4dma->cmd_q[hwq_id].ring_buff_count) {
					ae4dma->cmd_q[hwq_id].ring_buff_count--;
				}

				if (ae4dma->cmd_q[hwq_id].ring[tail].callback_fn) {
					ae4dma->cmd_q[hwq_id].ring[tail].callback_fn(
						ae4dma->cmd_q[hwq_id].ring[tail].callback_arg);
				}

				events_count++;
				tail = (tail + 1) % AE4DMA_DESCRITPTORS_PER_CMDQ;
				sub_desc_cnt--;
			}
		} while (!desc_status && retry_count --);
		if (desc_status == 0) { break; }
	}
	ae4dma->cmd_q[hwq_id].tail = tail;

	return events_count;
}

static void
ae4dma_channel_destruct(uint8_t hwqueues, struct spdk_ae4dma_chan *ae4dma)
{
	ae4dma_unmap_pci_bar(ae4dma);
	spdk_pci_device_detach(ae4dma->device);

	for (int i = 0; i < hwqueues; i++) {
		if (ae4dma->cmd_q[i].qbase_addr) {
			spdk_free(ae4dma->cmd_q[i].qbase_addr);
		}
		if (ae4dma->cmd_q[i].ring) {
			free(ae4dma->cmd_q[i].ring);
		}
	}
}


static int
ae4dma_channel_start(uint8_t hw_queues, struct spdk_ae4dma_chan *ae4dma)
{
	uint32_t i;

	void   *ae4dma_mmio_base_addr;
	struct ae4dma_cmd_queue *cmd_q;
	uint32_t dma_queue_base_addr_low, dma_queue_base_addr_hi;
	uint32_t q_per_eng = hw_queues;

	if (ae4dma_map_pci_bar(ae4dma) != 0) {
		SPDK_ERRLOG("ae4dma_map_pci_bar() failed\n");
		return -1;
	}
	ae4dma_mmio_base_addr = (uint8_t *)ae4dma->io_regs;

	/* Always support DMA copy */
	ae4dma->dma_capabilities = SPDK_AE4DMA_ENGINE_COPY_SUPPORTED;
	ae4dma->max_xfer_size = 1ULL << 32;

	/* Set the number of HW queues for this AE4DMA engine. */
	spdk_mmio_write_4((ae4dma_mmio_base_addr + AE4DMA_COMMON_CONFIG_OFFSET), q_per_eng);
	q_per_eng = spdk_mmio_read_4((ae4dma_mmio_base_addr + AE4DMA_COMMON_CONFIG_OFFSET));

	/* Filling up cmd_q; there would be 'n' cmd_q's for 'n' q_per_eng */
	for (i = 0; i < q_per_eng; i++) {

		/* AE4DMA queue initialization */

		/* Current cmd_q details (total 16) */
		cmd_q = &ae4dma->cmd_q[i];
		ae4dma->cmd_q_count++;

		/* Initialize queue's HW registers (8 dwords: 32 bytes(0x20)) */
		cmd_q->regs = (volatile struct spdk_ae4dma_hwq_regs *)ae4dma->io_regs + (i + 1);

		/* Queue_size: 32*sizeof(struct ae4dmadma_desc) */
		cmd_q->queue_size = AE4DMA_QUEUE_SIZE(AE4DMA_QUEUE_DESC_SIZE);

		/* DMA'ble desc address - for each cmd_q  */
		cmd_q->qbase_addr = spdk_dma_zmalloc(AE4DMA_DESCRITPTORS_PER_CMDQ * sizeof(struct spdk_ae4dma_desc),
						     ALIGN_4K, NULL);

		cmd_q->qring_buffer_pa = spdk_vtophys(cmd_q->qbase_addr, NULL);

		if (cmd_q->qring_buffer_pa == SPDK_VTOPHYS_ERROR) {
			SPDK_ERRLOG("Failed to translate descriptor %u to physical address\n", i);
			return EFAULT;
		}

		/* Max Index (cmd queue length) */
		spdk_mmio_write_4(&cmd_q->regs->max_idx, AE4DMA_DESCRITPTORS_PER_CMDQ);

		/* Queue Enable */
		spdk_mmio_write_4(&cmd_q->regs->control_reg.control_raw, AE4DMA_CMD_QUEUE_ENABLE);

		/* Disabling the interrupt */
		spdk_mmio_write_4(&cmd_q->regs->intr_status_reg.intr_status_raw, 0x1);

		cmd_q->write_index = spdk_mmio_read_4(&cmd_q->regs->write_idx);

		cmd_q->tail = spdk_mmio_read_4(&cmd_q->regs->read_idx);
		cmd_q->ring_buff_count = 0;

		/* Update the device registers with queue addresses. */
		cmd_q->qdma_tail = cmd_q->qring_buffer_pa;

		dma_queue_base_addr_low = lower_32_bits(cmd_q->qdma_tail);
		spdk_mmio_write_4(&cmd_q->regs->qbase_lo, (uint32_t)dma_queue_base_addr_low);
		dma_queue_base_addr_low = spdk_mmio_read_4(&cmd_q->regs->qbase_lo);

		dma_queue_base_addr_hi = upper_32_bits(cmd_q->qdma_tail);
		spdk_mmio_write_4(&cmd_q->regs->qbase_hi, (uint32_t)dma_queue_base_addr_hi);
		dma_queue_base_addr_low = spdk_mmio_read_4(&cmd_q->regs->qbase_hi);

		cmd_q->ring = calloc(AE4DMA_DESCRITPTORS_PER_CMDQ, sizeof(struct ae4dma_descriptor));
		if (!cmd_q->ring) {
			return ENOMEM;
		}
	}

	if (ae4dma->cmd_q_count == 0) {
		SPDK_ERRLOG("no command queues available\n");
		return -1;
	}

	printf("BB1.002 AE4DMA\n");
	return 0;
}

static struct spdk_ae4dma_chan *
ae4dma_attach(uint8_t hw_queues, struct spdk_pci_device *device)
{
	struct spdk_ae4dma_chan *ae4dma;
	uint32_t cmd_reg;

	ae4dma = calloc(1, sizeof(struct spdk_ae4dma_chan));
	if (ae4dma == NULL) {
		return NULL;
	}

	/* Enable PCI busmaster. */
	spdk_pci_device_cfg_read32(device, &cmd_reg, 4);
	cmd_reg |= 0x4;
	spdk_pci_device_cfg_write32(device, cmd_reg, 4);

	ae4dma->device = device;

	if (ae4dma_channel_start(hw_queues, ae4dma) != 0) {
		ae4dma_channel_destruct(hw_queues, ae4dma);
		free(ae4dma);
		return NULL;
	}

	return ae4dma;
}

struct ae4dma_enum_ctx {
	spdk_ae4dma_probe_cb probe_cb;
	spdk_ae4dma_attach_cb attach_cb;
	uint8_t hw_queues;
	void *cb_ctx;
};

static int
ae4dma_enum_cb(void *ctx, struct spdk_pci_device *pci_dev)
{
	struct ae4dma_enum_ctx *enum_ctx = ctx;
	struct spdk_ae4dma_chan *ae4dma;

	/* Verify that this device is not already attached */
	TAILQ_FOREACH(ae4dma, &g_ae4dma_driver.attached_chans, tailq) {
		/*
		 * NOTE: This assumes that the PCI abstraction layer will use the same device handle
		 *  across enumerations; we could compare by BDF instead if this is not true.
		 */
		if (pci_dev == ae4dma->device) {
			return 0;
		}
	}

	if (enum_ctx->probe_cb(enum_ctx->cb_ctx, pci_dev)) {
		/*
		 * Since AE4DMA init is relatively quick, just perform the full init during probing.
		 *  If this turns out to be a bottleneck later, this can be changed to work like
		 *  NVMe with a list of devices to initialize in parallel.
		 */
		ae4dma = ae4dma_attach(enum_ctx->hw_queues, pci_dev);
		if (ae4dma == NULL) {
			SPDK_ERRLOG("ae4dma_attach() failed\n");
			return -1;
		}

		TAILQ_INSERT_TAIL(&g_ae4dma_driver.attached_chans, ae4dma, tailq);

		enum_ctx->attach_cb(enum_ctx->cb_ctx, pci_dev, ae4dma);
	}

	return 0;
}

int
spdk_ae4dma_probe(void *cb_ctx, spdk_ae4dma_probe_cb probe_cb, spdk_ae4dma_attach_cb attach_cb)
{
	int rc;
	struct ae4dma_enum_ctx enum_ctx;

	pthread_mutex_lock(&g_ae4dma_driver.lock);

	enum_ctx.probe_cb = probe_cb;
	enum_ctx.attach_cb = attach_cb;
	enum_ctx.hw_queues =  AE4DMA_MAX_HW_QUEUES;
	enum_ctx.cb_ctx = cb_ctx;

	rc = spdk_pci_enumerate(spdk_pci_ae4dma_get_driver(), ae4dma_enum_cb, &enum_ctx);

	pthread_mutex_unlock(&g_ae4dma_driver.lock);

	return rc;
}

void
spdk_ae4dma_detach(struct spdk_ae4dma_chan *ae4dma)
{
	struct ae4dma_driver	*driver = &g_ae4dma_driver;

	/* ae4dma should be in the free list (not registered to a thread)
	 * when calling ae4dma_detach().
	 */
	pthread_mutex_lock(&driver->lock);
	TAILQ_REMOVE(&driver->attached_chans, ae4dma, tailq);
	pthread_mutex_unlock(&driver->lock);

	ae4dma_channel_destruct(AE4DMA_MAX_HW_QUEUES, ae4dma);
	free(ae4dma);
}

int
spdk_ae4dma_process_events(struct spdk_ae4dma_chan *ae4dma, int hwq_id)
{
	return ae4dma_process_channel_events(ae4dma, hwq_id);
}
