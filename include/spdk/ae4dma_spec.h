/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Advanced Micro Devices, Inc.
 *   All rights reserved.
 */

/**
 * AE4DMA specification definitions
 */

#ifndef SPDK_AE4DMA_SPEC_H
#define SPDK_AE4DMA_SPEC_H

#include "spdk/stdinc.h"
#include "spdk/assert.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * AE4DMA Device Details
 */

#define AE4DMA_MAX_HW_QUEUES			16
#define AE4DMA_CMD_QUEUE_LEN			32
#define AE4DMA_QUEUE_START_INDEX		0

#define Q_SIZE(n)			(AE4DMA_CMD_QUEUE_LEN * (n))

/* Descriptor status */
enum spdk_ae4dma_dma_status {
	AE4DMA_DMA_DESC_SUBMITTED = 0,
	AE4DMA_DMA_DESC_VALIDATED = 1,
	AE4DMA_DMA_DESC_PROCESSED = 2,
	AE4DMA_DMA_DESC_COMPLETED = 3,
	AE4DMA_DMA_DESC_ERROR = 4,
};

#define AE4DMA_CMD_QUEUE_ENABLE	0x1

/* Offset of each(i) queue */
#define QUEUE_START_OFFSET(i) ((i + 1) * 0x20)

/** Common to all queues */
#define AE4DMA_COMMON_CONFIG_OFFSET 0x00

#define AE4DMA_PCIE_BAR 0

/*
 * descriptor for AE4DMA commands
 * 8 32-bit words:
 * word 0: source memory type; destination memory type ; control bits
 * word 1: desc_id; error code; status
 * word 2: length
 * word 3: reserved
 * word 4: upper 32 bits of source pointer
 * word 5: low 32 bits of source pointer
 * word 6: upper 32 bits of destination pointer
 * word 7: low 32 bits of destination pointer
 */

/* Controls bits: Reserved for future use */
#define DWORD0_SOC	BIT(0)
#define DWORD0_IOC	BIT(1)
#define DWORD0_SOM	BIT(3)
#define DWORD0_EOM	BIT(4)

#define DWORD0_DMT	GENMASK(5, 4)
#define DWORD0_SMT	GENMASK(7, 6)

#define DWORD0_DMT_MEM	0x0
#define DWORD0_DMT_IO	1<<4
#define DWORD0_SMT_MEM	0x0
#define DWORD0_SMT_IO	1<<6


struct spdk_desc_dword0 {
	uint8_t	byte0;
	uint8_t	byte1;
	uint16_t timestamp;
};

struct spdk_desc_dword1 {
	uint8_t	status;
	uint8_t	err_code;
	uint16_t	desc_id;
};

struct spdk_ae4dma_desc {
	struct spdk_desc_dword0 dw0;
	struct spdk_desc_dword1 dw1;
	uint32_t length;
	uint32_t reserved;
	uint32_t src_hi;
	uint32_t src_lo;
	uint32_t dst_hi;
	uint32_t dst_lo;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_ae4dma_desc) == 32, "incorrect ae4dma_hw_desc layout");

/*
 * regs for each queue :4 bytes len
 * effective addr:offset+reg
 */
#define AE4DMA_REG_CONTROL		0x00
#define AE4DMA_REG_STATUS		0x04
#define AE4DMA_REG_MAX_IDX		0x08
#define AE4DMA_REG_READ_IDX		0x0C
#define AE4DMA_REG_WRITE_IDX		0x10
#define AE4DMA_REG_INTR_STATUS		0x14
#define AE4DMA_REG_QBASE_LO		0x18
#define AE4DMA_REG_QBASE_HI		0x1C

struct spdk_ae4dma_hwq_regs {
	union {
		uint32_t control_raw;
		struct {
			uint32_t queue_enable: 1;
			uint32_t reserved_internal: 31;
		} control;
	} control_reg;

	union {
		uint32_t status_raw;
		struct {
			uint32_t reserved0: 1;
			uint32_t queue_status: 2; /* 0–empty, 1–full, 2–stopped, 3–error , 4–Not Empty */
			uint32_t reserved1: 21;
			uint32_t interrupt_type: 4;
			uint32_t reserved2: 4;
		} status;
	} status_reg;

	uint32_t max_idx;
	uint32_t write_idx;
	uint32_t read_idx;

	union {
		uint32_t intr_status_raw;
		struct {
			uint32_t intr_status: 1;
			uint32_t reserved: 31;
		} intr_status;
	} intr_status_reg;

	uint32_t qbase_lo;
	uint32_t qbase_hi;

} __attribute__((packed)) __attribute__((aligned));


#ifdef __cplusplus
}
#endif

#endif /* SPDK_AE4DMA_SPEC_H */
