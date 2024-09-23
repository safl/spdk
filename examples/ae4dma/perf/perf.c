/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Advanced Micro Devices, Inc.
 *   All rights reserved.
 */

#include "spdk/stdinc.h"

#include "spdk/ae4dma.h"
#include "spdk/env.h"
#include "spdk/queue.h"
#include "spdk/string.h"

#define ALIGN_4K 0x1000
#define AE4DMA_MAX_HWQUEUES_PERDEVICE 16

struct user_config {
	int xfer_size_bytes;
	int queue_depth;
	int time_in_sec;
	bool verify;
	char *core_mask;
	int ae4dma_chan_num;
	int ae4dma_hw_queues;
};

typedef struct ae4dma_datapool {
	void *src;
	void *dst;
} ae4dma_datapool_t;

struct ae4dma_device {
	struct spdk_ae4dma_chan *ae4dma;
	TAILQ_ENTRY(ae4dma_device) tailq;
};

static TAILQ_HEAD(, ae4dma_device) g_devices = TAILQ_HEAD_INITIALIZER(g_devices);
static struct ae4dma_device *g_next_device;
static struct user_config g_user_config;

struct ae4dma_chan_entry {
	struct spdk_ae4dma_chan *chan;
	int ae4dma_chan_id;
	int ae4dma_hwq_id;
	uint64_t xfer_completed;
	uint64_t xfer_failed;
	uint64_t current_queue_depth;
	uint64_t waiting_for_flush;
	uint64_t flush_threshold;
	bool is_draining;
	ae4dma_datapool_t *data_pool;
	struct spdk_mempool *task_pool;
	struct ae4dma_chan_entry *next;
};

struct worker_thread {
	struct ae4dma_chan_entry	*ctx;
	struct worker_thread	*next;
	unsigned		core;
};

struct ae4dma_task {
	struct ae4dma_chan_entry *ae4dma_chan_entry;
	void *src;
	void *dst;
};

static struct worker_thread *g_workers = NULL;
static int g_num_workers = 0;
static int g_ae4dma_chan_num = 0;

ae4dma_datapool_t *ae4dma_datapool_create(uint64_t xfer_size, uint8_t count);

static int submit_single_xfer(struct ae4dma_chan_entry *ae4dma_chan_entry,
			      struct ae4dma_task *ae4dma_task,
			      void *dst, void *src);

ae4dma_datapool_t *
ae4dma_datapool_create(uint64_t xfer_size, uint8_t count)
{
	ae4dma_datapool_t *data_pool = (ae4dma_datapool_t *) malloc(sizeof(ae4dma_datapool_t));
	if (data_pool == NULL) {
		printf("failed to alloc mem for data_pool\n");
		return NULL;
	}
	data_pool->src = spdk_dma_malloc((xfer_size * count), ALIGN_4K, NULL);
	data_pool->dst = spdk_dma_malloc((xfer_size * count), ALIGN_4K, NULL);
	if (data_pool->src == NULL || data_pool->dst == NULL) {
		printf("failed to alloc mem for data_pool src&dst\n");
		free(data_pool);
		return NULL;
	}
	return data_pool;
}

static void
construct_user_config(struct user_config *self)
{
	self->xfer_size_bytes = 4096;
	self->ae4dma_chan_num = 1;
	self->ae4dma_hw_queues = AE4DMA_MAX_HWQUEUES_PERDEVICE ;
	self->queue_depth = 28;
	self->time_in_sec = 5;
	self->verify = false;
	self->core_mask = "0x1";
}

static void
dump_user_config(struct user_config *self)
{
	printf("User configuration:\n");
	printf("Number of channels:    %u\n", self->ae4dma_chan_num);
	printf("Number of hw queues per device:    %u\n", self->ae4dma_hw_queues);
	printf("Transfer size:  %u bytes\n", self->xfer_size_bytes);
	printf("Queue depth:    %u\n", self->queue_depth);
	printf("Run time:       %u seconds\n", self->time_in_sec);
	printf("Core mask:      %s\n", self->core_mask);
	printf("Verify:         %s\n\n", self->verify ? "Yes" : "No");
}

static void
ae4dma_exit(void)
{
	struct ae4dma_device *dev;

	while (!TAILQ_EMPTY(&g_devices)) {
		dev = TAILQ_FIRST(&g_devices);
		TAILQ_REMOVE(&g_devices, dev, tailq);
		if (dev->ae4dma) {
			spdk_ae4dma_detach(dev->ae4dma);
		}
		spdk_dma_free(dev);
	}
}

static int
register_workers(void)
{
	uint32_t i;
	struct worker_thread *worker;

	g_workers = NULL;
	g_num_workers = 0;

	SPDK_ENV_FOREACH_CORE(i) {
		worker = calloc(1, sizeof(*worker));
		if (worker == NULL) {
			fprintf(stderr, "Unable to allocate worker\n");
			return 1;
		}

		worker->core = i;
		worker->next = g_workers;
		g_workers = worker;
		g_num_workers++;
	}

	return 0;
}

static void
unregister_workers(void)
{
	struct worker_thread *worker = g_workers;
	struct ae4dma_chan_entry *entry, *entry1;
	/* Free ae4dma_chan_entry and worker thread */
	while (worker) {
		struct worker_thread *next_worker = worker->next;
		entry = worker->ctx;
		while (entry) {
			entry1 = entry->next;
			spdk_dma_free(entry->data_pool->src);
			spdk_dma_free(entry->data_pool->dst);
			free(entry->data_pool);
			spdk_mempool_free(entry->task_pool);
			free(entry);
			entry = entry1;
		}
		free(worker);
		worker = next_worker;
	}
}

static bool
probe_cb(void *cb_ctx, struct spdk_pci_device *pci_dev)
{
	printf("Found matching device at %04x:%02x:%02x.%x "
	       "vendor:0x%04x device:0x%04x\n",
	       spdk_pci_device_get_domain(pci_dev),
	       spdk_pci_device_get_bus(pci_dev), spdk_pci_device_get_dev(pci_dev),
	       spdk_pci_device_get_func(pci_dev),
	       spdk_pci_device_get_vendor_id(pci_dev), spdk_pci_device_get_device_id(pci_dev));

	return true;
}

static void
attach_cb(void *cb_ctx, struct spdk_pci_device *pci_dev, struct spdk_ae4dma_chan *ae4dma)
{
	struct ae4dma_device *dev;

	if (g_ae4dma_chan_num >= g_user_config.ae4dma_chan_num) {
		return;
	}

	dev = spdk_dma_zmalloc(sizeof(*dev), 0, NULL);
	if (dev == NULL) {
		printf("Failed to allocate device struct\n");
		return;
	}

	dev->ae4dma = ae4dma;
	g_ae4dma_chan_num++;
	TAILQ_INSERT_TAIL(&g_devices, dev, tailq);
}

static int
ae4dma_init(void)
{
	if (spdk_ae4dma_probe(&g_user_config.ae4dma_hw_queues, probe_cb, attach_cb) != 0) {
		fprintf(stderr, "ae4dma_probe() failed\n");
		return 1;
	}
	return 0;
}

static void
usage(char *program_name)
{
	printf("%s options\n", program_name);
	printf("\t[-h help message]\n");
	printf("\t[-c core mask for distributing I/O submission/completion work]\n");
	printf("\t[-q queue depth must be <= 30\n");
	printf("\t[-n number of channels]\n");
	printf("\t[-m number of hw queues per device ( <= 16 )]\n");
	printf("\t[-o transfer size in bytes]\n");
	printf("\t[-t time in seconds]\n");
	printf("\t[-v verify copy result if this switch is on]\n");
}

static int
parse_args(int argc, char **argv)
{
	int op;

	construct_user_config(&g_user_config);
	while ((op = getopt(argc, argv, "c:hn:m:o:q:t:v")) != -1) {
		switch (op) {
		case 'o':
			g_user_config.xfer_size_bytes = spdk_strtol(optarg, 10);
			break;
		case 'n':
			g_user_config.ae4dma_chan_num = spdk_strtol(optarg, 10);
			break;
		case 'm':
			g_user_config.ae4dma_hw_queues = spdk_strtol(optarg, 10);
			break;
		case 'q':
			g_user_config.queue_depth = spdk_strtol(optarg, 10);
			break;
		case 't':
			g_user_config.time_in_sec = spdk_strtol(optarg, 10);
			break;
		case 'c':
			g_user_config.core_mask = optarg;
			break;
		case 'v':
			g_user_config.verify = true;
			break;
		case 'h':
			usage(argv[0]);
			exit(0);
		default:
			usage(argv[0]);
			return 1;
		}
	}
	if (g_user_config.xfer_size_bytes <= 0 || g_user_config.queue_depth <= 0 ||
	    g_user_config.queue_depth > 28 ||
	    g_user_config.time_in_sec <= 0 || !g_user_config.core_mask ||
	    g_user_config.ae4dma_chan_num <= 0 ||
	    g_user_config.ae4dma_hw_queues > AE4DMA_MAX_HWQUEUES_PERDEVICE) {
		usage(argv[0]);
		return 1;
	}

	return 0;
}

static void
drain_io(struct ae4dma_chan_entry *ae4dma_chan_entry)
{
	spdk_ae4dma_flush(ae4dma_chan_entry->chan, ae4dma_chan_entry->ae4dma_hwq_id);
	while (ae4dma_chan_entry->current_queue_depth > 0) {
		spdk_ae4dma_process_events(ae4dma_chan_entry->chan, ae4dma_chan_entry->ae4dma_hwq_id);
	}
}

static void
ae4dma_done(void *cb_arg)
{
	struct ae4dma_task *ae4dma_task = (struct ae4dma_task *)cb_arg;
	struct ae4dma_chan_entry *ae4dma_chan_entry = ae4dma_task->ae4dma_chan_entry;

	if (g_user_config.verify &&
	    memcmp(ae4dma_task->src, ae4dma_task->dst, g_user_config.xfer_size_bytes)) {
		ae4dma_chan_entry->xfer_failed++;
	} else {
		if (!(ae4dma_chan_entry->is_draining)) {
			ae4dma_chan_entry->xfer_completed++;
		}
	}

	if (ae4dma_chan_entry->current_queue_depth) {
		ae4dma_chan_entry->current_queue_depth--;
	}

	if (ae4dma_chan_entry->is_draining) {
		spdk_mempool_put(ae4dma_chan_entry->task_pool, ae4dma_task);
	} else {
		submit_single_xfer(ae4dma_chan_entry, ae4dma_task, ae4dma_task->dst, ae4dma_task->src);
	}
}

static int
submit_single_xfer(struct ae4dma_chan_entry *ae4dma_chan_entry, struct ae4dma_task *ae4dma_task,
		   void *dst,
		   void *src)
{
	static uint64_t test_data = 0;
	int ret;
	ae4dma_task->ae4dma_chan_entry = ae4dma_chan_entry;
	ae4dma_task->src = src;
	ae4dma_task->dst = dst;

	*(uint64_t *) src = test_data++;

	do {
		ret = spdk_ae4dma_build_copy(ae4dma_chan_entry->chan, ae4dma_chan_entry->ae4dma_hwq_id,
					     ae4dma_task, ae4dma_done, dst, src, g_user_config.xfer_size_bytes);
	} while (ret == 1);

	spdk_ae4dma_flush(ae4dma_chan_entry->chan, ae4dma_chan_entry->ae4dma_hwq_id);

	ae4dma_chan_entry->current_queue_depth++;

	return 0;
}

static int
submit_xfers(struct ae4dma_chan_entry *ae4dma_chan_entry, uint64_t queue_depth)
{
	uint8_t i = 0;
	while (queue_depth-- > 0) {
		void *src = NULL, *dst = NULL;
		struct ae4dma_task *ae4dma_task = NULL;

		src = (ae4dma_chan_entry->data_pool->src) + (i * g_user_config.xfer_size_bytes);
		dst = (ae4dma_chan_entry->data_pool->dst) + (i * g_user_config.xfer_size_bytes);
		ae4dma_task = spdk_mempool_get(ae4dma_chan_entry->task_pool);
		i++;

		if (!ae4dma_task) {
			printf(" ae4dma_task allocation failed\n");
			fprintf(stderr, "Unable to get ae4dma_task\n");
			return 1;
		}

		submit_single_xfer(ae4dma_chan_entry, ae4dma_task, dst, src);
	}
	return 0;
}

static int
work_fn(void *arg)
{
	uint64_t tsc_end;
	struct worker_thread *worker = (struct worker_thread *)arg;
	struct ae4dma_chan_entry *t = NULL;

	printf("Starting thread on core %u\n", worker->core);

	tsc_end = spdk_get_ticks() + g_user_config.time_in_sec * spdk_get_ticks_hz();
	t = worker->ctx;

	while (t != NULL) {
		/* begin to submit transfers */
		t->waiting_for_flush = 0;
		t->flush_threshold = g_user_config.queue_depth / 2;

		if (submit_xfers(t, g_user_config.queue_depth) != 0) {
			return 1;
		}
		t = t->next;
	}

	while (1) {
		t = worker->ctx;
		while (t != NULL) {
			spdk_ae4dma_process_events(t->chan, t->ae4dma_hwq_id);
			t = t->next;
		}
		if (spdk_get_ticks() > tsc_end) {
			break;
		}
	}

	t = worker->ctx;
	while (t != NULL) {
		/* begin to drain io */
		t->is_draining = true;
		drain_io(t);
		t = t->next;
	}
	return 0;
}

static int
init(void)
{
	struct spdk_env_opts opts;

	spdk_env_opts_init(&opts);
	opts.name = "ae4dma_perf";
	opts.core_mask = g_user_config.core_mask;
	if (spdk_env_init(&opts) < 0) {
		return 1;
	}

	return 0;
}

static int
dump_result(void)
{
	uint64_t total_completed = 0;
	uint64_t total_failed = 0;
	uint64_t total_xfer_per_sec, total_bw_in_MiBps;
	struct worker_thread *worker = g_workers;

	printf("Channel_ID    HWQueue_ID	Core     Transfers     Bandwidth     Failed\n");
	printf("----------------------------------------------------------------------------\n");
	while (worker != NULL) {
		struct ae4dma_chan_entry *t = worker->ctx;
		while (t) {
			uint64_t xfer_per_sec = t->xfer_completed / g_user_config.time_in_sec;
			uint64_t bw_in_MiBps = (t->xfer_completed * g_user_config.xfer_size_bytes) /
					       (g_user_config.time_in_sec * 1024 * 1024);

			total_completed += t->xfer_completed;
			total_failed += t->xfer_failed;

			if (xfer_per_sec) {
				printf("%10d%12d%14d%12" PRIu64 "/s%8" PRIu64 " MiB/s%11" PRIu64 "\n",
				       t->ae4dma_chan_id, t->ae4dma_hwq_id, worker->core, xfer_per_sec,
				       bw_in_MiBps, t->xfer_failed);
			}
			t = t->next;
		}
		worker = worker->next;
	}

	total_xfer_per_sec = total_completed / g_user_config.time_in_sec;
	total_bw_in_MiBps = (total_completed * g_user_config.xfer_size_bytes) /
			    (g_user_config.time_in_sec * 1024 * 1024);

	printf("============================================================================\n");
	printf("Total:%42" PRIu64 "/s%8" PRIu64 " MiB/s%11" PRIu64 "\n",
	       total_xfer_per_sec, total_bw_in_MiBps, total_failed);

	return total_failed ? 1 : 0;
}

static struct spdk_ae4dma_chan *
get_next_chan(void)
{
	struct spdk_ae4dma_chan *chan;

	if (g_next_device == NULL) {
		return NULL;
	}

	chan = g_next_device->ae4dma;
	g_next_device = TAILQ_NEXT(g_next_device, tailq);

	return chan;
}

static int
associate_workers_with_chan(void)
{
	struct spdk_ae4dma_chan *chan = get_next_chan();
	struct worker_thread	*worker = g_workers;
	struct ae4dma_chan_entry	*t;
	char buf_pool_name[40], task_pool_name[40];
	int i = 0, j = 0;

	while (chan != NULL) {
		for (j = (g_user_config.ae4dma_hw_queues - 1); j >=  0; j--) {
			t = calloc(1, sizeof(struct ae4dma_chan_entry));
			if (!t) {
				return 1;
			}
			t->ae4dma_chan_id = i;
			t->ae4dma_hwq_id = j;
			snprintf(buf_pool_name, sizeof(buf_pool_name), "buf_pool_%d_%d", i, j);
			snprintf(task_pool_name, sizeof(task_pool_name), "task_pool_%d_%d", i, j);

			t->data_pool = ae4dma_datapool_create(g_user_config.xfer_size_bytes, g_user_config.queue_depth);

			t->task_pool = spdk_mempool_create(task_pool_name,
							   g_user_config.queue_depth,
							   sizeof(struct ae4dma_task),
							   SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
							   SPDK_ENV_SOCKET_ID_ANY);
			if (!t->task_pool || !t->data_pool) {
				fprintf(stderr, "Could not allocate buffer pool.\n");
				free(t->data_pool);
				spdk_mempool_free(t->task_pool);
				free(t);
				return 1;
			}
			t->chan = chan;
			t->next = worker->ctx;
			worker->ctx = t;
			worker = worker->next;
			if (worker == NULL) {
				worker = g_workers;
			}
		}
		chan = get_next_chan();
		i++;
	}
	return 0;
}

int
main(int argc, char **argv)
{
	int rc;
	struct worker_thread *worker, *main_worker;
	unsigned main_core;

	if (parse_args(argc, argv) != 0) {
		return 1;
	}

	if (init() != 0) {
		return 1;
	}

	if (register_workers() != 0) {
		rc = 1;
		goto cleanup;
	}

	if (ae4dma_init() != 0) {
		rc = 1;
		goto cleanup;
	}

	if (g_ae4dma_chan_num == 0) {
		printf("No channels found\n");
		rc = 1;
		goto cleanup;
	}

	if (g_user_config.ae4dma_chan_num > g_ae4dma_chan_num) {
		printf("%d channels are requested, but only %d are found,"
		       "so only test %d channels\n", g_user_config.ae4dma_chan_num,
		       g_ae4dma_chan_num, g_ae4dma_chan_num);
		g_user_config.ae4dma_chan_num = g_ae4dma_chan_num;
	}

	g_next_device = TAILQ_FIRST(&g_devices);
	dump_user_config(&g_user_config);

	if (associate_workers_with_chan() != 0) {
		rc = 1;
		goto cleanup;
	}

	/* Launch all of the secondary workers */
	main_core = spdk_env_get_current_core();
	main_worker = NULL;
	worker = g_workers;
	while (worker != NULL) {
		if (worker->core != main_core) {
			spdk_env_thread_launch_pinned(worker->core, work_fn, worker);
		} else {
			assert(main_worker == NULL);
			main_worker = worker;
		}
		worker = worker->next;
	}

	assert(main_worker != NULL);
	rc = work_fn(main_worker);
	if (rc != 0) {
		goto cleanup;
	}

	spdk_env_thread_wait_all();
	rc = dump_result();

cleanup:
	unregister_workers();
	ae4dma_exit();
	spdk_env_fini();
	return rc;
}
