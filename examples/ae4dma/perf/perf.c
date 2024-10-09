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

static struct user_config g_user_config;
static struct ae4dma_device *g_next_device;
static TAILQ_HEAD(, ae4dma_device) g_devices = TAILQ_HEAD_INITIALIZER(g_devices);

struct ae4dma_device {
	struct spdk_ae4dma_chan *ae4dma;
	TAILQ_ENTRY(ae4dma_device) tailq;
};

struct ae4dma_chan_entry {
	struct spdk_ae4dma_chan *chan;
	int ae4dma_chan_id;
	int ae4dma_hwq_id;
	uint64_t xfer_completed;
	uint64_t xfer_failed;
	uint64_t current_queue_depth;
	struct spdk_mempool *data_pool;
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
static int g_ae4dma_chan_num = 0;
static int g_num_workers = 0;

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
ae4dma_done(void *cb_arg)
{
	struct ae4dma_task *ae4dma_task = (struct ae4dma_task *)cb_arg;
	struct ae4dma_chan_entry *ae4dma_chan_entry = ae4dma_task->ae4dma_chan_entry;

	if (memcmp(ae4dma_task->src, ae4dma_task->dst, g_user_config.xfer_size_bytes)) {
		ae4dma_chan_entry->xfer_failed++;
	} else {
		ae4dma_chan_entry->xfer_completed++;
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
			spdk_mempool_free(entry->data_pool);
			spdk_mempool_free(entry->task_pool);
			free(entry);
			entry = entry1;
		}
		free(worker);
		worker = next_worker;
	}
}

static void
submit_single_xfer(struct ae4dma_chan_entry *ae4dma_chan_entry, struct ae4dma_task *ae4dma_task,
		   void *dst,
		   void *src)
{
	ae4dma_task->ae4dma_chan_entry = ae4dma_chan_entry;

	spdk_ae4dma_build_copy(ae4dma_chan_entry->chan, ae4dma_chan_entry->ae4dma_hwq_id,
			       ae4dma_task, ae4dma_done, dst, src, g_user_config.xfer_size_bytes);
	/* Flushing the copy request */
	spdk_ae4dma_flush(ae4dma_chan_entry->chan, ae4dma_chan_entry->ae4dma_hwq_id);
	ae4dma_chan_entry->current_queue_depth++;
}

static int
submit_xfers(struct ae4dma_chan_entry *ae4dma_chan_entry, uint64_t queue_depth)
{
	while (queue_depth-- > 0) {
		void *src = NULL, *dst = NULL;
		struct ae4dma_task *ae4dma_task = NULL;

		submit_single_xfer(ae4dma_chan_entry, ae4dma_task, dst, src);
		spdk_ae4dma_process_events(ae4dma_chan_entry->chan, ae4dma_chan_entry->ae4dma_hwq_id);

	}
	return 0;
}

static bool
probe_cb(void *cb_ctx, struct spdk_pci_device *pci_dev)
{
	/* probe call back */
	return true;
}

static void
attach_cb(void *cb_ctx, struct spdk_pci_device *pci_dev, struct spdk_ae4dma_chan *ae4dma)
{
	/* attach call back */
}

static int
work_fn(void *arg)
{
	struct ae4dma_chan_entry *t = NULL;

	if (submit_xfers(t, g_user_config.queue_depth) != 0) {
		return 1;
	}
	return 0;
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
	char buf_pool_name[30], task_pool_name[30];
	int i = 0;

	while (chan != NULL) {
		t = calloc(1, sizeof(struct ae4dma_chan_entry));
		if (!t) {
			return 1;
		}

		t->ae4dma_chan_id = i;
		snprintf(buf_pool_name, sizeof(buf_pool_name), "buf_pool_%d", i);
		snprintf(task_pool_name, sizeof(task_pool_name), "task_pool_%d", i);
		t->data_pool = spdk_mempool_create(buf_pool_name,
						   g_user_config.queue_depth * 2, /* src + dst */
						   g_user_config.xfer_size_bytes,
						   SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
						   SPDK_ENV_SOCKET_ID_ANY);
		t->task_pool = spdk_mempool_create(task_pool_name,
						   g_user_config.queue_depth,
						   sizeof(struct ae4dma_task),
						   SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
						   SPDK_ENV_SOCKET_ID_ANY);
		if (!t->data_pool || !t->task_pool) {
			fprintf(stderr, "Could not allocate buffer pool.\n");
			spdk_mempool_free(t->data_pool);
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

		chan = get_next_chan();
		i++;
	}

	return 0;
}


static int
ae4dma_init(void)
{
	if (spdk_ae4dma_probe(NULL, probe_cb, attach_cb) != 0) {
		fprintf(stderr, "ae4dma_probe() failed\n");
		return 1;
	}

	return 0;
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

static void
usage(char *program_name)
{
	printf("%s options\n", program_name);
	printf("\t[-h help message]\n");
	printf("\t[-c core mask for distributing I/O submission/completion work]\n");
	printf("\t[-q queue depth]\n");
	printf("\t[-n number of channels]\n");
	printf("\t[-o transfer size in bytes]\n");
	printf("\t[-t time in seconds]\n");
	printf("\t[-v verify copy result if this switch is on]\n");
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
parse_args(int argc, char **argv)
{
	int op;

	construct_user_config(&g_user_config);
	while ((op = getopt(argc, argv, "c:hn:o:q:t:v")) != -1) {
		switch (op) {
		case 'o':
			g_user_config.xfer_size_bytes = spdk_strtol(optarg, 10);
			break;
		case 'n':
			g_user_config.ae4dma_chan_num = spdk_strtol(optarg, 10);
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
	    g_user_config.time_in_sec <= 0 || !g_user_config.core_mask ||
	    g_user_config.ae4dma_chan_num <= 0) {
		usage(argv[0]);
		return 1;
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

cleanup:
	unregister_workers();
	ae4dma_exit();

	spdk_env_fini();
	return rc;
}
