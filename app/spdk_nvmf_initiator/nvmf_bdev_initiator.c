/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Nutanix Inc. All rights reserved.
 */

#include "spdk/stdinc.h"
#include "spdk/string.h"
#include "spdk/event.h"
#include "module/bdev/nvme/bdev_nvme.h"

static struct spdk_nvme_transport_id g_trid;
static char g_hostnqn[SPDK_NVMF_NQN_MAX_LEN + 1];
static pthread_mutex_t g_pending_test_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_pending_test_cond = PTHREAD_COND_INITIALIZER;
static bool fatal_flag = false;       /* To capture any error occured in the callback functions */

/* START: Code taken from hello_bdev.c with some modification */
struct hello_context_t {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;
	char *buff;
	uint32_t buff_size;
	char *bdev_name;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
};

static void
read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct hello_context_t *hello_context = cb_arg;

	if (success) {
		SPDK_NOTICELOG("Read string from bdev : %s\n", hello_context->buff);
		if (strcmp(hello_context->buff, "Hello World!")) {
			SPDK_ERRLOG("Read string different from the written string.\n");
			success = false;
		}
	} else {
		SPDK_ERRLOG("bdev io read error\n");
	}

	/* Complete the bdev io and close the channel */
	spdk_bdev_free_io(bdev_io);
	spdk_put_io_channel(hello_context->bdev_io_channel);
	spdk_bdev_close(hello_context->bdev_desc);
	fatal_flag = success ? false : true;
	pthread_cond_signal(&g_pending_test_cond);
}

static void
hello_read(void *arg)
{
	struct hello_context_t *hello_context = arg;
	int rc = 0;

	SPDK_NOTICELOG("Reading io\n");
	rc = spdk_bdev_read(hello_context->bdev_desc, hello_context->bdev_io_channel,
			    hello_context->buff, 0, hello_context->buff_size, read_complete,
			    hello_context);

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("Queueing io\n");
		/* In case we cannot perform I/O now, queue I/O */
		hello_context->bdev_io_wait.bdev = hello_context->bdev;
		hello_context->bdev_io_wait.cb_fn = hello_read;
		hello_context->bdev_io_wait.cb_arg = hello_context;
		spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
					&hello_context->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		fatal_flag = true;
		pthread_cond_signal(&g_pending_test_cond);
	}
}

static void
write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct hello_context_t *hello_context = cb_arg;

	/* Complete the I/O */
	spdk_bdev_free_io(bdev_io);

	if (success) {
		SPDK_NOTICELOG("bdev io write completed successfully\n");
	} else {
		SPDK_ERRLOG("bdev io write error: %d\n", EIO);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		fatal_flag = true;
		pthread_cond_signal(&g_pending_test_cond);
		return;
	}

	/* Zero the buffer so that we can use it for reading */
	memset(hello_context->buff, 0, hello_context->buff_size);
	hello_read(hello_context);
}

static void
hello_write(void *arg)
{
	struct hello_context_t *hello_context = arg;
	int rc = 0;

	SPDK_NOTICELOG("Writing to the bdev\n");
	rc = spdk_bdev_write(hello_context->bdev_desc, hello_context->bdev_io_channel,
			     hello_context->buff, 0, hello_context->buff_size, write_complete,
			     hello_context);

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("Queueing io\n");
		/* In case we cannot perform I/O now, queue I/O */
		hello_context->bdev_io_wait.bdev = hello_context->bdev;
		hello_context->bdev_io_wait.cb_fn = hello_write;
		hello_context->bdev_io_wait.cb_arg = hello_context;
		spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
					&hello_context->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		fatal_flag = true;
		pthread_cond_signal(&g_pending_test_cond);
	}
}

static void
hello_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		    void *event_ctx)
{
	SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

/* END of code taken from hello_bdev.c */

static int
parse_args(int argc, char **argv)
{
	int op;
	char *hostnqn;

	spdk_nvme_trid_populate_transport(&g_trid, SPDK_NVME_TRANSPORT_PCIE);
	snprintf(g_trid.subnqn, sizeof(g_trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);

	while ((op = getopt(argc, argv, "r:")) != -1) {
		switch (op) {
		case 'r':
			if (spdk_nvme_transport_id_parse(&g_trid, optarg) != 0) {
				fprintf(stderr, "Error parsing transport address\n");
				return 1;
			}

			assert(optarg != NULL);
			hostnqn = strcasestr(optarg, "hostnqn:");
			if (hostnqn) {
				size_t len;

				hostnqn += strlen("hostnqn:");

				len = strcspn(hostnqn, " \t\n");
				if (len > (sizeof(g_hostnqn) - 1)) {
					fprintf(stderr, "Host NQN is too long\n");
					return 1;
				}

				memcpy(g_hostnqn, hostnqn, len);
				g_hostnqn[len] = '\0';
			}
			break;
		default:
			return 1;
		}
	}
	return 0;
}

static int
test_multipathing(void *arg)
{
	/* TODO: implement multipathing in this function
	 */
	return -1;
}

static int
test_persistent_reservation(void *arg)
{
	/* TODO: implement persistent reservation in this function
	 */
	return -1;
}

static int
test_io_operations(void *ctx)
{
	struct hello_context_t *hello_context = ctx;
	uint32_t buf_align;

	/* Allocate memory for the write buffer.
	 * Initialize the write buffer with the string "Hello World!"
	 */
	hello_context->buff_size = spdk_bdev_get_block_size(hello_context->bdev) *
				   spdk_bdev_get_write_unit_size(hello_context->bdev);
	buf_align = spdk_bdev_get_buf_align(hello_context->bdev);
	hello_context->buff = spdk_dma_zmalloc(hello_context->buff_size, buf_align, NULL);
	if (!hello_context->buff) {
		SPDK_ERRLOG("Failed to allocate buffer\n");
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		return -1;
	}

	snprintf(hello_context->buff, hello_context->buff_size, "%s", "Hello World!");

	hello_write(hello_context);

	return 0;
}

static void
discovery_and_connect_cb_fn(void *ctx, int rc)
{
	if (!rc) {
		struct hello_context_t *hello_context = ctx;
		struct spdk_bdev *bdev;

		bdev = spdk_bdev_first_leaf();
		if (bdev == NULL) {
			SPDK_ERRLOG("Could not found the bdev.\n");
			goto end;
		}
		hello_context->bdev = bdev;
		hello_context->bdev_name = bdev->name;

		SPDK_NOTICELOG("Opening the bdev %s\n", hello_context->bdev_name);
		rc = spdk_bdev_open_ext(hello_context->bdev_name, true, hello_bdev_event_cb, NULL,
					&hello_context->bdev_desc);
		if (rc) {
			SPDK_ERRLOG("Could not open bdev: %s\n", hello_context->bdev_name);
			goto end;
		}

		/* Open I/O channel */
		hello_context->bdev_io_channel = spdk_bdev_get_io_channel(hello_context->bdev_desc);
		if (hello_context->bdev_io_channel == NULL) {
			SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
			spdk_bdev_close(hello_context->bdev_desc);
			goto end;
		}

		pthread_cond_signal(&g_pending_test_cond);
		return;

	} else {
		SPDK_ERRLOG("Failed to get the bdev\n");
	}
end:
	fatal_flag = true;
	pthread_cond_signal(&g_pending_test_cond);
}

static int
test_discovery_and_connect(void *ctx)
{
	int rc;
	struct spdk_nvme_ctrlr_opts ctrlr_opts;
	struct spdk_bdev_nvme_ctrlr_opts bdev_opts = {};

	spdk_nvme_ctrlr_get_default_ctrlr_opts(&ctrlr_opts, sizeof(ctrlr_opts));
	if (g_hostnqn[0] != '\0') {
		memcpy(ctrlr_opts.hostnqn, g_hostnqn, sizeof(ctrlr_opts.hostnqn));
	}

	rc = bdev_nvme_start_discovery(&g_trid, ctrlr_opts.hostnqn, &ctrlr_opts, &bdev_opts,
				       0, false, discovery_and_connect_cb_fn, ctx);

	return rc;
}

static void *
test_bdev_initiator(void *ctx)
{
	int rc;
	struct hello_context_t *hello_context = ctx;

	rc = test_discovery_and_connect(hello_context);
	if (rc) {
		SPDK_ERRLOG("test_discovery_and_connect failed to start.\n");
		goto exit;
	}
	/* Waiting for test_discovery_and_connect to complete beforing testing io operations */
	pthread_cond_wait(&g_pending_test_cond, &g_pending_test_mtx);
	if (fatal_flag) {
		SPDK_ERRLOG("test_discovery_and_connect failed.\n");
		goto exit;
	}
	SPDK_NOTICELOG("test_discoery_and_connect is successfull.\n");

	rc = test_io_operations(hello_context);
	if (rc) {
		SPDK_ERRLOG("test_io_oprations failed to start.\n");
		goto exit;
	}
	/* Waiting for test_io_operations to complete */
	pthread_cond_wait(&g_pending_test_cond, &g_pending_test_mtx);
	if (fatal_flag) {
		SPDK_ERRLOG("test_io_oprations failed.\n");
		goto exit;
	}
	SPDK_NOTICELOG("test_io_operations is successfull.\n");

	rc = test_persistent_reservation(hello_context);
	if (rc) {
		SPDK_ERRLOG("test_persistent_reservation failed to start\n");
		goto exit;
	}
	/* Waiting for test_persistent_reservation to complete */
	pthread_cond_wait(&g_pending_test_cond, &g_pending_test_mtx);
	if (fatal_flag) {
		SPDK_ERRLOG("test_persistent_reservation failed.\n");
		goto exit;
	}
	SPDK_NOTICELOG("test_persistent_reservation is successfull.\n");

	rc = test_multipathing(hello_context);
	if (rc) {
		SPDK_ERRLOG("test_multipathing failed to start\n");
		goto exit;
	}
	/* Waiting for test_multipathing to complete */
	pthread_cond_wait(&g_pending_test_cond, &g_pending_test_mtx);
	if (fatal_flag) {
		SPDK_ERRLOG("test_multipathing failed.\n");
		goto exit;
	}
	SPDK_NOTICELOG("test_multipathing is successfull.\n");

exit:
	SPDK_NOTICELOG("Stopping app\n");
	spdk_app_stop(fatal_flag ? -1 : rc);
	if (hello_context->buff) {
		spdk_dma_free(hello_context->buff);
	}
	pthread_exit(NULL);
}

static void
start_spdk_application(void *ctx)
{
	SPDK_NOTICELOG("Successfully started the application\n");

	pthread_t tid;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, 1);
	pthread_create(&tid, &attr, &test_bdev_initiator, ctx);
}

int
main(int argc, char **argv)
{
	int rc;
	rc = parse_args(argc, argv);
	if (rc != 0) {
		return rc;
	}

	struct hello_context_t *hello_context = calloc(1, sizeof(*hello_context));
	if (!hello_context) {
		SPDK_ERRLOG("Unable to allocate memory.\n");
		return -ENOMEM;
	}

	struct spdk_app_opts app_opts = {};
	spdk_app_opts_init(&app_opts, sizeof(app_opts));
	app_opts.name = "nvmf_bdev_initiator";
	app_opts.rpc_addr = NULL;
	app_opts.no_huge = true;
	app_opts.mem_size = 1024;

	rc = spdk_app_start(&app_opts, start_spdk_application, hello_context);
	if (rc == 1) {
		SPDK_ERRLOG("ERROR starting application\n");
	} else if (rc) {
		SPDK_ERRLOG("nvmf_bdev_initiator test failed\n");
	}

	spdk_app_fini();
	return rc;
}
