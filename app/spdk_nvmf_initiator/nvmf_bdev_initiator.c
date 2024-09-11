/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Nutanix Inc. All rights reserved.
 */

#include "spdk/stdinc.h"
#include "spdk/string.h"
#include "spdk/event.h"
#include "module/bdev/nvme/bdev_nvme.h"

#define CR_KEY		0xDEADBEAF5A5A5A5B
#define NR_KEY		0xDEADBEAF5A5A5A5A

static struct spdk_nvme_transport_id g_trid;
static struct spdk_thread *g_app_thread;
static char g_hostnqn[SPDK_NVMF_NQN_MAX_LEN + 1];
static pthread_mutex_t g_pending_test_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_pending_test_cond = PTHREAD_COND_INITIALIZER;
static bool test_failed = false;	/* To capture any error occured in callback functions */
static int g_dpdk_mem = 0;
static bool g_no_huge = false;

struct bdev_context_t {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;
	struct spdk_nvme_cmd cmd;
	char *buff;
	uint32_t buff_size;
	char *bdev_name;
	uint8_t host_id;
};

struct callback_arg {
	struct bdev_context_t *bdev_context;
	bool success_expected;
};

static int test_io_operations(void *ctx, bool success_expected);

static void
finalize_bdev_context(void *ctx)
{
	struct bdev_context_t *bdev_context = ctx;
	if (bdev_context->bdev_io_channel) {
		spdk_put_io_channel(bdev_context->bdev_io_channel);
	}
	if (bdev_context->bdev_desc) {
		spdk_bdev_close(bdev_context->bdev_desc);
	}
}

static void
nvmf_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		   void *event_ctx)
{
	SPDK_NOTICELOG("unsupported bdev event: type %d\n", type);
}

static void
stop_app(void *ctx, int rc)
{
	struct bdev_context_t *bdev_context = ctx;
	SPDK_NOTICELOG("stopping app\n");
	spdk_thread_send_msg(g_app_thread, finalize_bdev_context, bdev_context);
	spdk_app_stop(test_failed ? -1 : rc);
	if (bdev_context->buff) {
		spdk_dma_free(bdev_context->buff);
	}
}

static inline void
cond_signal_other_thread(void)
{
	pthread_mutex_lock(&g_pending_test_mtx);
	pthread_cond_signal(&g_pending_test_cond);
	pthread_mutex_unlock(&g_pending_test_mtx);
}

static inline void
cond_wait_on_other_thread(void)
{
	pthread_mutex_lock(&g_pending_test_mtx);
	pthread_cond_wait(&g_pending_test_cond, &g_pending_test_mtx);
	pthread_mutex_unlock(&g_pending_test_mtx);
}

static inline int
check_pr_test_status(int rc, char *msg)
{
	if (rc) {
		SPDK_ERRLOG("%s failed\n", msg);
	} else {
		SPDK_NOTICELOG("%s is successful\n", msg);
	}
	return rc;
}

static void
usage(const char *program_name)
{
	printf("%s [options]", program_name);
	printf("\n");
	printf("options:\n");
	printf(" -r trid    remote NVMe over Fabrics target address\n");
	printf("    Format: 'key:value [key:value] ...'\n");
	printf("    Keys:\n");
	printf("     trtype      Transport type (e.g. TCP)\n");
	printf("     adrfam      Address family (e.g. IPv4, IPv6)\n");
	printf("     traddr      Transport address (e.g. 192.168.100.8)\n");
	printf("     trsvcid     Transport service identifier (e.g. 4420)\n");
	printf("     subnqn      Subsystem NQN (default: %s)\n", SPDK_NVMF_DISCOVERY_NQN);
	printf("     hostnqn     Host NQN\n");
	printf("    Example: -r 'trtype:RDMA adrfam:IPv4 traddr:192.168.100.8 trsvcid:4420'\n");

	printf(" -n         set no_huge to true\n");
	printf(" -d         DPDK huge memory size in MB\n");
	printf(" -H         show this usage\n");
}

static int
parse_args(int argc, char **argv)
{
	int op;
	char *hostnqn;

	if (argc < 2) {
		usage(argv[0]);
		return 1;
	}
	spdk_nvme_trid_populate_transport(&g_trid, SPDK_NVME_TRANSPORT_TCP);
	snprintf(g_trid.subnqn, sizeof(g_trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);

	while ((op = getopt(argc, argv, "nd:p:r:H")) != -1) {
		switch (op) {
		case 'n':
			g_no_huge = true;
			break;
		case 'd':
			g_dpdk_mem = spdk_strtol(optarg, 10);
			if (g_dpdk_mem < 0) {
				fprintf(stderr, "invalid DPDK memory size\n");
				return g_dpdk_mem;
			}
			break;
		case 'r':
			if (spdk_nvme_transport_id_parse(&g_trid, optarg) != 0) {
				fprintf(stderr, "error parsing transport address\n");
				return 1;
			}

			assert(optarg != NULL);
			hostnqn = strcasestr(optarg, "hostnqn:");
			if (hostnqn) {
				size_t len;
				hostnqn += strlen("hostnqn:");
				len = strcspn(hostnqn, " \t\n");
				if (len > (sizeof(g_hostnqn) - 1)) {
					fprintf(stderr, "host NQN is too long\n");
					return 1;
				}
				memcpy(g_hostnqn, hostnqn, len);
				g_hostnqn[len] = '\0';
			}
			break;
		case 'H':
			usage(argv[0]);
			exit(EXIT_SUCCESS);
		default:
			usage(argv[0]);
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

static void
reservation_request_cb_fn(struct spdk_bdev_io *bdev_io, bool success, void *ctx)
{
	/* Complete the I/O */
	spdk_bdev_free_io(bdev_io);
	struct callback_arg *cb_arg = ctx;

	if (success) {
		SPDK_NOTICELOG("bdev reservation request is successful\n");
	} else {
		SPDK_NOTICELOG("bdev reservation request failed\n");
	}

	test_failed = (success != cb_arg->success_expected);
	cond_signal_other_thread();
}

static int
bdev_reservation_register(void *ctx, enum spdk_nvme_reservation_register_action opc,
			  bool success_expected)
{
	int rc = 0;
	struct bdev_context_t *bdev_context = ctx;
	struct spdk_nvme_cmd *cmd = &(bdev_context->cmd);
	struct spdk_nvme_reservation_register_data *rr_data = calloc(1, sizeof(*rr_data));
	struct callback_arg *resv_cb_arg = calloc(1, sizeof(*resv_cb_arg));
	resv_cb_arg->bdev_context = ctx;
	resv_cb_arg->success_expected = success_expected;
	memset(cmd, 0, sizeof(*cmd));

	if (opc == SPDK_NVME_RESERVE_REGISTER_KEY) {
		rr_data->crkey = 0;
		rr_data->nrkey = CR_KEY;
	} else if (opc == SPDK_NVME_RESERVE_UNREGISTER_KEY) {
		rr_data->crkey = CR_KEY;
		rr_data->nrkey = 0;
	} else if (opc == SPDK_NVME_RESERVE_REPLACE_KEY) {
		rr_data->crkey = CR_KEY;
		rr_data->nrkey = NR_KEY;
	}

	cmd->opc = SPDK_NVME_OPC_RESERVATION_REGISTER;
	cmd->cdw10_bits.resv_register.rrega = opc;
	cmd->cdw10_bits.resv_register.iekey = false;
	cmd->cdw10_bits.resv_register.cptpl = SPDK_NVME_RESERVE_PTPL_CLEAR_POWER_ON;
	rc = spdk_bdev_nvme_io_passthru(bdev_context->bdev_desc, bdev_context->bdev_io_channel,
					cmd, rr_data, sizeof(*rr_data), reservation_request_cb_fn,
					resv_cb_arg);
	if (rc) {
		SPDK_ERRLOG("failed to submit NVMe I/O command to bdev\n");
		return rc;
	}
	/* wait for callback function to complete */
	cond_wait_on_other_thread();
	if (test_failed) {
		rc = -1;
	}
	free(rr_data);
	free(resv_cb_arg);

	return rc;
}

static int
bdev_reservation_acquire(void *ctx, enum spdk_nvme_reservation_acquire_action opc,
			 bool success_expected)
{
	int rc = 0;
	struct bdev_context_t *bdev_context = ctx;
	struct spdk_nvme_cmd *cmd = &(bdev_context->cmd);
	struct spdk_nvme_reservation_acquire_data *cdata = calloc(1, sizeof(*cdata));
	struct callback_arg *resv_cb_arg = calloc(1, sizeof(*resv_cb_arg));
	resv_cb_arg->bdev_context = ctx;
	resv_cb_arg->success_expected = success_expected;
	memset(cmd, 0, sizeof(*cmd));

	cdata->crkey = CR_KEY;
	cdata->prkey = 0;
	cmd->opc = SPDK_NVME_OPC_RESERVATION_ACQUIRE;
	cmd->cdw10_bits.resv_acquire.racqa = opc;
	cmd->cdw10_bits.resv_acquire.iekey = false;
	cmd->cdw10_bits.resv_acquire.rtype = SPDK_NVME_RESERVE_WRITE_EXCLUSIVE;

	rc = spdk_bdev_nvme_io_passthru(bdev_context->bdev_desc, bdev_context->bdev_io_channel,
					cmd, cdata, sizeof(*cdata), reservation_request_cb_fn,
					resv_cb_arg);
	if (rc) {
		SPDK_ERRLOG("failed to submit NVMe I/O command to bdev.\n");
		return rc;
	}
	/* wait for callback function to complete */
	cond_wait_on_other_thread();
	if (test_failed) {
		rc = -1;
	}
	free(cdata);
	free(resv_cb_arg);

	return rc;
}

static int
bdev_reservation_release(void *ctx, enum spdk_nvme_reservation_release_action action_opc,
			 bool success_expected)
{
	int rc = 0;
	struct bdev_context_t *bdev_context = ctx;
	struct spdk_nvme_cmd *cmd = &(bdev_context->cmd);
	struct spdk_nvme_reservation_key_data *rdata = calloc(1, sizeof(*rdata));
	struct callback_arg *resv_cb_arg = calloc(1, sizeof(*resv_cb_arg));
	resv_cb_arg->bdev_context = ctx;
	resv_cb_arg->success_expected = success_expected;
	memset(cmd, 0, sizeof(*cmd));

	rdata->crkey = CR_KEY;
	cmd->opc = SPDK_NVME_OPC_RESERVATION_RELEASE;
	cmd->cdw10_bits.resv_acquire.racqa = action_opc;
	cmd->cdw10_bits.resv_acquire.iekey = false;
	cmd->cdw10_bits.resv_acquire.rtype = SPDK_NVME_RESERVE_WRITE_EXCLUSIVE;

	rc = spdk_bdev_nvme_io_passthru(bdev_context->bdev_desc, bdev_context->bdev_io_channel,
					cmd, rdata, sizeof(*rdata), reservation_request_cb_fn,
					resv_cb_arg);
	if (rc) {
		SPDK_ERRLOG("failed to submit NVMe I/O command to bdev.\n");
		return rc;
	}
	/* wait for callback function to complete */
	cond_wait_on_other_thread();
	if (test_failed) {
		rc = -1;
	}
	free(rdata);
	free(resv_cb_arg);

	return rc;
}

static int
test_persistent_reservation_multi_host(void *ctx)
{
	/* TODO: implement pr testing from secondary host in this function
	 */
	return -1;
}

static int
test_persistent_reservation_single_host(void *ctx)
{
	int rc;
	struct bdev_context_t *bdev_context = ctx;

	/* sending reservation_register command */
	rc = bdev_reservation_register(bdev_context, SPDK_NVME_RESERVE_REGISTER_KEY, true);
	if (check_pr_test_status(rc, "expected bdev_reservation_register request")) {
		return rc;
	}

	/* sending reservation acquire command */
	rc = bdev_reservation_acquire(bdev_context, SPDK_NVME_RESERVE_ACQUIRE, true);
	if (check_pr_test_status(rc, "expected bdev_reservation_acquire request")) {
		return rc;
	}

	/* sending io commands */
	rc = test_io_operations(bdev_context, true);
	if (check_pr_test_status(rc, "expected test_io_operations with bdev reservation")) {
		return rc;
	}

	/* sending reservation release command */
	rc = bdev_reservation_release(bdev_context, SPDK_NVME_RESERVE_RELEASE, true);
	if (check_pr_test_status(rc, "expected bdev_reservation_release request")) {
		return rc;
	}

	/* sending reservation unregister command */
	rc = bdev_reservation_register(bdev_context, SPDK_NVME_RESERVE_UNREGISTER_KEY, true);
	if (check_pr_test_status(rc, "expected bdev_reservation_unregister request")) {
		return rc;
	}

	return 0;
}

static void
bdev_read_cb_fn(struct spdk_bdev_io *bdev_io, bool success, void *ctx)
{
	struct callback_arg *io_cb_arg = ctx;
	struct bdev_context_t *bdev_context = io_cb_arg->bdev_context;

	if (success) {
		SPDK_NOTICELOG("read string from bdev : %s\n", bdev_context->buff);
		if (strcmp(bdev_context->buff, "Hello World!")) {
			SPDK_ERRLOG("read string different from the written string\n");
			success = false;
		}
	}
	if (!success) {
		SPDK_ERRLOG("bdev io read error\n");
	}

	test_failed = (success != io_cb_arg->success_expected);
	spdk_bdev_free_io(bdev_io);
	cond_signal_other_thread();
}

static int
test_bdev_read(void *ctx)
{
	struct callback_arg *io_cb_arg = ctx;
	struct bdev_context_t *bdev_context = io_cb_arg->bdev_context;
	int rc = 0;

	SPDK_NOTICELOG("reading from bdev\n");
	rc = spdk_bdev_read(bdev_context->bdev_desc, bdev_context->bdev_io_channel,
			    bdev_context->buff, 0, bdev_context->buff_size, bdev_read_cb_fn,
			    io_cb_arg);
	if (rc) {
		SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
		return rc;
	}
	/* wait for the callback funcion */
	cond_wait_on_other_thread();
	if (test_failed) {
		rc = -1;
	}
	return rc;
}

static void
bdev_write_cb_fn(struct spdk_bdev_io *bdev_io, bool success, void *ctx)
{
	/* Complete the I/O */
	spdk_bdev_free_io(bdev_io);
	struct callback_arg *io_cb_arg = ctx;

	if (success) {
		SPDK_NOTICELOG("bdev io write completed successfully\n");
	} else {
		SPDK_NOTICELOG("bdev io write error: %d\n", EIO);
	}

	test_failed = (success != io_cb_arg->success_expected);
	cond_signal_other_thread();
}

static int
test_bdev_write(void *ctx)
{
	struct callback_arg *io_cb_arg = ctx;
	struct bdev_context_t *bdev_context = io_cb_arg->bdev_context;
	int rc = 0;

	snprintf(bdev_context->buff, bdev_context->buff_size, "%s", "Hello World!");

	SPDK_NOTICELOG("writing to the bdev\n");
	rc = spdk_bdev_write(bdev_context->bdev_desc, bdev_context->bdev_io_channel,
			     bdev_context->buff, 0, bdev_context->buff_size, bdev_write_cb_fn,
			     io_cb_arg);
	if (rc) {
		SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
		return rc;
	}
	/* wait for callback function to complete */
	cond_wait_on_other_thread();
	if (test_failed) {
		rc = -1;
	}
	return rc;
}

static int
test_io_operations(void *ctx, bool success_expected)
{
	struct bdev_context_t *bdev_context = ctx;
	struct callback_arg *io_cb_arg = calloc(1, sizeof(*io_cb_arg));
	io_cb_arg->bdev_context = ctx;
	io_cb_arg->success_expected = success_expected;
	uint32_t buf_align;
	int rc = 0;

	/* Allocate memory for the io buffer */
	if (!bdev_context->buff) {
		bdev_context->buff_size = spdk_bdev_get_block_size(bdev_context->bdev) *
					  spdk_bdev_get_write_unit_size(bdev_context->bdev);
		buf_align = spdk_bdev_get_buf_align(bdev_context->bdev);
		bdev_context->buff = spdk_dma_zmalloc(bdev_context->buff_size, buf_align, NULL);
		if (!bdev_context->buff) {
			SPDK_ERRLOG("failed to allocate buffer\n");
			return -1;
		}
	}

	rc = test_bdev_write(io_cb_arg);
	if (rc) {
		SPDK_ERRLOG("expected write operation failed\n");
		return rc;
	}

	/* Zero the buffer so that we can use it for reading */
	memset(bdev_context->buff, 0, bdev_context->buff_size);

	rc = test_bdev_read(io_cb_arg);
	if (rc) {
		SPDK_ERRLOG("expected read operation failed\n");
		return rc;
	}

	free(io_cb_arg);
	return 0;
}

static void
discovery_and_connect_cb_fn(void *ctx, int rc)
{
	if (rc) {
		SPDK_ERRLOG("failed to get the bdev\n");
		goto end;
	}
	struct bdev_context_t *bdev_context = ctx;
	struct spdk_bdev *bdev;

	bdev = spdk_bdev_first_leaf();
	if (bdev == NULL) {
		SPDK_ERRLOG("could not find the bdev\n");
		goto end;
	}
	bdev_context->bdev = bdev;
	bdev_context->bdev_name = bdev->name;

	SPDK_NOTICELOG("opening the bdev %s\n", bdev_context->bdev_name);
	rc = spdk_bdev_open_ext(bdev_context->bdev_name, true, nvmf_bdev_event_cb, NULL,
				&bdev_context->bdev_desc);
	if (rc) {
		SPDK_ERRLOG("could not open bdev: %s\n", bdev_context->bdev_name);
		goto end;
	}

	/* Open I/O channel */
	bdev_context->bdev_io_channel = spdk_bdev_get_io_channel(bdev_context->bdev_desc);
	if (bdev_context->bdev_io_channel == NULL) {
		SPDK_ERRLOG("could not create bdev I/O channel!\n");
		goto end;
	}

	cond_signal_other_thread();
	return;

end:
	test_failed = true;
	cond_signal_other_thread();
}

static int
test_discovery_and_connect(void *ctx)
{
	int rc;
	struct bdev_context_t *bdev_context = ctx;
	struct spdk_nvme_ctrlr_opts ctrlr_opts;
	struct spdk_bdev_nvme_ctrlr_opts bdev_opts = {};

	spdk_nvme_ctrlr_get_default_ctrlr_opts(&ctrlr_opts, sizeof(ctrlr_opts));
	if (g_hostnqn[0] != '\0') {
		memcpy(ctrlr_opts.hostnqn, g_hostnqn, sizeof(ctrlr_opts.hostnqn));
	}

	rc = bdev_nvme_start_discovery(&g_trid, ctrlr_opts.hostnqn, &ctrlr_opts, &bdev_opts,
				       0, false, discovery_and_connect_cb_fn, bdev_context);
	if (rc) {
		SPDK_ERRLOG("test_discovery_and_connect failed to start\n");
		return rc;
	}

	/* wait for callback function to complete */
	cond_wait_on_other_thread();
	if (test_failed) {
		rc = -1;
	}
	return rc;
}

static void
check_test_status(char *test_name, int rc, void *ctx)
{
	if (rc) {
		SPDK_ERRLOG("test %s failed\n", test_name);
		stop_app(ctx, rc);
		pthread_exit(NULL);
	} else {
		SPDK_NOTICELOG("test %s is successful\n", test_name);
	}
}

static void *
test_bdev_initiator(void *ctx)
{
	check_test_status("discovery_and_connect", test_discovery_and_connect(ctx), ctx);
	check_test_status("io_operations", test_io_operations(ctx, true), ctx);
	check_test_status("single_host_pr",  test_persistent_reservation_single_host(ctx), ctx);
	check_test_status("multi_host_pr", test_persistent_reservation_multi_host(ctx), ctx);
	check_test_status("multipathing", test_multipathing(ctx), ctx);

	stop_app(ctx, 0);
	pthread_exit(NULL);
}

static void
start_spdk_application(void *ctx)
{
	SPDK_NOTICELOG("successfully started the application\n");

	pthread_t tid;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, 1);
	pthread_create(&tid, &attr, &test_bdev_initiator, ctx);
	g_app_thread = spdk_get_thread();
}

int
main(int argc, char **argv)
{
	int rc;
	rc = parse_args(argc, argv);
	if (rc != 0) {
		return rc;
	}

	struct bdev_context_t *bdev_context = calloc(1, sizeof(*bdev_context));
	if (!bdev_context) {
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	struct spdk_app_opts app_opts = {};
	spdk_app_opts_init(&app_opts, sizeof(app_opts));
	app_opts.name = "nvmf_bdev_initiator";
	app_opts.rpc_addr = NULL;
	app_opts.no_huge = g_no_huge;
	app_opts.mem_size = g_dpdk_mem;

	rc = spdk_app_start(&app_opts, start_spdk_application, bdev_context);
	if (rc == 0) {
		SPDK_NOTICELOG("nvmf_bdev_initiator test is successful\n");
	} else {
		if (rc == 1) {
			SPDK_ERRLOG("error starting application\n");
		}
		SPDK_ERRLOG("nvmf_bdev_initiator test failed\n");
	}

	spdk_app_fini();
	free(bdev_context);
	pthread_mutex_destroy(&g_pending_test_mtx);
	pthread_cond_destroy(&g_pending_test_cond);
	return rc;
}
