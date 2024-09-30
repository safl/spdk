/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2016 Intel Corporation.
 *   All rights reserved.
 */

#include "spdk/stdinc.h"

#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk_internal/event.h"

struct wake_up_ctx {
	uint64_t event_tsc_start;
};

struct call_stat {
	uint64_t call_count;
	uint64_t total_tsc;
};

static int g_events_count;
static struct call_stat *g_call_stat;
static uint32_t g_slave_num = 0;
static uint32_t g_event_num = 0;
static bool g_app_stopped = false;
static struct spdk_poller *g_poller;

static void
send_wake_up_event(void *arg1, void *arg2)
{
	struct wake_up_ctx *ctx = arg1;
	uint32_t lcore = spdk_env_get_current_core();

	g_call_stat[lcore].total_tsc += spdk_get_ticks() - ctx->event_tsc_start;
	g_call_stat[lcore].call_count++;
	free(ctx);

	printf("wake up core %d, count %lu\n", lcore, g_call_stat[lcore].call_count);
	g_event_num--;
}

static int
wake_up_reactors(void *args)
{
	uint32_t i;
	struct wake_up_ctx *ctx;
	uint32_t main_core = spdk_env_get_main_core();
	uint32_t lcore = spdk_env_get_next_core(main_core);

	if (g_event_num == 0) {
		/* Check g_events_count - 1 here since reactor will run one more round before it stops */
		if (g_call_stat[lcore].call_count == (long unsigned int)(g_events_count - 1)) {
			if (__sync_bool_compare_and_swap(&g_app_stopped, false, true)) {
				spdk_poller_unregister(&g_poller);
				spdk_app_stop(0);
			}
		}

		sleep(5);
		SPDK_ENV_FOREACH_CORE(i) {
			if (i == main_core) {
				continue;
			}
			g_event_num++;
			ctx = calloc(1, sizeof(*ctx));
			ctx->event_tsc_start = spdk_get_ticks();
			spdk_event_call(spdk_event_allocate(i, send_wake_up_event, ctx, NULL));
		}

		return SPDK_POLLER_BUSY;
	}

	return SPDK_POLLER_IDLE;
}

static void
set_interrupt_mode_cb(void *arg1, void *arg2)
{
	if (--g_slave_num > 0) {
		return;
	}

	sleep(5);

	g_poller = SPDK_POLLER_REGISTER(wake_up_reactors, NULL, 0);
}

static void
event_perf_start(void *arg1)
{
	uint32_t i;
	uint32_t main_core = spdk_env_get_main_core();

	g_call_stat = calloc(spdk_env_get_last_core() + 1, sizeof(*g_call_stat));
	if (g_call_stat == NULL) {
		fprintf(stderr, "g_call_stat allocation failed\n");
		spdk_app_stop(1);
		return;
	}

	SPDK_ENV_FOREACH_CORE(i) {
		if (i == main_core) {
			continue;
		}
		g_slave_num++;
		spdk_reactor_set_interrupt_mode(i, true,
						set_interrupt_mode_cb, NULL);
	}
}

static void
usage(char *program_name)
{
	printf("%s options\n", program_name);
	printf("\t[-m core mask for distributing I/O submission/completion work\n");
	printf("\t\t(default: 0x1 - use core 0 only)]\n");
	printf("\t[-c events calls for each worker core (non-main)]\n");
}

static void
performance_dump(void)
{
	uint32_t i;
	uint32_t main_core = spdk_env_get_main_core();

	if (g_call_stat == NULL) {
		return;
	}

	SPDK_ENV_FOREACH_CORE(i) {
		if (i == main_core) {
			continue;
		}
		printf("lcore %2d: event count: %8ld, wake up time per event: %8llu us\n", i,
		       g_call_stat[i].call_count,
		       g_call_stat[i].total_tsc * SPDK_SEC_TO_USEC / spdk_get_ticks_hz() / g_call_stat[i].call_count);
	}

	fflush(stdout);
	free(g_call_stat);
}

int
main(int argc, char **argv)
{
	struct spdk_app_opts opts = {};
	int op;
	int rc = 0;

	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "interrupt_event_perf";
	opts.rpc_addr = NULL;

	g_events_count = 0;

	while ((op = getopt(argc, argv, "m:c:")) != -1) {
		switch (op) {
		case 'm':
			opts.reactor_mask = optarg;
			break;
		case 'c':
			g_events_count = spdk_strtol(optarg, 10);
			if (g_events_count < 0) {
				fprintf(stderr, "Invalid events count\n");
				return g_events_count;
			}
			break;
		default:
			usage(argv[0]);
			exit(1);
		}
	}

	if (!g_events_count) {
		usage(argv[0]);
		exit(1);
	}

	printf("Running %d events calls", g_events_count);
	fflush(stdout);

	rc = spdk_app_start(&opts, event_perf_start, NULL);

	spdk_app_fini();
	performance_dump();

	printf("done.\n");
	return rc;
}
