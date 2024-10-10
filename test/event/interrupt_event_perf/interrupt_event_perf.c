/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Intel Corporation.
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
	uint32_t called_events_count;
	uint64_t total_tsc;
};

static struct call_stat *g_call_stat;
static uint32_t g_events_count = 0;
static uint32_t g_num_secondary = 0;
static uint32_t g_num_events = 0;
static struct spdk_poller *g_poller = NULL;

static void
send_wake_up_event(void *arg1, void *arg2)
{
	struct wake_up_ctx *ctx = arg1;
	uint32_t lcore = spdk_env_get_current_core();

	g_call_stat[lcore].total_tsc += spdk_get_ticks() - ctx->event_tsc_start;
	g_call_stat[lcore].called_events_count++;
	free(ctx);

	printf("wake up core %d, count %u\n", lcore, g_call_stat[lcore].called_events_count);
	g_num_events--;
}

static int
wake_up_reactors(void *args)
{
	uint32_t i;
	struct wake_up_ctx *ctx;
	uint32_t main_core = spdk_env_get_main_core();
	uint32_t lcore = spdk_env_get_next_core(main_core);

	if (g_num_events == 0) {
		/* Check g_events_count - 1 here since reactor will run one more round before it stops */
		if (g_call_stat[lcore].called_events_count == g_events_count - 1) {
			spdk_poller_unregister(&g_poller);
			spdk_app_stop(0);
		}

		/* Wait several seoncds here to make sure other cores entered sleep mode */
		sleep(2);
		SPDK_ENV_FOREACH_CORE(i) {
			if (i == main_core) {
				continue;
			}
			g_num_events++;
			ctx = calloc(1, sizeof(*ctx));
			ctx->event_tsc_start = spdk_get_ticks();
			spdk_event_call(spdk_event_allocate(i, send_wake_up_event, ctx, NULL));
		}

		return SPDK_POLLER_BUSY;
	}

	return SPDK_POLLER_IDLE;
}

static void
register_poller(void *ctx)
{
	g_poller = SPDK_POLLER_REGISTER(wake_up_reactors, NULL, 0);
	if (g_poller == NULL) {
		fprintf(stderr, "Failed to register poller on app thread\n");
		spdk_app_stop(-1);
	}
}

static void
set_interrupt_mode_cb(void *arg1, void *arg2)
{
	if (--g_num_secondary > 0) {
		return;
	}

	spdk_thread_send_msg(spdk_thread_get_app_thread(), register_poller, NULL);
}

static void
usage(char *program_name)
{
	printf("%s options:\n", program_name);
	printf("\t[-m core mask for distributing events\n");
	printf("\t\t(at least two cores - number of cores of the core mask must be larger than 1)]\n");
	printf("\t[-c number of events calls to each reactor\n");
	printf("\t\t(at least one event - number of events must be larger than 0)]\n");
}

static void
event_perf_start(void *arg1)
{
	uint32_t i, num_cores;
	uint32_t main_core = spdk_env_get_main_core();
	char *program_name = arg1;

	num_cores = spdk_env_get_core_count();
	if (num_cores <= 1) {
		fprintf(stderr, "Invalid core mask\n");
		usage(program_name);
		spdk_app_stop(-1);
		return;
	}

	g_call_stat = calloc(spdk_env_get_last_core() + 1, sizeof(*g_call_stat));
	if (g_call_stat == NULL) {
		fprintf(stderr, "g_call_stat allocation failed\n");
		spdk_app_stop(-1);
		return;
	}

	SPDK_ENV_FOREACH_CORE(i) {
		if (i == main_core) {
			continue;
		}
		g_num_secondary++;
		spdk_reactor_set_interrupt_mode(i, true,
						set_interrupt_mode_cb, NULL);
	}
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
		printf("lcore %2d: event count: %8u, wake up time per event: %8llu us\n", i,
		       g_call_stat[i].called_events_count,
		       g_call_stat[i].total_tsc * SPDK_SEC_TO_USEC / spdk_get_ticks_hz() /
		       g_call_stat[i].called_events_count);
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

	while ((op = getopt(argc, argv, "c:m:")) != -1) {
		switch (op) {
		case 'm':
			opts.reactor_mask = optarg;
			break;
		case 'c':
			rc = spdk_strtol(optarg, 10);
			if (rc <= 0) {
				fprintf(stderr, "Invalid events count\n");
				usage(argv[0]);
				exit(-1);
			}
			g_events_count = rc;
			break;
		default:
			usage(argv[0]);
			exit(-1);
		}
	}

	if (!g_events_count || !opts.reactor_mask) {
		usage(argv[0]);
		exit(-1);
	}

	printf("Running %d events calls\n", g_events_count);
	fflush(stdout);

	rc = spdk_app_start(&opts, event_perf_start, argv[0]);

	spdk_app_fini();
	performance_dump();

	printf("done.\n");
	return rc;
}
