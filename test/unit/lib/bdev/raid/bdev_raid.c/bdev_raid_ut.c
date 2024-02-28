/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2022-2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "spdk/stdinc.h"
#include "spdk_internal/cunit.h"
#include "spdk/env.h"
#include "spdk_internal/mock.h"
#include "thread/thread_internal.h"
#include "bdev/raid/bdev_raid.c"
#include "bdev/raid/bdev_raid_rpc.c"
#include "bdev/raid/raid0.c"
#include "common/lib/ut_multithread.c"

#define MAX_BASE_DRIVES 32
#define MAX_RAIDS 2
#define INVALID_IO_SUBMIT 0xFFFF
#define MAX_TEST_IO_RANGE (3 * 3 * 3 * (MAX_BASE_DRIVES + 5))
#define BLOCK_CNT (1024ul * 1024ul * 1024ul * 1024ul)
#define MD_SIZE 8

struct spdk_bdev_channel {
	struct spdk_io_channel *channel;
};

struct spdk_bdev_desc {
	struct spdk_bdev *bdev;
};

/* Data structure to capture the output of IO for verification */
struct io_output {
	struct spdk_bdev_desc       *desc;
	struct spdk_io_channel      *ch;
	uint64_t                    offset_blocks;
	uint64_t                    num_blocks;
	spdk_bdev_io_completion_cb  cb;
	void                        *cb_arg;
	enum spdk_bdev_io_type      iotype;
	struct iovec                *iovs;
	int                         iovcnt;
	void                        *md_buf;
	uint32_t                    dif_check_flags_exclude_mask;
};

struct raid_io_ranges {
	uint64_t lba;
	uint64_t nblocks;
};

/* Globals */
int g_bdev_io_submit_status;
struct io_output *g_io_output = NULL;
uint32_t g_io_output_index;
uint32_t g_io_comp_status;
bool g_child_io_status_flag;
void *g_rpc_req;
uint32_t g_rpc_req_size;
TAILQ_HEAD(bdev, spdk_bdev);
struct bdev g_bdev_list;
TAILQ_HEAD(waitq, spdk_bdev_io_wait_entry);
struct waitq g_io_waitq;
uint32_t g_block_len;
uint32_t g_strip_size;
uint32_t g_max_io_size;
uint8_t g_max_base_drives;
uint8_t g_max_raids;
uint8_t g_ignore_io_output;
uint8_t g_rpc_err;
char *g_get_raids_output[MAX_RAIDS];
uint32_t g_get_raids_count;
uint8_t g_json_decode_obj_err;
uint8_t g_json_decode_obj_create;
uint8_t g_config_level_create = 0;
uint8_t g_test_multi_raids;
struct raid_io_ranges g_io_ranges[MAX_TEST_IO_RANGE];
uint32_t g_io_range_idx;
uint64_t g_lba_offset;
uint64_t g_bdev_ch_io_device;
bool g_bdev_io_defer_completion;
TAILQ_HEAD(, spdk_bdev_io) g_deferred_ios = TAILQ_HEAD_INITIALIZER(g_deferred_ios);
bool g_enable_dif;
uint32_t g_dif_check_flags_exclude_mask;

DEFINE_STUB_V(spdk_bdev_module_examine_done, (struct spdk_bdev_module *module));
DEFINE_STUB_V(spdk_bdev_module_list_add, (struct spdk_bdev_module *bdev_module));
DEFINE_STUB(spdk_bdev_io_type_supported, bool, (struct spdk_bdev *bdev,
		enum spdk_bdev_io_type io_type), true);
DEFINE_STUB_V(spdk_bdev_close, (struct spdk_bdev_desc *desc));
DEFINE_STUB(spdk_bdev_flush_blocks, int, (struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		uint64_t offset_blocks, uint64_t num_blocks, spdk_bdev_io_completion_cb cb,
		void *cb_arg), 0);
DEFINE_STUB(spdk_conf_next_section, struct spdk_conf_section *, (struct spdk_conf_section *sp),
	    NULL);
DEFINE_STUB_V(spdk_rpc_register_method, (const char *method, spdk_rpc_method_handler func,
		uint32_t state_mask));
DEFINE_STUB_V(spdk_rpc_register_alias_deprecated, (const char *method, const char *alias));
DEFINE_STUB_V(spdk_jsonrpc_end_result, (struct spdk_jsonrpc_request *request,
					struct spdk_json_write_ctx *w));
DEFINE_STUB_V(spdk_jsonrpc_send_bool_response, (struct spdk_jsonrpc_request *request,
		bool value));
DEFINE_STUB(spdk_json_decode_string, int, (const struct spdk_json_val *val, void *out), 0);
DEFINE_STUB(spdk_json_decode_uint32, int, (const struct spdk_json_val *val, void *out), 0);
DEFINE_STUB(spdk_json_decode_uuid, int, (const struct spdk_json_val *val, void *out), 0);
DEFINE_STUB(spdk_json_decode_array, int, (const struct spdk_json_val *values,
		spdk_json_decode_fn decode_func,
		void *out, size_t max_size, size_t *out_size, size_t stride), 0);
DEFINE_STUB(spdk_json_decode_bool, int, (const struct spdk_json_val *val, void *out), 0);
DEFINE_STUB(spdk_json_write_name, int, (struct spdk_json_write_ctx *w, const char *name), 0);
DEFINE_STUB(spdk_json_write_object_begin, int, (struct spdk_json_write_ctx *w), 0);
DEFINE_STUB(spdk_json_write_named_object_begin, int, (struct spdk_json_write_ctx *w,
		const char *name), 0);
DEFINE_STUB(spdk_json_write_string, int, (struct spdk_json_write_ctx *w, const char *val), 0);
DEFINE_STUB(spdk_json_write_object_end, int, (struct spdk_json_write_ctx *w), 0);
DEFINE_STUB(spdk_json_write_array_begin, int, (struct spdk_json_write_ctx *w), 0);
DEFINE_STUB(spdk_json_write_array_end, int, (struct spdk_json_write_ctx *w), 0);
DEFINE_STUB(spdk_json_write_named_array_begin, int, (struct spdk_json_write_ctx *w,
		const char *name), 0);
DEFINE_STUB(spdk_json_write_bool, int, (struct spdk_json_write_ctx *w, bool val), 0);
DEFINE_STUB(spdk_json_write_null, int, (struct spdk_json_write_ctx *w), 0);
DEFINE_STUB(spdk_json_write_named_uint64, int, (struct spdk_json_write_ctx *w, const char *name,
		uint64_t val), 0);
DEFINE_STUB(spdk_strerror, const char *, (int errnum), NULL);
DEFINE_STUB(spdk_bdev_queue_io_wait, int, (struct spdk_bdev *bdev, struct spdk_io_channel *ch,
		struct spdk_bdev_io_wait_entry *entry), 0);
DEFINE_STUB(spdk_bdev_get_memory_domains, int, (struct spdk_bdev *bdev,
		struct spdk_memory_domain **domains,	int array_size), 0);
DEFINE_STUB(spdk_bdev_get_name, const char *, (const struct spdk_bdev *bdev), "test_bdev");
DEFINE_STUB(spdk_bdev_is_dif_head_of_md, bool, (const struct spdk_bdev *bdev), false);
DEFINE_STUB(spdk_bdev_notify_blockcnt_change, int, (struct spdk_bdev *bdev, uint64_t size), 0);
DEFINE_STUB_V(raid_bdev_init_superblock, (struct raid_bdev *raid_bdev));


uint32_t
spdk_bdev_get_data_block_size(const struct spdk_bdev *bdev)
{
	return g_block_len;
}

typedef enum spdk_dif_type spdk_dif_type_t;

spdk_dif_type_t
spdk_bdev_get_dif_type(const struct spdk_bdev *bdev)
{
	if (bdev->md_len != 0) {
		return bdev->dif_type;
	} else {
		return SPDK_DIF_DISABLE;
	}
}

bool
spdk_bdev_is_md_interleaved(const struct spdk_bdev *bdev)
{
	return (bdev->md_len != 0) && bdev->md_interleave;
}

bool
spdk_bdev_is_md_separate(const struct spdk_bdev *bdev)
{
	return (bdev->md_len != 0) && !bdev->md_interleave;
}

uint32_t
spdk_bdev_get_md_size(const struct spdk_bdev *bdev)
{
	return bdev->md_len;
}

uint32_t
spdk_bdev_get_block_size(const struct spdk_bdev *bdev)
{
	return bdev->blocklen;
}

int
raid_bdev_load_base_bdev_superblock(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
				    raid_bdev_load_sb_cb cb, void *cb_ctx)
{
	cb(NULL, -EINVAL, cb_ctx);

	return 0;
}

void
raid_bdev_write_superblock(struct raid_bdev *raid_bdev, raid_bdev_write_sb_cb cb, void *cb_ctx)
{
	cb(0, raid_bdev, cb_ctx);
}

const struct spdk_uuid *
spdk_bdev_get_uuid(const struct spdk_bdev *bdev)
{
	return &bdev->uuid;
}

struct spdk_io_channel *
spdk_bdev_get_io_channel(struct spdk_bdev_desc *desc)
{
	return spdk_get_io_channel(&g_bdev_ch_io_device);
}

static int
set_test_opts(void)
{

	g_max_base_drives = MAX_BASE_DRIVES;
	g_max_raids = MAX_RAIDS;
	g_block_len = 4096;
	g_strip_size = 64;
	g_max_io_size = 1024;
	g_enable_dif = false;

	printf("Test Options\n");
	printf("blocklen = %u, strip_size = %u, max_io_size = %u, g_max_base_drives = %u, "
	       "g_max_raids = %u, g_enable_dif = %d\n",
	       g_block_len, g_strip_size, g_max_io_size, g_max_base_drives, g_max_raids,
	       g_enable_dif);

	return 0;
}

static int
set_test_opts_dif(void)
{

	g_max_base_drives = MAX_BASE_DRIVES;
	g_max_raids = MAX_RAIDS;
	g_block_len = 4096;
	g_strip_size = 64;
	g_max_io_size = 1024;
	g_enable_dif = true;
	g_dif_check_flags_exclude_mask = SPDK_DIF_FLAGS_GUARD_CHECK;

	printf("Test Options\n");
	printf("blocklen = %u, strip_size = %u, max_io_size = %u, g_max_base_drives = %u, "
	       "g_max_raids = %u, g_enable_dif = %d\n",
	       g_block_len, g_strip_size, g_max_io_size, g_max_base_drives, g_max_raids,
	       g_enable_dif);

	return 0;
}

/* Set globals before every test run */
static void
set_globals(void)
{
	uint32_t max_splits;

	g_bdev_io_submit_status = 0;
	if (g_max_io_size < g_strip_size) {
		max_splits = 2;
	} else {
		max_splits = (g_max_io_size / g_strip_size) + 1;
	}
	if (max_splits < g_max_base_drives) {
		max_splits = g_max_base_drives;
	}

	g_io_output = calloc(max_splits, sizeof(struct io_output));
	SPDK_CU_ASSERT_FATAL(g_io_output != NULL);
	g_io_output_index = 0;
	memset(g_get_raids_output, 0, sizeof(g_get_raids_output));
	g_get_raids_count = 0;
	g_io_comp_status = 0;
	g_ignore_io_output = 0;
	g_config_level_create = 0;
	g_rpc_err = 0;
	g_test_multi_raids = 0;
	g_child_io_status_flag = true;
	TAILQ_INIT(&g_bdev_list);
	TAILQ_INIT(&g_io_waitq);
	g_rpc_req = NULL;
	g_rpc_req_size = 0;
	g_json_decode_obj_err = 0;
	g_json_decode_obj_create = 0;
	g_lba_offset = 0;
	g_bdev_io_defer_completion = false;
}

static void
base_bdevs_cleanup(void)
{
	struct spdk_bdev *bdev;
	struct spdk_bdev *bdev_next;

	if (!TAILQ_EMPTY(&g_bdev_list)) {
		TAILQ_FOREACH_SAFE(bdev, &g_bdev_list, internal.link, bdev_next) {
			free(bdev->name);
			TAILQ_REMOVE(&g_bdev_list, bdev, internal.link);
			free(bdev);
		}
	}
}

static void
check_and_remove_raid_bdev(struct raid_bdev *raid_bdev)
{
	struct raid_base_bdev_info *base_info;

	assert(raid_bdev != NULL);
	assert(raid_bdev->base_bdev_info != NULL);

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		if (base_info->desc) {
			raid_bdev_free_base_bdev_resource(base_info);
		}
	}
	assert(raid_bdev->num_base_bdevs_discovered == 0);
	raid_bdev_cleanup_and_free(raid_bdev);
}

/* Reset globals */
static void
reset_globals(void)
{
	if (g_io_output) {
		free(g_io_output);
		g_io_output = NULL;
	}
	g_rpc_req = NULL;
	g_rpc_req_size = 0;
}

void
spdk_bdev_io_get_buf(struct spdk_bdev_io *bdev_io, spdk_bdev_io_get_buf_cb cb,
		     uint64_t len)
{
	cb(bdev_io->internal.ch->channel, bdev_io, true);
}

static void
generate_dif(struct iovec *iovs, int iovcnt, void *md_buf,
	     uint64_t offset_blocks, uint32_t num_blocks, struct spdk_bdev *bdev)
{
	struct spdk_dif_ctx dif_ctx;
	int rc;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	spdk_dif_type_t dif_type;
	bool md_interleaved;
	struct iovec md_iov;

	dif_type = spdk_bdev_get_dif_type(bdev);
	md_interleaved = spdk_bdev_is_md_interleaved(bdev);

	if (dif_type == SPDK_DIF_DISABLE) {
		return;
	}

	dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_16;
	rc = spdk_dif_ctx_init(&dif_ctx,
			       spdk_bdev_get_block_size(bdev),
			       spdk_bdev_get_md_size(bdev),
			       md_interleaved,
			       spdk_bdev_is_dif_head_of_md(bdev),
			       dif_type,
			       bdev->dif_check_flags,
			       offset_blocks,
			       0xFFFF, 0x123, 0, 0, &dif_opts);
	SPDK_CU_ASSERT_FATAL(rc == 0);

	if (!md_interleaved) {
		md_iov.iov_base = md_buf;
		md_iov.iov_len	= spdk_bdev_get_md_size(bdev) * num_blocks;

		rc = spdk_dix_generate(iovs, iovcnt, &md_iov, num_blocks, &dif_ctx);
		SPDK_CU_ASSERT_FATAL(rc == 0);
	}
}

static void
verify_dif(struct iovec *iovs, int iovcnt, void *md_buf,
	   uint64_t offset_blocks, uint32_t num_blocks, struct spdk_bdev *bdev)
{
	struct spdk_dif_ctx dif_ctx;
	int rc;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	struct spdk_dif_error errblk;
	spdk_dif_type_t dif_type;
	bool md_interleaved;
	struct iovec md_iov;

	dif_type = spdk_bdev_get_dif_type(bdev);
	md_interleaved = spdk_bdev_is_md_interleaved(bdev);

	if (dif_type == SPDK_DIF_DISABLE) {
		return;
	}

	dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_16;
	rc = spdk_dif_ctx_init(&dif_ctx,
			       spdk_bdev_get_block_size(bdev),
			       spdk_bdev_get_md_size(bdev),
			       md_interleaved,
			       spdk_bdev_is_dif_head_of_md(bdev),
			       dif_type,
			       bdev->dif_check_flags,
			       offset_blocks,
			       0xFFFF, 0x123, 0, 0, &dif_opts);
	SPDK_CU_ASSERT_FATAL(rc == 0);

	if (!md_interleaved) {
		md_iov.iov_base = md_buf;
		md_iov.iov_len	= spdk_bdev_get_md_size(bdev) * num_blocks;

		rc = spdk_dix_verify(iovs, iovcnt,
				     &md_iov, num_blocks, &dif_ctx, &errblk);
		SPDK_CU_ASSERT_FATAL(rc == 0);
	}
}

/* Store the IO completion status in global variable to verify by various tests */
void
spdk_bdev_io_complete(struct spdk_bdev_io *bdev_io, enum spdk_bdev_io_status status)
{
	g_io_comp_status = ((status == SPDK_BDEV_IO_STATUS_SUCCESS) ? true : false);

	if (g_io_comp_status && bdev_io->type == SPDK_BDEV_IO_TYPE_READ) {
		verify_dif(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.md_buf,
			   bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->bdev);
	}
}

static void
set_io_output(struct io_output *output,
	      struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
	      uint64_t offset_blocks, uint64_t num_blocks,
	      spdk_bdev_io_completion_cb cb, void *cb_arg,
	      enum spdk_bdev_io_type iotype, struct iovec *iovs,
	      int iovcnt, void *md, uint32_t dif_check_flags_exclude_mask)
{
	output->desc = desc;
	output->ch = ch;
	output->offset_blocks = offset_blocks;
	output->num_blocks = num_blocks;
	output->cb = cb;
	output->cb_arg = cb_arg;
	output->iotype = iotype;
	output->iovs = iovs;
	output->iovcnt = iovcnt;
	output->md_buf = md;
	output->dif_check_flags_exclude_mask = dif_check_flags_exclude_mask;
}

static void
child_io_complete(struct spdk_bdev_io *child_io, spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	if (g_bdev_io_defer_completion) {
		child_io->internal.cb = cb;
		child_io->internal.caller_ctx = cb_arg;
		TAILQ_INSERT_TAIL(&g_deferred_ios, child_io, internal.link);
	} else {
		cb(child_io, g_child_io_status_flag, cb_arg);
	}
}

static void
complete_deferred_ios(void)
{
	struct spdk_bdev_io *child_io, *tmp;

	TAILQ_FOREACH_SAFE(child_io, &g_deferred_ios, internal.link, tmp) {
		TAILQ_REMOVE(&g_deferred_ios, child_io, internal.link);
		child_io->internal.cb(child_io, g_child_io_status_flag, child_io->internal.caller_ctx);
	}
}

/* It will cache the split IOs for verification */
int
spdk_bdev_writev_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			struct iovec *iov, int iovcnt,
			uint64_t offset_blocks, uint64_t num_blocks,
			spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	return spdk_bdev_writev_blocks_ext(desc, ch, iov, iovcnt, offset_blocks,
					   num_blocks, cb, cb_arg, NULL);
}

int
spdk_bdev_writev_blocks_ext(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			    struct iovec *iov, int iovcnt,
			    uint64_t offset_blocks, uint64_t num_blocks,
			    spdk_bdev_io_completion_cb cb, void *cb_arg,
			    struct spdk_bdev_ext_io_opts *opts)
{
	struct io_output *output = &g_io_output[g_io_output_index];
	struct spdk_bdev_io *child_io;

	if (g_ignore_io_output) {
		return 0;
	}

	if (g_max_io_size < g_strip_size) {
		SPDK_CU_ASSERT_FATAL(g_io_output_index < 2);
	} else {
		SPDK_CU_ASSERT_FATAL(g_io_output_index < (g_max_io_size / g_strip_size) + 1);
	}
	if (g_bdev_io_submit_status == 0) {
		set_io_output(output, desc, ch, offset_blocks, num_blocks, cb, cb_arg,
			      SPDK_BDEV_IO_TYPE_WRITE, iov, iovcnt, opts->metadata,
			      opts->dif_check_flags_exclude_mask);
		g_io_output_index++;

		child_io = calloc(1, sizeof(struct spdk_bdev_io));
		SPDK_CU_ASSERT_FATAL(child_io != NULL);
		child_io_complete(child_io, cb, cb_arg);
	}

	return g_bdev_io_submit_status;
}

int
spdk_bdev_writev_blocks_with_md(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
				struct iovec *iov, int iovcnt, void *md,
				uint64_t offset_blocks, uint64_t num_blocks,
				spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct spdk_bdev_ext_io_opts opts = {
		.metadata = md
	};

	return spdk_bdev_writev_blocks_ext(desc, ch, iov, iovcnt, offset_blocks,
					   num_blocks, cb, cb_arg, &opts);
}

int
spdk_bdev_reset(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct io_output *output = &g_io_output[g_io_output_index];
	struct spdk_bdev_io *child_io;

	if (g_ignore_io_output) {
		return 0;
	}

	if (g_bdev_io_submit_status == 0) {
		set_io_output(output, desc, ch, 0, 0, cb, cb_arg, SPDK_BDEV_IO_TYPE_RESET,
			      NULL, 0, NULL, 0);
		g_io_output_index++;

		child_io = calloc(1, sizeof(struct spdk_bdev_io));
		SPDK_CU_ASSERT_FATAL(child_io != NULL);
		child_io_complete(child_io, cb, cb_arg);
	}

	return g_bdev_io_submit_status;
}

int
spdk_bdev_unmap_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		       uint64_t offset_blocks, uint64_t num_blocks,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct io_output *output = &g_io_output[g_io_output_index];
	struct spdk_bdev_io *child_io;

	if (g_ignore_io_output) {
		return 0;
	}

	if (g_bdev_io_submit_status == 0) {
		set_io_output(output, desc, ch, offset_blocks, num_blocks, cb, cb_arg,
			      SPDK_BDEV_IO_TYPE_UNMAP, NULL, 0, NULL, 0);
		g_io_output_index++;

		child_io = calloc(1, sizeof(struct spdk_bdev_io));
		SPDK_CU_ASSERT_FATAL(child_io != NULL);
		child_io_complete(child_io, cb, cb_arg);
	}

	return g_bdev_io_submit_status;
}

void
spdk_bdev_destruct_done(struct spdk_bdev *bdev, int bdeverrno)
{
	CU_ASSERT(bdeverrno == 0);
	SPDK_CU_ASSERT_FATAL(bdev->internal.unregister_cb != NULL);
	bdev->internal.unregister_cb(bdev->internal.unregister_ctx, bdeverrno);
}

int
spdk_bdev_register(struct spdk_bdev *bdev)
{
	TAILQ_INSERT_TAIL(&g_bdev_list, bdev, internal.link);
	return 0;
}

void
spdk_bdev_unregister(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	int ret;

	SPDK_CU_ASSERT_FATAL(spdk_bdev_get_by_name(bdev->name) == bdev);
	TAILQ_REMOVE(&g_bdev_list, bdev, internal.link);

	bdev->internal.unregister_cb = cb_fn;
	bdev->internal.unregister_ctx = cb_arg;

	ret = bdev->fn_table->destruct(bdev->ctxt);
	CU_ASSERT(ret == 1);

	poll_threads();
}

int
spdk_bdev_open_ext(const char *bdev_name, bool write, spdk_bdev_event_cb_t event_cb,
		   void *event_ctx, struct spdk_bdev_desc **_desc)
{
	struct spdk_bdev *bdev;

	bdev = spdk_bdev_get_by_name(bdev_name);
	if (bdev == NULL) {
		return -ENODEV;
	}

	*_desc = (void *)bdev;
	return 0;
}

struct spdk_bdev *
spdk_bdev_desc_get_bdev(struct spdk_bdev_desc *desc)
{
	return (void *)desc;
}

int
spdk_json_write_named_uint32(struct spdk_json_write_ctx *w, const char *name, uint32_t val)
{
	if (!g_test_multi_raids) {
		struct rpc_bdev_raid_create *req = g_rpc_req;
		if (strcmp(name, "strip_size_kb") == 0) {
			CU_ASSERT(req->strip_size_kb == val);
		} else if (strcmp(name, "blocklen_shift") == 0) {
			CU_ASSERT(spdk_u32log2(g_block_len) == val);
		} else if (strcmp(name, "num_base_bdevs") == 0) {
			CU_ASSERT(req->base_bdevs.num_base_bdevs == val);
		} else if (strcmp(name, "state") == 0) {
			CU_ASSERT(val == RAID_BDEV_STATE_ONLINE);
		} else if (strcmp(name, "destruct_called") == 0) {
			CU_ASSERT(val == 0);
		} else if (strcmp(name, "num_base_bdevs_discovered") == 0) {
			CU_ASSERT(req->base_bdevs.num_base_bdevs == val);
		}
	}
	return 0;
}

int
spdk_json_write_named_string(struct spdk_json_write_ctx *w, const char *name, const char *val)
{
	if (g_test_multi_raids) {
		if (strcmp(name, "name") == 0) {
			g_get_raids_output[g_get_raids_count] = strdup(val);
			SPDK_CU_ASSERT_FATAL(g_get_raids_output[g_get_raids_count] != NULL);
			g_get_raids_count++;
		}
	} else {
		struct rpc_bdev_raid_create *req = g_rpc_req;
		if (strcmp(name, "raid_level") == 0) {
			CU_ASSERT(strcmp(val, raid_bdev_level_to_str(req->level)) == 0);
		}
	}
	return 0;
}

int
spdk_json_write_named_bool(struct spdk_json_write_ctx *w, const char *name, bool val)
{
	if (!g_test_multi_raids) {
		struct rpc_bdev_raid_create *req = g_rpc_req;
		if (strcmp(name, "superblock") == 0) {
			CU_ASSERT(val == req->superblock_enabled);
		}
	}
	return 0;
}

void
spdk_bdev_free_io(struct spdk_bdev_io *bdev_io)
{
	if (bdev_io) {
		free(bdev_io);
	}
}

/* It will cache split IOs for verification */
int
spdk_bdev_readv_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		       struct iovec *iov, int iovcnt,
		       uint64_t offset_blocks, uint64_t num_blocks,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	return spdk_bdev_readv_blocks_ext(desc, ch, iov, iovcnt, offset_blocks,
					  num_blocks, cb, cb_arg, NULL);
}

int
spdk_bdev_readv_blocks_ext(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			   struct iovec *iov, int iovcnt,
			   uint64_t offset_blocks, uint64_t num_blocks,
			   spdk_bdev_io_completion_cb cb, void *cb_arg,
			   struct spdk_bdev_ext_io_opts *opts)
{
	struct io_output *output = &g_io_output[g_io_output_index];
	struct spdk_bdev_io *child_io;

	if (g_ignore_io_output) {
		return 0;
	}

	SPDK_CU_ASSERT_FATAL(g_io_output_index <= (g_max_io_size / g_strip_size) + 1);
	if (g_bdev_io_submit_status == 0) {
		set_io_output(output, desc, ch, offset_blocks, num_blocks, cb, cb_arg,
			      SPDK_BDEV_IO_TYPE_READ, iov, iovcnt, opts->metadata,
			      opts->dif_check_flags_exclude_mask);
		generate_dif(iov, iovcnt, opts->metadata, offset_blocks, num_blocks,
			     spdk_bdev_desc_get_bdev(desc));
		g_io_output_index++;

		child_io = calloc(1, sizeof(struct spdk_bdev_io));
		SPDK_CU_ASSERT_FATAL(child_io != NULL);
		child_io_complete(child_io, cb, cb_arg);
	}

	return g_bdev_io_submit_status;
}

int
spdk_bdev_readv_blocks_with_md(struct spdk_bdev_desc *desc,	struct spdk_io_channel *ch,
			       struct iovec *iov, int iovcnt, void *md,
			       uint64_t offset_blocks, uint64_t num_blocks,
			       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct spdk_bdev_ext_io_opts opts = {
		.metadata = md
	};

	return spdk_bdev_readv_blocks_ext(desc, ch, iov, iovcnt, offset_blocks,
					  num_blocks, cb, cb_arg, &opts);
}


void
spdk_bdev_module_release_bdev(struct spdk_bdev *bdev)
{
	CU_ASSERT(bdev->internal.claim_type == SPDK_BDEV_CLAIM_EXCL_WRITE);
	CU_ASSERT(bdev->internal.claim.v1.module != NULL);
	bdev->internal.claim_type = SPDK_BDEV_CLAIM_NONE;
	bdev->internal.claim.v1.module = NULL;
}

int
spdk_bdev_module_claim_bdev(struct spdk_bdev *bdev, struct spdk_bdev_desc *desc,
			    struct spdk_bdev_module *module)
{
	if (bdev->internal.claim_type != SPDK_BDEV_CLAIM_NONE) {
		CU_ASSERT(bdev->internal.claim.v1.module != NULL);
		return -1;
	}
	CU_ASSERT(bdev->internal.claim.v1.module == NULL);
	bdev->internal.claim_type = SPDK_BDEV_CLAIM_EXCL_WRITE;
	bdev->internal.claim.v1.module = module;
	return 0;
}

int
spdk_json_decode_object(const struct spdk_json_val *values,
			const struct spdk_json_object_decoder *decoders, size_t num_decoders,
			void *out)
{
	struct rpc_bdev_raid_create *req, *_out;
	size_t i;

	if (g_json_decode_obj_err) {
		return -1;
	} else if (g_json_decode_obj_create) {
		req = g_rpc_req;
		_out = out;

		_out->name = strdup(req->name);
		SPDK_CU_ASSERT_FATAL(_out->name != NULL);
		_out->strip_size_kb = req->strip_size_kb;
		_out->level = req->level;
		_out->superblock_enabled = req->superblock_enabled;
		_out->base_bdevs.num_base_bdevs = req->base_bdevs.num_base_bdevs;
		for (i = 0; i < req->base_bdevs.num_base_bdevs; i++) {
			_out->base_bdevs.base_bdevs[i] = strdup(req->base_bdevs.base_bdevs[i]);
			SPDK_CU_ASSERT_FATAL(_out->base_bdevs.base_bdevs[i]);
		}
	} else {
		memcpy(out, g_rpc_req, g_rpc_req_size);
	}

	return 0;
}

struct spdk_json_write_ctx *
spdk_jsonrpc_begin_result(struct spdk_jsonrpc_request *request)
{
	return (void *)1;
}

void
spdk_jsonrpc_send_error_response(struct spdk_jsonrpc_request *request,
				 int error_code, const char *msg)
{
	g_rpc_err = 1;
}

void
spdk_jsonrpc_send_error_response_fmt(struct spdk_jsonrpc_request *request,
				     int error_code, const char *fmt, ...)
{
	g_rpc_err = 1;
}

struct spdk_bdev *
spdk_bdev_get_by_name(const char *bdev_name)
{
	struct spdk_bdev *bdev;

	if (!TAILQ_EMPTY(&g_bdev_list)) {
		TAILQ_FOREACH(bdev, &g_bdev_list, internal.link) {
			if (strcmp(bdev_name, bdev->name) == 0) {
				return bdev;
			}
		}
	}

	return NULL;
}

int
spdk_bdev_quiesce(struct spdk_bdev *bdev, struct spdk_bdev_module *module,
		  spdk_bdev_quiesce_cb cb_fn, void *cb_arg)
{
	if (cb_fn) {
		cb_fn(cb_arg, 0);
	}

	return 0;
}

int
spdk_bdev_unquiesce(struct spdk_bdev *bdev, struct spdk_bdev_module *module,
		    spdk_bdev_quiesce_cb cb_fn, void *cb_arg)
{
	if (cb_fn) {
		cb_fn(cb_arg, 0);
	}

	return 0;
}

int
spdk_bdev_quiesce_range(struct spdk_bdev *bdev, struct spdk_bdev_module *module,
			uint64_t offset, uint64_t length,
			spdk_bdev_quiesce_cb cb_fn, void *cb_arg)
{
	if (cb_fn) {
		cb_fn(cb_arg, 0);
	}

	return 0;
}

int
spdk_bdev_unquiesce_range(struct spdk_bdev *bdev, struct spdk_bdev_module *module,
			  uint64_t offset, uint64_t length,
			  spdk_bdev_quiesce_cb cb_fn, void *cb_arg)
{
	if (cb_fn) {
		cb_fn(cb_arg, 0);
	}

	return 0;
}

static void
bdev_io_cleanup(struct spdk_bdev_io *bdev_io)
{
	if (bdev_io->u.bdev.iovs) {
		int i;

		for (i = 0; i < bdev_io->u.bdev.iovcnt; i++) {
			free(bdev_io->u.bdev.iovs[i].iov_base);
		}
		free(bdev_io->u.bdev.iovs);
	}

	free(bdev_io->u.bdev.md_buf);
	free(bdev_io);
}

static void
_bdev_io_initialize(struct spdk_bdev_io *bdev_io, struct spdk_io_channel *ch,
		    struct spdk_bdev *bdev, uint64_t lba, uint64_t blocks, int16_t iotype,
		    int iovcnt, size_t iov_len)
{
	struct spdk_bdev_channel *channel = spdk_io_channel_get_ctx(ch);
	int i;

	bdev_io->bdev = bdev;
	bdev_io->u.bdev.offset_blocks = lba;
	bdev_io->u.bdev.num_blocks = blocks;
	bdev_io->type = iotype;
	bdev_io->internal.ch = channel;
	bdev_io->u.bdev.iovcnt = iovcnt;
	if (g_enable_dif) {
		bdev_io->u.bdev.dif_check_flags = bdev->dif_check_flags & ~g_dif_check_flags_exclude_mask;
	}

	if (iovcnt == 0) {
		bdev_io->u.bdev.iovs = NULL;
		bdev_io->u.bdev.md_buf = NULL;
		return;
	}

	SPDK_CU_ASSERT_FATAL(iov_len * iovcnt == blocks * g_block_len);

	bdev_io->u.bdev.iovs = calloc(iovcnt, sizeof(struct iovec));
	SPDK_CU_ASSERT_FATAL(bdev_io->u.bdev.iovs != NULL);

	for (i = 0; i < iovcnt; i++) {
		struct iovec *iov = &bdev_io->u.bdev.iovs[i];

		iov->iov_base = calloc(1, iov_len);
		SPDK_CU_ASSERT_FATAL(iov->iov_base != NULL);
		iov->iov_len = iov_len;
	}

	if (spdk_bdev_get_dif_type(bdev) != SPDK_DIF_DISABLE && !spdk_bdev_is_md_interleaved(bdev)) {
		bdev_io->u.bdev.md_buf = calloc(1, blocks * spdk_bdev_get_md_size(bdev));
	}
}

static void
bdev_io_initialize(struct spdk_bdev_io *bdev_io, struct spdk_io_channel *ch, struct spdk_bdev *bdev,
		   uint64_t lba, uint64_t blocks, int16_t iotype)
{
	int iovcnt;
	size_t iov_len;

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_UNMAP || bdev_io->type == SPDK_BDEV_IO_TYPE_FLUSH) {
		iovcnt = 0;
		iov_len = 0;
	} else {
		iovcnt = 1;
		iov_len = blocks * g_block_len;
	}

	_bdev_io_initialize(bdev_io, ch, bdev, lba, blocks, iotype, iovcnt, iov_len);
}

static void
verify_reset_io(struct spdk_bdev_io *bdev_io, uint8_t num_base_drives,
		struct raid_bdev_io_channel *ch_ctx, struct raid_bdev *raid_bdev, uint32_t io_status)
{
	uint8_t index = 0;
	struct io_output *output;

	SPDK_CU_ASSERT_FATAL(raid_bdev != NULL);
	SPDK_CU_ASSERT_FATAL(num_base_drives != 0);
	SPDK_CU_ASSERT_FATAL(io_status != INVALID_IO_SUBMIT);
	SPDK_CU_ASSERT_FATAL(ch_ctx->base_channel != NULL);

	CU_ASSERT(g_io_output_index == num_base_drives);
	for (index = 0; index < g_io_output_index; index++) {
		output = &g_io_output[index];
		CU_ASSERT(ch_ctx->base_channel[index] == output->ch);
		CU_ASSERT(raid_bdev->base_bdev_info[index].desc == output->desc);
		CU_ASSERT(bdev_io->type == output->iotype);
	}
	CU_ASSERT(g_io_comp_status == io_status);
}

static void
verify_io(struct spdk_bdev_io *bdev_io, uint8_t num_base_drives,
	  struct raid_bdev_io_channel *ch_ctx, struct raid_bdev *raid_bdev, uint32_t io_status)
{
	uint32_t strip_shift = spdk_u32log2(g_strip_size);
	uint64_t start_strip = bdev_io->u.bdev.offset_blocks >> strip_shift;
	uint64_t end_strip = (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) >>
			     strip_shift;
	uint32_t splits_reqd = (end_strip - start_strip + 1);
	uint32_t strip;
	uint64_t pd_strip;
	uint8_t pd_idx;
	uint32_t offset_in_strip;
	uint64_t pd_lba;
	uint64_t pd_blocks;
	uint32_t index = 0;
	struct io_output *output;

	if (io_status == INVALID_IO_SUBMIT) {
		CU_ASSERT(g_io_comp_status == false);
		return;
	}
	SPDK_CU_ASSERT_FATAL(raid_bdev != NULL);
	SPDK_CU_ASSERT_FATAL(num_base_drives != 0);

	CU_ASSERT(splits_reqd == g_io_output_index);
	for (strip = start_strip; strip <= end_strip; strip++, index++) {
		pd_strip = strip / num_base_drives;
		pd_idx = strip % num_base_drives;
		if (strip == start_strip) {
			offset_in_strip = bdev_io->u.bdev.offset_blocks & (g_strip_size - 1);
			pd_lba = (pd_strip << strip_shift) + offset_in_strip;
			if (strip == end_strip) {
				pd_blocks = bdev_io->u.bdev.num_blocks;
			} else {
				pd_blocks = g_strip_size - offset_in_strip;
			}
		} else if (strip == end_strip) {
			pd_lba = pd_strip << strip_shift;
			pd_blocks = ((bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) &
				     (g_strip_size - 1)) + 1;
		} else {
			pd_lba = pd_strip << raid_bdev->strip_size_shift;
			pd_blocks = raid_bdev->strip_size;
		}
		output = &g_io_output[index];
		CU_ASSERT(pd_lba == output->offset_blocks);
		CU_ASSERT(pd_blocks == output->num_blocks);
		CU_ASSERT(ch_ctx->base_channel[pd_idx] == output->ch);
		CU_ASSERT(raid_bdev->base_bdev_info[pd_idx].desc == output->desc);
		CU_ASSERT(bdev_io->type == output->iotype);
		if (bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE) {
			verify_dif(output->iovs, output->iovcnt, output->md_buf,
				   output->offset_blocks, output->num_blocks,
				   spdk_bdev_desc_get_bdev(raid_bdev->base_bdev_info[pd_idx].desc));
		}
		if (g_enable_dif) {
			CU_ASSERT(output->dif_check_flags_exclude_mask == g_dif_check_flags_exclude_mask);
		}
	}
	CU_ASSERT(g_io_comp_status == io_status);
}

static void
verify_io_without_payload(struct spdk_bdev_io *bdev_io, uint8_t num_base_drives,
			  struct raid_bdev_io_channel *ch_ctx, struct raid_bdev *raid_bdev,
			  uint32_t io_status)
{
	uint32_t strip_shift = spdk_u32log2(g_strip_size);
	uint64_t start_offset_in_strip = bdev_io->u.bdev.offset_blocks % g_strip_size;
	uint64_t end_offset_in_strip = (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) %
				       g_strip_size;
	uint64_t start_strip = bdev_io->u.bdev.offset_blocks >> strip_shift;
	uint64_t end_strip = (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) >>
			     strip_shift;
	uint8_t n_disks_involved;
	uint64_t start_strip_disk_idx;
	uint64_t end_strip_disk_idx;
	uint64_t nblocks_in_start_disk;
	uint64_t offset_in_start_disk;
	uint8_t disk_idx;
	uint64_t base_io_idx;
	uint64_t sum_nblocks = 0;
	struct io_output *output;

	if (io_status == INVALID_IO_SUBMIT) {
		CU_ASSERT(g_io_comp_status == false);
		return;
	}
	SPDK_CU_ASSERT_FATAL(raid_bdev != NULL);
	SPDK_CU_ASSERT_FATAL(num_base_drives != 0);
	SPDK_CU_ASSERT_FATAL(bdev_io->type != SPDK_BDEV_IO_TYPE_READ);
	SPDK_CU_ASSERT_FATAL(bdev_io->type != SPDK_BDEV_IO_TYPE_WRITE);

	n_disks_involved = spdk_min(end_strip - start_strip + 1, num_base_drives);
	CU_ASSERT(n_disks_involved == g_io_output_index);

	start_strip_disk_idx = start_strip % num_base_drives;
	end_strip_disk_idx = end_strip % num_base_drives;

	offset_in_start_disk = g_io_output[0].offset_blocks;
	nblocks_in_start_disk = g_io_output[0].num_blocks;

	for (base_io_idx = 0, disk_idx = start_strip_disk_idx; base_io_idx < n_disks_involved;
	     base_io_idx++, disk_idx++) {
		uint64_t start_offset_in_disk;
		uint64_t end_offset_in_disk;

		output = &g_io_output[base_io_idx];

		/* round disk_idx */
		if (disk_idx >= num_base_drives) {
			disk_idx %= num_base_drives;
		}

		/* start_offset_in_disk aligned in strip check:
		 * The first base io has a same start_offset_in_strip with the whole raid io.
		 * Other base io should have aligned start_offset_in_strip which is 0.
		 */
		start_offset_in_disk = output->offset_blocks;
		if (base_io_idx == 0) {
			CU_ASSERT(start_offset_in_disk % g_strip_size == start_offset_in_strip);
		} else {
			CU_ASSERT(start_offset_in_disk % g_strip_size == 0);
		}

		/* end_offset_in_disk aligned in strip check:
		 * Base io on disk at which end_strip is located, has a same end_offset_in_strip
		 * with the whole raid io.
		 * Other base io should have aligned end_offset_in_strip.
		 */
		end_offset_in_disk = output->offset_blocks + output->num_blocks - 1;
		if (disk_idx == end_strip_disk_idx) {
			CU_ASSERT(end_offset_in_disk % g_strip_size == end_offset_in_strip);
		} else {
			CU_ASSERT(end_offset_in_disk % g_strip_size == g_strip_size - 1);
		}

		/* start_offset_in_disk compared with start_disk.
		 * 1. For disk_idx which is larger than start_strip_disk_idx: Its start_offset_in_disk
		 *    mustn't be larger than the start offset of start_offset_in_disk; And the gap
		 *    must be less than strip size.
		 * 2. For disk_idx which is less than start_strip_disk_idx, Its start_offset_in_disk
		 *    must be larger than the start offset of start_offset_in_disk; And the gap mustn't
		 *    be less than strip size.
		 */
		if (disk_idx > start_strip_disk_idx) {
			CU_ASSERT(start_offset_in_disk <= offset_in_start_disk);
			CU_ASSERT(offset_in_start_disk - start_offset_in_disk < g_strip_size);
		} else if (disk_idx < start_strip_disk_idx) {
			CU_ASSERT(start_offset_in_disk > offset_in_start_disk);
			CU_ASSERT(output->offset_blocks - offset_in_start_disk <= g_strip_size);
		}

		/* nblocks compared with start_disk:
		 * The gap between them must be within a strip size.
		 */
		if (output->num_blocks <= nblocks_in_start_disk) {
			CU_ASSERT(nblocks_in_start_disk - output->num_blocks <= g_strip_size);
		} else {
			CU_ASSERT(output->num_blocks - nblocks_in_start_disk < g_strip_size);
		}

		sum_nblocks += output->num_blocks;

		CU_ASSERT(ch_ctx->base_channel[disk_idx] == output->ch);
		CU_ASSERT(raid_bdev->base_bdev_info[disk_idx].desc == output->desc);
		CU_ASSERT(bdev_io->type == output->iotype);
	}

	/* Sum of each nblocks should be same with raid bdev_io */
	CU_ASSERT(bdev_io->u.bdev.num_blocks == sum_nblocks);

	CU_ASSERT(g_io_comp_status == io_status);
}

static void
verify_raid_bdev_present(const char *name, bool presence)
{
	struct raid_bdev *pbdev;
	bool   pbdev_found;

	pbdev_found = false;
	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, name) == 0) {
			pbdev_found = true;
			break;
		}
	}
	if (presence == true) {
		CU_ASSERT(pbdev_found == true);
	} else {
		CU_ASSERT(pbdev_found == false);
	}
}

static void
verify_raid_bdev(struct rpc_bdev_raid_create *r, bool presence, uint32_t raid_state)
{
	struct raid_bdev *pbdev;
	struct raid_base_bdev_info *base_info;
	struct spdk_bdev *bdev = NULL;
	bool   pbdev_found;
	uint64_t min_blockcnt = 0xFFFFFFFFFFFFFFFF;

	pbdev_found = false;
	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, r->name) == 0) {
			pbdev_found = true;
			if (presence == false) {
				break;
			}
			CU_ASSERT(pbdev->base_bdev_info != NULL);
			CU_ASSERT(pbdev->strip_size == ((r->strip_size_kb * 1024) / g_block_len));
			CU_ASSERT(pbdev->strip_size_shift == spdk_u32log2(((r->strip_size_kb * 1024) /
					g_block_len)));
			CU_ASSERT(pbdev->blocklen_shift == spdk_u32log2(g_block_len));
			CU_ASSERT((uint32_t)pbdev->state == raid_state);
			CU_ASSERT(pbdev->num_base_bdevs == r->base_bdevs.num_base_bdevs);
			CU_ASSERT(pbdev->num_base_bdevs_discovered == r->base_bdevs.num_base_bdevs);
			CU_ASSERT(pbdev->level == r->level);
			CU_ASSERT(pbdev->base_bdev_info != NULL);
			RAID_FOR_EACH_BASE_BDEV(pbdev, base_info) {
				CU_ASSERT(base_info->desc != NULL);
				bdev = spdk_bdev_desc_get_bdev(base_info->desc);
				CU_ASSERT(bdev != NULL);
				CU_ASSERT(base_info->remove_scheduled == false);
				CU_ASSERT((pbdev->sb != NULL && base_info->data_offset != 0) ||
					  (pbdev->sb == NULL && base_info->data_offset == 0));
				CU_ASSERT(base_info->data_offset + base_info->data_size == bdev->blockcnt);

				if (bdev && base_info->data_size < min_blockcnt) {
					min_blockcnt = base_info->data_size;
				}
			}
			CU_ASSERT((((min_blockcnt / (r->strip_size_kb * 1024 / g_block_len)) *
				    (r->strip_size_kb * 1024 / g_block_len)) *
				   r->base_bdevs.num_base_bdevs) == pbdev->bdev.blockcnt);
			CU_ASSERT(strcmp(pbdev->bdev.product_name, "Raid Volume") == 0);
			CU_ASSERT(pbdev->bdev.write_cache == 0);
			CU_ASSERT(pbdev->bdev.blocklen == g_block_len);
			if (pbdev->num_base_bdevs > 1) {
				CU_ASSERT(pbdev->bdev.optimal_io_boundary == pbdev->strip_size);
				CU_ASSERT(pbdev->bdev.split_on_optimal_io_boundary == true);
			} else {
				CU_ASSERT(pbdev->bdev.optimal_io_boundary == 0);
				CU_ASSERT(pbdev->bdev.split_on_optimal_io_boundary == false);
			}
			CU_ASSERT(pbdev->bdev.ctxt == pbdev);
			CU_ASSERT(pbdev->bdev.fn_table == &g_raid_bdev_fn_table);
			CU_ASSERT(pbdev->bdev.module == &g_raid_if);
			break;
		}
	}
	if (presence == true) {
		CU_ASSERT(pbdev_found == true);
	} else {
		CU_ASSERT(pbdev_found == false);
	}
}

static void
verify_get_raids(struct rpc_bdev_raid_create *construct_req,
		 uint8_t g_max_raids,
		 char **g_get_raids_output, uint32_t g_get_raids_count)
{
	uint8_t i, j;
	bool found;

	CU_ASSERT(g_max_raids == g_get_raids_count);
	if (g_max_raids == g_get_raids_count) {
		for (i = 0; i < g_max_raids; i++) {
			found = false;
			for (j = 0; j < g_max_raids; j++) {
				if (construct_req[i].name &&
				    strcmp(construct_req[i].name, g_get_raids_output[i]) == 0) {
					found = true;
					break;
				}
			}
			CU_ASSERT(found == true);
		}
	}
}

static void
create_base_bdevs(uint32_t bbdev_start_idx)
{
	uint8_t i;
	struct spdk_bdev *base_bdev;
	char name[16];

	for (i = 0; i < g_max_base_drives; i++, bbdev_start_idx++) {
		snprintf(name, 16, "%s%u%s", "Nvme", bbdev_start_idx, "n1");
		base_bdev = calloc(1, sizeof(struct spdk_bdev));
		SPDK_CU_ASSERT_FATAL(base_bdev != NULL);
		base_bdev->name = strdup(name);
		spdk_uuid_generate(&base_bdev->uuid);
		SPDK_CU_ASSERT_FATAL(base_bdev->name != NULL);
		base_bdev->blocklen = g_block_len;
		base_bdev->blockcnt = BLOCK_CNT;
		if (g_enable_dif) {
			base_bdev->md_interleave = false;
			base_bdev->md_len = MD_SIZE;
			base_bdev->dif_check_flags =
				SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_REFTAG_CHECK |
				SPDK_DIF_FLAGS_APPTAG_CHECK;
			base_bdev->dif_type = SPDK_DIF_TYPE1;
		}
		TAILQ_INSERT_TAIL(&g_bdev_list, base_bdev, internal.link);
	}
}

static void
create_test_req(struct rpc_bdev_raid_create *r, const char *raid_name,
		uint8_t bbdev_start_idx, bool create_base_bdev, bool superblock_enabled)
{
	uint8_t i;
	char name[16];
	uint8_t bbdev_idx = bbdev_start_idx;

	r->name = strdup(raid_name);
	SPDK_CU_ASSERT_FATAL(r->name != NULL);
	r->strip_size_kb = (g_strip_size * g_block_len) / 1024;
	r->level = RAID0;
	r->superblock_enabled = superblock_enabled;
	r->base_bdevs.num_base_bdevs = g_max_base_drives;
	for (i = 0; i < g_max_base_drives; i++, bbdev_idx++) {
		snprintf(name, 16, "%s%u%s", "Nvme", bbdev_idx, "n1");
		r->base_bdevs.base_bdevs[i] = strdup(name);
		SPDK_CU_ASSERT_FATAL(r->base_bdevs.base_bdevs[i] != NULL);
	}
	if (create_base_bdev == true) {
		create_base_bdevs(bbdev_start_idx);
	}
	g_rpc_req = r;
	g_rpc_req_size = sizeof(*r);
}

static void
create_raid_bdev_create_req(struct rpc_bdev_raid_create *r, const char *raid_name,
			    uint8_t bbdev_start_idx, bool create_base_bdev,
			    uint8_t json_decode_obj_err, bool superblock_enabled)
{
	create_test_req(r, raid_name, bbdev_start_idx, create_base_bdev, superblock_enabled);

	g_rpc_err = 0;
	g_json_decode_obj_create = 1;
	g_json_decode_obj_err = json_decode_obj_err;
	g_config_level_create = 0;
	g_test_multi_raids = 0;
}

static void
free_test_req(struct rpc_bdev_raid_create *r)
{
	uint8_t i;

	free(r->name);
	for (i = 0; i < r->base_bdevs.num_base_bdevs; i++) {
		free(r->base_bdevs.base_bdevs[i]);
	}
}

static void
create_raid_bdev_delete_req(struct rpc_bdev_raid_delete *r, const char *raid_name,
			    uint8_t json_decode_obj_err)
{
	r->name = strdup(raid_name);
	SPDK_CU_ASSERT_FATAL(r->name != NULL);

	g_rpc_req = r;
	g_rpc_req_size = sizeof(*r);
	g_rpc_err = 0;
	g_json_decode_obj_create = 0;
	g_json_decode_obj_err = json_decode_obj_err;
	g_config_level_create = 0;
	g_test_multi_raids = 0;
}

static void
create_get_raids_req(struct rpc_bdev_raid_get_bdevs *r, const char *category,
		     uint8_t json_decode_obj_err)
{
	r->category = strdup(category);
	SPDK_CU_ASSERT_FATAL(r->category != NULL);

	g_rpc_req = r;
	g_rpc_req_size = sizeof(*r);
	g_rpc_err = 0;
	g_json_decode_obj_create = 0;
	g_json_decode_obj_err = json_decode_obj_err;
	g_config_level_create = 0;
	g_test_multi_raids = 1;
	g_get_raids_count = 0;
}

static void
test_create_raid(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete delete_req;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	free_test_req(&req);

	create_raid_bdev_delete_req(&delete_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_delete_raid(void)
{
	struct rpc_bdev_raid_create construct_req;
	struct rpc_bdev_raid_delete delete_req;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&construct_req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&construct_req, true, RAID_BDEV_STATE_ONLINE);
	free_test_req(&construct_req);

	create_raid_bdev_delete_req(&delete_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_create_raid_invalid_args(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *raid_bdev;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	req.level = INVALID_RAID_LEVEL;
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free_test_req(&req);
	verify_raid_bdev_present("raid1", false);

	create_raid_bdev_create_req(&req, "raid1", 0, false, 1, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free_test_req(&req);
	verify_raid_bdev_present("raid1", false);

	create_raid_bdev_create_req(&req, "raid1", 0, false, 0, false);
	req.strip_size_kb = 1231;
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free_test_req(&req);
	verify_raid_bdev_present("raid1", false);

	create_raid_bdev_create_req(&req, "raid1", 0, false, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	free_test_req(&req);

	create_raid_bdev_create_req(&req, "raid1", 0, false, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free_test_req(&req);

	create_raid_bdev_create_req(&req, "raid2", 0, false, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free_test_req(&req);
	verify_raid_bdev_present("raid2", false);

	create_raid_bdev_create_req(&req, "raid2", g_max_base_drives, true, 0, false);
	free(req.base_bdevs.base_bdevs[g_max_base_drives - 1]);
	req.base_bdevs.base_bdevs[g_max_base_drives - 1] = strdup("Nvme0n1");
	SPDK_CU_ASSERT_FATAL(req.base_bdevs.base_bdevs[g_max_base_drives - 1] != NULL);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free_test_req(&req);
	verify_raid_bdev_present("raid2", false);

	create_raid_bdev_create_req(&req, "raid2", g_max_base_drives, true, 0, false);
	free(req.base_bdevs.base_bdevs[g_max_base_drives - 1]);
	req.base_bdevs.base_bdevs[g_max_base_drives - 1] = strdup("Nvme100000n1");
	SPDK_CU_ASSERT_FATAL(req.base_bdevs.base_bdevs[g_max_base_drives - 1] != NULL);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	free_test_req(&req);
	verify_raid_bdev_present("raid2", true);
	raid_bdev = raid_bdev_find_by_name("raid2");
	SPDK_CU_ASSERT_FATAL(raid_bdev != NULL);
	check_and_remove_raid_bdev(raid_bdev);

	create_raid_bdev_create_req(&req, "raid2", g_max_base_drives, false, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	free_test_req(&req);
	verify_raid_bdev_present("raid2", true);
	verify_raid_bdev_present("raid1", true);

	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	create_raid_bdev_delete_req(&destroy_req, "raid2", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_delete_raid_invalid_args(void)
{
	struct rpc_bdev_raid_create construct_req;
	struct rpc_bdev_raid_delete destroy_req;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&construct_req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&construct_req, true, RAID_BDEV_STATE_ONLINE);
	free_test_req(&construct_req);

	create_raid_bdev_delete_req(&destroy_req, "raid2", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);

	create_raid_bdev_delete_req(&destroy_req, "raid1", 1);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free(destroy_req.name);
	verify_raid_bdev_present("raid1", true);

	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_io_channel(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_io_channel *ch;
	struct raid_bdev_io_channel *ch_ctx;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	verify_raid_bdev_present("raid1", false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);

	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	ch = spdk_get_io_channel(pbdev);
	SPDK_CU_ASSERT_FATAL(ch != NULL);

	ch_ctx = spdk_io_channel_get_ctx(ch);
	SPDK_CU_ASSERT_FATAL(ch_ctx != NULL);

	free_test_req(&req);

	spdk_put_io_channel(ch);

	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_write_io(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_io_channel *ch;
	struct raid_bdev_io_channel *ch_ctx;
	uint8_t i;
	struct spdk_bdev_io *bdev_io;
	uint64_t io_len;
	uint64_t lba = 0;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	verify_raid_bdev_present("raid1", false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	ch = spdk_get_io_channel(pbdev);
	SPDK_CU_ASSERT_FATAL(ch != NULL);

	ch_ctx = spdk_io_channel_get_ctx(ch);
	SPDK_CU_ASSERT_FATAL(ch_ctx != NULL);

	/* test 2 IO sizes based on global strip size set earlier */
	for (i = 0; i < 2; i++) {
		bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
		SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
		io_len = (g_strip_size / 2) << i;
		bdev_io_initialize(bdev_io, ch, &pbdev->bdev, lba, io_len, SPDK_BDEV_IO_TYPE_WRITE);
		lba += g_strip_size;
		memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
		g_io_output_index = 0;
		generate_dif(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.md_buf,
			     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->bdev);
		raid_bdev_submit_request(ch, bdev_io);
		verify_io(bdev_io, req.base_bdevs.num_base_bdevs, ch_ctx, pbdev,
			  g_child_io_status_flag);
		bdev_io_cleanup(bdev_io);
	}

	free_test_req(&req);
	spdk_put_io_channel(ch);
	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_read_io(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_io_channel *ch;
	struct raid_bdev_io_channel *ch_ctx;
	uint8_t i;
	struct spdk_bdev_io *bdev_io;
	uint64_t io_len;
	uint64_t lba;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	ch = spdk_get_io_channel(pbdev);
	SPDK_CU_ASSERT_FATAL(ch != NULL);

	ch_ctx = spdk_io_channel_get_ctx(ch);
	SPDK_CU_ASSERT_FATAL(ch_ctx != NULL);

	/* test 2 IO sizes based on global strip size set earlier */
	lba = 0;
	for (i = 0; i < 2; i++) {
		bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
		SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
		io_len = (g_strip_size / 2) << i;
		bdev_io_initialize(bdev_io, ch, &pbdev->bdev, lba, io_len, SPDK_BDEV_IO_TYPE_READ);
		lba += g_strip_size;
		memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
		g_io_output_index = 0;
		raid_bdev_submit_request(ch, bdev_io);
		verify_io(bdev_io, req.base_bdevs.num_base_bdevs, ch_ctx, pbdev,
			  g_child_io_status_flag);
		bdev_io_cleanup(bdev_io);
	}

	free_test_req(&req);
	spdk_put_io_channel(ch);
	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
raid_bdev_io_generate_by_strips(uint64_t n_strips)
{
	uint64_t lba;
	uint64_t nblocks;
	uint64_t start_offset;
	uint64_t end_offset;
	uint64_t offsets_in_strip[3];
	uint64_t start_bdev_idx;
	uint64_t start_bdev_offset;
	uint64_t start_bdev_idxs[3];
	int i, j, l;

	/* 3 different situations of offset in strip */
	offsets_in_strip[0] = 0;
	offsets_in_strip[1] = g_strip_size >> 1;
	offsets_in_strip[2] = g_strip_size - 1;

	/* 3 different situations of start_bdev_idx */
	start_bdev_idxs[0] = 0;
	start_bdev_idxs[1] = g_max_base_drives >> 1;
	start_bdev_idxs[2] = g_max_base_drives - 1;

	/* consider different offset in strip */
	for (i = 0; i < 3; i++) {
		start_offset = offsets_in_strip[i];
		for (j = 0; j < 3; j++) {
			end_offset = offsets_in_strip[j];
			if (n_strips == 1 && start_offset > end_offset) {
				continue;
			}

			/* consider at which base_bdev lba is started. */
			for (l = 0; l < 3; l++) {
				start_bdev_idx = start_bdev_idxs[l];
				start_bdev_offset = start_bdev_idx * g_strip_size;
				lba = g_lba_offset + start_bdev_offset + start_offset;
				nblocks = (n_strips - 1) * g_strip_size + end_offset - start_offset + 1;

				g_io_ranges[g_io_range_idx].lba = lba;
				g_io_ranges[g_io_range_idx].nblocks = nblocks;

				SPDK_CU_ASSERT_FATAL(g_io_range_idx < MAX_TEST_IO_RANGE);
				g_io_range_idx++;
			}
		}
	}
}

static void
raid_bdev_io_generate(void)
{
	uint64_t n_strips;
	uint64_t n_strips_span = g_max_base_drives;
	uint64_t n_strips_times[5] = {g_max_base_drives + 1, g_max_base_drives * 2 - 1,
				      g_max_base_drives * 2, g_max_base_drives * 3,
				      g_max_base_drives * 4
				     };
	uint32_t i;

	g_io_range_idx = 0;

	/* consider different number of strips from 1 to strips spanned base bdevs,
	 * and even to times of strips spanned base bdevs
	 */
	for (n_strips = 1; n_strips < n_strips_span; n_strips++) {
		raid_bdev_io_generate_by_strips(n_strips);
	}

	for (i = 0; i < SPDK_COUNTOF(n_strips_times); i++) {
		n_strips = n_strips_times[i];
		raid_bdev_io_generate_by_strips(n_strips);
	}
}

static void
test_unmap_io(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_io_channel *ch;
	struct raid_bdev_io_channel *ch_ctx;
	struct spdk_bdev_io *bdev_io;
	uint32_t count;
	uint64_t io_len;
	uint64_t lba;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	ch = spdk_get_io_channel(pbdev);
	SPDK_CU_ASSERT_FATAL(ch != NULL);

	ch_ctx = spdk_io_channel_get_ctx(ch);
	SPDK_CU_ASSERT_FATAL(ch_ctx != NULL);

	CU_ASSERT(raid_bdev_io_type_supported(pbdev, SPDK_BDEV_IO_TYPE_UNMAP) == true);
	CU_ASSERT(raid_bdev_io_type_supported(pbdev, SPDK_BDEV_IO_TYPE_FLUSH) == true);

	raid_bdev_io_generate();
	for (count = 0; count < g_io_range_idx; count++) {
		bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
		SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
		io_len = g_io_ranges[count].nblocks;
		lba = g_io_ranges[count].lba;
		bdev_io_initialize(bdev_io, ch, &pbdev->bdev, lba, io_len, SPDK_BDEV_IO_TYPE_UNMAP);
		memset(g_io_output, 0, g_max_base_drives * sizeof(struct io_output));
		g_io_output_index = 0;
		raid_bdev_submit_request(ch, bdev_io);
		verify_io_without_payload(bdev_io, req.base_bdevs.num_base_bdevs, ch_ctx, pbdev,
					  g_child_io_status_flag);
		bdev_io_cleanup(bdev_io);
	}

	free_test_req(&req);
	spdk_put_io_channel(ch);
	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

/* Test IO failures */
static void
test_io_failure(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_io_channel *ch;
	struct raid_bdev_io_channel *ch_ctx;
	struct spdk_bdev_io *bdev_io;
	uint32_t count;
	uint64_t io_len;
	uint64_t lba;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, req.name) == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	ch = spdk_get_io_channel(pbdev);
	SPDK_CU_ASSERT_FATAL(ch != NULL);

	ch_ctx = spdk_io_channel_get_ctx(ch);
	SPDK_CU_ASSERT_FATAL(ch_ctx != NULL);

	lba = 0;
	for (count = 0; count < 1; count++) {
		bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
		SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
		io_len = (g_strip_size / 2) << count;
		bdev_io_initialize(bdev_io, ch, &pbdev->bdev, lba, io_len, SPDK_BDEV_IO_TYPE_INVALID);
		lba += g_strip_size;
		memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
		g_io_output_index = 0;
		raid_bdev_submit_request(ch, bdev_io);
		verify_io(bdev_io, req.base_bdevs.num_base_bdevs, ch_ctx, pbdev,
			  INVALID_IO_SUBMIT);
		bdev_io_cleanup(bdev_io);
	}


	lba = 0;
	g_child_io_status_flag = false;
	for (count = 0; count < 1; count++) {
		bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
		SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
		io_len = (g_strip_size / 2) << count;
		bdev_io_initialize(bdev_io, ch, &pbdev->bdev, lba, io_len, SPDK_BDEV_IO_TYPE_WRITE);
		lba += g_strip_size;
		memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
		g_io_output_index = 0;
		generate_dif(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.md_buf,
			     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->bdev);
		raid_bdev_submit_request(ch, bdev_io);
		verify_io(bdev_io, req.base_bdevs.num_base_bdevs, ch_ctx, pbdev,
			  g_child_io_status_flag);
		bdev_io_cleanup(bdev_io);
	}

	free_test_req(&req);
	spdk_put_io_channel(ch);
	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

/* Test reset IO */
static void
test_reset_io(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_io_channel *ch;
	struct raid_bdev_io_channel *ch_ctx;
	struct spdk_bdev_io *bdev_io;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	ch = spdk_get_io_channel(pbdev);
	SPDK_CU_ASSERT_FATAL(ch != NULL);

	ch_ctx = spdk_io_channel_get_ctx(ch);
	SPDK_CU_ASSERT_FATAL(ch_ctx != NULL);

	g_bdev_io_submit_status = 0;
	g_child_io_status_flag = true;

	CU_ASSERT(raid_bdev_io_type_supported(pbdev, SPDK_BDEV_IO_TYPE_RESET) == true);

	bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
	SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
	bdev_io_initialize(bdev_io, ch, &pbdev->bdev, 0, 1, SPDK_BDEV_IO_TYPE_RESET);
	memset(g_io_output, 0, g_max_base_drives * sizeof(struct io_output));
	g_io_output_index = 0;
	raid_bdev_submit_request(ch, bdev_io);
	verify_reset_io(bdev_io, req.base_bdevs.num_base_bdevs, ch_ctx, pbdev,
			true);
	bdev_io_cleanup(bdev_io);

	free_test_req(&req);
	spdk_put_io_channel(ch);
	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

/* Create multiple raids, destroy raids without IO, get_raids related tests */
static void
test_multi_raid_no_io(void)
{
	struct rpc_bdev_raid_create *construct_req;
	struct rpc_bdev_raid_delete destroy_req;
	struct rpc_bdev_raid_get_bdevs get_raids_req;
	uint8_t i;
	char name[16];
	uint8_t bbdev_idx = 0;

	set_globals();
	construct_req = calloc(MAX_RAIDS, sizeof(struct rpc_bdev_raid_create));
	SPDK_CU_ASSERT_FATAL(construct_req != NULL);
	CU_ASSERT(raid_bdev_init() == 0);
	for (i = 0; i < g_max_raids; i++) {
		snprintf(name, 16, "%s%u", "raid", i);
		verify_raid_bdev_present(name, false);
		create_raid_bdev_create_req(&construct_req[i], name, bbdev_idx, true, 0, false);
		bbdev_idx += g_max_base_drives;
		rpc_bdev_raid_create(NULL, NULL);
		CU_ASSERT(g_rpc_err == 0);
		verify_raid_bdev(&construct_req[i], true, RAID_BDEV_STATE_ONLINE);
	}

	create_get_raids_req(&get_raids_req, "all", 0);
	rpc_bdev_raid_get_bdevs(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_get_raids(construct_req, g_max_raids, g_get_raids_output, g_get_raids_count);
	for (i = 0; i < g_get_raids_count; i++) {
		free(g_get_raids_output[i]);
	}

	create_get_raids_req(&get_raids_req, "online", 0);
	rpc_bdev_raid_get_bdevs(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_get_raids(construct_req, g_max_raids, g_get_raids_output, g_get_raids_count);
	for (i = 0; i < g_get_raids_count; i++) {
		free(g_get_raids_output[i]);
	}

	create_get_raids_req(&get_raids_req, "configuring", 0);
	rpc_bdev_raid_get_bdevs(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	CU_ASSERT(g_get_raids_count == 0);

	create_get_raids_req(&get_raids_req, "offline", 0);
	rpc_bdev_raid_get_bdevs(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	CU_ASSERT(g_get_raids_count == 0);

	create_get_raids_req(&get_raids_req, "invalid_category", 0);
	rpc_bdev_raid_get_bdevs(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	CU_ASSERT(g_get_raids_count == 0);

	create_get_raids_req(&get_raids_req, "all", 1);
	rpc_bdev_raid_get_bdevs(NULL, NULL);
	CU_ASSERT(g_rpc_err == 1);
	free(get_raids_req.category);
	CU_ASSERT(g_get_raids_count == 0);

	create_get_raids_req(&get_raids_req, "all", 0);
	rpc_bdev_raid_get_bdevs(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	CU_ASSERT(g_get_raids_count == g_max_raids);
	for (i = 0; i < g_get_raids_count; i++) {
		free(g_get_raids_output[i]);
	}

	for (i = 0; i < g_max_raids; i++) {
		SPDK_CU_ASSERT_FATAL(construct_req[i].name != NULL);
		snprintf(name, 16, "%s", construct_req[i].name);
		create_raid_bdev_delete_req(&destroy_req, name, 0);
		rpc_bdev_raid_delete(NULL, NULL);
		CU_ASSERT(g_rpc_err == 0);
		verify_raid_bdev_present(name, false);
	}
	raid_bdev_exit();
	for (i = 0; i < g_max_raids; i++) {
		free_test_req(&construct_req[i]);
	}
	free(construct_req);
	base_bdevs_cleanup();
	reset_globals();
}

/* Create multiple raids, fire IOs on raids */
static void
test_multi_raid_with_io(void)
{
	struct rpc_bdev_raid_create *construct_req;
	struct rpc_bdev_raid_delete destroy_req;
	uint8_t i;
	char name[16];
	uint8_t bbdev_idx = 0;
	struct raid_bdev *pbdev;
	struct spdk_io_channel **channels;
	struct spdk_bdev_io *bdev_io;
	uint64_t io_len;
	uint64_t lba = 0;
	int16_t iotype;

	set_globals();
	construct_req = calloc(g_max_raids, sizeof(struct rpc_bdev_raid_create));
	SPDK_CU_ASSERT_FATAL(construct_req != NULL);
	CU_ASSERT(raid_bdev_init() == 0);
	channels = calloc(g_max_raids, sizeof(*channels));
	SPDK_CU_ASSERT_FATAL(channels != NULL);

	for (i = 0; i < g_max_raids; i++) {
		snprintf(name, 16, "%s%u", "raid", i);
		verify_raid_bdev_present(name, false);
		create_raid_bdev_create_req(&construct_req[i], name, bbdev_idx, true, 0, false);
		bbdev_idx += g_max_base_drives;
		rpc_bdev_raid_create(NULL, NULL);
		CU_ASSERT(g_rpc_err == 0);
		verify_raid_bdev(&construct_req[i], true, RAID_BDEV_STATE_ONLINE);
		TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
			if (strcmp(pbdev->bdev.name, construct_req[i].name) == 0) {
				break;
			}
		}
		CU_ASSERT(pbdev != NULL);

		channels[i] = spdk_get_io_channel(pbdev);
		SPDK_CU_ASSERT_FATAL(channels[i] != NULL);
	}

	/* This will perform a write on the first raid and a read on the second. It can be
	 * expanded in the future to perform r/w on each raid device in the event that
	 * multiple raid levels are supported.
	 */
	for (i = 0; i < g_max_raids; i++) {
		struct spdk_io_channel *ch = channels[i];
		struct raid_bdev_io_channel *ch_ctx = spdk_io_channel_get_ctx(ch);

		SPDK_CU_ASSERT_FATAL(ch_ctx != NULL);
		bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
		SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
		io_len = g_strip_size;
		iotype = (i) ? SPDK_BDEV_IO_TYPE_WRITE : SPDK_BDEV_IO_TYPE_READ;
		memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
		g_io_output_index = 0;
		TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
			if (strcmp(pbdev->bdev.name, construct_req[i].name) == 0) {
				break;
			}
		}
		bdev_io_initialize(bdev_io, ch, &pbdev->bdev, lba, io_len, iotype);
		CU_ASSERT(pbdev != NULL);
		if (iotype == SPDK_BDEV_IO_TYPE_WRITE) {
			generate_dif(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.md_buf,
				     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->bdev);
		}
		raid_bdev_submit_request(ch, bdev_io);
		verify_io(bdev_io, g_max_base_drives, ch_ctx, pbdev,
			  g_child_io_status_flag);
		bdev_io_cleanup(bdev_io);
	}

	for (i = 0; i < g_max_raids; i++) {
		spdk_put_io_channel(channels[i]);
		snprintf(name, 16, "%s", construct_req[i].name);
		create_raid_bdev_delete_req(&destroy_req, name, 0);
		rpc_bdev_raid_delete(NULL, NULL);
		CU_ASSERT(g_rpc_err == 0);
		verify_raid_bdev_present(name, false);
	}
	raid_bdev_exit();
	for (i = 0; i < g_max_raids; i++) {
		free_test_req(&construct_req[i]);
	}
	free(construct_req);
	free(channels);
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_io_type_supported(void)
{
	CU_ASSERT(raid_bdev_io_type_supported(NULL, SPDK_BDEV_IO_TYPE_READ) == true);
	CU_ASSERT(raid_bdev_io_type_supported(NULL, SPDK_BDEV_IO_TYPE_WRITE) == true);
	CU_ASSERT(raid_bdev_io_type_supported(NULL, SPDK_BDEV_IO_TYPE_INVALID) == false);
}

static void
test_raid_json_dump_info(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);

	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	CU_ASSERT(raid_bdev_dump_info_json(pbdev, NULL) == 0);

	free_test_req(&req);

	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_context_size(void)
{
	CU_ASSERT(raid_bdev_get_ctx_size() == sizeof(struct raid_bdev_io));
}

static void
test_raid_level_conversions(void)
{
	const char *raid_str;

	CU_ASSERT(raid_bdev_str_to_level("abcd123") == INVALID_RAID_LEVEL);
	CU_ASSERT(raid_bdev_str_to_level("0") == RAID0);
	CU_ASSERT(raid_bdev_str_to_level("raid0") == RAID0);
	CU_ASSERT(raid_bdev_str_to_level("RAID0") == RAID0);

	raid_str = raid_bdev_level_to_str(INVALID_RAID_LEVEL);
	CU_ASSERT(raid_str != NULL && strlen(raid_str) == 0);
	raid_str = raid_bdev_level_to_str(1234);
	CU_ASSERT(raid_str != NULL && strlen(raid_str) == 0);
	raid_str = raid_bdev_level_to_str(RAID0);
	CU_ASSERT(raid_str != NULL && strcmp(raid_str, "raid0") == 0);
}

static void
test_create_raid_superblock(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete delete_req;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, true);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	free_test_req(&req);

	create_raid_bdev_delete_req(&delete_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
complete_process_request(void *ctx)
{
	struct raid_bdev_process_request *process_req = ctx;

	raid_bdev_process_request_complete(process_req, 0);
}

static int
submit_process_request(struct raid_bdev_process_request *process_req,
		       struct raid_bdev_io_channel *raid_ch)
{
	struct raid_bdev *raid_bdev = spdk_io_channel_get_io_device(spdk_io_channel_from_ctx(raid_ch));

	*(uint64_t *)raid_bdev->module_private += process_req->num_blocks;

	spdk_thread_send_msg(spdk_get_thread(), complete_process_request, process_req);

	return process_req->num_blocks;
}

static void
test_raid_process(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_bdev *base_bdev;
	struct spdk_thread *process_thread;
	uint64_t num_blocks_processed = 0;

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	verify_raid_bdev_present("raid1", false);
	TAILQ_FOREACH(base_bdev, &g_bdev_list, internal.link) {
		base_bdev->blockcnt = 128;
	}
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);
	free_test_req(&req);

	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);

	pbdev->module->submit_process_request = submit_process_request;
	pbdev->module_private = &num_blocks_processed;

	CU_ASSERT(raid_bdev_start_rebuild(&pbdev->base_bdev_info[0]) == 0);
	poll_threads();

	SPDK_CU_ASSERT_FATAL(pbdev->process != NULL);

	process_thread = spdk_thread_get_by_id(spdk_thread_get_id(spdk_get_thread()) + 1);

	while (spdk_thread_poll(process_thread, 0, 0) > 0) {
		poll_threads();
	}

	CU_ASSERT(pbdev->process == NULL);
	CU_ASSERT(num_blocks_processed == pbdev->bdev.blockcnt);

	poll_threads();

	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static void
test_raid_io_split(void)
{
	struct rpc_bdev_raid_create req;
	struct rpc_bdev_raid_delete destroy_req;
	struct raid_bdev *pbdev;
	struct spdk_io_channel *ch;
	struct raid_bdev_io_channel *raid_ch;
	struct spdk_bdev_io *bdev_io;
	struct raid_bdev_io *raid_io;
	uint64_t split_offset;
	struct iovec iovs_orig[4];
	struct raid_bdev_process process = { };

	set_globals();
	CU_ASSERT(raid_bdev_init() == 0);

	verify_raid_bdev_present("raid1", false);
	create_raid_bdev_create_req(&req, "raid1", 0, true, 0, false);
	rpc_bdev_raid_create(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev(&req, true, RAID_BDEV_STATE_ONLINE);

	TAILQ_FOREACH(pbdev, &g_raid_bdev_list, global_link) {
		if (strcmp(pbdev->bdev.name, "raid1") == 0) {
			break;
		}
	}
	CU_ASSERT(pbdev != NULL);
	pbdev->bdev.md_len = 8;

	process.raid_bdev = pbdev;
	process.target = &pbdev->base_bdev_info[0];
	pbdev->process = &process;
	ch = spdk_get_io_channel(pbdev);
	SPDK_CU_ASSERT_FATAL(ch != NULL);
	raid_ch = spdk_io_channel_get_ctx(ch);
	g_bdev_io_defer_completion = true;

	/* test split of bdev_io with 1 iovec */
	bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
	SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
	raid_io = (struct raid_bdev_io *)bdev_io->driver_ctx;
	bdev_io_initialize(bdev_io, ch, &pbdev->bdev, 0, g_strip_size, SPDK_BDEV_IO_TYPE_WRITE);
	memcpy(iovs_orig, bdev_io->u.bdev.iovs, sizeof(*iovs_orig) * bdev_io->u.bdev.iovcnt);
	memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
	g_io_output_index = 0;

	split_offset = 1;
	raid_ch->process.offset = split_offset;
	generate_dif(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.md_buf,
		     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->bdev);
	raid_bdev_submit_request(ch, bdev_io);
	CU_ASSERT(raid_io->num_blocks == g_strip_size - split_offset);
	CU_ASSERT(raid_io->offset_blocks == split_offset);
	CU_ASSERT(raid_io->iovcnt == 1);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(raid_io->iovs == raid_io->split.iov);
	CU_ASSERT(raid_io->iovs[0].iov_base == iovs_orig->iov_base + split_offset * g_block_len);
	CU_ASSERT(raid_io->iovs[0].iov_len == iovs_orig->iov_len - split_offset * g_block_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf + split_offset * pbdev->bdev.md_len);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == split_offset);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 1);
	CU_ASSERT(raid_io->iovs[0].iov_base == iovs_orig->iov_base);
	CU_ASSERT(raid_io->iovs[0].iov_len == split_offset * g_block_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == g_strip_size);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 1);
	CU_ASSERT(raid_io->iovs[0].iov_base == iovs_orig->iov_base);
	CU_ASSERT(raid_io->iovs[0].iov_len == iovs_orig->iov_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}

	CU_ASSERT(g_io_comp_status == g_child_io_status_flag);
	CU_ASSERT(g_io_output_index == 2);
	CU_ASSERT(g_io_output[0].offset_blocks == split_offset);
	CU_ASSERT(g_io_output[0].num_blocks == g_strip_size - split_offset);
	CU_ASSERT(g_io_output[1].offset_blocks == 0);
	CU_ASSERT(g_io_output[1].num_blocks == split_offset);
	bdev_io_cleanup(bdev_io);

	/* test split of bdev_io with 4 iovecs */
	bdev_io = calloc(1, sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io));
	SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
	raid_io = (struct raid_bdev_io *)bdev_io->driver_ctx;
	_bdev_io_initialize(bdev_io, ch, &pbdev->bdev, 0, g_strip_size, SPDK_BDEV_IO_TYPE_WRITE,
			    4, g_strip_size / 4 * g_block_len);
	memcpy(iovs_orig, bdev_io->u.bdev.iovs, sizeof(*iovs_orig) * bdev_io->u.bdev.iovcnt);
	memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
	g_io_output_index = 0;

	split_offset = 1; /* split at the first iovec */
	raid_ch->process.offset = split_offset;
	generate_dif(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.md_buf,
		     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, bdev_io->bdev);
	raid_bdev_submit_request(ch, bdev_io);
	CU_ASSERT(raid_io->num_blocks == g_strip_size - split_offset);
	CU_ASSERT(raid_io->offset_blocks == split_offset);
	CU_ASSERT(raid_io->iovcnt == 4);
	CU_ASSERT(raid_io->split.iov == &bdev_io->u.bdev.iovs[0]);
	CU_ASSERT(raid_io->iovs == &bdev_io->u.bdev.iovs[0]);
	CU_ASSERT(raid_io->iovs[0].iov_base == iovs_orig[0].iov_base + g_block_len);
	CU_ASSERT(raid_io->iovs[0].iov_len == iovs_orig[0].iov_len -  g_block_len);
	CU_ASSERT(memcmp(raid_io->iovs + 1, iovs_orig + 1, sizeof(*iovs_orig) * 3) == 0);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf + split_offset * pbdev->bdev.md_len);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == split_offset);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 1);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(raid_io->iovs[0].iov_base == iovs_orig[0].iov_base);
	CU_ASSERT(raid_io->iovs[0].iov_len == g_block_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == g_strip_size);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 4);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig, sizeof(*iovs_orig) * raid_io->iovcnt) == 0);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}

	CU_ASSERT(g_io_comp_status == g_child_io_status_flag);
	CU_ASSERT(g_io_output_index == 2);
	CU_ASSERT(g_io_output[0].offset_blocks == split_offset);
	CU_ASSERT(g_io_output[0].num_blocks == g_strip_size - split_offset);
	CU_ASSERT(g_io_output[1].offset_blocks == 0);
	CU_ASSERT(g_io_output[1].num_blocks == split_offset);

	memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
	g_io_output_index = 0;

	split_offset = g_strip_size / 2; /* split exactly between second and third iovec */
	raid_ch->process.offset = split_offset;
	raid_bdev_submit_request(ch, bdev_io);
	CU_ASSERT(raid_io->num_blocks == g_strip_size - split_offset);
	CU_ASSERT(raid_io->offset_blocks == split_offset);
	CU_ASSERT(raid_io->iovcnt == 2);
	CU_ASSERT(raid_io->split.iov == NULL);
	CU_ASSERT(raid_io->iovs == &bdev_io->u.bdev.iovs[2]);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig + 2, sizeof(*iovs_orig) * raid_io->iovcnt) == 0);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf + split_offset * pbdev->bdev.md_len);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == split_offset);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 2);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig, sizeof(*iovs_orig) * raid_io->iovcnt) == 0);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == g_strip_size);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 4);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig, sizeof(*iovs_orig) * raid_io->iovcnt) == 0);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}

	CU_ASSERT(g_io_comp_status == g_child_io_status_flag);
	CU_ASSERT(g_io_output_index == 2);
	CU_ASSERT(g_io_output[0].offset_blocks == split_offset);
	CU_ASSERT(g_io_output[0].num_blocks == g_strip_size - split_offset);
	CU_ASSERT(g_io_output[1].offset_blocks == 0);
	CU_ASSERT(g_io_output[1].num_blocks == split_offset);

	memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
	g_io_output_index = 0;

	split_offset = g_strip_size / 2 + 1; /* split at the third iovec */
	raid_ch->process.offset = split_offset;
	raid_bdev_submit_request(ch, bdev_io);
	CU_ASSERT(raid_io->num_blocks == g_strip_size - split_offset);
	CU_ASSERT(raid_io->offset_blocks == split_offset);
	CU_ASSERT(raid_io->iovcnt == 2);
	CU_ASSERT(raid_io->split.iov == &bdev_io->u.bdev.iovs[2]);
	CU_ASSERT(raid_io->iovs == &bdev_io->u.bdev.iovs[2]);
	CU_ASSERT(raid_io->iovs[0].iov_base == iovs_orig[2].iov_base + g_block_len);
	CU_ASSERT(raid_io->iovs[0].iov_len == iovs_orig[2].iov_len - g_block_len);
	CU_ASSERT(raid_io->iovs[1].iov_base == iovs_orig[3].iov_base);
	CU_ASSERT(raid_io->iovs[1].iov_len == iovs_orig[3].iov_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf + split_offset * pbdev->bdev.md_len);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == split_offset);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 3);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig, sizeof(*iovs_orig) * 2) == 0);
	CU_ASSERT(raid_io->iovs[2].iov_base == iovs_orig[2].iov_base);
	CU_ASSERT(raid_io->iovs[2].iov_len == g_block_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == g_strip_size);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 4);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig, sizeof(*iovs_orig) * raid_io->iovcnt) == 0);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}

	CU_ASSERT(g_io_comp_status == g_child_io_status_flag);
	CU_ASSERT(g_io_output_index == 2);
	CU_ASSERT(g_io_output[0].offset_blocks == split_offset);
	CU_ASSERT(g_io_output[0].num_blocks == g_strip_size - split_offset);
	CU_ASSERT(g_io_output[1].offset_blocks == 0);
	CU_ASSERT(g_io_output[1].num_blocks == split_offset);

	memset(g_io_output, 0, ((g_max_io_size / g_strip_size) + 1) * sizeof(struct io_output));
	g_io_output_index = 0;

	split_offset = g_strip_size - 1; /* split at the last iovec */
	raid_ch->process.offset = split_offset;
	raid_bdev_submit_request(ch, bdev_io);
	CU_ASSERT(raid_io->num_blocks == g_strip_size - split_offset);
	CU_ASSERT(raid_io->offset_blocks == split_offset);
	CU_ASSERT(raid_io->iovcnt == 1);
	CU_ASSERT(raid_io->split.iov == &bdev_io->u.bdev.iovs[3]);
	CU_ASSERT(raid_io->iovs == &bdev_io->u.bdev.iovs[3]);
	CU_ASSERT(raid_io->iovs[0].iov_base == iovs_orig[3].iov_base + iovs_orig[3].iov_len - g_block_len);
	CU_ASSERT(raid_io->iovs[0].iov_len == g_block_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf + split_offset * pbdev->bdev.md_len);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == split_offset);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 4);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig, sizeof(*iovs_orig) * 3) == 0);
	CU_ASSERT(raid_io->iovs[3].iov_base == iovs_orig[3].iov_base);
	CU_ASSERT(raid_io->iovs[3].iov_len == iovs_orig[3].iov_len - g_block_len);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}
	complete_deferred_ios();
	CU_ASSERT(raid_io->num_blocks == g_strip_size);
	CU_ASSERT(raid_io->offset_blocks == 0);
	CU_ASSERT(raid_io->iovcnt == 4);
	CU_ASSERT(raid_io->iovs == bdev_io->u.bdev.iovs);
	CU_ASSERT(memcmp(raid_io->iovs, iovs_orig, sizeof(*iovs_orig) * raid_io->iovcnt) == 0);
	if (spdk_bdev_get_dif_type(&pbdev->bdev) != SPDK_DIF_DISABLE &&
	    !spdk_bdev_is_md_interleaved(&pbdev->bdev)) {
		CU_ASSERT(raid_io->md_buf == bdev_io->u.bdev.md_buf);
	}

	CU_ASSERT(g_io_comp_status == g_child_io_status_flag);
	CU_ASSERT(g_io_output_index == 2);
	CU_ASSERT(g_io_output[0].offset_blocks == split_offset);
	CU_ASSERT(g_io_output[0].num_blocks == g_strip_size - split_offset);
	CU_ASSERT(g_io_output[1].offset_blocks == 0);
	CU_ASSERT(g_io_output[1].num_blocks == split_offset);
	bdev_io_cleanup(bdev_io);

	spdk_put_io_channel(ch);
	free_test_req(&req);
	pbdev->process = NULL;

	create_raid_bdev_delete_req(&destroy_req, "raid1", 0);
	rpc_bdev_raid_delete(NULL, NULL);
	CU_ASSERT(g_rpc_err == 0);
	verify_raid_bdev_present("raid1", false);

	raid_bdev_exit();
	base_bdevs_cleanup();
	reset_globals();
}

static int
test_bdev_ioch_create(void *io_device, void *ctx_buf)
{
	return 0;
}

static void
test_bdev_ioch_destroy(void *io_device, void *ctx_buf)
{
}

int
main(int argc, char **argv)
{
	unsigned int    num_failures;

	CU_TestInfo tests[] = {
		{ "test_create_raid", test_create_raid },
		{ "test_create_raid_superblock", test_create_raid_superblock },
		{ "test_delete_raid", test_delete_raid },
		{ "test_create_raid_invalid_args", test_create_raid_invalid_args },
		{ "test_delete_raid_invalid_args", test_delete_raid_invalid_args },
		{ "test_io_channel", test_io_channel },
		{ "test_reset_io", test_reset_io },
		{ "test_write_io", test_write_io },
		{ "test_read_io", test_read_io },
		{ "test_unmap_io", test_unmap_io },
		{ "test_io_failure", test_io_failure },
		{ "test_multi_raid_no_io", test_multi_raid_no_io },
		{ "test_multi_raid_with_io", test_multi_raid_with_io },
		{ "test_io_type_supported", test_io_type_supported },
		{ "test_raid_json_dump_info", test_raid_json_dump_info },
		{ "test_context_size", test_context_size },
		{ "test_raid_level_conversions", test_raid_level_conversions },
		{ "test_raid_io_split", test_raid_io_split },
		CU_TEST_INFO_NULL,
	};
	/* TODO The RAID process test can only be run once for now, until the fix for getting the
	 * process thread is merged */
	CU_TestInfo tests_single_run[] = {
		{ "test_raid_process", test_raid_process },
		CU_TEST_INFO_NULL,
	};
	CU_SuiteInfo suites[] = {
		{ "raid", set_test_opts, NULL, NULL, NULL, tests },
		{ "raid_dif", set_test_opts_dif, NULL, NULL, NULL, tests },
		{ "raid_single_run", set_test_opts, NULL, NULL, NULL, tests_single_run },
		CU_SUITE_INFO_NULL,
	};

	CU_initialize_registry();
	CU_register_suites(suites);

	allocate_threads(1);
	set_thread(0);
	spdk_io_device_register(&g_bdev_ch_io_device, test_bdev_ioch_create, test_bdev_ioch_destroy, 0,
				NULL);

	num_failures = spdk_ut_run_tests(argc, argv, NULL);
	CU_cleanup_registry();

	spdk_io_device_unregister(&g_bdev_ch_io_device, NULL);
	free_threads();

	return num_failures;
}
