/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "spdk/stdinc.h"

#include "vbdev_dif.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"
#include "spdk/dif.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"

#define BDEV_DIF_NAMESPACE_UUID "4a2a2239-d323-493b-8664-f6f7713b011d"

static int vbdev_dif_init(void);
static int vbdev_dif_get_ctx_size(void);
static void vbdev_dif_examine(struct spdk_bdev *bdev);
static void vbdev_dif_finish(void);
static int vbdev_dif_config_json(struct spdk_json_write_ctx *w);

static struct spdk_bdev_module dif_if = {
	.name = "dif",
	.module_init = vbdev_dif_init,
	.get_ctx_size = vbdev_dif_get_ctx_size,
	.examine_config = vbdev_dif_examine,
	.module_fini = vbdev_dif_finish,
	.config_json = vbdev_dif_config_json
};

SPDK_BDEV_MODULE_REGISTER(dif, &dif_if)

struct bdev_names {
	char			*vbdev_name;
	char			*bdev_name;
	struct spdk_uuid	uuid;
	enum spdk_dif_type	dif_type;
	enum spdk_dif_pi_format dif_pi_format;
	bool			dif_is_head_of_md;
	bool			check_reftag;
	bool			check_guard;
	TAILQ_ENTRY(bdev_names)	link;
};
static TAILQ_HEAD(, bdev_names) g_bdev_names = TAILQ_HEAD_INITIALIZER(g_bdev_names);

struct vbdev_dif {
	struct spdk_bdev	*base_bdev;
	struct spdk_bdev_desc	*base_desc;
	struct spdk_bdev	dif_bdev;
	TAILQ_ENTRY(vbdev_dif)	link;
	struct spdk_thread	*thread;
};
static TAILQ_HEAD(, vbdev_dif) g_dif_nodes = TAILQ_HEAD_INITIALIZER(g_dif_nodes);

struct dif_io_channel {
	struct spdk_io_channel	*base_ch;
};

struct dif_bdev_io {
	struct spdk_io_channel		*ch;
	struct spdk_bdev_io_wait_entry	bdev_io_wait;
	struct spdk_dif_ctx		dif_ctx;
	struct spdk_bdev_io		*bdev_io;
};

static void vbdev_dif_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);


static void
_device_unregister_cb(void *io_device)
{
	struct vbdev_dif *dif_node = io_device;

	free(dif_node->dif_bdev.name);
	free(dif_node);
}

static void
_vbdev_dif_destruct(void *ctx)
{
	struct spdk_bdev_desc *desc = ctx;

	spdk_bdev_close(desc);
}

static int
vbdev_dif_destruct(void *ctx)
{
	struct vbdev_dif *dif_node = (struct vbdev_dif *)ctx;

	TAILQ_REMOVE(&g_dif_nodes, dif_node, link);

	spdk_bdev_module_release_bdev(dif_node->base_bdev);

	/* Close the underlying bdev on its same opened thread. */
	if (dif_node->thread && dif_node->thread != spdk_get_thread()) {
		spdk_thread_send_msg(dif_node->thread, _vbdev_dif_destruct, dif_node->base_desc);
	} else {
		spdk_bdev_close(dif_node->base_desc);
	}

	spdk_io_device_unregister(dif_node, _device_unregister_cb);

	return 0;
}

static void
_dif_complete_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	struct dif_bdev_io *io_ctx = (struct dif_bdev_io *)orig_io->driver_ctx;
	struct spdk_dif_error err_blk;
	int rc;

	if (!success) {
		spdk_bdev_io_complete(orig_io, status);
		spdk_bdev_free_io(bdev_io);

		return;
	}

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ) {
		rc = spdk_dif_verify(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				     bdev_io->u.bdev.num_blocks, &io_ctx->dif_ctx, &err_blk);
		if (rc != 0) {
			SPDK_ERRLOG("ERROR on DIF verify!\n");
			status = SPDK_BDEV_IO_STATUS_FAILED;
		}
	}

	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static void
_dif_complete_zcopy_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;

	spdk_bdev_io_set_buf(orig_io, bdev_io->u.bdev.iovs[0].iov_base, bdev_io->u.bdev.iovs[0].iov_len);
	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static void
vbdev_dif_resubmit_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct dif_bdev_io *io_ctx = (struct dif_bdev_io *)bdev_io->driver_ctx;

	vbdev_dif_submit_request(io_ctx->ch, bdev_io);
}

static void
vbdev_dif_queue_io(struct spdk_bdev_io *bdev_io)
{
	struct dif_bdev_io *io_ctx = (struct dif_bdev_io *)bdev_io->driver_ctx;
	struct dif_io_channel *dif_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	int rc;

	io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
	io_ctx->bdev_io_wait.cb_fn = vbdev_dif_resubmit_io;
	io_ctx->bdev_io_wait.cb_arg = bdev_io;

	/* Queue the IO using the channel of the base device. */
	rc = spdk_bdev_queue_io_wait(bdev_io->bdev, dif_ch->base_ch, &io_ctx->bdev_io_wait);
	if (rc != 0) {
		SPDK_ERRLOG("Queue io failed in vbdev_dif_queue_io, rc=%d.\n", rc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
dif_init_ext_io_opts(struct spdk_bdev_io *bdev_io, struct spdk_bdev_ext_io_opts *opts)
{
	memset(opts, 0, sizeof(*opts));
	opts->size = sizeof(*opts);
	opts->memory_domain = bdev_io->u.bdev.memory_domain;
	opts->memory_domain_ctx = bdev_io->u.bdev.memory_domain_ctx;
	opts->metadata = bdev_io->u.bdev.md_buf;
}

static void
dif_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct vbdev_dif *dif_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_dif,
				     dif_bdev);
	struct dif_io_channel *dif_ch = spdk_io_channel_get_ctx(ch);
	struct dif_bdev_io *io_ctx = (struct dif_bdev_io *)bdev_io->driver_ctx;
	struct spdk_bdev_ext_io_opts io_opts;
	int rc;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	dif_init_ext_io_opts(bdev_io, &io_opts);

	rc = spdk_bdev_readv_blocks_ext(dif_node->base_desc, dif_ch->base_ch, bdev_io->u.bdev.iovs,
					bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
					bdev_io->u.bdev.num_blocks, _dif_complete_io,
					bdev_io, &io_opts);

	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for dif.\n");
			io_ctx->ch = ch;
			vbdev_dif_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

static void
vbdev_dif_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_dif *dif_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_dif, dif_bdev);
	struct dif_io_channel *dif_ch = spdk_io_channel_get_ctx(ch);
	struct dif_bdev_io *io_ctx = (struct dif_bdev_io *)bdev_io->driver_ctx;
	struct spdk_bdev_ext_io_opts io_opts;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	int rc = 0;

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ ||
	    bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE) {
		dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
		dif_opts.dif_pi_format = dif_node->dif_bdev.dif_pi_format;
		rc = spdk_dif_ctx_init(&io_ctx->dif_ctx,
				       dif_node->dif_bdev.blocklen,
				       dif_node->dif_bdev.md_len,
				       dif_node->dif_bdev.md_interleave,
				       dif_node->dif_bdev.dif_is_head_of_md,
				       dif_node->dif_bdev.dif_type,
				       dif_node->dif_bdev.dif_check_flags,
				       bdev_io->u.bdev.offset_blocks & 0xFFFFFFFF,
				       0, 0, 0, 0, &dif_opts);
		if (rc != 0) {
			SPDK_ERRLOG("ERROR on DIF ctx init!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, dif_read_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE: {
		rc = spdk_dif_generate(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.num_blocks,
				       &io_ctx->dif_ctx);
		if (rc != 0) {
			SPDK_ERRLOG("ERROR on DIF generate!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		dif_init_ext_io_opts(bdev_io, &io_opts);

		rc = spdk_bdev_writev_blocks_ext(dif_node->base_desc, dif_ch->base_ch, bdev_io->u.bdev.iovs,
						 bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
						 bdev_io->u.bdev.num_blocks, _dif_complete_io,
						 bdev_io, &io_opts);
	}
	break;
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		rc = spdk_bdev_write_zeroes_blocks(dif_node->base_desc, dif_ch->base_ch,
						   bdev_io->u.bdev.offset_blocks,
						   bdev_io->u.bdev.num_blocks,
						   _dif_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = spdk_bdev_unmap_blocks(dif_node->base_desc, dif_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _dif_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		rc = spdk_bdev_flush_blocks(dif_node->base_desc, dif_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _dif_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_RESET:
		rc = spdk_bdev_reset(dif_node->base_desc, dif_ch->base_ch,
				     _dif_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ZCOPY:
		rc = spdk_bdev_zcopy_start(dif_node->base_desc, dif_ch->base_ch, NULL, 0,
					   bdev_io->u.bdev.offset_blocks,
					   bdev_io->u.bdev.num_blocks, bdev_io->u.bdev.zcopy.populate,
					   _dif_complete_zcopy_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = spdk_bdev_abort(dif_node->base_desc, dif_ch->base_ch, bdev_io->u.abort.bio_to_abort,
				     _dif_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_COPY:
		rc = spdk_bdev_copy_blocks(dif_node->base_desc, dif_ch->base_ch,
					   bdev_io->u.bdev.offset_blocks,
					   bdev_io->u.bdev.copy.src_offset_blocks,
					   bdev_io->u.bdev.num_blocks,
					   _dif_complete_io, bdev_io);
		break;
	default:
		SPDK_ERRLOG("dif: unknown I/O type %d\n", bdev_io->type);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}
	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for dif.\n");
			io_ctx->ch = ch;
			vbdev_dif_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

static bool
vbdev_dif_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_dif *dif_node = (struct vbdev_dif *)ctx;

	return spdk_bdev_io_type_supported(dif_node->base_bdev, io_type);
}

static struct spdk_io_channel *
vbdev_dif_get_io_channel(void *ctx)
{
	return spdk_get_io_channel((struct vbdev_dif *)ctx);
}

static int
vbdev_dif_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vbdev_dif *dif_node = (struct vbdev_dif *)ctx;

	spdk_json_write_name(w, "dif");
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&dif_node->dif_bdev));
	spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(dif_node->base_bdev));
	spdk_json_write_object_end(w);

	return 0;
}

static int
vbdev_dif_config_json(struct spdk_json_write_ctx *w)
{
	struct vbdev_dif *dif_node;

	TAILQ_FOREACH(dif_node, &g_dif_nodes, link) {
		const struct spdk_uuid *uuid = spdk_bdev_get_uuid(&dif_node->dif_bdev);

		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "bdev_dif_create");
		spdk_json_write_named_object_begin(w, "params");
		spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(dif_node->base_bdev));
		spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&dif_node->dif_bdev));
		if (!spdk_uuid_is_null(uuid)) {
			char uuid_str[SPDK_UUID_STRING_LEN];

			spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), uuid);
			spdk_json_write_named_string(w, "uuid", uuid_str);
		}
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);
	}
	return 0;
}

static int
dif_bdev_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct dif_io_channel *dif_ch = ctx_buf;
	struct vbdev_dif *dif_node = io_device;

	dif_ch->base_ch = spdk_bdev_get_io_channel(dif_node->base_desc);

	return 0;
}

static void
dif_bdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct dif_io_channel *dif_ch = ctx_buf;

	spdk_put_io_channel(dif_ch->base_ch);
}

static int
vbdev_dif_insert_name(const char *bdev_name, const char *vbdev_name,
		      const struct spdk_uuid *uuid, enum spdk_dif_type dif_type,
		      enum spdk_dif_pi_format dif_pi_format, bool dif_is_head_of_md,
		      bool check_reftag, bool check_guard)
{
	struct bdev_names *name;

	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(vbdev_name, name->vbdev_name) == 0) {
			SPDK_ERRLOG("dif bdev %s already exists\n", vbdev_name);
			return -EEXIST;
		}
	}

	name = calloc(1, sizeof(struct bdev_names));
	if (!name) {
		SPDK_ERRLOG("could not allocate bdev_names\n");
		return -ENOMEM;
	}

	name->bdev_name = strdup(bdev_name);
	if (!name->bdev_name) {
		SPDK_ERRLOG("could not allocate name->bdev_name\n");
		free(name);
		return -ENOMEM;
	}

	name->vbdev_name = strdup(vbdev_name);
	if (!name->vbdev_name) {
		SPDK_ERRLOG("could not allocate name->vbdev_name\n");
		free(name->bdev_name);
		free(name);
		return -ENOMEM;
	}

	if (uuid) {
		spdk_uuid_copy(&name->uuid, uuid);
	}

	name->dif_type = dif_type;
	name->dif_is_head_of_md = dif_is_head_of_md;
	name->check_reftag = check_reftag;
	name->check_guard = check_guard;
	name->dif_pi_format = dif_pi_format;

	TAILQ_INSERT_TAIL(&g_bdev_names, name, link);

	return 0;
}

static int
vbdev_dif_init(void)
{
	return 0;
}

static void
vbdev_dif_finish(void)
{
	struct bdev_names *name;

	while ((name = TAILQ_FIRST(&g_bdev_names))) {
		TAILQ_REMOVE(&g_bdev_names, name, link);
		free(name->bdev_name);
		free(name->vbdev_name);
		free(name);
	}
}

static int
vbdev_dif_get_ctx_size(void)
{
	return sizeof(struct dif_bdev_io);
}


static void
vbdev_dif_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	/* No config per bdev needed */
}

static int
vbdev_dif_get_memory_domains(void *ctx, struct spdk_memory_domain **domains, int array_size)
{
	struct vbdev_dif *dif_node = (struct vbdev_dif *)ctx;

	/* dif bdev doesn't work with data buffers, so it supports any memory domain used by base_bdev */
	return spdk_bdev_get_memory_domains(dif_node->base_bdev, domains, array_size);
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_dif_fn_table = {
	.destruct		= vbdev_dif_destruct,
	.submit_request		= vbdev_dif_submit_request,
	.io_type_supported	= vbdev_dif_io_type_supported,
	.get_io_channel		= vbdev_dif_get_io_channel,
	.dump_info_json		= vbdev_dif_dump_info_json,
	.write_config_json	= vbdev_dif_write_config_json,
	.get_memory_domains	= vbdev_dif_get_memory_domains,
};

static void
vbdev_dif_base_bdev_hotremove_cb(struct spdk_bdev *bdev_find)
{
	struct vbdev_dif *dif_node, *tmp;

	TAILQ_FOREACH_SAFE(dif_node, &g_dif_nodes, link, tmp) {
		if (bdev_find == dif_node->base_bdev) {
			spdk_bdev_unregister(&dif_node->dif_bdev, NULL, NULL);
		}
	}
}

/* Called when the underlying base bdev triggers asynchronous event such as bdev removal. */
static void
vbdev_dif_base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
			     void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		vbdev_dif_base_bdev_hotremove_cb(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

static int
vbdev_dif_register(const char *bdev_name)
{
	struct bdev_names *name;
	struct vbdev_dif *dif_node;
	struct spdk_bdev *bdev;
	struct spdk_uuid ns_uuid;
	int rc = 0;

	spdk_uuid_parse(&ns_uuid, BDEV_DIF_NAMESPACE_UUID);

	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(name->bdev_name, bdev_name) != 0) {
			continue;
		}

		SPDK_NOTICELOG("Match on %s\n", bdev_name);
		dif_node = calloc(1, sizeof(struct vbdev_dif));
		if (!dif_node) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate dif_node\n");
			break;
		}

		dif_node->dif_bdev.name = strdup(name->vbdev_name);
		if (!dif_node->dif_bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate dif_bdev name\n");
			free(dif_node);
			break;
		}
		dif_node->dif_bdev.product_name = "dif";
		spdk_uuid_copy(&dif_node->dif_bdev.uuid, &name->uuid);

		/* The base bdev that we're attaching to. */
		rc = spdk_bdev_open_ext(bdev_name, true, vbdev_dif_base_bdev_event_cb,
					NULL, &dif_node->base_desc);
		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
			}
			free(dif_node->dif_bdev.name);
			free(dif_node);
			break;
		}
		SPDK_NOTICELOG("base bdev opened\n");

		bdev = spdk_bdev_desc_get_bdev(dif_node->base_desc);
		dif_node->base_bdev = bdev;

		/* Only DIF is supported (interleaved metadata) */
		if (!bdev->md_interleave) {
			SPDK_ERRLOG("Non-interleaved metadata (DIX) not supported with "
				    "the DIF virtual bdev.\n");
			spdk_bdev_close(dif_node->base_desc);
			free(dif_node->dif_bdev.name);
			free(dif_node);
			break;
		}

		/* Generate UUID based on namespace UUID + base bdev UUID. */
		rc = spdk_uuid_generate_sha1(&dif_node->dif_bdev.uuid, &ns_uuid,
					     (const char *)&dif_node->base_bdev->uuid, sizeof(struct spdk_uuid));
		if (rc) {
			SPDK_ERRLOG("Unable to generate new UUID for dif bdev\n");
			spdk_bdev_close(dif_node->base_desc);
			free(dif_node->dif_bdev.name);
			free(dif_node);
			break;
		}

		/* Copy some properties from the underlying base bdev. */
		dif_node->dif_bdev.write_cache = bdev->write_cache;
		dif_node->dif_bdev.required_alignment = bdev->required_alignment;
		dif_node->dif_bdev.optimal_io_boundary = bdev->optimal_io_boundary;
		dif_node->dif_bdev.blocklen = bdev->blocklen;
		dif_node->dif_bdev.md_interleave = bdev->md_interleave;
		dif_node->dif_bdev.md_len = bdev->md_len;
		dif_node->dif_bdev.dif_type = name->dif_type;
		dif_node->dif_bdev.dif_pi_format = name->dif_pi_format;
		dif_node->dif_bdev.dif_is_head_of_md = name->dif_is_head_of_md;

		if (name->check_reftag) {
			dif_node->dif_bdev.dif_check_flags |= SPDK_DIF_FLAGS_REFTAG_CHECK;
		}

		if (name->check_guard) {
			dif_node->dif_bdev.dif_check_flags |= SPDK_DIF_FLAGS_GUARD_CHECK;
		}

		dif_node->dif_bdev.blockcnt = bdev->blockcnt;
		dif_node->dif_bdev.ctxt = dif_node;
		dif_node->dif_bdev.fn_table = &vbdev_dif_fn_table;
		dif_node->dif_bdev.module = &dif_if;
		TAILQ_INSERT_TAIL(&g_dif_nodes, dif_node, link);

		spdk_io_device_register(dif_node, dif_bdev_ch_create_cb, dif_bdev_ch_destroy_cb,
					sizeof(struct dif_io_channel),
					name->vbdev_name);
		SPDK_NOTICELOG("io_device created at: 0x%p\n", dif_node);

		/* Save the thread where the base device is opened */
		dif_node->thread = spdk_get_thread();

		rc = spdk_bdev_module_claim_bdev(bdev, dif_node->base_desc, dif_node->dif_bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
			spdk_bdev_close(dif_node->base_desc);
			TAILQ_REMOVE(&g_dif_nodes, dif_node, link);
			spdk_io_device_unregister(dif_node, NULL);
			free(dif_node->dif_bdev.name);
			free(dif_node);
			break;
		}
		SPDK_NOTICELOG("bdev claimed\n");

		rc = spdk_bdev_register(&dif_node->dif_bdev);
		if (rc) {
			SPDK_ERRLOG("could not register dif_bdev\n");
			spdk_bdev_module_release_bdev(&dif_node->dif_bdev);
			spdk_bdev_close(dif_node->base_desc);
			TAILQ_REMOVE(&g_dif_nodes, dif_node, link);
			spdk_io_device_unregister(dif_node, NULL);
			free(dif_node->dif_bdev.name);
			free(dif_node);
			break;
		}
		SPDK_NOTICELOG("dif_bdev registered\n");
		SPDK_NOTICELOG("created dif_bdev for: %s\n", name->vbdev_name);
	}

	return rc;
}

int
bdev_dif_create_disk(const char *bdev_name, const char *vbdev_name,
		     const struct spdk_uuid *uuid, enum spdk_dif_type dif_type,
		     enum spdk_dif_pi_format dif_pi_format, bool dif_is_head_of_md,
		     bool check_reftag, bool check_guard)
{
	int rc;

	rc = vbdev_dif_insert_name(bdev_name, vbdev_name, uuid,
				   dif_type, dif_pi_format, dif_is_head_of_md,
				   check_reftag, check_guard);
	if (rc) {
		return rc;
	}

	rc = vbdev_dif_register(bdev_name);
	if (rc == -ENODEV) {
		SPDK_NOTICELOG("vbdev creation deferred pending base bdev arrival\n");
		rc = 0;
	}

	return rc;
}

void
bdev_dif_delete_disk(const char *bdev_name, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	struct bdev_names *name;
	int rc;

	rc = spdk_bdev_unregister_by_name(bdev_name, &dif_if, cb_fn, cb_arg);
	if (rc == 0) {
		TAILQ_FOREACH(name, &g_bdev_names, link) {
			if (strcmp(name->vbdev_name, bdev_name) == 0) {
				TAILQ_REMOVE(&g_bdev_names, name, link);
				free(name->bdev_name);
				free(name->vbdev_name);
				free(name);
				break;
			}
		}
	} else {
		cb_fn(cb_arg, rc);
	}
}

static void
vbdev_dif_examine(struct spdk_bdev *bdev)
{
	vbdev_dif_register(bdev->name);

	spdk_bdev_module_examine_done(&dif_if);
}

SPDK_LOG_REGISTER_COMPONENT(vbdev_dif)
