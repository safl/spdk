/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   Copyright (c) 2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *   All rights reserved.
 */

#include "vbdev_dif.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"

/* Structure to hold the parameters for this RPC method. */
struct rpc_bdev_dif_create {
	char *base_bdev_name;
	char *name;
	char *uuid;
	enum spdk_dif_type dif_type;
	enum spdk_dif_pi_format dif_pi_format;
	bool dif_is_head_of_md;
	bool check_reftag;
	bool check_guard;
};

/* Free the allocated memory resource after the RPC handling. */
static void
free_rpc_bdev_dif_create(struct rpc_bdev_dif_create *r)
{
	free(r->base_bdev_name);
	free(r->name);
	free(r->uuid);
}

/* Structure to decode the input parameters for this RPC method. */
static const struct spdk_json_object_decoder rpc_bdev_dif_create_decoders[] = {
	{"base_bdev_name", offsetof(struct rpc_bdev_dif_create, base_bdev_name), spdk_json_decode_string},
	{"name", offsetof(struct rpc_bdev_dif_create, name), spdk_json_decode_string},
	{"uuid", offsetof(struct rpc_bdev_dif_create, uuid), spdk_json_decode_string, true},
	{"dif_type", offsetof(struct rpc_bdev_dif_create, dif_type), spdk_json_decode_int32, true},
	{"dif_pi_format", offsetof(struct rpc_bdev_dif_create, dif_pi_format), spdk_json_decode_int32, true},
	{"dif_is_head_of_md", offsetof(struct rpc_bdev_dif_create, dif_is_head_of_md), spdk_json_decode_bool, true},
	{"check_reftag", offsetof(struct rpc_bdev_dif_create, check_reftag), spdk_json_decode_bool, true},
	{"check_guard", offsetof(struct rpc_bdev_dif_create, check_guard), spdk_json_decode_bool, true}
};

/* Decode the parameters for this RPC method and properly construct the dif
 * device. Error status returned in the failed cases.
 */
static void
rpc_bdev_dif_create(struct spdk_jsonrpc_request *request,
		    const struct spdk_json_val *params)
{
	struct rpc_bdev_dif_create req = {NULL};
	struct spdk_json_write_ctx *w;
	struct spdk_uuid *uuid = NULL;
	struct spdk_uuid decoded_uuid;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_dif_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_dif_create_decoders),
				    &req)) {
		SPDK_DEBUGLOG(vbdev_dif, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (req.uuid) {
		if (spdk_uuid_parse(&decoded_uuid, req.uuid)) {
			spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
							 "Failed to parse bdev UUID");
			goto cleanup;
		}
		uuid = &decoded_uuid;
	}

	rc = bdev_dif_create_disk(req.base_bdev_name, req.name, uuid,
				  req.dif_type, req.dif_pi_format, req.dif_is_head_of_md,
				  req.check_reftag, req.check_guard);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, req.name);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_dif_create(&req);
}
SPDK_RPC_REGISTER("bdev_dif_create", rpc_bdev_dif_create, SPDK_RPC_RUNTIME)

struct rpc_bdev_dif_delete {
	char *name;
};

static void
free_rpc_bdev_dif_delete(struct rpc_bdev_dif_delete *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_dif_delete_decoders[] = {
	{"name", offsetof(struct rpc_bdev_dif_delete, name), spdk_json_decode_string},
};

static void
rpc_bdev_dif_delete_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (bdeverrno == 0) {
		spdk_jsonrpc_send_bool_response(request, true);
	} else {
		spdk_jsonrpc_send_error_response(request, bdeverrno, spdk_strerror(-bdeverrno));
	}
}

static void
rpc_bdev_dif_delete(struct spdk_jsonrpc_request *request,
		    const struct spdk_json_val *params)
{
	struct rpc_bdev_dif_delete req = {NULL};

	if (spdk_json_decode_object(params, rpc_bdev_dif_delete_decoders,
				    SPDK_COUNTOF(rpc_bdev_dif_delete_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev_dif_delete_disk(req.name, rpc_bdev_dif_delete_cb, request);

cleanup:
	free_rpc_bdev_dif_delete(&req);
}
SPDK_RPC_REGISTER("bdev_dif_delete", rpc_bdev_dif_delete, SPDK_RPC_RUNTIME)
