/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2023 Intel Corporation.
 *   All rights reserved.
 */

#include "spdk_internal/cunit.h"
#include "spdk_internal/mock.h"
#include "spdk_internal/idxd.h"
#include "common/lib/test_env.c"

#include "idxd/idxd.c"

static void
test_idxd_validate_dif_common_params(void)
{
	struct spdk_dif_ctx dif_ctx;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	int rc;

	dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_16;

	/* Check all supported combinations of the block size and metadata size */
	/* ## supported: block-size = 512, metadata = 8 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == 0);

	/* ## supported: block-size = 512, metadata = 16 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 16,
			       16,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == 0);

	/* ## supported: block-size = 4096, metadata = 8 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       4096 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == 0);

	/* ## supported: block-size = 4096, metadata = 16 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       4096 + 16,
			       16,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == 0);

	/* Check byte offset from the start of the whole data buffer */
	/* ## not-supported: data_offset != 0 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 10, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);

	/* Check seed value for guard computation */
	/* ## not-supported: guard_seed != 0 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 10, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);

	/* Check for supported metadata sizes */
	/* ## not-supported: md_size != 8 or md_size != 16 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       4096 + 32,
			       32,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);

	/* Check for supported metadata locations */
	/* ## not-supported: md_interleave == false (separated metadata location) */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       4096,
			       16,
			       false,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);

	/* Check for supported DIF alignments */
	/* ## not-supported: dif_loc == true (DIF left alignment) */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       4096 + 16,
			       16,
			       true,
			       true,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);

	/* Check for supported DIF block sizes */
	/* ## not-supported: block_size (without metadata) != 512,520,4096,4104 */
	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 10,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);

	/* Check for supported DIF PI formats */
	/* ## not-supported: DIF PI format == 32 */
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_32;
	rc = spdk_dif_ctx_init(&dif_ctx,
			       4096 + 16,
			       16,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);

	/* ## not-supported: DIF PI format == 64 */
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_64;
	rc = spdk_dif_ctx_init(&dif_ctx,
			       4096 + 16,
			       16,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);
	rc = idxd_validate_dif_common_params(&dif_ctx);
	CU_ASSERT(rc == -EINVAL);
}

static void
test_idxd_validate_dif_check_params(void)
{
	struct spdk_dif_ctx dif_ctx;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	int rc;

	dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_16;

	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);

	rc = idxd_validate_dif_check_params(&dif_ctx);
	CU_ASSERT(rc == 0);
}

static void
test_idxd_validate_dif_check_buf_align(void)
{
	struct spdk_dif_ctx dif_ctx;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	int rc;

	dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_16;

	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);

	rc = idxd_validate_dif_check_buf_align(&dif_ctx, 4 * (512 + 8));
	CU_ASSERT(rc == 0);

	/* The memory buffer length is not a multiple of block size with metadata */
	rc = idxd_validate_dif_check_buf_align(&dif_ctx, 4 * (512 + 8) + 10);
	CU_ASSERT(rc == -EINVAL);
}

static void
test_idxd_get_dif_flags(void)
{
	struct spdk_dif_ctx dif_ctx;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	uint8_t flags;
	int rc;

	dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_16;

	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);

	rc = idxd_get_dif_flags(&dif_ctx, &flags);
	CU_ASSERT(rc == 0);

	dif_ctx.guard_interval = 100;
	rc = idxd_get_dif_flags(&dif_ctx, &flags);
	CU_ASSERT(rc == -EINVAL);
}

static void
test_idxd_get_source_dif_flags(void)
{
	struct spdk_dif_ctx dif_ctx;
	struct spdk_dif_ctx_init_ext_opts dif_opts;
	uint8_t flags;
	int rc;

	dif_opts.size = SPDK_SIZEOF(&dif_opts, dif_pi_format);
	dif_opts.dif_pi_format = SPDK_DIF_PI_FORMAT_16;

	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_APPTAG_CHECK |
			       SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);

	rc = idxd_get_source_dif_flags(&dif_ctx, &flags);
	CU_ASSERT(rc == 0);

	dif_ctx.dif_type = 0xF;
	rc = idxd_get_source_dif_flags(&dif_ctx, &flags);
	CU_ASSERT(rc == -EINVAL);

	rc = spdk_dif_ctx_init(&dif_ctx,
			       512 + 8,
			       8,
			       true,
			       false,
			       SPDK_DIF_TYPE1,
			       SPDK_DIF_FLAGS_GUARD_CHECK | SPDK_DIF_FLAGS_REFTAG_CHECK,
			       0, 0, 0, 0, 0, &dif_opts);
	CU_ASSERT(rc == 0);

	rc = idxd_get_source_dif_flags(&dif_ctx, &flags);
	CU_ASSERT(rc == -EINVAL);
}

int
main(int argc, char **argv)
{
	CU_pSuite   suite = NULL;
	unsigned int    num_failures;

	CU_initialize_registry();

	suite = CU_add_suite("idxd", NULL, NULL);

	CU_ADD_TEST(suite, test_idxd_validate_dif_common_params);
	CU_ADD_TEST(suite, test_idxd_validate_dif_check_params);
	CU_ADD_TEST(suite, test_idxd_validate_dif_check_buf_align);
	CU_ADD_TEST(suite, test_idxd_get_dif_flags);
	CU_ADD_TEST(suite, test_idxd_get_source_dif_flags);

	num_failures = spdk_ut_run_tests(argc, argv, NULL);
	CU_cleanup_registry();
	return num_failures;
}
