/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2024 Samsung Electronics Co., Ltd.
 *   All rights reserved.
 */

#include "spdk/stdinc.h"
#include "spdk_internal/cunit.h"
#include "util/fd_group.c"

#ifdef __linux__
#include <sys/epoll.h>

static int
fd_group_cb_fn(void *ctx)
{
	return 0;
}

static void
test_fd_group_basic(void)
{
	struct spdk_fd_group *parent, *child;
	int fd1, fd2, fd3;
	int rc;
	int cb_arg;

	fd1 = epoll_create1(0);
	fd2 = epoll_create1(0);
	fd3 = epoll_create1(0);

	rc = spdk_fd_group_create(&parent);
	CU_ASSERT(rc == 0);

	rc = spdk_fd_group_create(&child);
	CU_ASSERT(rc == 0);

	rc = SPDK_FD_GROUP_ADD(parent, fd1, fd_group_cb_fn, &cb_arg);
	CU_ASSERT(rc == 0);
	CU_ASSERT(parent->num_fds == 1);

	rc = SPDK_FD_GROUP_ADD(child, fd2, fd_group_cb_fn, &cb_arg);
	CU_ASSERT(rc == 0);
	CU_ASSERT(child->num_fds == 1);

	/* Nest child fd group to a parent fd group and verify it. */
	rc = spdk_fd_group_nest(parent, child);
	CU_ASSERT(rc == 0);
	CU_ASSERT(parent->num_descendant_fds == 1);
	CU_ASSERT(child->parent == parent);

	/* Add a new fd to child fd group and verify number of descendant fds in parent. */
	rc = SPDK_FD_GROUP_ADD(child, fd3, fd_group_cb_fn, &cb_arg);
	CU_ASSERT(rc == 0);
	CU_ASSERT(child->num_fds == 2);
	CU_ASSERT(parent->num_descendant_fds == 2);

	/* Unnest child fd group from a parent fd group and verify it. */
	rc = spdk_fd_group_unnest(parent, child);
	CU_ASSERT(rc == 0);
	CU_ASSERT(parent->num_descendant_fds == 0);
	CU_ASSERT(child->parent == NULL);

	spdk_fd_group_remove(child, fd2);
	spdk_fd_group_remove(child, fd3);
	CU_ASSERT(child->num_fds == 0);

	spdk_fd_group_remove(parent, fd1);
	CU_ASSERT(parent->num_fds == 0);

	spdk_fd_group_destroy(child);
	spdk_fd_group_destroy(parent);
}

#else

static void
test_fd_group_basic(void)
{
	CU_ASSERT(1);
}

#endif

int
main(int argc, char **argv)
{
	CU_pSuite	suite = NULL;
	unsigned int	num_failures;

	CU_initialize_registry();

	suite = CU_add_suite("fd_group", NULL, NULL);

	CU_ADD_TEST(suite, test_fd_group_basic);

	num_failures = spdk_ut_run_tests(argc, argv, NULL);

	CU_cleanup_registry();

	return num_failures;
}
