#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2022 Intel Corporation.
#  All rights reserved.


def iobuf_set_options(client, small_pool_count, large_pool_count, small_bufsize, large_bufsize,
                      enable_numa=None, numa_policy=None, numa_id=None):
    """Set iobuf pool options.

    Args:
        small_pool_count: number of small buffers in the global pool
        large_pool_count: number of large buffers in the global pool
        small_bufsize: size of a small buffer
        large_bufsize: size of a large buffer
        enable_numa: enable per-NUMA buffer pools
        numa_policy: "source", "destination" or "fixed"
        numa_id: NUMA ID for fixed policy
    """
    params = {}

    if small_pool_count is not None:
        params['small_pool_count'] = small_pool_count
    if large_pool_count is not None:
        params['large_pool_count'] = large_pool_count
    if small_bufsize is not None:
        params['small_bufsize'] = small_bufsize
    if large_bufsize is not None:
        params['large_bufsize'] = large_bufsize
    if enable_numa is not None:
        params['enable_numa'] = enable_numa
    if numa_policy is not None:
        params['numa_policy'] = numa_policy
    if numa_id is not None:
        params['numa_id'] = numa_id

    return client.call('iobuf_set_options', params)


def iobuf_get_stats(client):
    """Get iobuf statistics"""

    return client.call('iobuf_get_stats')
