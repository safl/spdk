/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024 Intel Corporation. All rights reserved.
 */

#include "spdk/keyring.h"
#include "spdk/keyring_module.h"
#include "spdk/log.h"
#include "spdk/queue.h"
#include "spdk/string.h"

struct spdk_key {
	char				*name;
	int				refcnt;
	bool				removed;
	struct spdk_keyring_module	*module;
	TAILQ_ENTRY(spdk_key)		tailq;
};

struct spdk_keyring {
	pthread_mutex_t				mutex;
	TAILQ_HEAD(, spdk_keyring_module)	modules;
	TAILQ_HEAD(, spdk_key)			keys;
	TAILQ_HEAD(, spdk_key)			removed_keys;
};

static struct spdk_keyring g_keyring = {
	.mutex = PTHREAD_MUTEX_INITIALIZER,
	.keys = TAILQ_HEAD_INITIALIZER(g_keyring.keys),
	.removed_keys = TAILQ_HEAD_INITIALIZER(g_keyring.removed_keys),
	.modules = TAILQ_HEAD_INITIALIZER(g_keyring.modules),
};

static const char *
keyring_get_key_name(const char *name)
{
	const char *keyname;

	/* Both "key0" and ":key0" refer to "key0" in the global keyring */
	keyname = strstr(name, ":");
	if (keyname == NULL) {
		return name;
	}

	return keyname + 1;
}

static struct spdk_key *
keyring_find_key(const char *name)
{
	struct spdk_key *key;

	TAILQ_FOREACH(key, &g_keyring.keys, tailq) {
		if (strcmp(keyring_get_key_name(key->name),
			   keyring_get_key_name(name)) == 0) {
			return key;
		}
	}

	return NULL;
}

static void
keyring_free_key(struct spdk_key *key)
{
	assert(key->refcnt == 0);

	free(key->name);
	free(key);
}

static void
keyring_put_key(struct spdk_key *key)
{
	assert(key->refcnt > 0);
	key->refcnt--;

	if (key->refcnt == 0) {
		assert(key->removed);
		TAILQ_REMOVE(&g_keyring.removed_keys, key, tailq);
		keyring_free_key(key);
	}
}

int
spdk_keyring_add_key(const struct spdk_key_opts *opts)
{
	struct spdk_key *key = NULL;
	struct spdk_keyring_module *module = opts->module;
	const char *keyname;
	int rc = 0;

	/* For now, only global keyring is supported */
	keyname = strstr(opts->name, ":");
	if (keyname != NULL && keyname != opts->name) {
		SPDK_ERRLOG("Couldn't add key '%s' to the keyring: keyring doesn't exist\n",
			    opts->name);
		return -EINVAL;
	}

	pthread_mutex_lock(&g_keyring.mutex);
	if (keyring_find_key(opts->name) != NULL) {
		SPDK_ERRLOG("Key '%s' already exists\n", opts->name);
		rc = -EEXIST;
		goto out;
	}

	key = calloc(1, sizeof(*key) + module->get_ctx_size());
	if (key == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	key->name = strdup(opts->name);
	if (key->name == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	rc = module->add_key(key, opts->ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to add key '%s' to the keyring\n", opts->name);
		goto out;
	}

	key->module = module;
	key->refcnt = 1;
	TAILQ_INSERT_TAIL(&g_keyring.keys, key, tailq);
out:
	pthread_mutex_unlock(&g_keyring.mutex);
	if (rc != 0 && key != NULL) {
		keyring_free_key(key);
	}

	return rc;
}

static void
keyring_remove_key(struct spdk_key *key)
{
	assert(!key->removed);
	key->removed = true;
	key->module->remove_key(key);
	TAILQ_REMOVE(&g_keyring.keys, key, tailq);
	TAILQ_INSERT_TAIL(&g_keyring.removed_keys, key, tailq);
	keyring_put_key(key);
}

void
spdk_keyring_remove_key(const char *name)
{
	struct spdk_key *key;

	pthread_mutex_lock(&g_keyring.mutex);
	key = keyring_find_key(name);
	if (key == NULL) {
		SPDK_WARNLOG("Key '%s' does not exist\n", name);
		goto out;
	}

	keyring_remove_key(key);
out:
	pthread_mutex_unlock(&g_keyring.mutex);
}

struct spdk_key *
spdk_keyring_get_key(const char *name)
{
	struct spdk_key *key;

	pthread_mutex_lock(&g_keyring.mutex);
	key = keyring_find_key(name);
	if (key == NULL) {
		goto out;
	}

	key->refcnt++;
out:
	pthread_mutex_unlock(&g_keyring.mutex);

	return key;
}

void
spdk_keyring_put_key(struct spdk_key *key)
{
	if (key == NULL) {
		return;
	}

	pthread_mutex_lock(&g_keyring.mutex);
	keyring_put_key(key);
	pthread_mutex_unlock(&g_keyring.mutex);
}

const char *
spdk_key_get_name(struct spdk_key *key)
{
	return key->name;
}

int
spdk_key_get_key(struct spdk_key *key, void *buf, int len)
{
	struct spdk_keyring_module *module = key->module;

	if (key->removed) {
		return -ENOKEY;
	}

	return module->get_key(key, buf, len);
}

void *
spdk_key_get_ctx(struct spdk_key *key)
{
	return key + 1;
}

void
spdk_keyring_register_module(struct spdk_keyring_module *module)
{
	TAILQ_INSERT_TAIL(&g_keyring.modules, module, tailq);
}

int
spdk_keyring_init(void)
{
	struct spdk_keyring_module *module, *tmp;
	int rc = 0;

	TAILQ_FOREACH(module, &g_keyring.modules, tailq) {
		if (module->init != NULL) {
			rc = module->init();
			if (rc != 0) {
				SPDK_ERRLOG("Failed to initialize module %s: %s\n", module->name,
					    spdk_strerror(-rc));
				break;
			}
		}

		SPDK_INFOLOG(keyring, "Initialized module %s\n", module->name);
	}

	if (rc != 0) {
		TAILQ_FOREACH(tmp, &g_keyring.modules, tailq) {
			if (tmp == module) {
				break;
			}
			if (tmp->cleanup != NULL) {
				tmp->cleanup();
			}
		}
	}

	return rc;
}

void
spdk_keyring_cleanup(void)
{
	struct spdk_keyring_module *module;
	struct spdk_key *key;

	while (!TAILQ_EMPTY(&g_keyring.keys)) {
		key = TAILQ_FIRST(&g_keyring.keys);
		keyring_remove_key(key);
	}

	while (!TAILQ_EMPTY(&g_keyring.removed_keys)) {
		key = TAILQ_FIRST(&g_keyring.removed_keys);
		SPDK_WARNLOG("Key '%s' still has %d references\n", key->name, key->refcnt);
		key->refcnt = 0;
		TAILQ_REMOVE(&g_keyring.removed_keys, key, tailq);
		keyring_free_key(key);
	}

	TAILQ_FOREACH(module, &g_keyring.modules, tailq) {
		if (module->cleanup != NULL) {
			module->cleanup();
		}
	}
}

SPDK_LOG_REGISTER_COMPONENT(keyring)
