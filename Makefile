# netmirror - build system
#
# Default build is the sanitizer-enabled development build:
#   make                 # all binaries, ASan + UBSan
#   make test            # every unit/integration suite + E2E fire test
#   make demo            # build image + docker compose up (3 nodes + gateway)
#   make MODE=release    # optimized binaries without sanitizers
#
# Build artifacts live under build/<mode>/ and must never be committed.

SHELL := /bin/sh
.DEFAULT_GOAL := all

MODE ?= dev
ifeq ($(filter $(MODE),dev release),)
$(error MODE must be either "dev" or "release")
endif

CC ?= cc
AR ?= ar
RM ?= rm -f

CPPFLAGS += -Iinclude -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE
CFLAGS += -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wformat=2 \
          -Wstrict-prototypes -Wmissing-prototypes -Wvla -pthread

ifeq ($(MODE),dev)
CFLAGS += -O1 -g3 -fsanitize=address,undefined \
          -fno-omit-frame-pointer -fno-sanitize-recover=all
LDFLAGS += -fsanitize=address,undefined
else
CFLAGS += -O2 -g -DNDEBUG -fstack-protector-strong
endif

CFLAGS += $(EXTRA_CFLAGS)
LDFLAGS += $(EXTRA_LDFLAGS)
LDLIBS += -lssl -lcrypto -pthread

BUILD_ROOT ?= build
BUILD_DIR := $(BUILD_ROOT)/$(MODE)
OBJ_DIR := $(BUILD_DIR)/obj
BIN_DIR := $(BUILD_DIR)/bin

# Shared data-plane modules. Cluster is linked into the gateway/CLI now and
# will be linked into the node in the replication stage.
NET_SRCS := src/util.c src/blob.c src/network.c
NET_OBJS := $(NET_SRCS:src/%.c=$(OBJ_DIR)/%.o)
CLUSTER_OBJ := $(OBJ_DIR)/cluster.o

NODE_OBJS := $(OBJ_DIR)/node.o $(NET_OBJS) $(CLUSTER_OBJ)
GATEWAY_OBJS := $(OBJ_DIR)/gateway.o $(NET_OBJS) $(CLUSTER_OBJ)
CLI_OBJS := $(OBJ_DIR)/cli.o $(NET_OBJS) $(CLUSTER_OBJ)

BIN_NODE := $(BIN_DIR)/netmirror-node
BIN_GATEWAY := $(BIN_DIR)/netmirror-gateway
BIN_CLI := $(BIN_DIR)/netmirror-cli
BIN_TEST_BLOB := $(BIN_DIR)/test-blob
BIN_TEST_NETWORK := $(BIN_DIR)/test-network
BIN_TEST_NODE_CLIENT := $(BIN_DIR)/test-node-client
BIN_TEST_CLUSTER_CLIENT := $(BIN_DIR)/test-cluster-client
BINS := $(BIN_NODE) $(BIN_GATEWAY) $(BIN_CLI)

TEST_NETWORK_OBJS := $(OBJ_DIR)/test_network.o $(NET_OBJS)
TEST_NODE_CLIENT_OBJS := $(OBJ_DIR)/test_node_client.o $(NET_OBJS)
TEST_CLUSTER_CLIENT_OBJS := $(OBJ_DIR)/test_cluster_client.o $(NET_OBJS)

ALL_OBJS := $(sort $(NODE_OBJS) $(GATEWAY_OBJS) $(CLI_OBJS) \
                   $(TEST_NETWORK_OBJS) $(TEST_NODE_CLIENT_OBJS) \
                   $(TEST_CLUSTER_CLIENT_OBJS))
DEPS := $(ALL_OBJS:.o=.d)

SANITIZER_ENV := ASAN_OPTIONS=detect_leaks=1:abort_on_error=1:strict_string_checks=1 \
                 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1

.PHONY: all check clean demo dev help node print-config release test \
        test-cluster test-core test-gateway test-net test-network test-node

all: $(BINS)

# Compile-only contract check. Useful before the first complete link and as a
# fast CI smoke test for headers.
check:
	@printf '%s\n' \
		'#include "netmirror.h"' \
		'_Static_assert(NM_ACK_WIRE_SIZE == 34u, "ACK layout changed");' \
		'_Static_assert(NM_SYNC_RESP_HEADER_SIZE == 8u, "SYNC layout changed");' \
		'int main(void) {' \
		'    nm_manifest_t m = {0};' \
		'    nm_endpoint_t e = {0};' \
		'    nm_frame_t f = {0};' \
		'    (void)m; (void)e; (void)f;' \
		'    return nm_quorum_size(3u) == 2u ? 0 : 1;' \
		'}' \
	| $(CC) $(CPPFLAGS) $(CFLAGS) -Werror -x c -fsyntax-only -
	@echo "OK: netmirror.h compiles cleanly (MODE=$(MODE))"

$(OBJ_DIR)/%.o: src/%.c
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -MMD -MP -c $< -o $@

$(OBJ_DIR)/test_%.o: tests/test_%.c include/netmirror.h include/network.h \
                     include/blob.h
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -MMD -MP -c $< -o $@

$(BIN_NODE): $(NODE_OBJS) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_GATEWAY): $(GATEWAY_OBJS) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_CLI): $(CLI_OBJS) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_TEST_BLOB): tests/test_blob.c src/blob.c include/blob.h include/netmirror.h | $(BIN_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) tests/test_blob.c src/blob.c -o $@ \
		$(LDFLAGS) $(LDLIBS)

$(BIN_TEST_NETWORK): $(TEST_NETWORK_OBJS) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_TEST_NODE_CLIENT): $(TEST_NODE_CLIENT_OBJS) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_TEST_CLUSTER_CLIENT): $(TEST_CLUSTER_CLIENT_OBJS) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_DIR):
	@mkdir -p $@

test-core: $(BIN_TEST_BLOB)
	$(SANITIZER_ENV) $(BIN_TEST_BLOB)

test-network: $(BIN_TEST_NETWORK)
	$(SANITIZER_ENV) $(BIN_TEST_NETWORK)

test-node: $(BIN_NODE) $(BIN_TEST_NODE_CLIENT)
	NM_NODE_BIN="$(abspath $(BIN_NODE))" \
	NM_TEST_CLIENT_BIN="$(abspath $(BIN_TEST_NODE_CLIENT))" \
	$(SANITIZER_ENV) bash tests/test_node.sh

test-net: test-network test-node

test-cluster: $(BIN_NODE) $(BIN_TEST_CLUSTER_CLIENT)
	NM_NODE_BIN="$(abspath $(BIN_NODE))" \
	NM_CLUSTER_CLIENT_BIN="$(abspath $(BIN_TEST_CLUSTER_CLIENT))" \
	$(SANITIZER_ENV) bash tests/test_cluster.sh

test-gateway: $(BIN_NODE) $(BIN_GATEWAY)
	NM_NODE_BIN="$(abspath $(BIN_NODE))" \
	NM_GATEWAY_BIN="$(abspath $(BIN_GATEWAY))" \
	$(SANITIZER_ENV) bash tests/test_gateway.sh

node: $(BIN_NODE)

test: all test-core test-network test-node test-cluster test-gateway
	MODE="$(MODE)" $(SANITIZER_ENV) bash tests/test.sh

demo:
	docker compose build
	docker compose up --remove-orphans

dev:
	@$(MAKE) --no-print-directory MODE=dev all

release:
	@$(MAKE) --no-print-directory MODE=release all

clean:
	$(RM) -r -- $(BUILD_ROOT)

print-config:
	@echo "MODE=$(MODE)"
	@echo "CC=$(CC)"
	@echo "CPPFLAGS=$(CPPFLAGS)"
	@echo "CFLAGS=$(CFLAGS)"
	@echo "LDFLAGS=$(LDFLAGS)"
	@echo "LDLIBS=$(LDLIBS)"
	@echo "BINS=$(BINS)"

help:
	@echo "netmirror build targets:"
	@echo "  make / make all       build all binaries (dev: ASan+UBSan)"
	@echo "  make node             build just the replication node"
	@echo "  make check            compile-check the public contract header"
	@echo "  make test-core        run local storage unit tests (ASan)"
	@echo "  make test-network     run framing/parser/socket unit tests"
	@echo "  make test-node        boot a node and run binary-protocol tests"
	@echo "  make test-net         run network unit + node integration tests"
	@echo "  make test-cluster     run 3-node quorum/heartbeat/repair tests"
	@echo "  make test-gateway     run HTTP streaming/keep-alive/fallback tests"
	@echo "  make test             run every test suite + full E2E fire test"
	@echo "  make demo             build image + docker compose up (3 nodes + gateway)"
	@echo "  make MODE=release     optimized build under build/release/"
	@echo "  make clean            remove build artifacts"
	@echo "  make print-config     show effective build settings"

-include $(DEPS)
