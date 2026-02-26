##############################################################################
# Makefile for gossip-based P2P network
# Targets: seed, peer, clean
##############################################################################

CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pthread -O2
TARGETS  = seed peer

NUM_SEEDS ?= 3
SEED_BASE  = 5000
IP         = 127.0.0.1

.PHONY: all clean

all: $(TARGETS)

seed: seed.cpp common.h
	$(CXX) $(CXXFLAGS) -o seed seed.cpp

peer: peer.cpp common.h
	$(CXX) $(CXXFLAGS) -o peer peer.cpp

clean:
	rm -f $(TARGETS) *_output.txt *.log
	@echo "# config.txt — $(NUM_SEEDS) seeds" > config.txt
	@for i in $$(seq 1 $(NUM_SEEDS)); do \
		echo "$(IP):$$(($(SEED_BASE) + $$i))" >> config.txt; \
	done
	@echo "[clean] config.txt reset with $(NUM_SEEDS) seeds:"
	@cat config.txt