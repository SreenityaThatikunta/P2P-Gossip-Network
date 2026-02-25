##############################################################################
# Makefile for gossip-based P2P network
# Targets: seed, peer, clean
##############################################################################

CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pthread -O2
TARGETS  = seed peer

.PHONY: all clean

all: $(TARGETS)

seed: seed.cpp common.h
	$(CXX) $(CXXFLAGS) -o seed seed.cpp

peer: peer.cpp common.h
	$(CXX) $(CXXFLAGS) -o peer peer.cpp

clean:
	rm -f $(TARGETS) *.txt *.log
	touch config.txt
	echo "127.0.0.1:5001" > config.txt
	echo "127.0.0.1:5002" >> config.txt
	echo "127.0.0.1:5003" >> config.txt
