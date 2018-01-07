CCOMPILE=/opt/mpich/bin/mpic++
PLATFORM=Linux-amd64-64
LIB = -L/usr/local/lib
# LDFLAGS = -lzmq -lsnappy -lz -lpthread -lglog -O3 -m64 --force-addr  -ftree-vectorize -msse4 -ftree-vectorizer-verbose=1 -maccumulate-outgoing-args -fprefetch-loop-arrays -fopenmp 
LDFLAGS = -lzmq -lsnappy -lz -lpthread -lglog -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -ljemalloc -O3 -m64 -ftree-vectorize -msse4 -fopenmp 

all: run

run: *.cpp
	$(CCOMPILE) -std=c++11  pagerank.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o pagerank
	$(CCOMPILE) -std=c++11  sssp.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o sssp
	$(CCOMPILE) -std=c++11  cc.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o cc

clean:
	-rm pagerank
	-rm sssp
	-rm cc
