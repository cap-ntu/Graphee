CCOMPILE=/opt/mpich/bin/mpic++
PLATFORM=Linux-amd64-64
LIB = -L/usr/local/lib
LDFLAGS = -lzmq -lsnappy -lz -lpthread -lglog -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -ljemalloc -O3 -m64 -ftree-vectorize -msse4 -fopenmp 
#LDFLAGS = -lzmq -lsnappy -lz -lpthread -lglog -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -ltcmalloc -O3 -m64 -ftree-vectorize -msse4 -fopenmp 

all: run

run: *.cpp
	$(CCOMPILE) -std=c++11  tc.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o tc
	$(CCOMPILE) -std=c++11  pagerank.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o pagerank

clean:
	-rm tc
