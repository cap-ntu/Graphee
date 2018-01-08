#ifndef INCLUDE_INCLUDE_H_
#define INCLUDE_INCLUDE_H_

#include "../thrift/gen-cpp/VertexUpdate.h"
#include "defines.h"
#include "cnpy.h"
#include "bloom_filter.hpp"
#include <thrift/transport/TServerSocket.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TZlibTransport.h>
#include <boost/make_shared.hpp>
#include <thread>
#include <map>
#include <chrono>
#include <array>
#include <unordered_map>
#include <tuple>
#include <chrono>
#include <random>
#include <future>
#include <exception>
#include <bitset>
#include <atomic>
#include <algorithm>
#include <vector>
#include <iostream>
#include <cmath>
#include <stdlib.h>
#include <omp.h>
#include <sched.h>
#include <string.h>
#include <sys/stat.h>
#include <snappy.h>
#include <zlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <glog/logging.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;
using namespace  ::graphps;

std::array<std::vector<int>, VERTEXCOLNUM> _col_to_ranks;
std::chrono::steady_clock::time_point INIT_TIME_START;
std::chrono::steady_clock::time_point INIT_TIME_END;
std::chrono::steady_clock::time_point COMP_TIME_START;
std::chrono::steady_clock::time_point COMP_TIME_END;
std::chrono::steady_clock::time_point APP_TIME_START;
std::chrono::steady_clock::time_point APP_TIME_END;
std::chrono::steady_clock::time_point ITER_TIME_START;
std::chrono::steady_clock::time_point ITER_TIME_END;

std::string shell_network_traffic 
  = "ifconfig | awk -F: '/TX bytes/{sum += $2+0};END {print sum}' 2>&1";
long network_traffic;
int64_t INIT_TIME;
int64_t COMP_TIME;
int64_t APP_TIME;
int64_t ITER_TIME;
struct EdgeCacheData {
  char * data;
  size_t compressed_length;
  size_t uncompressed_length;
};

int _col_split[10];
std::unordered_map<int32_t, EdgeCacheData> _EdgeCache;
std::atomic<int32_t> _EdgeCache_Size;
std::atomic<int32_t> _EdgeCache_Size_Uncompress;
std::atomic<int32_t> _Computing_Num;
std::atomic<int32_t> _Missed_Num;
std::atomic<size_t> _Network_Compressed;
std::atomic<size_t> _Network_Uncompressed;
std::atomic<size_t> _Changed_Vertex;

std::unordered_map<int, char*> _Edge_Buffer;
std::unordered_map<int, size_t> _Edge_Buffer_Len;
std::unordered_map<int, std::atomic<int>> _Edge_Buffer_Lock;
std::unordered_map<int, char*> _Uncompressed_Buffer;
std::unordered_map<int, size_t> _Uncompressed_Buffer_Len;
std::unordered_map<int, std::atomic<int>> _Uncompressed_Buffer_Lock;
std::unordered_map<int, std::unordered_map<int, void*>> _Socket_Pool;

double BF_THRE = 0.005;
int _server_port = SERVER_PORT;
int16_t _comp_level = COMPRESS_COMMU_LEVEL;
int _my_rank;
int _my_col;
int _my_row;
int _num_workers;
int _hostname_len;
char _hostname[HOST_LEN];
char *_all_hostname;
std::map<int, std::string> _map_hosts;

int DEFAULT_URBUF_SIZE_SP = 5120;
int DEFAULT_CRBUF_SIZE_SP = 5120;
int DEFAULT_UWBUF_SIZE_SP = 5120;
int DEFAULT_CWBUF_SIZE_SP = 5120;

int COMPRESS_CACHE_LEVEL;

#endif
