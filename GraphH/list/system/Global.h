/*
 * Global.h
 *
 *  Created on: 25 Feb 2017
 *      Author: sunshine
 */

#ifndef SYSTEM_GLOBAL_H_
#define SYSTEM_GLOBAL_H_

#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <glog/logging.h>
#include <zmq.h>
#include <snappy.h>
#include <zlib.h>
#include <omp.h>
#include <sched.h>
#include <iostream>
#include <cmath>
#include <thread>
#include <vector>
#include <map>
#include <unordered_map>
#include <tuple>
#include <chrono>
#include <future>
#include <exception>
#include <atomic>
#include <deque>
#include <algorithm>
#include <dirent.h>
#include <sys/stat.h>
#include "cnpy.h"
#include "bloom_filter.hpp"

#define MASTER_RANK 0
#define HOST_LEN 20
#define ZMQ_PREFIX "tcp://*:"
#define ZMQ_PORT 15555
#define ZMQ_BUFFER 200*1024*1024
#define GPS_INF 10000
#define EDGE_CACHE_SIZE 50*1024 //MB
#define DENSITY_VALUE 20
//#define USE_HDFS
#define ZMQNUM 23
#define CMPNUM 23

int COMPRESS_NETWORK_LEVEL;  //0, 1, 2
int COMPRESS_CACHE_LEVEL; //0, 1, 2, 3
int  _my_rank;
int  _num_workers;
int  _hostname_len;
char _hostname[HOST_LEN];
char *_all_hostname;
void *_zmq_context;
std::chrono::steady_clock::time_point INIT_TIME_START;
std::chrono::steady_clock::time_point INIT_TIME_END;
std::chrono::steady_clock::time_point COMP_TIME_START;
std::chrono::steady_clock::time_point COMP_TIME_END;
std::chrono::steady_clock::time_point APP_TIME_START;
std::chrono::steady_clock::time_point APP_TIME_END;
std::chrono::steady_clock::time_point HDFS_TIME_START;
std::chrono::steady_clock::time_point HDFS_TIME_END;
int64_t INIT_TIME;
int64_t COMP_TIME;
int64_t APP_TIME;
int64_t HDFS_TIME;
struct EdgeCacheData {
  char * data;
  int32_t compressed_length;
  int32_t uncompressed_length;
};
std::unordered_map<int32_t, EdgeCacheData> _EdgeCache;
std::atomic<int32_t> _EdgeCache_Size;
std::atomic<int32_t> _EdgeCache_Size_Uncompress;
std::atomic<int32_t> _Computing_Num;
std::atomic<int32_t> _Missed_Num;
std::atomic<int32_t> _Changed_Vertex;
std::atomic<long> _Network_Compressed;
std::atomic<long> _Network_Uncompressed;
std::atomic<int32_t> _Pending_Request;
std::unordered_map<int, char*> _Send_Buffer;
std::unordered_map<int, size_t> _Send_Buffer_Len;
std::unordered_map<int, std::atomic<int>> _Send_Buffer_Lock;
std::unordered_map<int, char*> _Edge_Buffer;
std::unordered_map<int, size_t> _Edge_Buffer_Len;
std::unordered_map<int, std::atomic<int>> _Edge_Buffer_Lock;
std::unordered_map<int, char*> _Uncompressed_Buffer;
std::unordered_map<int, size_t> _Uncompressed_Buffer_Len;
std::unordered_map<int, std::atomic<int>> _Uncompressed_Buffer_Lock;
std::unordered_map<int, std::unordered_map<int, void*>> _Socket_Pool;

size_t GetDataSize(std::string dir_name) {
  DIR *d;
  struct dirent *de;
  struct stat buf;
  int exists;
  size_t total_size;
  d = opendir(dir_name.c_str());
  if (d == NULL) {
    assert(0 == 1);
  }
  total_size = 0;
  for (de = readdir(d); de != NULL; de = readdir(d)) {
    exists = stat((dir_name+de->d_name).c_str(), &buf);
    if (exists < 0) {
      fprintf(stderr, "Couldn't stat %s\n", de->d_name);
      assert(0 == 1);
    } else {
      total_size += buf.st_size;
    }
  }
  closedir(d);
  return total_size;
}

char *load_edge(int32_t p_id, std::string &DataPath) {
  int omp_id = omp_get_thread_num();
  assert(_Edge_Buffer_Lock[omp_id] == 0);
  _Edge_Buffer_Lock[omp_id]++;
  if (_EdgeCache.find(p_id) != _EdgeCache.end()) {
    char* uncompressed = NULL;
    if (COMPRESS_CACHE_LEVEL == 1) {
      int required_len = _EdgeCache[p_id].uncompressed_length;
      if (_Edge_Buffer_Len[omp_id] < required_len) {
        if (_Edge_Buffer_Len[omp_id] > 0) {delete [] (_Edge_Buffer[omp_id]);}
        _Edge_Buffer[omp_id] = new char[int(required_len*1.1)];
        _Edge_Buffer_Len[omp_id] = int(required_len*1.1);
      }
      uncompressed = _Edge_Buffer[omp_id];
      assert (snappy::RawUncompress(_EdgeCache[p_id].data, _EdgeCache[p_id].compressed_length, uncompressed) == true);
    } else if (COMPRESS_CACHE_LEVEL == 2 || COMPRESS_CACHE_LEVEL == 3) {
      size_t uncompressed_length = _EdgeCache[p_id].uncompressed_length;
      if (_Edge_Buffer_Len[omp_id] < uncompressed_length) {
        if (_Edge_Buffer_Len[omp_id] > 0) {delete [] (_Edge_Buffer[omp_id]);}
        _Edge_Buffer[omp_id] = new char[int(uncompressed_length*1.1)];
        _Edge_Buffer_Len[omp_id] = int(uncompressed_length*1.1);
      }
      uncompressed = _Edge_Buffer[omp_id];
      int uncompress_result = 0;
      uncompress_result = uncompress((Bytef *)uncompressed,
                                    &uncompressed_length,
                                    (Bytef *)_EdgeCache[p_id].data,
                                    _EdgeCache[p_id].compressed_length);
      assert (uncompress_result == Z_OK);
    } else if (COMPRESS_CACHE_LEVEL == 0){
      uncompressed = _EdgeCache[p_id].data;
    } else {
      assert(1 == 0);
    }
    _Edge_Buffer_Lock[omp_id]--;
    return uncompressed;
  }
  // Cannot finf target data in cache
  _Missed_Num++;
  cnpy::NpyArray npz = cnpy::npy_load_to_buffer(DataPath, &_Edge_Buffer[omp_id], std::ref(_Edge_Buffer_Len), omp_id);
  std::srand(std::time(0));
  if (_EdgeCache_Size < EDGE_CACHE_SIZE && _EdgeCache.find(p_id) == _EdgeCache.end()) {
    EdgeCacheData newdata;
    char* compressed_data_tmp = NULL;
    char* compressed_data = NULL;
    size_t compressed_length = 0;
    int32_t max_compressed_len = 0;

    if (COMPRESS_CACHE_LEVEL == 1) {
      max_compressed_len = snappy::MaxCompressedLength(sizeof(int32_t)*npz.shape);
      if (_Uncompressed_Buffer_Len[omp_id] < max_compressed_len) {
        if (_Uncompressed_Buffer_Len[omp_id] > 0) {delete [] (_Uncompressed_Buffer[omp_id]);}
        _Uncompressed_Buffer[omp_id] = new char[int(max_compressed_len*1.1)];
        _Uncompressed_Buffer_Len[omp_id] = int(max_compressed_len*1.1);
      }
      compressed_data_tmp = _Uncompressed_Buffer[omp_id];
      snappy::RawCompress(npz.data,
                         sizeof(int32_t)*npz.shape,
                         compressed_data_tmp,
                         &compressed_length);
    } else if (COMPRESS_CACHE_LEVEL == 2) {
      size_t buf_size = compressBound(sizeof(int32_t)*npz.shape);
      compressed_length = buf_size;
      max_compressed_len = buf_size;
      if (_Uncompressed_Buffer_Len[omp_id] < max_compressed_len) {
        if (_Uncompressed_Buffer_Len[omp_id] > 0) {delete [] (_Uncompressed_Buffer[omp_id]);}
        _Uncompressed_Buffer[omp_id] = new char[int(max_compressed_len*1.1)];
        _Uncompressed_Buffer_Len[omp_id] = int(max_compressed_len*1.1);
      }
      compressed_data_tmp = _Uncompressed_Buffer[omp_id];
      int compress_result = 0;
      compress_result = compress2((Bytef *)compressed_data_tmp,
                                &compressed_length,
                                (Bytef *)npz.data,
                                sizeof(int32_t)*npz.shape,
                                1);
      assert(compress_result == Z_OK);
    } else if (COMPRESS_CACHE_LEVEL == 3) {
      size_t buf_size = compressBound(sizeof(int32_t)*npz.shape);
      compressed_length = buf_size;
      max_compressed_len = buf_size;
      if (_Uncompressed_Buffer_Len[omp_id] < max_compressed_len) {
        if (_Uncompressed_Buffer_Len[omp_id] > 0) {delete [] (_Uncompressed_Buffer[omp_id]);}
        _Uncompressed_Buffer[omp_id] = new char[int(max_compressed_len*1.1)];
        _Uncompressed_Buffer_Len[omp_id] = int(max_compressed_len*1.1);
      }
      compressed_data_tmp = _Uncompressed_Buffer[omp_id];
      int compress_result = 0;
      compress_result = compress2((Bytef *)compressed_data_tmp,
                                &compressed_length,
                                (Bytef *)npz.data,
                                sizeof(int32_t)*npz.shape,
                                3);
      assert(compress_result == Z_OK);
    } else if (COMPRESS_CACHE_LEVEL == 0) {
      newdata.data = new char[sizeof(int32_t)*npz.shape];
      memcpy(newdata.data, npz.data, sizeof(int32_t)*npz.shape);
      newdata.uncompressed_length = sizeof(int32_t)*npz.shape;
      newdata.compressed_length = sizeof(int32_t)*npz.shape;
    } else {
      assert (1 == 0);
    }

    if (COMPRESS_CACHE_LEVEL > 0) {
      compressed_data = new char[compressed_length];
      memcpy(compressed_data, compressed_data_tmp, compressed_length);
      newdata.data = compressed_data;
      newdata.compressed_length = compressed_length;
      newdata.uncompressed_length = sizeof(int32_t)*npz.shape;
    }
    _EdgeCache[p_id] = newdata;
    int32_t new_cache_size = std::ceil(newdata.compressed_length*1.0/1024/1024);
    _EdgeCache_Size.fetch_add(new_cache_size, std::memory_order_relaxed);
    new_cache_size = std::ceil(newdata.uncompressed_length*1.0/1024/1024);
    _EdgeCache_Size_Uncompress.fetch_add(new_cache_size, std::memory_order_relaxed);
  }
  // int required_len = sizeof(int32_t)*npz.shape[0];
  /*if (_Edge_Buffer_Len[omp_id] < required_len) {
    if (_Edge_Buffer_Len[omp_id] > 0) {delete [] (_Edge_Buffer[omp_id]);}
      _Edge_Buffer[omp_id] = new char[required_len];
      _Edge_Buffer_Len[omp_id] = required_len;
  }*/
  // memcpy(_Edge_Buffer[omp_id], npz.data, required_len);
  // delete [] (npz.data);
  _Edge_Buffer_Lock[omp_id]--;
  return _Edge_Buffer[omp_id];
}

void clean_edge(int32_t p_id, char *data) {
  // if (_EdgeCache.find(p_id) == _EdgeCache.end()) {
  //  delete [] (data);
  // }
}

inline int get_worker_id() {
  return _my_rank;
}
inline int get_worker_num() {
  return _num_workers;
}

void barrier_workers() {
  MPI_Barrier(MPI_COMM_WORLD);
}

void finalize_workers() {
  LOG(INFO) << "Finalizing the application";
  zmq_ctx_destroy (_zmq_context);
  delete [] (_all_hostname);
  for (auto t_it = _EdgeCache.begin(); t_it != _EdgeCache.end(); t_it++) {
    delete [] t_it->second.data;
  }
  for (int32_t i = 0; i < CMPNUM; i++) {
    delete [] (_Send_Buffer[i]);
    delete [] (_Edge_Buffer[i]);
    delete [] (_Uncompressed_Buffer[i]);
  }
  for (auto& Sockets:_Socket_Pool) {
    for (auto & Socket: Sockets.second) {
      zmq_close(Socket.second);
    }
  }


  MPI_Finalize();
}

void start_time_app() {
  APP_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_app() {
  APP_TIME_END = std::chrono::steady_clock::now();
  APP_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
             (APP_TIME_END-APP_TIME_START).count();
}

void start_time_hdfs() {
  HDFS_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_hdfs() {
  HDFS_TIME_END = std::chrono::steady_clock::now();
  HDFS_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
             (HDFS_TIME_END-HDFS_TIME_START).count();
}

void start_time_init() {
  INIT_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_init() {
  INIT_TIME_END = std::chrono::steady_clock::now();
  INIT_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
              (INIT_TIME_END-INIT_TIME_START).count();
}

void start_time_comp() {
  COMP_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_comp() {
  COMP_TIME_END = std::chrono::steady_clock::now();
  COMP_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
              (COMP_TIME_END-COMP_TIME_START).count();
}

void init_workers() {
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
  MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
  MPI_Get_processor_name(_hostname, &_hostname_len);
  _all_hostname = new char[HOST_LEN * _num_workers];
  memset(_all_hostname, 0, HOST_LEN * _num_workers);
  MPI_Allgather(_hostname, HOST_LEN, MPI_CHAR, _all_hostname, HOST_LEN, MPI_CHAR, MPI_COMM_WORLD);
  if (_my_rank == 0) {
    LOG(INFO) << "Processors: " << _num_workers;
    for (int i = 0; i < _num_workers; i++) {
      LOG(INFO) << "Rank " << i << ": " << _all_hostname + HOST_LEN *i;
    }
  }
  _zmq_context = zmq_ctx_new ();
  _EdgeCache_Size = 0;
  _EdgeCache_Size_Uncompress = 0;
  _Computing_Num = 0;
  _Missed_Num = 0;
  _Pending_Request = 0;
  _Network_Compressed = 0;
  _Network_Uncompressed = 0;
  _Changed_Vertex = 0;
  barrier_workers();
  COMPRESS_NETWORK_LEVEL = 1;  //0, 1, 2
  COMPRESS_CACHE_LEVEL = 1; //0, 1, 2, 3
}

void graphps_sleep(uint32_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

void barrier_threadpool(std::vector<std::future<bool>> &comp_pool, int32_t threshold) {
  while (1) {
    for (auto it = comp_pool.begin(); it!=comp_pool.end();) {
      auto status = it->wait_for(std::chrono::milliseconds(0));
      if (status == std::future_status::ready) {
        it = comp_pool.erase(it);
      } else {
        it++;
      }
    }
    if (comp_pool.size() > threshold) {
      graphps_sleep(5);
      continue;
    } else {
      break;
    }
  }
}

#endif /* SYSTEM_GLOBAL_H_ */
