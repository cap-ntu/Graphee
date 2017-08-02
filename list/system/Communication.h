/*
 * Communication.h
 *
 *  Created on: 25 Feb 2017
 *      Author: sunshine
 */

#ifndef SYSTEM_COMMUNICATION_H_
#define SYSTEM_COMMUNICATION_H_

#include "Global.h"
#include <ctime>

void zmq_send(const char * data, const int length, const int rank, const int id) {
  int  omp_id = omp_get_thread_num();
  void *requester = NULL;
  if (_Socket_Pool.find(omp_id) == _Socket_Pool.end()) {
    std::unordered_map<int, void*> _Sockets;
    _Socket_Pool[omp_id] = _Sockets;
  }
  if (_Socket_Pool[omp_id].find(rank) == _Socket_Pool[omp_id].end()) {
    std::string dst("tcp://");
    dst += std::string(_all_hostname + rank*HOST_LEN);
    dst += ":";
    dst += std::to_string(ZMQ_PORT+id);
    _Socket_Pool[omp_id][rank] = zmq_socket(_zmq_context, ZMQ_REQ);
    requester = _Socket_Pool[omp_id][rank];
    zmq_connect (requester, dst.c_str());
  }
  requester = _Socket_Pool[omp_id][rank];
  char buffer [5];
  zmq_send (requester, data, length, 0);
  zmq_recv (requester, buffer, 5, 0);
  // zmq_close (requester);
}

void graphps_send(const char * data, const int length, const int rank) {
  if (COMPRESS_NETWORK_LEVEL == 0) {
    zmq_send(data, length, rank,  0);
  } else if (COMPRESS_NETWORK_LEVEL == 1) {
    std::string compressed_data;
    int compressed_length = snappy::Compress(data, length, &compressed_data);
    zmq_send(compressed_data.c_str(), compressed_length, rank, 0);
  } else if (COMPRESS_NETWORK_LEVEL > 1) {
    size_t compressed_length = 0;
    char* compressed_data = NULL;
    size_t buf_size = compressBound(length);
    compressed_length = buf_size;
    compressed_data = new char[buf_size];
    int compress_result = 0;
    compress_result = compress2((Bytef *)compressed_data,
                              &compressed_length,
                              (Bytef *)data,
                              length,
                              1);
    assert(compress_result == Z_OK);
    zmq_send(compressed_data, compressed_length, rank, 0);
    delete [] (compressed_data);
  } else {
    assert (1 == 0);
  }
}

template<class T>
void graphps_sendall(std::vector<T> & data_vector, int32_t start_id, int32_t end_id) {
  int  omp_id = omp_get_thread_num();
  assert (_Send_Buffer_Lock[omp_id] == 0);
  _Send_Buffer_Lock[omp_id]++;
  int32_t length = 0;
  char* data = NULL;
  data = reinterpret_cast<char*>(&data_vector[0]);
  length = sizeof(T)*data_vector.size();
  
  std::srand(std::time(0));
  std::vector<int32_t> random_rank;
  for (int rank=0; rank<_num_workers; rank++) {
    random_rank.push_back(rank);
  }
  std::random_shuffle(random_rank.begin(), random_rank.end());

  if (COMPRESS_NETWORK_LEVEL == 0) {
    for (int rank = 0; rank < _num_workers; rank++) {
      int target_rank = random_rank[rank];
      if (target_rank != _my_rank)
        zmq_send(data, length, target_rank,  0);
        _Network_Uncompressed.fetch_add(sizeof(T)*data_vector.size(), std::memory_order_relaxed);
        _Network_Compressed.fetch_add(length, std::memory_order_relaxed);
    }
  } else if (COMPRESS_NETWORK_LEVEL == 1) {
    size_t max_compressed_length = snappy::MaxCompressedLength(length);
    size_t compressed_length = 0;
    if (_Send_Buffer_Len[omp_id] < max_compressed_length || 
        _Send_Buffer_Len[omp_id] > 2*max_compressed_length) {
      if (_Send_Buffer_Len[omp_id] > 0) {delete [] (_Send_Buffer[omp_id]);}
      _Send_Buffer[omp_id] = new char[int(max_compressed_length*1.5)];
      _Send_Buffer_Len[omp_id] = int(max_compressed_length*1.5);
    }
    char *compressed_data = _Send_Buffer[omp_id];
    snappy::RawCompress(data, length, compressed_data, &compressed_length);
    for (int rank = 0; rank < _num_workers; rank++) {
      int target_rank = random_rank[rank];
      if (target_rank != _my_rank)
        zmq_send(compressed_data, compressed_length, target_rank, 0);
        _Network_Uncompressed.fetch_add(sizeof(T)*data_vector.size(), std::memory_order_relaxed);
        _Network_Compressed.fetch_add(compressed_length, std::memory_order_relaxed);
    }
  } else if (COMPRESS_NETWORK_LEVEL > 1) {
    size_t buf_size = compressBound(length);
    int compress_result = 0;
    size_t compressed_length = 0;
    if (_Send_Buffer_Len[omp_id] < buf_size || 
        _Send_Buffer_Len[omp_id] > 2*buf_size) {
      if (_Send_Buffer_Len[omp_id] > 0) {delete [] (_Send_Buffer[omp_id]);}
      _Send_Buffer[omp_id] = new char[int(buf_size*1.5)];
      _Send_Buffer_Len[omp_id] = int(buf_size*1.5);
    }
    char *compressed_data = _Send_Buffer[omp_id];
    compressed_length = _Send_Buffer_Len[omp_id];
    compress_result = compress2((Bytef *)compressed_data,
                              &compressed_length,
                              (Bytef *)data,
                              length,
                              1);
    assert(compress_result == Z_OK);
    for (int rank = 0; rank < _num_workers; rank++) {
      int target_rank = random_rank[rank];
      if (target_rank != _my_rank)
        zmq_send(compressed_data, compressed_length, target_rank, 0);
        _Network_Uncompressed.fetch_add(sizeof(T)*data_vector.size(), std::memory_order_relaxed);
        _Network_Compressed.fetch_add(compressed_length, std::memory_order_relaxed);
    }
  } else {
    assert (1 == 0);
  }
  _Send_Buffer_Lock[omp_id]--;
}

template<class T>
void graphps_server_backend(std::vector<T*>& VertexMsgNew, 
    std::vector<int32_t>& VertexMsgNewLen, 
    int32_t id) {
  void *responder = zmq_socket (_zmq_context, ZMQ_REP);
  assert(zmq_connect (responder, "inproc://graphps") == 0);
  char *buffer = new char[ZMQ_BUFFER];
  char *uncompressed_c = new char[ZMQ_BUFFER];
  size_t uncompressed_length;
  memset(buffer, 0, ZMQ_BUFFER);
  while (1) {
    // memset(buffer, 0, ZMQ_BUFFER);
    int length = zmq_recv (responder, buffer, ZMQ_BUFFER, 0);
    if (length == -1) {break;}
    _Pending_Request++;
    assert(length < ZMQ_BUFFER);
    // zmq_send (responder, "ACK", 3, 0);
    if (COMPRESS_NETWORK_LEVEL == 0) {
      memcpy(uncompressed_c, buffer, length);
      uncompressed_length = length;
    } else if (COMPRESS_NETWORK_LEVEL == 1) {
      assert (snappy::RawUncompress(buffer, length, uncompressed_c) == true);
      assert (snappy::GetUncompressedLength(buffer, length, &uncompressed_length) == true);
    } else if (COMPRESS_NETWORK_LEVEL > 1) {
      int uncompress_result = 0;
      uncompressed_length = ZMQ_BUFFER*1.1;
      uncompress_result = uncompress((Bytef *)uncompressed_c,
                                    &uncompressed_length,
                                    (Bytef *)buffer,
                                    length);
      assert (uncompress_result == Z_OK);
    } else {
      assert (1 == 0);
    }
    T* raw_data = (T*) uncompressed_c;
    int32_t raw_data_len = uncompressed_length / sizeof(T);
    int32_t start_id = (int32_t)raw_data[raw_data_len-1]*10000 + (int32_t)raw_data[raw_data_len-2];
    int32_t end_id = (int32_t)raw_data[raw_data_len-3]*10000 + (int32_t)raw_data[raw_data_len-4];
    
    int32_t k = 0;
    while (true) {
     int32_t s_id  = raw_data[k++];
     int32_t s_len = raw_data[k++];
     assert (VertexMsgNew[start_id+s_id] == NULL);
     VertexMsgNew[start_id+s_id] = new T [s_len];
     memcpy(VertexMsgNew[start_id+s_id], raw_data+k, sizeof(T)*s_len);
     VertexMsgNewLen[start_id+s_id] = s_len;
     k = k + s_len;
     if (k>=raw_data_len-4) break;
    }
     zmq_send (responder, "ACK", 3, 0);
    _Pending_Request--;
  }
}

template<class T>
void graphps_server(std::vector<T*>& VertexMsgNew, std::vector<int32_t>& VertexMsgNewLen) {
  std::string server_addr(ZMQ_PREFIX);
  server_addr += std::to_string(ZMQ_PORT);
  void *server_frontend = zmq_socket (_zmq_context, ZMQ_ROUTER);
  assert (server_frontend);
  assert (zmq_bind (server_frontend, server_addr.c_str()) == 0);
  void *server_backend = zmq_socket (_zmq_context, ZMQ_DEALER);
  assert(server_backend);
  assert (zmq_bind (server_backend, "inproc://graphps") == 0);
  std::vector<std::thread> zmq_server_pool;
  for (int32_t i=0; i<ZMQNUM; i++)
    zmq_server_pool.push_back(std::thread(graphps_server_backend<T>, 
        std::ref(VertexMsgNew), 
        std::ref(VertexMsgNewLen), 
        i));
  // for (int32_t i=0; i<ZMQNUM; i++) 
  //   zmq_server_pool[i].detach();
  zmq_proxy (server_frontend, server_backend, NULL);
}

#endif /* SYSTEM_COMMUNICATION_H_ */
