/*
 * GraphPS.h
 *
 *  Created on: 25 Feb 2017
 *      Author: sunshine
 */

#ifndef GRAPHPS_H_
#define GRAPHPS_H_

#include "Global.h"
#include "Communication.h"

template<class T>
bool comp_tc(const int32_t P_ID,
               std::string DataPath,
               const int32_t VertexNum,
               std::vector<T>& VertexValue,
               std::vector<T*>& VertexMsg,
               std::vector<int32_t>& VertexMsgLen,
               std::vector<T*>& VertexMsgNew,
               std::vector<int32_t>& VertexMsgNewLen,
               const int32_t* _VertexOut,
               const int32_t* _VertexIn,
               const int32_t step) {
  _Computing_Num++;
  DataPath += std::to_string(P_ID);
  DataPath += ".edge.npy";
  char* EdgeDataNpy = load_edge(P_ID, DataPath);
  int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
  int32_t start_id = EdgeData[3];
  int32_t end_id = EdgeData[4];
  int32_t indices_len = EdgeData[1];
  int32_t indptr_len = EdgeData[2];
  int32_t * indices = EdgeData + 5;
  int32_t * indptr = EdgeData + 5 + indices_len;
  int32_t vertex_num = VertexNum;
  std::vector<std::vector<T>> result;
  for (int32_t i=0; i<end_id-start_id; i++) {
    result.push_back(std::vector<T>());
  }
  int32_t i   = 0;
  int32_t j   = 0;
  int32_t f   = 0;
  size_t n1   = 0;
  size_t k = 0;
  size_t c = 600000;
  T tmp;
  size_t skip = step*c;
  for (i = 0; i < end_id-start_id; i++) {
    f = 0;
    for (j = 0; j < indptr[i+1] - indptr[i]; j++) {
      int32_t id = indices[indptr[i] + j];
      for (k=0; k <  VertexMsgLen[id]; k++) {
        tmp = VertexMsg[id][k];
        if (tmp == i+start_id) {VertexValue[i+start_id]++; f = 1;}
      }
      n1++;
      if (n1 > skip && n1 <= skip+c) {result[i].push_back(id);}
    }
    if (f == 1) { _Changed_Vertex++;}
    result[i].shrink_to_fit();
  }
  clean_edge(P_ID, EdgeDataNpy);

  for (int32_t k=0; k<(end_id-start_id); k++) {
    assert (VertexMsgNew[k+start_id] == NULL);
    VertexMsgNew[k+start_id] = new T [result[k].size()];
    memcpy(VertexMsgNew[k+start_id], &result[k][0], sizeof(T)*result[k].size());
    VertexMsgNewLen[k+start_id] = result[k].size();
  }
  std::vector<T> result_v;
  int32_t message_v_size = 0;
  for (int32_t k=0; k<(end_id-start_id); k++) {
    if (result[k].size() > 0) {
      message_v_size++;
      result_v.push_back(k);
      result_v.push_back(result[k].size());
      for (auto & tmp : result[k]) 
        result_v.push_back(tmp);
    }
    result[k].clear();
    result[k].shrink_to_fit();
  }
  result.clear();
  result.shrink_to_fit();
  result_v.push_back((int32_t)end_id%10000);
  result_v.push_back((int32_t)std::floor(end_id*1.0/10000));
  result_v.push_back((int32_t)start_id%10000);
  result_v.push_back((int32_t)std::floor(start_id*1.0/10000));
  _Computing_Num--;
  if (message_v_size > 0)
    graphps_sendall<T>(std::ref(result_v), start_id, end_id);
  return true;
}


template<class T>
bool comp_pagerank(const int32_t P_ID,
               std::string DataPath,
               const int32_t VertexNum,
               std::vector<T>& VertexValue,
               std::vector<T*>& VertexMsg,
               std::vector<int32_t>& VertexMsgLen,
               std::vector<T*>& VertexMsgNew,
               std::vector<int32_t>& VertexMsgNewLen,
               const int32_t* _VertexOut,
               const int32_t* _VertexIn,
               const int32_t step) {
  _Computing_Num++;
  DataPath += std::to_string(P_ID);
  DataPath += ".edge.npy";
  char* EdgeDataNpy = load_edge(P_ID, DataPath);
  int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
  int32_t start_id = EdgeData[3];
  int32_t end_id = EdgeData[4];
  int32_t indices_len = EdgeData[1];
  int32_t indptr_len = EdgeData[2];
  int32_t * indices = EdgeData + 5;
  int32_t * indptr = EdgeData + 5 + indices_len;
  int32_t vertex_num = VertexNum;
  std::vector<std::vector<T>> result;
  for (int32_t i=0; i<end_id-start_id; i++) {
    result.push_back(std::vector<T>());
  }
  int32_t i   = 0;
  int32_t k   = 0;
  int32_t tmp = 0;
  T   rel = 0;

  for (i=0; i < end_id-start_id; i++) {
    rel = 0;
    if (step > 0) {
      for (k = 0; k < indptr[i+1] - indptr[i]; k++) {
        tmp = indices[indptr[i] + k];
        rel += VertexMsg[tmp][0]/_VertexOut[tmp];
      }
    }
    rel = rel*0.85 + 1.0/vertex_num;
    result[i].push_back(rel);
    if (rel != VertexValue[start_id+i]) {
      _Changed_Vertex++;
      VertexValue[start_id+i] = rel;
    }
  }
  clean_edge(P_ID, EdgeDataNpy);
  for (int32_t k=0; k<(end_id-start_id); k++) {
    assert (VertexMsgNew[k+start_id] == NULL);
    VertexMsgNew[k+start_id] = new T [result[k].size()];
    memcpy(VertexMsgNew[k+start_id], &result[k][0], sizeof(T)*result[k].size());
    VertexMsgNewLen[k+start_id] = result[k].size();
  }
  std::vector<T> result_v;
  int32_t message_v_size = 0;
  for (int32_t k=0; k<(end_id-start_id); k++) {
    if (result[k].size() > 0) {
      message_v_size++;
      result_v.push_back(k);
      result_v.push_back(result[k].size());
      for (auto & tmp : result[k]) 
        result_v.push_back(tmp);
    }
    result[k].clear();
    result[k].shrink_to_fit();
  }
  result.clear();
  result.shrink_to_fit();
  result_v.push_back((int32_t)end_id%10000);
  result_v.push_back((int32_t)std::floor(end_id*1.0/10000));
  result_v.push_back((int32_t)start_id%10000);
  result_v.push_back((int32_t)std::floor(start_id*1.0/10000));
  _Computing_Num--;
  if (message_v_size > 0)
    graphps_sendall<T>(std::ref(result_v), start_id, end_id);
  return true;
}

template<class T>
class GraphPS {
public:
  bool (*_comp)(const int32_t,
                std::string,
                const int32_t,
                std::vector<T> &,
                std::vector<T*> &,
                std::vector<int32_t> &,
                std::vector<T*> &,
                std::vector<int32_t> &,
                const int32_t*,
                const int32_t*,
                const int32_t
               ) = NULL;
  T _FilterThreshold;
  std::string _DataPath;
  std::string _Scheduler;
  int32_t _ThreadNum;
  int32_t _VertexNum;
  int32_t _PartitionNum;
  int32_t _MaxIteration;
  int32_t _PartitionID_Start;
  int32_t _PartitionID_End;
  std::vector<int32_t> _Allocated_Partition;
  std::map<int, std::string> _AllHosts;
  std::vector<T> _VertexValue;
  std::vector<int32_t> _VertexOut;
  std::vector<int32_t> _VertexIn;
  std::vector<T*> _VertexMsg;
  std::vector<int32_t> _VertexMsgLen;
  std::vector<T*> _VertexMsgNew;
  std::vector<int32_t> _VertexMsgNewLen;
  bloom_parameters _bf_parameters;
  std::map<int32_t, bloom_filter> _bf_pool;
  GraphPS();
  void init(std::string DataPath,
            const int32_t VertexNum,
            const int32_t PartitionNum,
            const int32_t MaxIteration=10);

  virtual void init_vertex()=0;
  void set_threadnum (const int32_t ThreadNum);
  void run();
  void load_vertex_in();
  void load_vertex_out();
};

template<class T>
GraphPS<T>::GraphPS() {
  _VertexNum = 0;
  _PartitionNum = 0;
  _MaxIteration = 0;
  _ThreadNum = 1;
  _PartitionID_Start = 0;
  _PartitionID_End = 0;
}

template<class T>
void GraphPS<T>::init(std::string DataPath,
                      const int32_t VertexNum,
                      const int32_t PartitionNum,
                      const int32_t MaxIteration) {
  start_time_init();
  _ThreadNum = CMPNUM;
  _DataPath = DataPath;
  _VertexNum = VertexNum;
  _PartitionNum = PartitionNum;
  _MaxIteration = MaxIteration;
  for (int i = 0; i < _num_workers; i++) {
    std::string host_name(_all_hostname + i * HOST_LEN);
    _AllHosts[i] = host_name;
  }
  _Scheduler = _AllHosts[0];
  int32_t n = std::ceil(_PartitionNum*1.0/_num_workers);
  _PartitionID_Start = (_my_rank*n < _PartitionNum) ? _my_rank*n:-1;
  _PartitionID_End = ((1+_my_rank)*n > _PartitionNum) ? _PartitionNum:(1+_my_rank)*n;
  
  /*int p_s[9] = {0,   580,  1146, 1722, 2291, 2853, 3414, 4014, 4570};
  int p_e[9] = {579, 1145, 1721, 2290, 2852, 3413, 4013, 4569, 5095};
  for (int i = p_s[_my_rank]; i <= p_e[_my_rank]; i++) {
    _Allocated_Partition.push_back(i);
  }*/

  for (int i = _PartitionID_Start; i < _PartitionID_End; i++) {
    _Allocated_Partition.push_back(i);
  }

  LOG(INFO) << "Rank " << _my_rank << " "
            << " With Partitions From " << _PartitionID_Start << " To " << _PartitionID_End;

  _EdgeCache.reserve(_PartitionNum*2/_num_workers);
  for (int i = 0; i < _ThreadNum; i++) {
    _Send_Buffer[i] = NULL;
    _Send_Buffer_Lock[i] = 0;
    _Send_Buffer_Len[i] = 0;
    _Edge_Buffer[i] = NULL;
    _Edge_Buffer_Lock[i] = 0;
    _Edge_Buffer_Len[i] = 0;
    _Uncompressed_Buffer[i] = NULL;
    _Uncompressed_Buffer_Lock[i] = 0;
    _Uncompressed_Buffer_Len[i] = 0;
  }
  int32_t data_size = GetDataSize(DataPath) * 1.0 / 1024 / 1024 / 1024; //GB 
  int32_t cache_size = _num_workers * EDGE_CACHE_SIZE / 1024; //GB
  //0:1, 1:0.45, 2:0.25, 3:0.19
  if (data_size <= cache_size*1.2) 
    COMPRESS_CACHE_LEVEL = 0;
  else
    COMPRESS_CACHE_LEVEL = 2;
  //#########################
  COMPRESS_CACHE_LEVEL = 0;
  LOG(INFO) << "data size "  << data_size << " GB, "
            << "cache size " << cache_size << " GB, "
            << "compress level " << COMPRESS_CACHE_LEVEL;
}

template<class T>
void  GraphPS<T>::load_vertex_in() {
  std::string vin_path = _DataPath + "vertexin.npy";
  cnpy::NpyArray npz = cnpy::npy_load(vin_path);
  int32_t *data = reinterpret_cast<int32_t*>(npz.data);
  _VertexIn.assign(data, data+_VertexNum);
  npz.destruct();
}

template<class T>
void  GraphPS<T>::load_vertex_out() {
  std::string vout_path = _DataPath + "vertexout.npy";
  cnpy::NpyArray npz = cnpy::npy_load(vout_path);
  int32_t *data = reinterpret_cast<int32_t*>(npz.data);
  _VertexOut.assign(data, data+_VertexNum);
  npz.destruct();
}

template<class T>
void GraphPS<T>::run() {
  /////////////////
  #ifdef USE_HDFS
  LOG(INFO) << "Rank " << _my_rank << " Loading Edge From HDFS";
  start_time_hdfs();
  int hdfs_re = 0;
  hdfs_re = system("rm /home/mapred/tmp/satgraph/*");
  std::string hdfs_bin = "/opt/hadoop-1.2.1/bin/hadoop fs -get ";
  std::string hdfs_dst = "/home/mapred/tmp/satgraph/";
  #pragma omp parallel for num_threads(6) schedule(static)
  for (int32_t k=0; k<_Allocated_Partition.size(); k++) {
    std::string hdfs_command;
    hdfs_command = hdfs_bin + _DataPath;
    hdfs_command += std::to_string(_Allocated_Partition[k]);
    hdfs_command += ".edge.npy ";
    hdfs_command += hdfs_dst;
    hdfs_re = system(hdfs_command.c_str());
  }

  LOG(INFO) << "Rank " << _my_rank << " Loading Vertex From HDFS";
  std::string hdfs_command;
  hdfs_command = hdfs_bin + _DataPath;
  hdfs_command += "vertexin.npy ";
  hdfs_command += hdfs_dst;
  hdfs_re = system(hdfs_command.c_str());
  hdfs_command.clear();
  hdfs_command = hdfs_bin + _DataPath;
  hdfs_command += "vertexout.npy ";
  hdfs_command += hdfs_dst;
  hdfs_re = system(hdfs_command.c_str());
  stop_time_hdfs();
  barrier_workers();
  if (_my_rank==0)
    LOG(INFO) << "HDFS  Load Time: " << HDFS_TIME << " ms";
  _DataPath.clear();
  _DataPath = hdfs_dst;
  #endif
  ////////////////

  init_vertex();
  std::thread graphps_server_mt(graphps_server<T>, std::ref(_VertexMsgNew), std::ref(_VertexMsgNewLen));
  std::vector<int32_t> Partitions(_Allocated_Partition.size(), 0);
  float updated_ratio = 1.0;
  int32_t step = 0;

  barrier_workers();
  stop_time_init();
  if (_my_rank==0)
    LOG(INFO) << "Init Time: " << INIT_TIME << " ms";
  LOG(INFO) << "Rank " << _my_rank << " use " << _ThreadNum << " comp threads";

  // start computation
  for (step = 0; step < _MaxIteration; step++) {
    start_time_comp();
    std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    updated_ratio = 1.0;
    for (int32_t k = 0; k < _Allocated_Partition.size(); k++) {
      Partitions[k] = _Allocated_Partition[k];
    }
    std::random_shuffle(Partitions.begin(), Partitions.end());

    #pragma omp parallel for num_threads(_ThreadNum) schedule(dynamic)
    for (int32_t k=0; k<Partitions.size(); k++) {
      int32_t P_ID = Partitions[k];
      (*_comp)(P_ID,  _DataPath, _VertexNum,
               std::ref(_VertexValue), 
               std::ref(_VertexMsg), std::ref(_VertexMsgLen),
               std::ref(_VertexMsgNew), std::ref(_VertexMsgNewLen),
               _VertexOut.data(), _VertexIn.data(),
               step);
    }
    while(_Pending_Request > 0) {
      graphps_sleep(10);
    }
    std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
    int local_comp_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1).count();
    LOG(INFO) << "Iter: " << step << " Worker: " << _my_rank << " Use: " << local_comp_time;
    barrier_workers();
    int changed_num = _Changed_Vertex;
    int total_changed_num = _Changed_Vertex;
    MPI_Allreduce(&changed_num, &total_changed_num, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    _Changed_Vertex = 0;

    #pragma omp parallel for num_threads(_ThreadNum)  schedule(static)
    for (int32_t result_id = 0; result_id < _VertexNum; result_id++) {
      delete [] (_VertexMsg[result_id]);
      _VertexMsgLen[result_id] = _VertexMsgNewLen[result_id];
      _VertexMsg[result_id] = _VertexMsgNew[result_id];
      // _VertexMsg[result_id] = new T [_VertexMsgNewLen[result_id]];
      // memcpy(_VertexMsg[result_id], _VertexMsgNew[result_id], 
      //       sizeof(T)*_VertexMsgNewLen[result_id]);
      _VertexMsgNewLen[result_id] = 0;
      _VertexMsgNew[result_id] = NULL;
    }

    stop_time_comp();
    updated_ratio = total_changed_num * 1.0 / _VertexNum;
    MPI_Bcast(&updated_ratio, 1, MPI_FLOAT, 0, MPI_COMM_WORLD);
    int missed_num = _Missed_Num;
    int total_missed_num = _Missed_Num;
    int cache_size = _EdgeCache_Size;
    int total_cache_size = _EdgeCache_Size;
    int cache_size_uncompress = _EdgeCache_Size_Uncompress;
    int total_cache_size_uncompress = _EdgeCache_Size_Uncompress;
    long network_compress = _Network_Compressed;
    long network_uncompress = _Network_Uncompressed;
    long total_network_compress = _Network_Compressed;
    long total_network_uncompress = _Network_Uncompressed;
    ///*
    MPI_Reduce(&missed_num, &total_missed_num, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&cache_size, &total_cache_size, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&cache_size_uncompress, &total_cache_size_uncompress, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&network_compress, &total_network_compress, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&network_uncompress, &total_network_uncompress, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    //*/
    _Missed_Num = 0;
    _Network_Compressed = 0;
    _Network_Uncompressed = 0;

    if (_my_rank==0)
      LOG(INFO) << "Iteration: " << step
                << ", uses "<< COMP_TIME
                << " ms, Update " << total_changed_num
                << ", Ratio " << updated_ratio
                << ", Miss " << total_missed_num
                << ", Cache(MB) " << total_cache_size
                << ", Before(MB) " << total_cache_size_uncompress
                << ", Compress Net(MB) " << total_network_compress*1.0/1024/1024
                << ", Uncompress Net(MB) " << total_network_uncompress*1.0/1024/1024;
    if (total_changed_num == 0 && step > 1) {
      break;
    }
    barrier_workers();
  }
}

#endif /* GRAPHPS_H_ */
