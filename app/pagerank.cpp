/*
 *Copyright 2015 NTU (http://www.ntu.edu.sg/)
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License.
 *You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
*/

#include "../system/GraphPS.h"

using namespace std;

template<class T>
bool comp_pagerank(const int32_t P_ID,
                   std::string DataPath,
                   const int32_t VertexNum,
                   T* VertexValue,
                   T* VertexMsg,
                   T* VertexMsgNew,
                   const int32_t* _VertexOut,
                   const int32_t* _VertexIn,
                   const int32_t step) {
  auto T_START = std::chrono::steady_clock::now();

  int32_t *EdgeData, *indices, *indptr;
  int32_t start_id, end_id;
  std::vector<T> result;
  init_comp<T>(P_ID, DataPath, &EdgeData, &start_id, &end_id, &indices, &indptr, std::ref(result));

  int32_t i   = 0;
  int32_t k   = 0;
  int32_t tmp = 0;
  T   rel = 0;
  int changed_num = 0;
  for (i=0; i < end_id-start_id; i++) {
    rel = 0;
    for (k = 0; k < indptr[i+1] - indptr[i]; k++) {
      tmp = indices[indptr[i] + k];
      rel += VertexMsg[tmp]/_VertexOut[tmp];
    }
    rel = rel*0.85 + 1.0/VertexNum;
    result[i] = rel;
#ifdef USE_ASYNC
    VertexMsg[start_id+i] = rel;
#endif
    if (rel != VertexMsg[start_id+i]) {
      changed_num++;
    }
    if (rel != VertexValue[start_id+i]) {
      _Changed_Vertex++;
      VertexValue[start_id+i] = rel;
    }
  }
  end_comp<T>(P_ID, EdgeData, start_id, end_id, changed_num, VertexMsg, VertexMsgNew, std::ref(result));
  auto T_END = std::chrono::steady_clock::now();
  auto TIME =  std::chrono::duration_cast<std::chrono::milliseconds>(T_END - T_START).count();
  // LOG(INFO) << "### " << TIME;
  return true;
}

template<class T>
class PagerankPS : public GraphPS<T> {
public:
  PagerankPS():GraphPS<T>() {
    this->_comp = comp_pagerank<T>;
  }
  void init_vertex() {
    this->load_vertex_out();
    #pragma omp parallel for num_threads(this->_ThreadNum) schedule(static)
    for (int32_t i=0; i<this->_VertexNum; i++) {
      if (this->_VertexOut[i] == 0)
        this->_VertexOut[i] = 1;
    }
    this->_VertexMsg.assign(this->_VertexNum, 1.0/this->_VertexNum);
    this->_VertexValue.assign(this->_VertexNum, 0);
  }
};

int main(int argc, char *argv[]) {
  start_time_app();
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  init_workers();
  PagerankPS<double> pg;
  //PagerankPS<float> pg;
  // Data Path, VertexNum number, Partition number,  Max Iteration
  // pg.init("/home/mapred/GraphData/eu/edge/", 1070560000, 5096, 10);
  // pg.init("/home/mapred/GraphData/twitter/edge2/", 41652250, 294,  10);
  // pg.init("/home/mapred/GraphData/uk/edge3/", 787803000, 2379,  10);
  pg.init("/home/mapred/GraphData/webuk_3/", 133633040, 300, 1000);
  pg.run();
  stop_time_app();
  LOG(INFO) << "Used " << APP_TIME/1000.0 << " s";
  finalize_workers();
  return 0;
}
