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
bool comp_cc(const int32_t P_ID,
             std::string DataPath,
             const int32_t VertexNum,
             T* VertexValue,
             T* VertexMsg,
             T* VertexMsgNew,
             const int32_t* _VertexOut,
             const int32_t* _VertexIn,
             const int32_t step) {
  int32_t *EdgeData, *indices, *indptr;
  int32_t start_id, end_id;
  std::vector<T> result;
  init_comp<T>(P_ID, DataPath, &EdgeData, &start_id, &end_id, &indices, &indptr, std::ref(result));

  int32_t i   = 0;
  int32_t j   = 0;
  T   max = 0;
  int32_t changed_num = 0;
  for (i = 0; i < end_id-start_id; i++) {
    max = VertexMsg[start_id+i];
    for (j = 0; j < indptr[i+1] - indptr[i]; j++) {
      if (max < VertexMsg[indices[indptr[i]+j]])
        max = VertexMsg[indices[indptr[i] + j]];
    }
    result[i] = max;
#ifdef USE_ASYNC
    VertexMsg[start_id+i] = max;
#endif
    if (max != VertexMsg[start_id+i]) {
      changed_num++;
    }
    if (max != VertexValue[start_id+i]) {
      _Changed_Vertex++;
      VertexValue[start_id+i] = max;
    }
  }
  end_comp<T>(P_ID, EdgeData, start_id, end_id, changed_num, VertexMsg, VertexMsgNew, std::ref(result));
  return true;
}

template<class T>
class PagerankPS : public GraphPS<T> {
public:
  PagerankPS():GraphPS<T>() {
    this->_comp = comp_cc<T>;
  }
  void init_vertex() {
    this->_VertexValue.assign(this->_VertexNum, 0);
    this->_VertexMsg.assign(this->_VertexNum, 0);
    for (int32_t i = 0; i < this->_VertexNum; i++) {
      this->_VertexMsg[i] = i;
    }
  }
};

int main(int argc, char *argv[]) {
  start_time_app();
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  init_workers();
  PagerankPS<int32_t> pg;
  //PagerankPS<float> pg;
  // Data Path, VertexNum number, Partition number, thread number, Max Iteration
  pg.init("/home/mapred/GraphData/eu/edge/", 1070560000, 5096,  2000);
  // pg.init("/home/mapred/GraphData/twitter/edge2/", 41652250, 294,  2000);
  // pg.init("/home/mapred/GraphData/uk/edge3/", 787803000, 2379,  2000);
  // pg.init("/home/mapred/GraphData/webuk_3/", 133633040, 300, 2000);
  pg.run();
  stop_time_app();
  LOG(INFO) << "Used " << APP_TIME/1000.0 << " s";
  finalize_workers();
  return 0;
}
