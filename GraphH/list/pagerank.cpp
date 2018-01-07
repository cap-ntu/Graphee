/*
 ============================================================================
 Name        : GraphPS.c
 Author      : Sun Peng
 Version     :
 Copyright   : Your copyright notice
 Description : Compute Pi in MPI C++
 ============================================================================
 */

#include "system/GraphPS.h"

using namespace std;

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
    this->_VertexValue.assign(this->_VertexNum, 1.0/this->_VertexNum);
    this->_VertexMsg.assign(this->_VertexNum, NULL);
    this->_VertexMsgLen.assign(this->_VertexNum, 0);
    this->_VertexMsgNew.assign(this->_VertexNum, NULL);
    this->_VertexMsgNewLen.assign(this->_VertexNum, 0);
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
  // pg.init("/home/mapred/GraphData/eu/edge/", 1070560000, 5096, 2000);
  // pg.init("/home/mapred/GraphData/twitter/edge2/", 41652250, 294,  2000);
  //pg.init("/home/mapred/GraphData/uk/edge3/", 787803000, 2379,  2000);
  pg.init("/home/mapred/GraphData/webuk_3/", 133633040, 300, 2000);
  pg.run();
  stop_time_app();
  LOG(INFO) << "Used " << APP_TIME/1000.0 << " s";
  finalize_workers();
  return 0;
}

