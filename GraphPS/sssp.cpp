#include "graphps.h"

shared_ptr<GraphPS> _GraphPS;

class VertexUpdateHandler : virtual public VertexUpdateIf {
 public:
  VertexUpdateHandler() {}

  int32_t ping(int32_t id) {
    LOG(INFO) << "ping " << id;
    return 0;
  }

  int32_t update_vertex_sparse(const int32_t pid, const VidDtype vlen, const std::vector<VidDtype> & vid, const std::vector<VmsgDtype> & vmsg) {
    VidDtype id = 0;
    assert(vlen == int(vid.size()));
    assert(vlen == int(vmsg.size()));
    while(CAS(&(_GraphPS->_PartitionLock[pid]), false, true) == false) {sleep_ms(1);}
    for (int i=0; i<vlen; i++) {
      id = vid[i];
      if (_GraphPS->_VertexData[id-_GraphPS->_VertexStartID].msg > vmsg[i]) {
        _GraphPS->_VertexData[id-_GraphPS->_VertexStartID].msg = vmsg[i];
      }
    }
    CAS(&(_GraphPS->_PartitionLock[pid]), true, false);
    return 0;
  }

  int32_t update_vertex_dense(const int32_t pid, const VidDtype vlen, const VidDtype start_id, const std::vector<VmsgDtype> & vmsg) {
    VidDtype id = 0;
    assert(vlen == int(vmsg.size()));
    while(CAS(&(_GraphPS->_PartitionLock[pid]), false, true) == false) {sleep_ms(1);}
    for (int i=0; i<vlen; i++) {
      id = start_id + i;
      if (_GraphPS->_VertexData[id-_GraphPS->_VertexStartID].msg > vmsg[i]) {
        _GraphPS->_VertexData[id-_GraphPS->_VertexStartID].msg = vmsg[i];
      }
    }
    CAS(&(_GraphPS->_PartitionLock[pid]), true, false);
    return 0;
  }
};

void GraphPS::init_vertex_data() {
  for (int i=0; i<_VertexEndID-_VertexStartID; i++) {
    if (_VertexStartID + i == 1) {
      _VertexData[i].value = 0;
      _VertexData[i].msg = 0;
      _VertexData[i].state = true;
    } else {
      _VertexData[i].value = GPS_INF;
      _VertexData[i].msg = GPS_INF;
      _VertexData[i].state = true;
    }
  }
}

void GraphPS::comp(int32_t P_ID) {
  int32_t *PartitionData = load_partition(P_ID);
  int32_t v_num = PartitionData[0];
  int32_t v_end_id = PartitionData[2] + 1;
  int32_t v_start_id = PartitionData[3];
  int32_t *p = PartitionData+3;
  VidDtype vid = 0;
  VidDtype len = 0;
  int32_t c_start_id = get_col_id(v_start_id);
  int32_t c_end_id = get_col_id(v_end_id-1);
  assert(c_end_id - c_start_id <=1);

  std::vector<VidDtype> vid_vec[2];
  std::vector<VmsgDtype> vmsg_vec[2];
  VmsgDtype vmsg = 0;
  int vector_id = 0;
  VidDtype offset=v_end_id;
  bool updated = false;

  for (int i=0; i < v_num; i++) {
    updated = false;
    p++; vid = *p; p++; len = *p;
    vmsg = GPS_INF; 
    if (c_start_id == c_end_id){vector_id = 0;}
    else {
      vector_id = get_col_id(vid) - c_start_id;
      if (vector_id==1 && offset > vid) {
        offset = vid;
      }
    }
    if (len==0) {continue;} 
    for (int k=0; k < len; k++) {
      p++;
      if (_VertexData[*p-_VertexStartID].state == true 
        && vmsg > _VertexData[*p-_VertexStartID].value + 1) {
        vmsg = _VertexData[*p-_VertexStartID].value + 1;
        updated = true;
      }
    }
    if (updated == true) {
      vid_vec[vector_id].push_back(vid);
      vmsg_vec[vector_id].push_back(vmsg);
    }
  }  

  clean_edge(P_ID, reinterpret_cast<char*>(PartitionData));

  if (vid_vec[0].size() > 0) {
    if (vid_vec[0].size()*1.0/(offset-v_start_id) < SPARSE_COMMU) {
       send_msg_sparse(P_ID, std::ref(vid_vec[0]), std::ref(vmsg_vec[0]), v_start_id);
    } else {
      std::vector<VmsgDtype> vmsg_dense_v;
      vmsg_dense_v.assign(offset - v_start_id, GPS_INF);
      for (int k=0; k<int(vid_vec[0].size()); k++) {
        vmsg_dense_v[vid_vec[0][k]-v_start_id] = vmsg_vec[0][k];
      }
      send_msg_dense(P_ID, std::ref(vmsg_dense_v), v_start_id);
    }
  } 

  if (vid_vec[1].size() > 0) {
    if (vid_vec[1].size()*1.0/(v_end_id-offset) < SPARSE_COMMU) {
      send_msg_sparse(P_ID, std::ref(vid_vec[1]), std::ref(vmsg_vec[1]), offset);
    } else {
      std::vector<VmsgDtype> vmsg_dense_v;
      vmsg_dense_v.assign(v_end_id - offset, GPS_INF);
      for (int k=0; k<int(vid_vec[1].size()); k++) {
        vmsg_dense_v[vid_vec[1][k]-offset] = vmsg_vec[1][k];
      }
      send_msg_dense(P_ID, std::ref(vmsg_dense_v), offset);
    }
  }
}

void GraphPS::pre_process() {
  // BF_THRE = 0; //force to activate all partitions
}

void GraphPS::post_process() {
  #pragma omp parallel for num_threads(CMPNUM) schedule(static)
  for (int i=0; i<_VertexEndID-_VertexStartID; i++) {
    VvalueDtype value  = _VertexData[i].msg;
    if (_VertexData[i].value <= value) {
      _VertexData[i].state = false;
    } else {
      _VertexData[i].value = value;
      _VertexData[i].state = true;
    }
  }
}

void start_gserver() {
  shared_ptr<VertexUpdateHandler> handler(new VertexUpdateHandler());
  shared_ptr<TProcessor> processor(new VertexUpdateProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(_server_port));
  #ifdef COMPRESS
  shared_ptr<TTransportFactory> transportFactory(new TZlibBufferdTransportFactory());
  #else
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  #endif
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(COMNUM);
  shared_ptr<PosixThreadFactory> threadFactory = shared_ptr<PosixThreadFactory>(new PosixThreadFactory());    
  threadManager->threadFactory(threadFactory);    
  threadManager->start();    
  TThreadPoolServer gserver(processor, serverTransport, transportFactory, protocolFactory, threadManager);
  gserver.serve();
}

int main(int argc, char **argv) {
  start_time_app();
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  init_nodes();
  std::thread gserver_thread(start_gserver);
  sleep_ms(10);
  _GraphPS = boost::make_shared<GraphPS>();
  // Data Path, VertexNum number, Partition number,  Max Iteration

  // _GraphPS->init("/data/3/gps-1/twitter/", 41652230, 450, 200);
  // _GraphPS->init("/data/3/gps-1/webuk/", 133633040, 900, 5);
  // _GraphPS->init("/data/3/gps-1/uk/",    787801471, 4500, 5);
  // _GraphPS->init("/data/3/gps-1/eu/", 1070557254, 5400, 5);

  // _GraphPS->init("/data/3/gps-9/twitter/", 41652230, 50, 200);
  // _GraphPS->init("/data/3/gps-9/webuk/", 133633040, 100, 200);
  // _GraphPS->init("/data/3/gps-9/uk/",    787801471, 500, 200);
  // _GraphPS->init("/data/3/gps-9/eu/", 1070557254, 600, 200);

  // int a[10] = {0, 16210487, 21469474, 21516335, 21627246, 23491206, 23934080, 24893432, 33047735, 41652230};
  // memcpy(_col_split, a, sizeof(int)*10);
  // _GraphPS->init("/data/3/ps-9/twitter/", 41652230, 60,  10);

  int a[10] = {0, 20815361, 37170387, 49007341, 63087641, 77794058, 90254794, 106507476, 122011708, 133633040};
  memcpy(_col_split, a, sizeof(int)*10);
  _GraphPS->init("/data/3/ps-9/webuk/", 133633040, 100, 100);

  // int a[10] = {0, 88151819, 183178636, 276616717, 371916221, 453978069, 551574640, 630438645, 715178718, 787801471};
  // memcpy(_col_split, a, sizeof(int)*10);
  // _GraphPS->init("/data/3/ps-9/uk/", 787801471, 500,  100);

  // int a[10] = {0, 115848295, 234689314, 351159484, 470107331, 590866731, 709630706, 819058151, 942110302, 1070557254};
  // memcpy(_col_split, a, sizeof(int)*10);
  // _GraphPS->init("/data/3/ps-9/eu/", 1070557254, 600, 100); 

  sleep_ms(10);

  _GraphPS->run();

  gserver_thread.join();
  stop_time_app();
  return 0;
}
