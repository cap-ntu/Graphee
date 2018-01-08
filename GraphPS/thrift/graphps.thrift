namespace cpp graphps

struct ComponentID {
  1: VidDtype minID,
  2: VidDtype maxID,
}

typedef i32 VidDtype
typedef i32 VdegDtype
typedef double VvalueDtype
typedef double VmsgDtype
# typedef ComponentID VvalueDtype
# typedef ComponentID VmsgDtype

struct VertexData {
  1: VvalueDtype value,
  2: VmsgDtype msg,
  3: bool state,
  4: optional VdegDtype outdegree,
}

service VertexUpdate {
   i32 ping(1:i32 id),
   i32 update_vertex_sparse(1: i32 pid, 2:VidDtype vlen, 3:list<VidDtype> vid, 4:list<VmsgDtype> vmsg)
   i32 update_vertex_dense(1: i32 pid, 2:VidDtype vlen, 3:VidDtype start_id, 4:list<VmsgDtype> vmsg)
}
