/opt/mpich/bin/mpirun -n 1 -f ../hostfile2  numactl -i all ./sssp
/opt/mpich/bin/mpirun -n 3 -f ../hostfile2  numactl -i all ./sssp
/opt/mpich/bin/mpirun -n 6 -f ../hostfile2  numactl -i all ./sssp
/opt/mpich/bin/mpirun -n 9 -f ../hostfile2  numactl -i all ./sssp
