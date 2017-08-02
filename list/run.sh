#/opt/openmpi/bin/mpirun -n 1 -use-hwthread-cpus --map-by node -cpus-per-proc 24 ./main
#/opt/openmpi/bin/mpirun -n 1 -hostfile ../hostfile2  -use-hwthread-cpus --map-by node -cpus-per-proc 24 ./main
/opt/mpich/bin/mpirun -n 9 -f hostfile2  numactl -i all ./tc
