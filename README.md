# Graphee
Graphee is the big graph analysis system stack in Cloud Application and Platform (CAP) Group at School of Computer Science Engineering, NTU. It is a fast and scalable big graph platform for both academia and industry.

Graphee contains three components:

## GraphMP
GraphMP is an efficient semi-external-memory big graph processing system on a single machine. It could achieve low disk I/O overhead with a vertex-centric sliding window computation model, a selective scheduling method, and a compressed edge cache mechanism. Extensive evaluations have shown that GraphMP could outperform state-of-the-art systems such as GraphChi, X-Stream and GridGraph by 31.6x, 54.5x and 23.1x respectively, when running popular graph applications (such as PageRank, Single-Source-Shortest Path) on billion-vertex graphs.

## GraphH
GraphH extends GraphMP to a distributed setting using a GAB (GatherApply-Broadcast) computation model. Extensive evaluations have shown that GraphH could be up to 7.8x faster compared to popular in-memory systems, such as Pregel+ and PowerGraph when processing generic graphs, and more than 100x faster than recently proposed out-of-core systems, such as GraphD and Chaos when processing big graphs.

## GraphPS
GraphPS is our next-generation distributed graph processing system. It leverages the Parameter Server (PS) framework for distributed graph processing, and could offer higher performance than existing solutions for processing big graphs in small commodity clusters.

##GraphStream (Comming Soon)
GraphStream provides efficient support for streaming graph scenarios using the lambda architecture. It is still under implementation.
