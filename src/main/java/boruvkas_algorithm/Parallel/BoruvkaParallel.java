package boruvkas_algorithm.Parallel;

import boruvkas_algorithm.Sequential.Edge;
import boruvkas_algorithm.Sequential.Node;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BoruvkaParallel {
    protected Queue<Integer> newNodes;
    protected Map<Integer, Boolean> connectedMap;
    protected final AtomicInteger numActiveThreads; // threads currently executing code
    protected final AtomicInteger numWorkingThreads; // total threads either executing code or busy waiting
    protected final int numProcessors;
    protected CSRGraph graph;

    protected final Map<Integer, Set<Integer>> threadToLocalNodesMap;
    protected final Set<Thread> threads;

    public BoruvkaParallel(int numProcessors, Set<Node> nodes, Set<Edge> edges){
        this.numProcessors = numProcessors;

        newNodes = new ConcurrentLinkedQueue<>();
        connectedMap = new ConcurrentHashMap<>();
        threadToLocalNodesMap = new HashMap<>();

        threads = new HashSet<>();

        numActiveThreads = new AtomicInteger(numProcessors);
        numWorkingThreads = new AtomicInteger(numProcessors);

        graph = new CSRGraph(nodes, edges);

    }

    public class Worker implements Runnable{
        private Set<Integer> localNodes;
        private final boolean isAuthority;

        public Worker(boolean isAuthority, Set<Integer> localNodes){
            this.isAuthority = isAuthority;
            this.localNodes = localNodes;
        }

        @Override
        public void run() {
            // (a) find edge with minimum weight & add it to vertexMinEdge
            while(!localNodes.isEmpty()){
                for(Integer node: localNodes){
                    // iterate over outgoing edges from node
                    int minWeight = Integer.MIN_VALUE;
                    int firstEdgeIndex = graph.getFirstEdge(node);
                    for(int i = firstEdgeIndex; i < firstEdgeIndex + graph.getOutDegree(node); i++){
                        int weight = graph.getWeight(i);
                        if(weight < minWeight){
                            minWeight = weight;
                        }
                    }
                    graph.setNodeMinEdge(node, minWeight);
                }
            }

            // (b) Remove mirrored edges from vertexMinEdge
            for(Integer node: localNodes){
                if(node < graph.getDestination(node) &&
                        // the successor of node is node itself
                        graph.getDestination(graph.getNodeMinEdge(
                                graph.getDestination(graph.getNodeMinEdge(node)))) == node){
                    graph.setNodeMinEdge(node, null);
                }
            }

            // (c) initialize and propagate colors (components)
            for(Integer node: localNodes){
                Integer edge = graph.getNodeMinEdge(node);
                // if edge is null, node is a representative for the component
                if(edge == null){
                    graph.setColor(node, node);
                    newNodes.add(node);
                }
                // otherwise, find the representative for the component
                else{
                    graph.setColor(node, graph.getDestination(graph.getNodeMinEdge(node)));
                }
            }
            // initialize new CSRGraph
            CSRGraph newGraph = new CSRGraph(newNodes);

            // (e) Count & assign edges after contraction
            for(Integer node: localNodes){
                // iterate over edges of node
                for(int i = graph.getFirstEdge(node); i < graph.getFirstEdge(node) + graph.getOutDegree(node); i++){
                    int color = graph.getColor(node);

                    // if the edge crosses components
                    if(color != graph.getColor(graph.getDestination(i))){
                        newGraph.incOutDegree(color);
                    }
                    else{
                        connectedMap.put(node, true);
                    }

                }
            }
            // authority thread builds newFirstEdges, non-authority threads wait
            buildNewFirstEdges(newGraph);

            // (f) build new edges
            for(Integer node: localNodes){
                int newEdgePos = graph.getFirstEdge(graph.getColor(node));
                for(int i = graph.getFirstEdge(node); i < graph.getFirstEdge(node) + graph.getOutDegree(node); i++){
                    if(!connectedMap.get(i)){
                        graph.addDestination(newEdgePos, graph.getDestination(i));
                        graph.addWeight(newEdgePos, graph.getWeight(i));
                        newEdgePos++;
                    }
                }
            }

        }

        public void buildNewFirstEdges(CSRGraph newGraph){
            if(isAuthority){
                while(numActiveThreads.get() > 1){}
                newGraph.addFirstEdge(0, 0);

                for(int i = 1; i < graph.numNodes; i++){
                    graph.addFirstEdge(i, graph.getOutDegree(i-1) + graph.getFirstEdge(i-1));
                }
                numActiveThreads.set(numWorkingThreads.get());
            }
            else{
                numActiveThreads.decrementAndGet();
                // busy wait until authority indicates finished by setting numActiveThreads to numWorkingThreads
                while(numActiveThreads.get() != numWorkingThreads.get()){}
            }
        }

        public void handoffNewStructures(CSRGraph newGraph){
            if(isAuthority){
                while(numActiveThreads.get() > 1){}

                Collection<Set<Integer>> allNodes = threadToLocalNodesMap.values();
                for(Set<Integer> nodes: allNodes){
                    nodes.clear();
                }

                int numNodesPerThread = (int) Math.ceil(newNodes.size()/numWorkingThreads.get());

                // evenly distribute new nodes amongst each thread
                for(Set<Integer> nodes: allNodes){
                    for(int i = 0; i < numNodesPerThread; i++){
                        if(i == numNodesPerThread-1){
                            if(!nodes.isEmpty()){
                                nodes.add(newNodes.remove());
                            }
                            else{
                                break;
                            }
                        }
                        else{
                            nodes.add(newNodes.remove());
                        }
                    }
                }
                assert(newNodes.isEmpty());

                connectedMap.clear();

                graph = newGraph;
            }
            else{
                numActiveThreads.decrementAndGet();
                while(numActiveThreads.get() != numWorkingThreads.get()){}
            }
        }
    }

    public void run(){
        for(int i = 0; i < numProcessors; i++){
//            threads.add(new Thread(new Worker(i == 0 ? true: false, )));
        }
    }
}
