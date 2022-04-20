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
    protected final AtomicInteger threadsWaitingNextStep;
    protected final int numProcessors;
    protected CSRGraph graph;
    protected CSRGraph nextGraph;
    protected ArrayList<Unit> nextOutDegrees;

    protected final Map<Integer, Set<Integer>> threadToLocalNodesMap;
    protected final Set<Thread> threads;

    protected final Set<Node> oldNodes;
    protected final Set<Edge> oldEdges;

    public BoruvkaParallel(int numProcessors, Set<Node> nodes, Set<Edge> edges){
        this.numProcessors = numProcessors;
        oldNodes = nodes;
        oldEdges = edges;

        newNodes = new ConcurrentLinkedQueue<>();
        connectedMap = new ConcurrentHashMap<>();
        threadToLocalNodesMap = new HashMap<>();

        threads = new HashSet<>();

        numActiveThreads = new AtomicInteger(numProcessors);
        numWorkingThreads = new AtomicInteger(numProcessors);
        threadsWaitingNextStep = new AtomicInteger(numProcessors);

        graph = new CSRGraph(nodes, edges);
        nextOutDegrees = new ArrayList<>(edges.size());

        int nodeNum = 0;
        int nodesPerThread = (int) Math.ceil(graph.numNodes / numWorkingThreads.get());
        for(int i = 0; i < numProcessors; i++){
            Set<Integer> localNodes = new HashSet<>();
            for(int j = 0; j < nodesPerThread; j++){
                if(nodeNum < graph.numNodes){
                    localNodes.add(nodeNum++);
                }
                else{
                    break;
                }
            }

            threads.add(new Thread(new Worker(i == 0 ? true : false, i, localNodes)));
            threadToLocalNodesMap.put(i, localNodes);
        }

    }

    private class Worker implements Runnable{
        private Set<Integer> localNodes;
        private final boolean isAuthority;
        private final int threadID;

        public Worker(boolean isAuthority, int threadID, Set<Integer> localNodes){
            this.isAuthority = isAuthority;
            this.localNodes = localNodes;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            while(!localNodes.isEmpty()){
                System.out.println();
                // (a) find edge with minimum weight & add it to vertexMinEdge
                for(Integer node: localNodes){
                    // iterate over outgoing edges from node
                    int minWeight = Integer.MAX_VALUE;
                    int edge = -1;
                    int firstEdgeIndex = graph.getFirstEdge(node);
                    for(int i = firstEdgeIndex; i < firstEdgeIndex + graph.getOutDegree(node); i++){
                        int weight = graph.getWeight(i);
                        if(weight < minWeight){
                            minWeight = weight;
                            edge = i;
                        }
                    }
                    graph.setNodeMinEdge(node, edge);
                }

                synchronizeStep();

                // (b) Remove mirrored edges from vertexMinEdge
                for(Integer node: localNodes){
                    int otherNode = graph.getDestination(graph.getNodeMinEdge(
                            graph.getDestination(graph.getNodeMinEdge(node))));
                    if(node < graph.getDestination(node) &&
                            // the successor of node is node itself
                             otherNode == node){
                        graph.setNodeMinEdge(node, null);
                    }
                }

                synchronizeStep();

                // (c) initialize and propagate colors (components)
                for(Integer node: localNodes){
                    Integer edge = graph.getNodeMinEdge(node);
                    // if edge is null, node is a representative for the component
                    if(edge == -1){
                        graph.setColor(node, node);
                        newNodes.add(node);
                    }
                    // otherwise, find the representative for the component
                    else{
                        int color = graph.getDestination(graph.getNodeMinEdge(node));
                        while(graph.getNodeMinEdge(color) != -1){
                            color = graph.getDestination(graph.getNodeMinEdge(color));
                        }
                        graph.setColor(node, color);
                    }
                }

                if(isAuthority){
                    nextGraph = new CSRGraph(newNodes);
                }
                synchronizeStep();
                System.out.println();

                // (e) Count & assign edges after contraction
                for(Integer node: localNodes){
                    // iterate over edges of node
                    for(int i = graph.getFirstEdge(node); i < graph.getFirstEdge(node) + graph.getOutDegree(node); i++){
                        int color = graph.getColor(node);

                        // if the edge crosses components
                        if(color != graph.getColor(graph.getDestination(i))){
                            nextGraph.incOutDegree(color); // color needs to be mapped to new index
                        }
                        else{
                            connectedMap.put(node, true);
                        }
                    }
                }
                synchronizeStep();

                // authority thread builds newFirstEdges, non-authority threads wait
                buildNewFirstEdges();

                // (f) build new edges
                for(Integer node: localNodes){
                    int newEdgePos = graph.getFirstEdge(graph.getColor(node));
                    for(int i = graph.getFirstEdge(node); i < graph.getFirstEdge(node) + graph.getOutDegree(node); i++){
                        if(!connectedMap.get(i)){
                            nextGraph.addDestination(newEdgePos, graph.getDestination(i));
                            nextGraph.addWeight(newEdgePos, graph.getWeight(i));
                            newEdgePos++;
                        }
                    }
                }
                handoffNewStructures();
            }
            if(isAuthority){
                while(numWorkingThreads.get() > 1){
                    buildNewFirstEdges();
                    handoffNewStructures();
                }
            }
            else{
                numWorkingThreads.decrementAndGet();
            }
        }

        public void synchronizeStep(){
            threadsWaitingNextStep.decrementAndGet();
            while(threadsWaitingNextStep.get() > 0){}
            if(isAuthority){
                threadsWaitingNextStep.set(numWorkingThreads.get());
            }
        }

        public void buildNewFirstEdges(){
            if(isAuthority){
                while(numActiveThreads.get() > 1){}
                nextGraph.addFirstEdge(0, 0);

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

        public void handoffNewStructures(){
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

                graph = nextGraph;

                numActiveThreads.set(numWorkingThreads.get());
            }
            else{
                numActiveThreads.decrementAndGet();
                while(numActiveThreads.get() != numWorkingThreads.get()){}
            }
        }
    }

    public void run(){
        for(Thread thread: threads){
            thread.start();
        }
    }
}
