package boruvkas_algorithm.Parallel;

import boruvkas_algorithm.Sequential.Edge;
import boruvkas_algorithm.Sequential.Node;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BoruvkaParallel {
    protected Queue<Integer> newNodes;
    protected Map<Integer, Boolean> connectedMap;
    protected final AtomicInteger numActiveThreads; // threads currently executing code
    protected final AtomicInteger numWorkingThreads; // total threads either executing code or busy waiting
    protected final AtomicInteger numThreadsLeftToSynchronize;
    protected final int numProcessors;
    protected CSRGraph graph;
    protected CSRGraph nextGraph;
    protected ArrayList<Unit> nextOutDegrees;

    protected final Map<Integer, Set<Integer>> threadToLocalNodesMap;
    protected final Set<Thread> threads;

    protected final Set<Node> oldNodes;
    protected final Set<Edge> oldEdges;

    protected final Lock lock;
    protected final Condition isSleeping;

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
        numThreadsLeftToSynchronize = new AtomicInteger(numProcessors);

        graph = new CSRGraph(nodes, edges);
        nextOutDegrees = new ArrayList<>(edges.size());

        lock = new ReentrantLock();
        isSleeping = lock.newCondition();

        int nodeNum = 0;
        int nodesPerThread = (int) Math.ceil(graph.numNodes / numWorkingThreads.get());
        if (nodesPerThread == 0) {
            nodesPerThread = 1;
        }
        for(int i = 0; i < numProcessors; i++){
            Set<Integer> localNodes = new LinkedHashSet<>();
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
        System.out.println();

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
                    // node has smaller ID than its successor
                    if(node < graph.getDestination(graph.getNodeMinEdge(node)) &&
                            // the successor was already found to be a representative node
                            (
//                            (graph.getNodeMinEdge(graph.getDestination(graph.getNodeMinEdge(node))) == -1 ||
                                    // the successor of node is node itself
                                    graph.getMapNewName(graph.getDestination(graph.getNodeMinEdge(graph.getMapNewName(
                                    graph.getDestination(graph.getNodeMinEdge(node)))))) == node)){
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
                        graph.setFlag(node, 1);
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

                synchronizeStep();
                if(isAuthority){
                    nextGraph = new CSRGraph(newNodes);
                    // create new indexes for nodes
                    nextGraph.setNameMap(0, 0);
                    for(int i = 1; i < graph.numNodes; i++){
                        int newName = graph.getFlag(i-1) + graph.getNewName(i-1);
                        graph.addNewName(i, newName);
                        nextGraph.setNameMap(i, newName);
                    }
                }
                synchronizeStep();

                // (e) Count & assign edges after contraction
                for(Integer node: localNodes){
                    // only iterate on representative nodes
                    if(graph.getFlag(node) != 1){
                        continue;
                    }
                    int color = graph.getColor(node);
                    // iterate over edges of node
                    for(int i = graph.getFirstEdge(node); i < graph.getFirstEdge(node) + graph.getOutDegree(node); i++){

                        // if the edge crosses components
                        if(color != graph.getColor(graph.getDestination(i))){
                            nextGraph.incOutDegree(graph.getNewName(color));
                            connectedMap.put(i, false);
                        }
                        else{
                            connectedMap.put(i, true);
                        }
                    }
                }
                synchronizeStep();

                // authority thread builds newFirstEdges, non-authority threads wait
                buildNewFirstEdges();

                // (f) build new edges from old graph
                for(Integer node: localNodes){
                    // only iterate on representative nodes
                    if(graph.getFlag(node) != 1){
                        continue;
                    }
                    // get the representative for the node's component & pass it to newName
                    // using the new name, get the first edge in the edges arrays
                    int newEdgePos = nextGraph.getFirstEdge(graph.getNewName(graph.getColor(node)));
                    for(int edge = graph.getFirstEdge(node); edge < graph.getFirstEdge(node) + graph.getOutDegree(node); edge++){
                        if(!connectedMap.get(edge)){
                            nextGraph.addDestination(newEdgePos, graph.getDestination(edge));
                            nextGraph.addWeight(newEdgePos, graph.getWeight(edge));
                            newEdgePos++;
                        }
                    }
                }
                handoffNewStructures();
                synchronizeStep();
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
            System.out.println();
        }

        public void synchronizeStep(){
            lock.lock();
            if(numThreadsLeftToSynchronize.get() == 1){
                numThreadsLeftToSynchronize.set(numWorkingThreads.get());
                isSleeping.signalAll();
            }
            else{
                try {
                    numThreadsLeftToSynchronize.decrementAndGet();
                    isSleeping.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            lock.unlock();
        }

        public void buildNewFirstEdges(){
            if(isAuthority){
                // wait until authority is only active thread
                while(numActiveThreads.get() > 1){}
                int i;

                // perform exclusive prefix sum
                nextGraph.addFirstEdge(0, 0);
                for(i = 1; i < nextGraph.numNodes; i++){
                    nextGraph.addFirstEdge(i, nextGraph.getOutDegree(i-1) + nextGraph.getFirstEdge(i-1));
                }
                // initialize edges
                nextGraph.initializeEdges(nextGraph.getFirstEdge(i-1)+nextGraph.getOutDegree(i-1));

                // send done signal
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
                if(numNodesPerThread == 0){
                    numNodesPerThread = 1;
                }

                // evenly distribute new nodes amongst each thread
                for(Set<Integer> nodes: allNodes){
                    for(int i = 0; i < numNodesPerThread; i++){
                        if(i == numNodesPerThread-1){
                            if(!newNodes.isEmpty()){
                                nodes.add(graph.getNewName(newNodes.remove()));
                            }
                            else{
                                break;
                            }
                        }
                        else{
                            nodes.add(graph.getNewName(newNodes.remove()));
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


    public void run() throws InterruptedException {
        for(Thread thread: threads){
            thread.start();
        }
    }
}
