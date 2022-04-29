package boruvkas_algorithm.Parallel;

import boruvkas_algorithm.Sequential.Edge;
import boruvkas_algorithm.Sequential.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BoruvkaParallel {
    private static final Logger log = LogManager.getLogger(BoruvkaParallel.class);
    protected Queue<Integer> newNodes;
    protected Map<Integer, Boolean> connectedMap;
    protected final AtomicInteger numWorkingThreads; // total threads either executing code or busy waiting
    protected final AtomicInteger numThreadsLeftToSynchronize;
    protected final int numProcessors;
    protected CSRGraph graph;
    protected CSRGraph nextGraph;
    protected ArrayList<Unit> nextOutDegrees;

    protected final Map<Integer, Set<Integer>> threadToLocalNodesMap;
    protected final Map<Integer, AtomicBoolean> threadToIsFinishedMap;
    protected final Set<Thread> threads;

    protected final Set<Node> oldNodes;
    protected final Set<Edge> oldEdges;

    protected final Lock lock;
    protected final Condition isSleeping;

    protected final Set<Edge> connectedEdges;
    protected final Map<Integer, Integer> vertexToComponentMap;

    protected int numConnectedEdges;

    public BoruvkaParallel(int numProcessors, Set<Node> nodes, Set<Edge> edges){
        this.numProcessors = numProcessors;
        oldNodes = nodes;
        oldEdges = edges;

        newNodes = new ConcurrentLinkedQueue<>();
        connectedMap = new ConcurrentSkipListMap<>();
        threadToLocalNodesMap = new HashMap<>();
        threadToIsFinishedMap = new HashMap<>();

        threads = new HashSet<>();

        numWorkingThreads = new AtomicInteger(numProcessors);
        numThreadsLeftToSynchronize = new AtomicInteger(numProcessors);

        graph = new CSRGraph(nodes, edges);
        nextOutDegrees = new ArrayList<>(edges.size());

        lock = new ReentrantLock();
        isSleeping = lock.newCondition();

        connectedEdges = ConcurrentHashMap.newKeySet();
        vertexToComponentMap = new HashMap<>();
        for(int i = 0; i < nodes.size(); i++){
            vertexToComponentMap.put(i, i);
        }

        numConnectedEdges = 0;

        int nodeNum = 0;
        int baseNodesPerThread = graph.numNodes / numWorkingThreads.get();
        int extraNodesPerThread = graph.numNodes % numWorkingThreads.get();
        for(int i = 0; i < numProcessors; i++){
            Set<Integer> localNodes = new LinkedHashSet<>();
            for(int j = 0; j < baseNodesPerThread; j++){
                localNodes.add(nodeNum++);
            }
            if(extraNodesPerThread > 0){
                localNodes.add(nodeNum++);
                extraNodesPerThread--;
            }

            AtomicBoolean isFinished = new AtomicBoolean(false);
            threads.add(new Thread(new Worker(i == 0 ? true : false, i, localNodes, isFinished)));
            threadToLocalNodesMap.put(i, localNodes);
            threadToIsFinishedMap.put(i, isFinished);
        }
    }

    private class Worker implements Runnable{
        private Set<Integer> localNodes;
        private final boolean isAuthority;
        private final int threadID;

        private final AtomicBoolean isFinished;

        public Worker(boolean isAuthority, int threadID, Set<Integer> localNodes, AtomicBoolean isFinished){
            this.isAuthority = isAuthority;
            this.localNodes = localNodes;
            this.threadID = threadID;
            this.isFinished = isFinished;
        }

        @Override
        public void run() {
            while(!localNodes.isEmpty() && graph.numNodes > 1){
                /** (a) Find minimum edge per vertex
                 *  The algorithm starts off by selecting the minimum weight edge for each vertex
                 *  When the vertex has multiple edges with the same minimum height, the edge with
                 *  the smallest destination vertex id is selected.
                 *  The selected minimum edge is stored in vertexMinEdge
                 */
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

                /** (b) Remove mirrored edges from vertexMinEdge
                 *  Mirrored edges are removed if the successor of a vertex successor is the vertex itself
                 *  When a mirrored edge is found, the edge is removed once from the vertexMinEdge list
                 */
                Set<Integer> nodesToRemove = new HashSet<>();
                for(Integer node: localNodes){
                    // node has smaller ID than its successor
                    if(node < (vertexToComponentMap.get(graph.getDestination(graph.getNodeMinEdge(node)))) &&
                            // the successor was already found to be a representative node
                            (
                                    // the successor of node is node itself
                                    vertexToComponentMap.get(graph.getDestination(graph.getNodeMinEdge(vertexToComponentMap.get(
                                    graph.getDestination(graph.getNodeMinEdge(node)))))).equals(node))){
                        nodesToRemove.add(node);
                    }
                }

                synchronizeStep();
                for(Integer node: nodesToRemove){
                    graph.setNodeMinEdge(node, -1);
                }
                synchronizeStep();

                /** (c) initialize and propagate colors (components)
                 *  In order to contract the graph, connected components must be identified.
                 *  Each connected component will be a super-vertex in the contracted graph
                 *  Each vertex is initialized with the same color as their successor's id in graph.colors.
                 *  If the vertex has no successor, its successor is set to itself.
                 *  This process repeats until it converges.
                 */
                for(Integer node: localNodes){
                    Integer edge = graph.getNodeMinEdge(node);
                    // if edge is null, node is a representative for the component
                    if(edge == -1){
                        graph.setColor(node, node);
                        newNodes.add(node);
                        graph.setFlag(node, 1);
                    }
                }
                synchronizeStep(); // unneeded
                for(Integer node: localNodes){
                    Integer edge = graph.getNodeMinEdge(node);
                    if(edge != -1){
                        // otherwise, find the representative for the component
                        int oldColor = graph.getDestination(graph.getNodeMinEdge(node));
                        int color = vertexToComponentMap.get(oldColor);
                        int cycleColor = color;
                        int numIterationsToWait = 0;
                        while(graph.getNodeMinEdge(color) != -1){
                            int prevColor = color;

                            oldColor = graph.getDestination(graph.getNodeMinEdge(color));
                            color = vertexToComponentMap.get(oldColor);

                            if(color == cycleColor){
                                graph.setNodeMinEdge(prevColor, -1);
                            }
                        }
                        graph.setColor(node, color);
//                        connectedEdges.add(new Edge(new Node(node), new Node(graph.getDestination(graph.getNodeMinEdge(node))), graph.getWeight(graph.getNodeMinEdge(node))));
                        numConnectedEdges += graph.getWeight(graph.getNodeMinEdge(node));
                    }
                }

                synchronizeStep();
                /** (d) create new vertex IDs
                 *  After converging, any vertex successor that is in the vertex itself will be called
                 *  the representative vertex for its component and is marked with a 1 in the flag array graph.flag.
                 *  An exclusive prefix sum is computed on the flag array to compute the new indices for the
                 *  representative nodes.
                 */
                if(isAuthority){
                    nextGraph = new CSRGraph(newNodes, graph.numNodes);
                    // create new indexes for nodes
                    nextGraph.setNameMap(0, 0);
                    for(int i = 1; i < graph.numNodes; i++){
                        int newName = graph.getFlag(i-1) + graph.getNewName(i-1);
                        graph.addNewName(i, newName);
                        // correctly set the name map for representative nodes
                        // from nextGraph's current node to its previously named node
                        if(graph.getFlag(i) == 1){
                            nextGraph.setNameMap(i, newName);
                        }
                    }
                    // correctly sets name map for non-representative nodes
                    for(int i = 0; i < graph.numNodes; i++){
                        if(graph.getFlag(i) == 0){
                            nextGraph.setNameMap(i, nextGraph.getMapNewName(graph.getColor(i)));
                        }
                    }
                }

                synchronizeStep();

                /** (e) Count, assign, and insert new edges after contraction
                 *  To build edge arrays for the contracted graph, it is first necessary to identify how
                 *  many edges each super-vertex will have, in order to assign new edge ids to the super-vertices.
                 *  This is achieved using a simple kernel that counts the number of edges that cross the
                 *  component for each vertex, and adds it to the outdegree list of its corresponding super-vertex.
                 *  An exclusive prefix sum is computed on the outdegree array in buildNewFirstEdges() to
                 *  build the firstedges list with the new edge indices.
                 */
                for(Integer node: localNodes){
                    int color = graph.getColor(node);
                    // iterate over edges of node
                    for(int i = graph.getFirstEdge(node); i < graph.getFirstEdge(node) + graph.getOutDegree(node); i++){

                        // if the edge crosses components
                        if(color != graph.getColor(vertexToComponentMap.get(graph.getDestination(i)))){
                            nextGraph.incOutDegree(graph.getNewName(color));
                        }
                    }
                }
                synchronizeStep();
                // authority thread builds newFirstEdges, non-authority threads wait
                buildNewFirstEdges();
                synchronizeStep();

                /** (f) build new edges from old graph
                 *  All edges that cross components are added to the contracted graph.
                 */
                for(Integer node: localNodes){
                    int color =graph.getColor(node);
                    for(int edge = graph.getFirstEdge(node); edge < graph.getFirstEdge(node) + graph.getOutDegree(node); edge++){
//                        if(!connectedMap.get(edge)){
                        // if the edge crosses components
                        if(color != graph.getColor(vertexToComponentMap.get(graph.getDestination(edge)))){
                            int newEdgePos = nextGraph.getNextEdge(graph.getNewName(graph.getColor(node)));
                            nextGraph.addDestination(newEdgePos, graph.getDestination(edge));
                            nextGraph.addWeight(newEdgePos, graph.getWeight(edge));
                        }
                    }
                }
                synchronizeStep();
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
//                numThreadsLeftToSynchronize.decrementAndGet();
                lock.lock();
                numWorkingThreads.decrementAndGet();
                numThreadsLeftToSynchronize.decrementAndGet();
                if(numThreadsLeftToSynchronize.get() == 0){
                    numThreadsLeftToSynchronize.set(numWorkingThreads.get());
                    isSleeping.signalAll(); // signal all sleeping threads to wake up
                }
                lock.unlock();
            }
            isFinished.set(true);
        }

        public void awakenThreads(){
            lock.lock();
            isSleeping.signalAll();
            lock.unlock();
        }

        public void synchronizeStep(){
            lock.lock();
            numThreadsLeftToSynchronize.decrementAndGet();
            if(numThreadsLeftToSynchronize.get() == 0){
                numThreadsLeftToSynchronize.set(numWorkingThreads.get());
                isSleeping.signalAll(); // signal all sleeping threads to wake up
            }
            else{
                try {
                    isSleeping.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            lock.unlock();
        }

        public void buildNewFirstEdges() {
            if(isAuthority){
                int i;

                // perform exclusive prefix sum
                if(nextGraph.numNodes > 0){
                    nextGraph.addFirstEdge(0, 0);
                    for(i = 1; i < nextGraph.numNodes; i++){
                        nextGraph.addFirstEdge(i, nextGraph.getOutDegree(i-1) + nextGraph.getFirstEdge(i-1));
                    }

                    // initialize edges
                    nextGraph.initializeEdges(nextGraph.getFirstEdge(i-1)+nextGraph.getOutDegree(i-1));
                }

                // initialize nextEdges
                nextGraph.initializeNextEdges();
            }
        }

        public void handoffNewStructures(){
            if(isAuthority){
                Collection<Set<Integer>> allNodes = threadToLocalNodesMap.values();
                for(Set<Integer> nodes: allNodes){
                    nodes.clear();
                }

                int baseNodesPerThread = newNodes.size() / numWorkingThreads.get();
                int extraNodesPerThread = newNodes.size() % numWorkingThreads.get();

                // evenly distribute new nodes amongst each thread

                for(Map.Entry<Integer, Set<Integer>> entry: threadToLocalNodesMap.entrySet()){
                    // if thread is finished, skip it
                    if(threadToIsFinishedMap.get(entry.getKey()).get()){
                        continue;
                    }

                    Set<Integer> nodes = entry.getValue();
                    for(int i = 0; i < baseNodesPerThread; i++){
                        nodes.add(graph.getNewName(newNodes.remove()));
                    }
                    if(extraNodesPerThread > 0){
                        nodes.add(graph.getNewName((newNodes.remove())));
                        extraNodesPerThread--;
                    }
                }

                // update vertex map
                for(Map.Entry<Integer, Integer> entry: vertexToComponentMap.entrySet()){
                    // set the new color of every vertex to the MapNewName value
                    entry.setValue(nextGraph.getMapNewName(entry.getValue()));
                }

                graph = nextGraph;
            }
        }
    }


    public int run() throws InterruptedException {
        for(Thread thread: threads){
            thread.start();
        }
        while(numWorkingThreads.get() > 1){}
        return numConnectedEdges;
    }
}
