package dijkstras_algorithm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class DijkstraParallel2 {
    protected final int numNodes;
    protected final int[][] edges;
    protected final int source;
    protected AtomicInteger numActiveThreads;
    protected AtomicInteger numThreads;
    protected final Set<GlobalData> decideGlobalMinSet;
    protected final AtomicMarkableReference<GlobalData> globalMinNodeReference;
    protected ArrayList<Integer> globalDist;
    protected ArrayList<Integer> globalPrev;

    protected final GlobalData nullNode = new GlobalData(Integer.MAX_VALUE, -1, -1);

    private Set<Thread> threads;

    public DijkstraParallel2(Set<Node> nodes, Node source){
        this.source = source.id;
        this.edges = new int[nodes.size()][nodes.size()];

        numNodes = nodes.size();
        numActiveThreads = new AtomicInteger(Runtime.getRuntime().availableProcessors());
        numThreads = new AtomicInteger(Runtime.getRuntime().availableProcessors());
        threads = new HashSet<>();
        decideGlobalMinSet = ConcurrentHashMap.newKeySet();
        globalMinNodeReference = new AtomicMarkableReference<>(
                new GlobalData(Integer.MAX_VALUE, -1, -1), true);
        globalDist = new ArrayList<>();
        globalPrev = new ArrayList<>();

        for(int i = 0; i < numNodes; i++){
            globalDist.add(Integer.MAX_VALUE);
            globalPrev.add(-1);
        }

        ArrayList<Node> sortedNodes = new ArrayList<>(nodes);
        sortedNodes.sort(Comparator.comparing(Node::getId));

        int i = 0;
        for(Node node: sortedNodes){
            int j = 0;
            for(Node node2: sortedNodes){
                this.edges[i][j] = node.getDistanceToNeighbor(node2);
                j++;
            }
            i++;
        }

        int nodesToAdd = numNodes;
        int extraNodes = numNodes % numThreads.get();
        int nodesPerThread = Math.floorDiv(numNodes, numThreads.get());

        for(i = 0; i < numThreads.get(); i++){
            int[] localNodes;
            if(extraNodes > 0){
                localNodes = new int[nodesPerThread+1];
                localNodes[localNodes.length-1] = --nodesToAdd;
                extraNodes--;
            }
            else{
                localNodes = new int[nodesPerThread];
            }

            for(int k = 0; k < nodesPerThread; k++){
                localNodes[k] = --nodesToAdd;
            }

            Set<Integer> nonClusterNodes = new HashSet<>();
            for(int k = 0; k < localNodes.length; k++){
                if(localNodes[k] != this.source){
                    nonClusterNodes.add(localNodes[k]);
                }
            }

            threads.add(new Thread(new Partition(i == 0 ? true : false, nonClusterNodes, localNodes)));
        }



    }

    protected class GlobalData{
        public int distance;
        public int nodeId;
        public int prevId;

        GlobalData(int distance, int nodeId, int prevId){
            this.distance = distance;
            this.nodeId = nodeId;
            this.prevId = prevId;
        }
    }

    public class Partition implements Runnable{
        private Set<Integer> nonClusterNodes;
        private int[] localNodes;
        private int[] localDistances;
        private int[] localPrevNodes;
        private int curGlobalMinNode;

        private static Logger log = LogManager.getLogger(DijkstraParallel.class);
        private final boolean isGlobalAuthority;

        public Partition(boolean isGlobalAuthority, Set<Integer> nonClusterNodes, int[] localNodes){
            this.isGlobalAuthority = isGlobalAuthority;
            this.localNodes = localNodes;
            this.curGlobalMinNode = source;
            this.nonClusterNodes = nonClusterNodes;

            localDistances = new int[numNodes];
            localPrevNodes = new int[numNodes];

            for(int i = 0; i < numNodes; i++){
                localDistances[i] = Integer.MAX_VALUE;
                localPrevNodes[i] = -1;
            }
            localDistances[source] = 0;
        }

        protected synchronized void updateGlobalLists(){
            for(int i = 0; i < numNodes; i++){
                if(localDistances[i] < globalDist.get(i)){
                    globalDist.set(i, localDistances[i]);
                    globalPrev.set(i, localPrevNodes[i]);
                }
            }
        }


        private GlobalData extractGlobalMin(GlobalData minData){
            if(isGlobalAuthority){
                log.info("AUTH: waiting if true: num threads " + numActiveThreads.get() + " > 1");
                // busy wait until the only active thread is the Authority
                while(numActiveThreads.get() > 1){}
                log.info("AUTH: active");

//                String out = "";

                GlobalData globalMinData = minData;

                if(globalMinData.nodeId != -1){
//                    out += "node: " + localMinNode+ " dist: " + localDistance + "\n";
                }

                // iterate over each thread's local minimum node to find global minimum
                for(GlobalData node: decideGlobalMinSet){
                    if(node.distance < globalMinData.distance){
                        globalMinData = node;
                    }
//                    out += "node: " + node.nodeId + " dist: " + node.distance + "\n";
                }

//                log.info("AUTH: declares minimum node: " + globalMinData.nodeId + "\n" +  out);
                log.info("AUTH: declares minimum node: " + globalMinData.nodeId + "\n");
                // clear min set
                decideGlobalMinSet.clear();
                numActiveThreads.set(numThreads.get());

                // set the global minimum node and set the mark to be the opposite of the previousAuthorityToggle
                globalMinNodeReference.set(globalMinData, !globalMinNodeReference.isMarked());
                log.info("numThreads: " + numThreads.get());
                log.info("numActiveThreads: " + numActiveThreads.get());
            }
            else{
                // previous status of Authority
                boolean prevAuth = globalMinNodeReference.isMarked();

                // Give Authority thread's local minimum node if it exists
                if(minData.nodeId != -1){
                    decideGlobalMinSet.add(minData);
                }

                int before = numActiveThreads.get();
                // set thread to inactive state
                numActiveThreads.decrementAndGet();
                int after = numActiveThreads.get();
                log.info("numActiveThreads: " + before + "-> " + after);

                log.info("REG: waiting");
                // busy wait until Authority has finished finding global minimum node
                while(globalMinNodeReference.isMarked() == prevAuth){}
                log.info("REG: activated");

            }
            return globalMinNodeReference.getReference();
        }

        @Override
        public void run() {
            while(!nonClusterNodes.isEmpty()){
                log.info("Non-cluster: " + nonClusterNodes);

                int localMinNode = -1;
                for(Integer outsideNode: nonClusterNodes){
                    int distFromRoot = localDistances[curGlobalMinNode] + edges[curGlobalMinNode][outsideNode];

                    if(distFromRoot < localDistances[outsideNode]){
                        localDistances[outsideNode] = distFromRoot;
                        localPrevNodes[outsideNode] = curGlobalMinNode;
                    }

                    if(localMinNode == -1 || localDistances[outsideNode] < localDistances[localMinNode]){
                        localMinNode = outsideNode;
                    }
                }
                log.info("local min node: " + localMinNode + " dist: " + localDistances[localMinNode]);

                GlobalData localData;
                if(localMinNode != -1){
                    localData = new GlobalData(
                            localDistances[localMinNode], localMinNode, localPrevNodes[localMinNode]);
                }
                else{
                    localData = nullNode;
                }
                GlobalData minEntry = extractGlobalMin(localData);
                curGlobalMinNode = minEntry.nodeId;
                localDistances[minEntry.nodeId] = minEntry.distance;
                localPrevNodes[minEntry.nodeId] = minEntry.prevId;

                if(nonClusterNodes.contains(curGlobalMinNode)){
                    nonClusterNodes.remove(curGlobalMinNode);
                }
            }
            if(isGlobalAuthority){
                log.info("Authority Thread FINISHED.");
                while(numThreads.get() > 1){
                    extractGlobalMin(nullNode);
                }
            }
            else{
                log.info("Thread FINISHED.");
                int threadsBefore = numThreads.get();
                int before = numActiveThreads.get();
                numThreads.decrementAndGet();
//                numActiveThreads.decrementAndGet();
//                int threadsAfter = numThreads.get();
//                int after = numActiveThreads.get();

//                log.info("Number of threads decreased from " + threadsBefore + " -> " + threadsAfter);
//                log.info("numActiveThreads: " + before + " -> " + after);
            }

            numActiveThreads.decrementAndGet();
            updateGlobalLists();
            log.info("numActiveThreads: " + numActiveThreads.get());
        }
    }

    public void run(){
        for(Thread thread: threads){
            thread.start();
        }
        while(numActiveThreads.get() > 0){}
    }

    public String toString(){
        StringBuilder out = new StringBuilder();

        for(int i = 0; i < numNodes; i++){
            if(i == source){
                continue;
            }
            out.append("\n\nShortest path ").append(source).append(" -> ").
                    append(i).append(" with total distance of ").append(globalDist.get(i)).append("\n\t");

            int curNode = i;
            int prevNode = globalPrev.get(i);
            Stack<String> stringStack = new Stack<>();

            stringStack.add(String.valueOf(i));
            do{
                stringStack.add(prevNode + " --[" + edges[curNode][prevNode] + "]-> ");
                curNode = prevNode;
                prevNode = globalPrev.get(curNode);
            }while(prevNode != -1);

            while(!stringStack.isEmpty()){
                out.append(stringStack.pop());
            }
        }

        return out.toString();
    }

}
