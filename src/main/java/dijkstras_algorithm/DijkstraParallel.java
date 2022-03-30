package dijkstras_algorithm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DijkstraParallel {
    private final Set<Thread> threads;
    protected final Set<Node> decideGlobalMinSet;
    protected final AtomicMarkableReference<Node> globalMinNodeReference;
    protected final AtomicBoolean previousAuthorityToggle;
    protected final AtomicInteger numActiveThreads;
    private final AtomicInteger numThreads;
    protected final Set<Node> nodes;
    protected final Node source;

    protected static final Node nullNode = new Node(-1);

    public DijkstraParallel(Set<Node> nodes, Node source) {
        this.nodes = nodes;
        this.source = source;
        threads = new HashSet<>();
        decideGlobalMinSet = ConcurrentHashMap.newKeySet();
        globalMinNodeReference = new AtomicMarkableReference<>(source, true);
        previousAuthorityToggle = new AtomicBoolean(false);
        numActiveThreads = new AtomicInteger(DijkstraDriver.PROCESSORS);
        this.numThreads = new AtomicInteger(numActiveThreads.get());

        LinkedList<Node> tempNodes = new LinkedList<>(nodes);

        ArrayList<Set<Node>> nodesPerThread = new ArrayList<>();
        for (int i = 0; i < numActiveThreads.get(); i++) {
            nodesPerThread.add(i, new HashSet<>());
        }

        while (!tempNodes.isEmpty()) {
            for (int i = 0; i < numActiveThreads.get(); i++) {
                if (tempNodes.isEmpty()) {
                    break;
                }
                nodesPerThread.get(i).add(tempNodes.remove());
            }
        }

        for (int i = 0; i < numActiveThreads.get(); i++) {
            threads.add(new Thread(new Partition(i == 0 ? true : false, nodesPerThread.get(i), numActiveThreads)));
        }

        nodesPerThread.clear();
        source.setDistToSource(0);
    }

    private class Partition implements Runnable{
        private static Logger log = LogManager.getLogger(DijkstraParallel.class);
        private final boolean isGlobalAuthority;
        private final Set<Node> localNodes;
//        private Set<Node> cluster;
        private Set<Node> nonCluster;
        private Node curGlobalMin;

        public Partition(boolean isGlobalAuthority, Set<Node> localNodes, AtomicInteger numActiveThreads){

            this.isGlobalAuthority = isGlobalAuthority;
            this.localNodes = localNodes;

//            cluster = new HashSet<>();
//            cluster.add(source);
            curGlobalMin = source;
            log.info("SOURCE: " + source.id);
            log.info("LOCAL NODES: " + localNodes);
        }

        private Node extractGlobalMin(Node localMinNode){
            if(isGlobalAuthority){
                log.info("AUTH: waiting if true: num threads " + numActiveThreads.get() + " > 1");
                // busy wait until the only active thread is the Authority
                while(numActiveThreads.get() > 1){}
                log.info("AUTH: active");

//                String out = "";

                Node globalMinNode = localMinNode;
                if(!globalMinNode.equals(nullNode)){
//                    out += "node: " + localMinNode.id + " dist: " + localMinNode.getDistToSource() + "\n";
                }

                // iterate over each thread's local minimum node to find global minimum
                for(Node node: decideGlobalMinSet){
                    if(node.getDistToSource() < globalMinNode.getDistToSource()){
                        globalMinNode = node;
                    }
//                    out += "node: " + node.id + " dist: " + node.getDistToSource() + "\n";
                }

//                log.info("AUTH: declares minimum node: " + globalMinNode + "\n" + out);
                // clear min set
                decideGlobalMinSet.clear();
                numActiveThreads.set(numThreads.get());

                // set the global minimum node and set the mark to be the opposite of the previousAuthorityToggle
                globalMinNodeReference.set(globalMinNode, !globalMinNodeReference.isMarked());
                log.info("numThreads: " + numThreads.get());
                log.info("numActiveThreads: " + numActiveThreads.get());
            }
            else{
                // previous status of Authority
                boolean prevAuth = globalMinNodeReference.isMarked();

                // Give Authority thread's local minimum node if it exists
                if(!localMinNode.equals(nullNode)){
                    decideGlobalMinSet.add(localMinNode);
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
            nonCluster = new HashSet<>(localNodes);
            localNodes.clear();

            if(nonCluster.contains(source)){
                nonCluster.remove(source);
            }

            while(!nonCluster.isEmpty()){

                log.info("Non-cluster: " + nonCluster);

                Node localMinNode = nullNode;
                for(Node outsideNode: nonCluster){
                    int distFromRoot = curGlobalMin.getDistToSource() + curGlobalMin.getDistanceToNeighbor(outsideNode);

                    if(distFromRoot < outsideNode.getDistToSource()){
                        outsideNode.setDistToSource(distFromRoot);
                        outsideNode.setPrevNode(curGlobalMin);
                    }

                    if(outsideNode.getDistToSource() < localMinNode.getDistToSource()){
                        localMinNode = outsideNode;
                    }
                }
                log.info("local min node: " + localMinNode.id + " dist: " + localMinNode.getDistToSource());

                curGlobalMin = extractGlobalMin(localMinNode);

//                cluster.add(curGlobalMin);
                if(nonCluster.contains(curGlobalMin)){
                    nonCluster.remove(curGlobalMin);
                }
            }

            if(isGlobalAuthority){
                log.info("Authority Thread FINISHED.");
//                numThreads.decrementAndGet();
                while(numThreads.get() > 1){
                    extractGlobalMin(nullNode);
                }
                numActiveThreads.decrementAndGet();
            }
            else{
                log.info("Thread FINISHED.");
                int threadsBefore = numThreads.get();
                int before = numActiveThreads.get();
                numThreads.decrementAndGet();
                numActiveThreads.decrementAndGet();
                int threadsAfter = numThreads.get();
                int after = numActiveThreads.get();

                log.info("Number of threads decreased from " + threadsBefore + " -> " + threadsAfter);
                log.info("numActiveThreads: " + before + " -> " + after);
            }
        }
    }

    public void run() {
        for(Thread thread: threads){
            thread.start();
        }
        while(numActiveThreads.get() > 0){}
    }


//    public String toString(){
//        StringBuilder out = new StringBuilder();
//
//        for(Node node: nodes){
//            if(node.equals(source)){
//                continue;
//            }
//            out.append("\n\nShortest path ").append(source.id).append(" -> ").
//                    append(node.id).append(" with total distance of ").append(node.getDistToSource()).append("\n\t");
//
//            Node curNode = node;
//            Node prevNode = node.getPrevNode();
//            Stack<String> stringStack = new Stack<>();
//
//            stringStack.add(String.valueOf(node.id));
//            do{
//                stringStack.add(prevNode.id + " --[" + prevNode.getDistanceToNeighbor(curNode) + "]-> ");
//                curNode = prevNode;
//                prevNode = curNode.getPrevNode();
//            }while(prevNode != null);
//
//            while(!stringStack.isEmpty()){
//                out.append(stringStack.pop());
//            }
//        }
//
//        return out.toString();
//    }
//
}
