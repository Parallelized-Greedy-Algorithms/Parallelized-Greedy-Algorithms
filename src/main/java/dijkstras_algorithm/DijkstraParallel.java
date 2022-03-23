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
    protected final Set<Node> nodes;
    protected final Node source;
    protected final ConcurrentMap<Node, Integer> globalDist;
    protected final ConcurrentMap<Node, Node> globalPrev;
    protected static Node nullNode = new Node(-1);

    public DijkstraParallel(Set<Node> nodes, Node source) {
        this.nodes = nodes;
        this.source = source;
        threads = new HashSet<>();
        decideGlobalMinSet = ConcurrentHashMap.newKeySet();
        globalMinNodeReference = new AtomicMarkableReference<>(source, true);
        previousAuthorityToggle = new AtomicBoolean(false);
        numActiveThreads = new AtomicInteger(Runtime.getRuntime().availableProcessors());
        globalDist = new ConcurrentHashMap<>();
        globalPrev = new ConcurrentHashMap<>();

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

        source.setDistToSource(0);
    }

    private class Partition implements Runnable{
        private static Logger log = LogManager.getLogger(DijkstraParallel.class);
        private final boolean isGlobalAuthority;
        private final Set<Node> localNodes;
        private final int numThreads;
        private final AtomicInteger numActiveThreads;
        private Set<Node> cluster;
        private Set<Node> nonCluster;
        private Node curGlobalMin;

        public Partition(boolean isGlobalAuthority, Set<Node> localNodes, AtomicInteger numActiveThreads){
            Configurator.initialize(new DefaultConfiguration());
            Configurator.setRootLevel(Level.INFO);

            this.isGlobalAuthority = isGlobalAuthority;
            this.localNodes = localNodes;
            this.numThreads = numActiveThreads.get();
            this.numActiveThreads = numActiveThreads;
            
            cluster = new HashSet<>();
            cluster.add(source);
            curGlobalMin = source;
            log.info("SOURCE: " + source.id);
            log.info("LOCAL NODES: " + localNodes);
        }

        private Node extractGlobalMin(Node localMinNode){
            if(isGlobalAuthority){
                log.info("AUTH: waiting");
                // busy wait until the only active thread is the Authority
                while(numActiveThreads.get() > 1){}
                log.info("AUTH: active");

                String out = "";

                // if localMin exists, initialize minEntry
                Node globalMinNode = null;
                if(!localMinNode.equals(nullNode)){
                    globalMinNode = localMinNode;
                    out += "node: " + localMinNode.id + " dist: " + localMinNode.getDistToSource() + "\n";
                }

                // iterate over each thread's local minimum node to find global minimum
                for(Node node: decideGlobalMinSet){
                    if(node.getDistToSource() < globalMinNode.getDistToSource()){
                        globalMinNode = node;
                    }
                    out += "node: " + node.id + " dist: " + node.getDistToSource() + "\n";
                }

                log.info("AUTH: declares minimum node: " + globalMinNode + "\n" + out);
                // set the global minimum node and set the mark to be the opposite of the previousAuthorityToggle
                globalMinNodeReference.set(globalMinNode, !globalMinNodeReference.isMarked());
                // clear min set
                decideGlobalMinSet.clear();
                return globalMinNodeReference.getReference();
            }
            else{
                // previous status of Authority
                boolean prevAuth = globalMinNodeReference.isMarked();

                // Give Authority thread's local minimum node if it exists
                if(!localMinNode.equals(nullNode)){
                    decideGlobalMinSet.add(localMinNode);
                }
                // set thread to inactive state
                numActiveThreads.decrementAndGet();

                log.info("REG: waiting");
                // busy wait until Authority has finished finding global minimum node
                while(globalMinNodeReference.isMarked() == prevAuth){}
                log.info("REG: activated");

                numActiveThreads.incrementAndGet();

                return globalMinNodeReference.getReference();
            }
        }

        @Override
        public void run() {
            nonCluster = new HashSet<>(localNodes);
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
                cluster.add(curGlobalMin);
                if(nonCluster.contains(curGlobalMin)){
                    nonCluster.remove(curGlobalMin);
                }
            }
            numActiveThreads.decrementAndGet();
        }
    }

    public void run() {
        for(Thread thread: threads){
            thread.start();
        }
        while(numActiveThreads.get() > 0){}
    }



    public String toString(){
        StringBuilder out = new StringBuilder();

        for(Node node: nodes){
            if(node.equals(source)){
                continue;
            }
            out.append("\n\nShortest path ").append(source.id).append(" -> ").
                    append(node.id).append(" with total distance of ").append(node.getDistToSource()).append("\n\t");

            Node curNode = node;
            Node prevNode = node.getPrevNode();
            Stack<String> stringStack = new Stack<>();

            stringStack.add(String.valueOf(node.id));
            do{
                stringStack.add(prevNode.id + " --[" + prevNode.getDistanceToNeighbor(curNode) + "]-> ");
                curNode = prevNode;
                prevNode = globalPrev.get(curNode);
            }while(prevNode != null);

            while(!stringStack.isEmpty()){
                out.append(stringStack.pop());
            }
        }

        return out.toString();
    }

}
