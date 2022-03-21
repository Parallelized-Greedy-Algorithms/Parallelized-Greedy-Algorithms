package dijkstras_algorithm;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class DijkstraParallel {
    private final Set<Thread> threads;
    protected final ThreadLocal<Map<Node, Integer>> dist;
    protected final ThreadLocal<Map<Node, Node>> prev;
    protected final ThreadLocal<Queue<Node>> queue;
    protected final ThreadLocal<Set<Node>> settledNodes;
    protected final ConcurrentMap<Node, Integer> decideGlobalMin;
    protected final AtomicMarkableReference<Node> globalMinimumNode;
    protected final AtomicBoolean previousAuthorityToggle;
    protected final AtomicInteger numActiveThreads;
    protected final Set<Node> nodes;
    protected final Node source;

    public DijkstraParallel(Set<Node> nodes, Node source) {
        dist = ThreadLocal.withInitial(HashMap::new);
        prev = ThreadLocal.withInitial(HashMap::new);
        queue = ThreadLocal.withInitial(LinkedList::new);
        settledNodes = ThreadLocal.withInitial(HashSet::new);

        this.nodes = nodes;
        this.source = source;
        threads = new HashSet<>();
        decideGlobalMin = new ConcurrentHashMap<>();
        globalMinimumNode = new AtomicMarkableReference<>(source, true);
        previousAuthorityToggle = new AtomicBoolean(true);
        numActiveThreads = new AtomicInteger(Runtime.getRuntime().availableProcessors());

        int numNodesPerPartition = (int) Math.floor(nodes.size()/numActiveThreads.get());
        LinkedList<Node> tempNodes = new LinkedList<>(nodes);

        for(int i = 0; i < numActiveThreads.get(); i++){
            // evenly distribute nodes across all partitions
            Set<Node> partitionNodes = new HashSet<>();
            for(int j = 0; j < numNodesPerPartition; j++){
                if(!tempNodes.isEmpty()){
                    partitionNodes.add(tempNodes.remove());
                }
            }
            threads.add(new Thread(new Partition(i == 0 ? true : false, partitionNodes, numActiveThreads)));
        }
    }

    private class Partition implements Runnable{
        private final boolean isGlobalAuthority;
        private final Set<Node> localNodes;
        private final int numThreads;
        private final AtomicInteger numActiveThreads;

        public Partition(boolean isGlobalAuthority, Set<Node> localNodes, AtomicInteger numActiveThreads){
            this.isGlobalAuthority = isGlobalAuthority;
            this.localNodes = localNodes;
            this.numThreads = numActiveThreads.get();
            this.numActiveThreads = numActiveThreads;
        }

        private Node extractLocalMin(){
            Node curNode = null;
            int smallestDist = Integer.MAX_VALUE;
            for(Node node: queue.get()){
                int curDist = dist.get().get(node);
                if(curDist < smallestDist){
                    smallestDist = curDist;
                    curNode = node;
                }
            }
            return curNode;
        }

        private Node extractGlobalMin(Node localMin){
            if(isGlobalAuthority){
                // busy wait until the only active thread is the Authority
                while(numActiveThreads.get() > 1){}
                Integer globalMin = dist.get().get(localMin);
                Node minNode = localMin;

                // iterate over each thread's local minimum node to find global minimum
                for(Map.Entry<Node, Integer> entry : decideGlobalMin.entrySet()){
                    if(entry.getValue() < globalMin){
                        globalMin = entry.getValue();
                        minNode = entry.getKey();
                    }
                }
                // set the global minimum node and set the mark to be the opposite of the previousAuthorityToggle
                globalMinimumNode.set(minNode, !previousAuthorityToggle.get());
                return minNode;
            }
            else{
                // previous status of Authority
                boolean prevAuth = previousAuthorityToggle.get();

                // Give Authority thread's local minimum node
                decideGlobalMin.put(localMin, dist.get().get(localMin));
                // set thread to inactive state
                numActiveThreads.decrementAndGet();

                // busy wait until Authority has finished finding global minimum node
                while(globalMinimumNode.isMarked() == prevAuth){}
                return globalMinimumNode.getReference();
            }
        }

        @Override
        public void run() {
            for(Node node: localNodes){
                dist.get().put(node, Integer.MAX_VALUE);
//                prev.get().put(node, null);
//                queue.get().add(node);
            }
            if(localNodes.contains(source)){
                settledNodes.get().add(source);
                dist.get().put(source, 0);
                queue.get().add(source);
            }
            boolean quit = false;

//            while(!quit){
//
//            }
            while(!queue.get().isEmpty()){
                Node curNode = extractGlobalMin(extractLocalMin());

                if(localNodes.contains(curNode)){
                    settledNodes.get().add(curNode);

                    for(Node neighbor: curNode.getNeighbors()){
                        int distFromRoot = dist.get().get(curNode) + curNode.getDistanceToNeighbor(neighbor);

                        if(distFromRoot < dist.get().get(neighbor)){
                            dist.get().put(neighbor, distFromRoot);
                            prev.get().put(neighbor, curNode);
                        }
                    }
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
}
