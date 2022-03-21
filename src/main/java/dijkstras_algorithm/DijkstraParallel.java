package dijkstras_algorithm;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DijkstraParallel extends DijkstraCore {
    private final Set<Thread> threads;
    private final AtomicInteger numActiveThreads;

    public DijkstraParallel(Set<Node> nodes, Node source) {
        super(nodes, source);
        dist = new HashMap<>();
        prev = new HashMap<>();
        queue = new LinkedList<>();
        threads = new HashSet<>();

        numActiveThreads = new AtomicInteger(nodes.size());
        for(Node node: nodes){
            DijkstraSSSP sssp = new DijkstraSSSP(node, numActiveThreads);
            Thread thread = new Thread(sssp);
            threads.add(thread);
        }
    }

    private class DijkstraSSSP implements Runnable{
        private final Node target;
        private final Map<Node, Integer> localDist; // distance from source to key
        private final Map<Node, Node> localPrev; // value is next node on the path from the key Node to the source
        private final Queue<Node> localQueue;
        private final AtomicInteger numActiveThreads;

        public DijkstraSSSP(Node target, AtomicInteger numActiveThreads){
            this.target = target;
            this.numActiveThreads = numActiveThreads;
            localDist = new HashMap<>();
            localPrev = new HashMap<>();
            localQueue = new LinkedList<>();

        }

        private Node getShortestDistance(Queue<Node> queue){
            Node curNode = null;
            int smallestDist = Integer.MAX_VALUE;
            for(Node node: queue){
                int curDist = localDist.get(node);
                if(curDist < smallestDist){
                    smallestDist = curDist;
                    curNode = node;
                }
            }
            return curNode;
        }

        @Override
        public void run() {
            for(Node node: nodes){
                localDist.put(node, Integer.MAX_VALUE);
                localPrev.put(node, null);
                localQueue.add(node);
            }
            localDist.put(source, 0);

            while(!localQueue.isEmpty()){
                Node curNode = getShortestDistance(localQueue);
                if(curNode.equals(target)){
                    dist.put(target, localDist.get(curNode));
                    prev.put(target, localPrev.get(curNode));
                    numActiveThreads.decrementAndGet();
                    return;
                }

                localQueue.remove(curNode);

                for(Node neighbor: curNode.getNeighbors()){
                    int distFromRoot = localDist.get(curNode) + curNode.getDistanceToNeighbor(neighbor);

                    if(distFromRoot < localDist.get(neighbor)){
                        localDist.put(neighbor, distFromRoot);
                        localPrev.put(neighbor, curNode);
                    }
                }
            }

            dist.put(target, Integer.MAX_VALUE);
            numActiveThreads.decrementAndGet();
        }
    }

    @Override
    public void run() {
        for(Thread thread: threads){
            thread.start();
        }
        while(numActiveThreads.get() > 0){}
    }
}
