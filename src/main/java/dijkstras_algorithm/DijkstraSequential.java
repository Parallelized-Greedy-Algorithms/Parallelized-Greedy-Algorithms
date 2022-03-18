package dijkstras_algorithm;

import java.util.*;

// https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
public class DijkstraSequential extends DijkstraCore {
    protected Queue<Node> queue;

    public DijkstraSequential(Set<Node> nodes, Node source){
        super(nodes, source);
        queue = new LinkedList<>();
    }

    private Node getShortestDistance(){
        Node curNode = null;
        int smallestDist = Integer.MAX_VALUE;
        for(Node node: queue){
            int curDist = dist.get(node);
            if(curDist < smallestDist){
                smallestDist = curDist;
                curNode = node;
            }
        }
        return curNode;
    }

    public void run(){
        for(Node node: nodes){
            dist.put(node, Integer.MAX_VALUE);
            prev.put(node, null);
            queue.add(node);
        }
        dist.put(source, 0);

        while(!queue.isEmpty()){
            Node curNode = getShortestDistance();

            queue.remove(curNode);

            for(Node neighbor: curNode.getNeighbors()){
                int distFromRoot = dist.get(curNode) + curNode.getDistanceToNeighbor(neighbor);

                if(distFromRoot < dist.get(neighbor)){
                    dist.put(neighbor, distFromRoot);
                    prev.put(neighbor, curNode);
                }
            }
        }
    }


}
