package dijkstras_algorithm;

import java.util.*;

public abstract class DijkstraCore {
    protected final Set<Node> nodes;
    protected final Node source;
    protected Map<Node, Integer> dist; // distance from source to key
    protected Map<Node, Node> prev; // value is next node on the path from the key Node to the source

    public DijkstraCore(Set<Node> nodes, Node source){
        this.nodes = nodes;
        this.source = source;
        dist = new HashMap<>();
        prev = new HashMap<>();
    }

    public Map<Node, Integer> getDistMap(){
        return dist;
    }

    public Map<Node, Node> getPrevMap(){
        return prev;
    }

    public abstract void run();

    @Override
    public String toString(){
        StringBuilder out = new StringBuilder();

        for(Map.Entry<Node, Integer> distEntry: dist.entrySet()){
            Node destination = distEntry.getKey();
            int totalDistance = distEntry.getValue();
            if(destination.equals(source)){
                continue;
            }

            out.append("\n\nShortest path ").append(source.id).append(" -> ").
                    append(destination.id).append(" with total distance of ").append(totalDistance).append("\n\t");

            Node curNode = destination;
            Node prevNode = prev.get(destination);
            Stack<String> stringStack = new Stack<>();

            stringStack.add(String.valueOf(destination.id));
            do{
                stringStack.add(prevNode.id + " --[" + prevNode.getDistanceToNeighbor(curNode) + "]-> ");
                curNode = prevNode;
                prevNode = prev.get(curNode);
            }while(prevNode != null);

            while(!stringStack.isEmpty()){
                out.append(stringStack.pop());
            }
        }

        return out.toString();
    }

}
