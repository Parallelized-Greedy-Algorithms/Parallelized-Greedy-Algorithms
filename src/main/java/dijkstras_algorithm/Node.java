package dijkstras_algorithm;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Node {
    public int id;
    private Map<Node, Integer> neighbors;
    private int distToSource;
    private Node prevNode;

    public Node(int id){
        this.id = id;
        this.neighbors = new HashMap<>();
        distToSource = Integer.MAX_VALUE;
        prevNode = null;
    }

    public Node(Node node){
        this.id = node.id;
        this.neighbors = new HashMap<>(node.neighbors);
        this.distToSource = node.distToSource;
        this.prevNode = node.prevNode;
    }

    public void setNeighbor(Node destination, int distance){
        neighbors.put(destination, distance);
    }

    public Set<Node> getNeighbors(){
        return neighbors.keySet();
    }

    public int getDistanceToNeighbor(Node neighbor){
        if(!neighbors.containsKey(neighbor)){
            return Integer.MAX_VALUE;
        }
        return neighbors.get(neighbor);
    }

    public int getDistToSource(){
        return distToSource;
    }

    public Node getPrevNode(){
        return prevNode;
    }

    public void setDistToSource(int dist){
        distToSource = dist;
    }

    public void setPrevNode(Node node){
        prevNode = node;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return id == node.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString(){
        return String.valueOf(id);
    }
}
