package dijkstras_algorithm;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Node {
    public int id;
    private Map<Node, Integer> neighbors;

    public Node(int id){
        this.id = id;
        this.neighbors = new HashMap<>();
    }

    public void setNeighbor(Node destination, int distance){
        neighbors.put(destination, distance);
    }

    public Set<Node> getNeighbors(){
        return neighbors.keySet();
    }

    public int getDistanceToNeighbor(Node neighbor){
        return neighbors.get(neighbor);
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
