package dijkstras_algorithm;

import java.util.HashSet;
import java.util.Set;

public class Graph {
    private final Set<Node> nodes;

    public Graph(){
        nodes = new HashSet<>();
    }

    public void createNode(int id){
        nodes.add(new Node(id));
    }

    public void createNodes(int numNodes){
        for(int i = 0; i < numNodes; i++){
            createNode(i);
        }
    }

    public void createEdge(Node a, Node b, int distance){
        a.setNeighbor(b, distance);
        b.setNeighbor(a, distance);
    }

    public Set<Node> getNodes(){
        return nodes;
    }
}
