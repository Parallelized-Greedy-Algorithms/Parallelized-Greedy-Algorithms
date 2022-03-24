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

    public String toString(){
        StringBuilder out = new StringBuilder();
        for(Node node: nodes){
            out.append("Node " + node.id + ":\n");
            for(Node neighbor: node.getNeighbors()){
                out.append("\t--[" + node.getDistanceToNeighbor(neighbor) + "]-> Node " + neighbor + "\n");
            }
            out.append("\n");
        }
        return out.toString();
    }
}
