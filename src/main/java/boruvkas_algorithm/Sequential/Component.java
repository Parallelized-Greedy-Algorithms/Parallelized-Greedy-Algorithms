package boruvkas_algorithm.Sequential;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Component {
    private static int id;
    private final int componentID;
    private final Set<Node> nodes;
    private final Map<Edge, Boolean> edges = new ConcurrentHashMap<>(); // edges connecting to other components
    private boolean newComponent;

    public Component(Node node){
        componentID = id++;
        this.nodes = new HashSet<>();
        nodes.add(node);
        newComponent = false;
    }

    public void addEdge(Edge edge, boolean isNode1){
        edges.put(edge, isNode1);
    }

    public boolean isNewComponent(){
        return newComponent;
    }

    public void addNodes(Set<Node> nodesToAdd){
        nodes.addAll(nodesToAdd);
    }

    public void setNewComponent(boolean val){
        newComponent = val;
    }

    public Set<Node> getNodes(){
        return nodes;
    }

    public void addEdges(Map<Edge, Boolean> edgesToAdd){
        edges.putAll(edgesToAdd);
    }

    public void removeEdge(Edge edge){
        edges.remove(edge);
    }

    public Map<Edge, Boolean> getEdges(){
        return edges;
    }

    @Override
    public String toString(){
        return nodes.toString();
    }

    public String getComponentID(){
        return "C" + componentID;
    }
}
