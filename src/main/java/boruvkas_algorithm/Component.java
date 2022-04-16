package boruvkas_algorithm;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Component {
    private final Set<Node> nodes;
    private final Map<Edge, Boolean> edges = new ConcurrentHashMap<>(); // edges connecting to other components
    private Map.Entry<Edge, Boolean> shortestEdge; // true designates node1 is in nodes
    private boolean newComponent;

    public Component(Node node){
        this.nodes = new HashSet<>();
        nodes.add(node);
//        edges = new HashMap<>();
        newComponent = false;
    }

    public Component(Component comp1, Component comp2){
        this.nodes = new HashSet<>();
//        this.edges = new HashMap<>();
        nodes.addAll(comp1.getNodes());
        nodes.addAll(comp2.getNodes());
        edges.putAll(comp1.getEdges());
        edges.putAll(comp2.getEdges());
//        newComponent = true;
    }

    public boolean contains(Node node){
        return nodes.contains(node);
    }

    public void addEdge(Edge edge, boolean isNode1){
        edges.put(edge, isNode1);
    }

    public void clearEdges(){
        edges.clear();
    }

    public boolean isNewComponent(){
        return newComponent;
    }

    public void addNodes(Set<Node> nodesToAdd){
        nodes.addAll(nodesToAdd);
    }

    public void addNode(Node nodeToAdd){
        nodes.add(nodeToAdd);
    }

    public void setNewComponent(boolean val){
        newComponent = val;
    }

    public Set<Node> getNodes(){
        return nodes;
    }

    public void setShortestEdge(Map.Entry<Edge, Boolean> entry){
        shortestEdge = entry;
    }

    public void removeEdgesExcept(Map<Edge, Boolean> edges, Edge except){
        Iterator<Map.Entry<Edge, Boolean>> it = edges.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<Edge, Boolean> curEdgeEntry = it.next();
            if(curEdgeEntry.getKey().equals(except)){
                continue;
            }
            it.remove();
        }
    }

    public Edge getShortestEdge(){
        return shortestEdge.getKey();
    }

    public void addEdges(Map<Edge, Boolean> edgesToAdd){
        edges.putAll(edgesToAdd);
    }

    public void removeEdge(Edge edge){
        edges.remove(edge);
    }

    public void clearShortestEdge(){
        shortestEdge = null;
    }

    public Map<Edge, Boolean> getEdges(){
        return edges;
    }

    // get node not in nodes set
    public Node getCurCompNode(){
        if(shortestEdge.getValue()){
            return shortestEdge.getKey().getNode1();
        }
        else{
            return shortestEdge.getKey().getNode2();
        }
    }

    public Node getNextCompNode(){
        if(shortestEdge.getValue()){
            return shortestEdge.getKey().getNode2();
        }
        else{
            return shortestEdge.getKey().getNode1();
        }
    }

    @Override
    public String toString(){
//        if(shortestEdge != null){
//            return nodes.toString() + " | : " + shortestEdge.getKey();
//        }
//        else{
//            return nodes.toString();
//        }
        return nodes.toString();
    }
}
