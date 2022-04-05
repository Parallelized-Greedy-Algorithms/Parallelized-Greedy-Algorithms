package boruvkas_algorithm;

import java.util.*;

public class Component {
    private final Set<Integer> nodes;
    private final Map<Edge, Boolean> edges;
    private Map.Entry<Edge, Boolean> shortestEdge; // true designates node1 is in nodes

    public Component(Integer node){
        this.nodes = new HashSet<>();
        nodes.add(node);
        edges = new HashMap<>();
    }

    public boolean contains(int node){
        return nodes.contains(node);
    }

    public void addEdge(Edge edge, boolean isNode1){
        edges.put(edge, isNode1);
    }

    public void clearEdges(){
        edges.clear();
    }

    public void addNodes(Set<Integer> nodesToAdd){
        nodes.addAll(nodesToAdd);
    }

    public void addNode(Integer nodeToAdd){
        nodes.add(nodeToAdd);
    }

    public Set<Integer> getNodes(){
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
    public int getCurCompNode(){
        if(shortestEdge.getValue()){
            return shortestEdge.getKey().getNode1();
        }
        else{
            return shortestEdge.getKey().getNode2();
        }
    }

    public int getNextCompNode(){
        if(shortestEdge.getValue()){
            return shortestEdge.getKey().getNode2();
        }
        else{
            return shortestEdge.getKey().getNode1();
        }
    }

    @Override
    public String toString(){
        if(shortestEdge != null){
            return nodes.toString() + " | shortestNode: " + shortestEdge.getKey();
        }
        else{
            return nodes.toString();
        }
    }
}
