package boruvkas_algorithm;

import java.util.*;

public class BoruvkaSequential {
    private final int numNodes;
    private final Set<Edge> edges;
    private final Set<Component> components;
    private final Map<Integer, Component> componentsMap; // maps node id to Component

    public BoruvkaSequential(int numNodes, int[][] edges){
        this.numNodes = numNodes;
        this.edges = convertEdges(edges);

        components = new HashSet<>();
        componentsMap = new HashMap<>();

        for(int i = 0; i < numNodes; i++){
            Component curComp = new Component(i);
            components.add(curComp);

            for(Edge edge: this.edges){
                if(edge.contains(i)){
                    curComp.addEdge(edge, edge.getNode1() == i ? true : false);
                }
            }
        }

        for(int i = 0; i < numNodes; i++){
            for(Component component: components){
                if(component.contains(i)){
                    componentsMap.put(i, component);
                }
            }
        }
    }

    private Set<Edge> convertEdges(int[][] edges){
        Set<Edge> newEdges = new HashSet<>();

        for(int i = 0; i < edges.length; i++){
            for(int j = 0; j < i; j++){
                newEdges.add(new Edge(i, j, edges[i][j]));
            }
        }
        return newEdges;
    }

    private boolean isPreferredOver(Edge edge1, Edge edge2){
        int dist1 = edge1.getDistance();
        int dist2 = edge2.getDistance();
        return dist1 < dist2 || (dist1 == dist2 && tieBreaker(edge1, edge2));
    }

    private boolean tieBreaker(Edge edge1, Edge edge2){
        return (edge1.getNode1()+edge1.getNode2()) < (edge2.getNode1()+edge2.getNode2());
    }

    public Set<Edge> run(){
        boolean completed = false;
        Set<Edge> edgesPrime = new HashSet<>();

        while(!completed){
            for(Component component: components){
                Map.Entry<Edge, Boolean> shortestEdgeEntry = null;
                for(Map.Entry<Edge, Boolean> entry: component.getEdges().entrySet()){
                    if(shortestEdgeEntry == null ||
                            entry.getKey().getDistance() < shortestEdgeEntry.getKey().getDistance()){
                        shortestEdgeEntry = entry;
                    }
                }
                assert(shortestEdgeEntry != null);
                component.setShortestEdge(shortestEdgeEntry);
            }

            Set<Component> toRemove = new HashSet<>();
            for(Component component: components){
                int curCompNode = component.getCurCompNode();
                int nextCompNode = component.getNextCompNode();

                Component newComp = componentsMap.get(nextCompNode);
                if(!newComp.equals(component)){
                    newComp.addNode(curCompNode);
                    newComp.removeEdgesExcept(component.getEdges(), component.getShortestEdge());
                    componentsMap.put(curCompNode, newComp);
                    toRemove.add(component);
                }
            }
            components.removeAll(toRemove);

            if(components.size() == 1){
                completed = true;
                edgesPrime = components.stream().findAny().get().getEdges().keySet();
            }
            for(Component component: components){
                component.clearShortestEdge();
            }

        }
        return edgesPrime;
    }
}
