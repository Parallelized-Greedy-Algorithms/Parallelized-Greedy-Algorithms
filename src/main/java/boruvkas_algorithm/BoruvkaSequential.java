package boruvkas_algorithm;

import java.util.*;

public class BoruvkaSequential {
    private final Set<Node> nodes;
    private final Set<Edge> edges;
    private final Set<Component> components;

    public BoruvkaSequential(int numNodes, int[][] edges){
        this.nodes = new HashSet<>();
        for(int i = 0; i < numNodes; i++){
            nodes.add(new Node(i));
        }
        this.edges = convertEdges(edges);

        components = new HashSet<>();

        for(Node curNode: nodes){
            Component curComp = new Component(curNode);
            curNode.setComponent(curComp);
            components.add(curComp);

            for(Edge edge: this.edges){
                if(edge.contains(curNode)){
                    curComp.addEdge(edge, edge.getNode1().getId() == curNode.getId() ? true : false);
                }
            }
        }
    }

    private Set<Edge> convertEdges(int[][] edges){
        Set<Edge> newEdges = new HashSet<>();

        for(int i = 0; i < edges.length; i++){
            for(int j = 0; j < i; j++){
                Node node1 = null;
                Node node2 = null;
                for(Node node: nodes){
                    if(node.getId() == i){
                        node1 = node;
                    }
                    else if(node.getId() == j){
                        node2 = node;
                    }
                    if(node1 != null && node2 != null){
                        break;
                    }
                }
                newEdges.add(new Edge(node1, node2, edges[i][j]));
            }
        }
        return newEdges;
    }

//    private boolean isPreferredOver(Edge edge1, Edge edge2){
//        int dist1 = edge1.getDistance();
//        int dist2 = edge2.getDistance();
//        return dist1 < dist2 || (dist1 == dist2 && tieBreaker(edge1, edge2));
//    }
//
//    private boolean tieBreaker(Edge edge1, Edge edge2){
//        return (edge1.getNode1()+edge1.getNode2()) < (edge2.getNode1()+edge2.getNode2());
//    }

    public Set<Edge> run(){
        boolean completed = false;
        Set<Edge> edgesPrime = new HashSet<>();

        while(!completed){
            Set<Edge> shortestEdges = new HashSet<>();
            for(Component component: components){
                // find shortest edge of component
                Map.Entry<Edge, Boolean> shortestEdgeEntry = null;
                for(Map.Entry<Edge, Boolean> entry: component.getEdges().entrySet()){
                    if(shortestEdgeEntry == null ||
                            entry.getKey().getDistance() < shortestEdgeEntry.getKey().getDistance()){
                        shortestEdgeEntry = entry;
                    }
                }
                assert(shortestEdgeEntry != null);
                component.setShortestEdge(shortestEdgeEntry);
                if(shortestEdgeEntry != null){
                    shortestEdges.add(shortestEdgeEntry.getKey());
                }
            }

            components.clear();
            for(Edge edge: shortestEdges){
                Component comp1 = edge.getNode1().getComponent();
                Component comp2 = edge.getNode2().getComponent();

                Component newComp = null;
                if(comp1.isNewComponent() || comp2.isNewComponent()){
                    Component oldComp = null;
                    if(comp1.isNewComponent()){
                        newComp = comp1;
                        oldComp = comp2;
                    }
                    else{
                        newComp = comp2;
                        oldComp = comp1;
                    }
                    newComp.addNodes(oldComp.getNodes());
                    newComp.addEdges(oldComp.getEdges());
                }
                else{
                    newComp = new Component(comp1, comp2);
                    components.add(newComp);
                }
//                for(Node node: newComp.getNodes()){
//                    node.setComponent(newComp);
//                }
                System.out.println();
            }

            for(Component component: components){
                for(Node node: component.getNodes()){
                    node.setComponent(component);
                }
            }

            // remove edges that aren't in-between two differing components
            for(Component component: components){
                component.setNewComponent(false);
                for(Map.Entry<Edge, Boolean> entry: component.getEdges().entrySet()){
                    // false boolean designates that node1 is outside of component
                    Node node = null; // node that should be in different component
                    if(entry.getValue()){ // node1 is in component
                        node = entry.getKey().getNode2();
                    }
                    else{
                        node = entry.getKey().getNode1();
                    }
                    // keep edges whose nodes have differing components
                    if(component.equals(node.getComponent())) {
                        component.removeEdge(entry.getKey());
                    }
                }
            }


            if(components.size() == 1){
                completed = true;
                edgesPrime = components.stream().findAny().get().getEdges().keySet();
            }
        }
        return edgesPrime;
    }
}
