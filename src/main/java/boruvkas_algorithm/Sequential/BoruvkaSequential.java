package boruvkas_algorithm.Sequential;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class BoruvkaSequential {
    private static final Logger log = LogManager.getLogger(BoruvkaSequential.class);
    private final Set<Node> nodes;
    private final Set<Edge> edges;
    private final Set<Component> components;

    public BoruvkaSequential(Set<Node> nodes, Set<Edge> edges){
        this.nodes = nodes;
        this.edges = edges;
        this.components = new HashSet<>();

        buildComponentsSet();
    }

    private void buildComponentsSet(){
        for(Node curNode: nodes){
            Component curComp = new Component(curNode);
            curNode.setComponent(curComp);

            for(Edge edge: this.edges){
                if(edge.contains(curNode)){
                    curComp.addEdge(edge, edge.getNode1().getId() == curNode.getId() ? true : false);
                }
            }
            components.add(curComp);
        }
    }

    public Set<Edge> run(){
        boolean completed = false;
        Set<Edge> edgesPrime = new HashSet<>();

        long startTime, endTime;
        startTime = System.currentTimeMillis();
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
                shortestEdges.add(shortestEdgeEntry.getKey());
            }

            for(Edge edge: shortestEdges){
                Component comp1 = edge.getNode1().getComponent();
                Component comp2 = edge.getNode2().getComponent();

                Component oldComp;
                Component newComp;
                if(comp1.isNewComponent()){
                    newComp = comp1;
                    oldComp = comp2;
                }
                else{
                    newComp = comp2;
                    oldComp = comp1;
                }
                components.remove(oldComp);
                newComp.setNewComponent(true);

                for(Node node: oldComp.getNodes()){
                    node.setComponent(newComp);
                }
                newComp.addNodes(oldComp.getNodes());
                newComp.addEdges(oldComp.getEdges());

                edgesPrime.add(edge);
            }
//            endTime = System.currentTimeMillis();
//            log.info("Number of components: " + components.size() + " | took " + (endTime-startTime) + " ms");
//            startTime = System.currentTimeMillis();

            // remove edges that aren't in-between two differing components
            for(Component component: components){
                component.setNewComponent(false);
                for(Map.Entry<Edge, Boolean> entry: component.getEdges().entrySet()){
                    // false boolean designates that node1 is outside of component
                    Node node; // node that should be in different component
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

            if(components.size() <= 1){
                completed = true;
            }
        }
        return edgesPrime;
    }
}
