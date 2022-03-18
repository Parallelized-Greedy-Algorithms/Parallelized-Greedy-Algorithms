package dijkstras_algorithm;

import java.util.Random;
import java.util.Set;

public class DijkstraDriver {
    static Random rng = new Random();

    public static void populateGraph(Graph graph){
        graph.createNodes(10);
        Set<Node> nodes = graph.getNodes();

        // generate complete graph
        for(Node node1: nodes){
            for(Node node2: nodes){
                if(!node1.equals(node2)){
                    graph.createEdge(node1, node2, rng.nextInt(1, 50));
                }
            }
        }
    }

    public static void main(String[] args){
        Graph graph = new Graph();
        populateGraph(graph);

        Set<Node> nodes = graph.getNodes();
        DijkstraSequential sequential = new DijkstraSequential(nodes, nodes.stream().findFirst().get());
        sequential.run();
        System.out.println(sequential);
    }
}
