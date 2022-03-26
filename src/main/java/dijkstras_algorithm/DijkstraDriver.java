package dijkstras_algorithm;

import java.util.Random;
import java.util.Set;

public class DijkstraDriver {
    static Random rng = new Random(4);

    public static void populateGraph(Graph graph){
        graph.createNodes(500);
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
    
    public static String runSequential(Set<Node> nodes, Node source){
        DijkstraSequential sequential = new DijkstraSequential(nodes, source);
        long start = System.nanoTime();
        sequential.run();
        long end = System.nanoTime();
//        System.out.println(sequential);
        float sequentialSeconds = ((float)(end-start))/1000000000;
        System.out.println("Sequential took " + sequentialSeconds);

//        return sequential.toString();
        return "";
    }

    public static String runParallel(Set<Node> nodes, Node source){
        DijkstraParallel parallel = new DijkstraParallel(nodes, source);
        long start = System.nanoTime();
        parallel.run();
        long end = System.nanoTime();
        float parallelSeconds = ((float)(end-start))/1000000000;
        System.out.println("Parallel took " + parallelSeconds);

//        return parallel.toString();
        return "";
    }

    public static String runNaiveParallel(Set<Node> nodes, Node source){
        dijkstras_algorithm.naive_parallel.DijkstraParallel parallel =
                new dijkstras_algorithm.naive_parallel.DijkstraParallel(nodes, source);
        long start = System.nanoTime();
        parallel.run();
        long end = System.nanoTime();
        float parallelSeconds = ((float)(end-start))/1000000000;
        System.out.println("Naive Parallel took " + parallelSeconds);

//        return parallel.toString();
        return "";
    }

    public static void main(String[] args){
        Graph graph = new Graph();
        populateGraph(graph);

        Set<Node> nodes = graph.getNodes();
        Node source = nodes.stream().findFirst().get();

        String sequential = runSequential(nodes, source);
        String parallel = runParallel(nodes, source);
        String naiveParallel = runNaiveParallel(nodes, source);


//        System.out.println(graph);
//        System.out.println(sequential);
//        System.out.println("PARALLEL: ");
//        System.out.println(parallel);
        System.out.println("Number of nodes in graph: " + nodes.size());
//        System.out.println("Sequential & parallel output equal: " + sequential.toString().equals(parallel.toString()));
//        System.out.println("sequential - parallel: " + (sequentialSeconds - parallelSeconds));
    }
}
