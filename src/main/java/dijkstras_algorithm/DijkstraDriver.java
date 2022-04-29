package dijkstras_algorithm;

import dijkstras_algorithm.naive_parallel.NaiveDijkstraParallel;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class DijkstraDriver {
    public static final int PROCESSORS = Runtime.getRuntime().availableProcessors()/2;
    static Random rng = new Random(4);

    public static void populateGraph(Graph graph, int numNodes){
        graph.createNodes(numNodes);
        Set<Node> nodes = graph.getNodes();

        // generate complete graph
        for(Node node1: nodes){
            for(Node node2: nodes){
                if(!node1.equals(node2)){
                    graph.createEdge(node1, node2, 1+rng.nextInt(49));
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

        return sequential.toString();
//        return "";
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
        NaiveDijkstraParallel parallel =
                new NaiveDijkstraParallel(nodes, source);
        long start = System.nanoTime();
        parallel.run();
        long end = System.nanoTime();
        float parallelSeconds = ((float)(end-start))/1000000000;
        System.out.println("Naive Parallel took " + parallelSeconds);

        return parallel.toString();
//        return "";
    }

    public static String runDijkstraParallel2(Set<Node> nodes, Node source){
        DijkstraParallel2 parallel = new DijkstraParallel2(nodes, source);
        long start = System.nanoTime();
        parallel.run();
        long end = System.nanoTime();
        float parallelSeconds = ((float)(end-start))/1000000000;
        System.out.println("Parallel2 took " + parallelSeconds);

        return parallel.toString();
//        return "";
    }

    public static void main(String[] args){
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.OFF);


        System.out.println("Threads: " + PROCESSORS);
        for(int i = 500; i < 3001; i+= 500){
            Graph graph = new Graph();
            populateGraph(graph, i);

            Set<Node> nodes = graph.getNodes();
            Node source = nodes.stream().findFirst().get();

            System.out.println("Number of nodes: " + i);
            String sequential = runSequential(nodes, source);
//            String naiveParallel = runNaiveParallel(nodes, source);
            String parallel = runParallel(nodes, source);
            String parallel2 = runDijkstraParallel2(nodes, source);
            System.out.println();
        }


    }
}
