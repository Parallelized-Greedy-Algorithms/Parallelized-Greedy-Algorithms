package boruvkas_algorithm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.jgrapht.generate.BarabasiAlbertGraphGenerator;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;

import java.util.*;
import java.util.function.Supplier;

public class BoruvkaDriver {
    private static final Logger log = LogManager.getLogger(BoruvkaDriver.class);
    static Random rng = new Random(7);
    static final int NUM_NODES = 300;
    static final int NODES_MULTIPLIER = 4;

    public static void main(String[] args){
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);
        Supplier<Node> nodeSupplier = new Supplier<>() {
            private int id = 0;
            @Override
            public Node get() {
                return new Node(id++);
            }
            @Override
            public String toString(){
                return String.valueOf(id);
            }
        };

        Supplier<Edge> edgeSupplier = () -> new Edge(rng);

        DefaultUndirectedWeightedGraph<Node, Edge> graph = new DefaultUndirectedWeightedGraph<>(
                nodeSupplier,
                edgeSupplier
                );
        BarabasiAlbertGraphGenerator<Node, Edge> generator = new
                BarabasiAlbertGraphGenerator<>(Math.round(NUM_NODES/((float) NODES_MULTIPLIER)), 1, NUM_NODES, rng);
        generator.generateGraph(graph);

        Set<Edge> edges = graph.edgeSet();

        for(Edge edge: edges){
            edge.assignValues();
        }
        System.out.println();

        BoruvkaSequential sequential = new BoruvkaSequential(graph.vertexSet(), edges);
        Set<Edge> minEdges = sequential.run();
        int sum = 0;
        for(Edge edge: minEdges){
            sum += edge.getDistance();
        }
        log.info("Number nodes: " + graph.vertexSet().size());
        log.info("Number edges: " + minEdges.size());
        log.info(minEdges);
        log.info("Minimum sum: " + sum);
    }
}
