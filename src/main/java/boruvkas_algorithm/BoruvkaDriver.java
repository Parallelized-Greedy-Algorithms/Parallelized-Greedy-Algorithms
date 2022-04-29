package boruvkas_algorithm;

import boruvkas_algorithm.Parallel.BoruvkaParallel;
import boruvkas_algorithm.Sequential.BoruvkaSequential;
import boruvkas_algorithm.Sequential.Edge;
import boruvkas_algorithm.Sequential.Node;
import com.mxgraph.layout.*;
import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.model.mxGraphModel;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.util.mxCellRenderer;
import com.mxgraph.util.mxConstants;
import com.mxgraph.util.mxUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.generate.WattsStrogatzGraphGenerator;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

public class BoruvkaDriver {
    private static final Logger log = LogManager.getLogger(BoruvkaDriver.class);
    public static final int PROCESSORS = Runtime.getRuntime().availableProcessors()/2;
//    public static final int PROCESSORS = 2;
    static final int SEED = 5;
    static Random rng = new Random(SEED);
    static final int NUM_NODES = 399;
    static final int K_NEIGHBORS = 4; // should be > ln(NUM_NODES) && even
    static final double PROB_REWIRE = 0.8;

    public static void writeGraph(Graph graph) throws IOException {
        JGraphXAdapter<String, Edge> graphAdapter = new JGraphXAdapter<String, Edge>(graph);
        mxGraphComponent graphComponent = new mxGraphComponent(graphAdapter);
        mxGraphModel graphModel  = (mxGraphModel)graphComponent.getGraph().getModel();
        Collection<Object> cells =  graphModel.getCells().values();
        mxUtils.setCellStyles(graphComponent.getGraph().getModel(),
                cells.toArray(), mxConstants.STYLE_ENDARROW, mxConstants.NONE);
        mxIGraphLayout layout = new mxHierarchicalLayout(graphAdapter);
        layout.execute(graphAdapter.getDefaultParent());

        BufferedImage image =
                mxCellRenderer.createBufferedImage(graphAdapter, null, 3, Color.WHITE, true, null);
        String fileName = "graph_SM_" + NUM_NODES + "-" + K_NEIGHBORS + "-" + PROB_REWIRE;
        File imgFile = new File("src/test/resources/" + fileName + ".png");
        ImageIO.write(image, "PNG", imgFile);
    }

    public static void stats(int sum){
        log.info("Minimum sum: " + sum);

    }
    public static void stats(Set<Edge> edges){
//        log.info("Edges: " + edges);
        log.info("Number edges: " + edges.size());
        log.info("Minimum sum: " + calculateSum(edges));
    }
    public static int calculateSum(Set<Edge> edges){
        int sum = 0;
        for(Edge edge: edges){
            sum += edge.getDistance();
        }
        return sum;
    }

    public static Graph createGraph(){
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

        return graph;
    }

    public static void generateGraph(Graph graph, int numNodes, int numNeighbors){
        WattsStrogatzGraphGenerator<Node, Edge> generator = new
                WattsStrogatzGraphGenerator<>(numNodes, numNeighbors, PROB_REWIRE, SEED);
        generator.generateGraph(graph);
    }


    public static void runTester(int start, int numNodes, int increment) throws InterruptedException {
        int maxSize = numNodes;

        int curK = 2;
        int curN = start;

        while(curN < maxSize){
//            log.info("N: " + curN + " | K: " +curK);
            System.out.println("number of nodes: " + curN);
            while(curK >= curN){
                curN++;
            }
            while(Math.log(curN) >= curK){
                curK += 2;
            }

            Graph graph = createGraph();
            generateGraph(graph, curN, curK);

            ConnectivityInspector<Node, Edge> inspector = new ConnectivityInspector<>(graph);
            boolean isConnected = inspector.isConnected();
            if(!isConnected){
//                log.info("NOT connected");
                continue;
            }
//            log.info("Graph is connected.");

//            log.info(curN + " >> " + curK + " >> " + Math.log(curN) + " >> 1");

            Set<Edge> edges = graph.edgeSet();
            for(Edge edge: edges){
                edge.assignValues();
            }

            BoruvkaSequential sequential = new BoruvkaSequential(graph.vertexSet(), edges);
            long startSeq = System.currentTimeMillis();
            sequential.run();
            long endSeq = System.currentTimeMillis();
            System.out.println("Sequential took: " + (endSeq - startSeq) + " ms");

            BoruvkaParallel parallel = new BoruvkaParallel(PROCESSORS, graph.vertexSet(), edges);
            long startPar = System.currentTimeMillis();
            parallel.run();
            long endPar = System.currentTimeMillis();

            System.out.println("Parallel took: " + (endPar-startPar) + " ms\n");

            curN += increment;
        }

    }

    public static void main(String[] args) throws InterruptedException {
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);

        runTester(5, 5000, 600);

//        try {
//            writeGraph(graph);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

    }
}
