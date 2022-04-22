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
//    public static final int PROCESSORS = Runtime.getRuntime().availableProcessors()/2;
    public static final int PROCESSORS = 2;
    static final int SEED = 5;
    static Random rng = new Random(SEED);
    static final int NUM_NODES = 10;
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

    public static void main(String[] args) throws InterruptedException {
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
        WattsStrogatzGraphGenerator<Node, Edge> generator = new
                WattsStrogatzGraphGenerator<>(NUM_NODES, K_NEIGHBORS, PROB_REWIRE, SEED);
        generator.generateGraph(graph);

        Set<Edge> edges = graph.edgeSet();

        for(Edge edge: edges){
            edge.assignValues();
        }

//        try {
//            writeGraph(graph);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }


//        BoruvkaSequential sequential = new BoruvkaSequential(graph.vertexSet(), edges);
//        Set<Edge> minEdges = sequential.run();
//        int sum = 0;
//        for(Edge edge: minEdges){
//            sum += edge.getDistance();
//        }
//        log.info("Number nodes: " + graph.vertexSet().size());
//        log.info("Number edges: " + minEdges.size());
//        log.info("Minimum sum: " + sum);

        BoruvkaParallel parallel = new BoruvkaParallel(PROCESSORS, graph.vertexSet(), edges);
        parallel.run();
    }
}
