package boruvkas_algorithm.Parallel;

import boruvkas_algorithm.Sequential.Edge;
import boruvkas_algorithm.Sequential.Node;

import java.util.*;

// structure built for shared memory architecture to avoid false sharing
public class CSRGraph {
    public int numNodes;
    private List<Unit> destinations;
    private List<Unit> weights;
    private final List<Unit> firstEdges;
    private final ArrayList<Unit> outDegrees;
    private final List<Unit> nodeMinEdges; // pointer to minimum edge of node at index
    private final List<Unit> colors;

    public CSRGraph(Collection<?> nodes){
        numNodes =  nodes.size();
        firstEdges = new ArrayList<>(numNodes);
        outDegrees = new ArrayList<>(numNodes);
        colors = new ArrayList<>(numNodes);
        nodeMinEdges = new ArrayList<>(numNodes);

        for(int i = 0; i < numNodes; i++){
            firstEdges.add(new Unit());
            outDegrees.add(new Unit());
            colors.add(new Unit(-1));
            nodeMinEdges.add(new Unit());
        }
    }

    public CSRGraph(Set<Node> nodes, Set<Edge> edges){
        this(nodes);
        int edgesSize = edges.size() * 2;
        destinations = new ArrayList<>(edgesSize);
        weights = new ArrayList<>(edgesSize);

        for(int i = 0; i < edgesSize; i++){
            destinations.add(new Unit());
            weights.add(new Unit());
        }

        int edgeCounter = 0;
        for(Node node: nodes){
            for(Edge edge: edges){
                if(node.getId() == edge.getNode1().getId()){
                    outDegrees.get(node.getId()).value++;
                    destinations.set(edgeCounter, new Unit(edge.getNode2().getId()));
                    weights.set(edgeCounter++, new Unit(edge.getDistance()));
                }
                else if(node.getId() == edge.getNode2().getId()){
                    outDegrees.get(node.getId()).value++;
                    destinations.set(edgeCounter, new Unit(edge.getNode1().getId()));
                    weights.set(edgeCounter++, new Unit(edge.getDistance()));
                }
            }
        }

        firstEdges.get(0).value = 0;
        for(int i = 1; i < numNodes; i++){
            firstEdges.get(i).value = outDegrees.get(i-1).value + firstEdges.get(i-1).value;
        }
        System.out.println();
    }

    public void setDestinations(List<Unit> destinations){
        this.destinations = destinations;
    }

    public void setWeights(List<Unit> weights){
        this.weights = weights;
    }

    public int getFirstEdge(int node){
        return firstEdges.get(node).value;
    }

    public int getOutDegree(int node){
        return outDegrees.get(node).value;
    }

    public int getWeight(int edge){
        return weights.get(edge).value;
    }

    public int getDestination(int edge){
        return destinations.get(edge).value;
    }

    public int getNodeMinEdge(int node){
        return nodeMinEdges.get(node).value;
    }

    public int getColor(int node){
        return colors.get(node).value;
    }

    public void setNodeMinEdge(int node, Integer edge){
        if(edge == null){
            nodeMinEdges.get(node).value = -1;
        }
        else{
            nodeMinEdges.get(node).value = edge;
        }
    }

    public void setColor(int node, int color){
        colors.get(node).value = color;
    }

    public void incOutDegree(int node){
        outDegrees.get(node).value++;
    }

    public void addFirstEdge(int node, int edge){
        firstEdges.get(node).value = edge;
    }

    public void addDestination(int edge, int node){
        destinations.get(edge).value = node;
    }

    public void addWeight(int edge, int weight){
        weights.get(edge).value = weight;
    }


}
