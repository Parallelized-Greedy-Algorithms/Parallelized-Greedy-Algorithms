package boruvkas_algorithm.Parallel;

import boruvkas_algorithm.Sequential.Edge;
import boruvkas_algorithm.Sequential.Node;

import java.util.*;

// structure built for shared memory architecture to avoid false sharing
public class CSRGraph {
    public int numNodes;
    private Unit[] destinations;
    private Unit[] weights;
    private final Unit[] firstEdges;
    private final Unit[] outDegrees;
    private final Unit[] nodeMinEdges;
    private final Unit[] colors;

    public CSRGraph(Collection<?> nodes){
        int size =  nodes.size();
        firstEdges = new Unit[size];
        outDegrees = new Unit[size];
        colors = new Unit[size];
        nodeMinEdges = new Unit[size];
        numNodes = size;

        for(int i = 0; i < size; i++){
            firstEdges[i] = new Unit();
            outDegrees[i] = new Unit();
            colors[i] = new Unit();
            nodeMinEdges[i] = new Unit();
        }
    }

    public CSRGraph(Set<Node> nodes, Set<Edge> edges){
        this(nodes);
        destinations = new Unit[edges.size()];
        weights = new Unit[edges.size()];

        int edgeCounter = 0;
        for(Node node: nodes){
            for(Edge edge: edges){
                if(node.getId() == edge.getNode1().getId()){
                    outDegrees[node.getId()].value++;
                    destinations[edgeCounter] = new Unit(edge.getNode2().getId());
                    weights[edgeCounter++] = new Unit(edge.getDistance());
                }
            }
        }

        firstEdges[0].value = 0;
        for(int i = 1; i < numNodes; i++){
            firstEdges[i].value = outDegrees[i-1].value + firstEdges[i-1].value;
        }
    }

    public void setDestinations(Unit[] destinations){
        this.destinations = destinations;
    }

    public void setWeights(Unit[] weights){
        this.weights = weights;
    }

    public int getFirstEdge(int node){
        return firstEdges[node].value;
    }

    public int getOutDegree(int node){
        return outDegrees[node].value;
    }

    public int getWeight(int edge){
        return weights[edge].value;
    }

    public int getDestination(int edge){
        return destinations[edge].value;
    }

    public int getNodeMinEdge(int node){
        return nodeMinEdges[node].value;
    }

    public int getColor(int node){
        return colors[node].value;
    }

    public void setNodeMinEdge(int node, Integer edge){
        if(edge == null){
            nodeMinEdges[node].value = -1;
        }
        else{
            nodeMinEdges[node].value = edge;
        }
    }

    public void setColor(int node, int color){
        colors[node].value = color;
    }

    public void incOutDegree(int node){
        outDegrees[node].value++;
    }

    public void addFirstEdge(int node, int edge){
        firstEdges[node].value = edge;
    }

    public void addDestination(int edge, int node){
        destinations[edge].value = node;
    }

    public void addWeight(int edge, int weight){
        weights[edge].value = weight;
    }


}
