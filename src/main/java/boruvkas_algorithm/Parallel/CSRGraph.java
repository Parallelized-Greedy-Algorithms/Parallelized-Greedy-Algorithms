package boruvkas_algorithm.Parallel;

import boruvkas_algorithm.Sequential.Edge;
import boruvkas_algorithm.Sequential.Node;

import javax.swing.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

// structure built for shared memory architecture to avoid false sharing
public class CSRGraph {
    public int numNodes;
    private List<Unit> destinations;
    private List<Unit> weights;
    private List<Unit> firstEdges;
    private ArrayList<Unit> outDegrees;
    private List<Unit> nodeMinEdges; // pointer to minimum edge of node at index
    private List<Unit> colors;
    private List<Unit> flag;
    private List<Unit> newNames; // map of node to new node name in nextGraph (ONLY for representatives)
    private List<Unit> mapOldNewNames;
    private Map<Integer, AtomicInteger> nextEdges;

    private CSRGraph(Collection<?> nodes){
        numNodes =  nodes.size();
        firstEdges = new ArrayList<>(numNodes);
        outDegrees = new ArrayList<>(numNodes);
        colors = new ArrayList<>(numNodes);
        nodeMinEdges = new ArrayList<>(numNodes);
        flag = new ArrayList<>(numNodes);
        newNames = new ArrayList<>(numNodes);
        mapOldNewNames = new ArrayList<>(numNodes);
        nextEdges = new ConcurrentHashMap<>(numNodes);

        for(int i = 0; i < numNodes; i++){
            firstEdges.add(new Unit());
            outDegrees.add(new Unit(new AtomicInteger(0)));
            colors.add(new Unit(-1));
            nodeMinEdges.add(new Unit());
            flag.add(new Unit());
            newNames.add(new Unit());
            mapOldNewNames.add(new Unit(i));
        }
    }

    public CSRGraph(Collection<?> nodes, int oldNumNodes){
        this(nodes);
        for(int i = nodes.size(); i < oldNumNodes; i++){
            mapOldNewNames.add(new Unit(i));
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
                    incOutDegree(node.getId());
                    destinations.set(edgeCounter, new Unit(edge.getNode2().getId()));
                    weights.set(edgeCounter++, new Unit(edge.getDistance()));
                }
                else if(node.getId() == edge.getNode2().getId()){
                    incOutDegree(node.getId());
                    destinations.set(edgeCounter, new Unit(edge.getNode1().getId()));
                    weights.set(edgeCounter++, new Unit(edge.getDistance()));
                }
            }
        }

        firstEdges.get(0).value = 0;
        for(int i = 1; i < numNodes; i++){
            firstEdges.get(i).value = getOutDegree(i-1) + firstEdges.get(i-1).value;
        }
    }

    public void initializeEdges(int size){
        destinations = new ArrayList<>(size);
        weights = new ArrayList<>(size);

        for(int i = 0; i < size; i++){
            destinations.add(new Unit());
            weights.add(new Unit());
        }
    }

    public void initializeNextEdges(){
        for(int i = 0; i < numNodes; i++){
            nextEdges.put(i, new AtomicInteger(firstEdges.get(i).value));
        }
    }

    public int getNextEdge(int node){
        return nextEdges.get(node).getAndIncrement();
    }

    public int getFirstEdge(int node){
        return firstEdges.get(node).value;
    }

    public int getOutDegree(int node){
        return outDegrees.get(node).atomic.get();
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

    public int getFlag(int node){
        return flag.get(node).value;
    }

    public int getNewName(int node){
        return newNames.get(node).value;
    }

    public int getMapNewName(int oldNode){
        return mapOldNewNames.get(oldNode).value;
    }

    public void setNodeMinEdge(int node, int edge){
        nodeMinEdges.get(node).value = edge;
    }

    public void setColor(int node, int color){
        colors.get(node).value = color;
    }

    public void setFlag(int node, int value){
        flag.get(node).value = value;
    }

    // for nextGraph
    public void setNameMap(int oldNode, int newNode){
        mapOldNewNames.get(oldNode).value = newNode;
    }

    public void incOutDegree(int node){
        outDegrees.get(node).atomic.incrementAndGet();
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
    public void addNewName(int node, int newName){
        newNames.get(node).value = newName;
    }
}
