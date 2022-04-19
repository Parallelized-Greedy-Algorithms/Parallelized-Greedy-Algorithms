package boruvkas_algorithm.Sequential;

import boruvkas_algorithm.Sequential.Node;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.Random;

public class Edge extends DefaultWeightedEdge {
    private Node node1;
    private Node node2;
    private int distance;

    private static final int MAX_DISTANCE = 100;

    private Random rng;

    public Edge(Random rng){
        this.rng = rng;
    }

    public Edge(Node node1, Node node2, int distance){
        this.node1 = node1;
        this.node2 = node2;
        this.distance = distance;
    }

    public boolean contains(Node node){
        return node.equals(node1) || node.equals(node2);
    }

    public Node getNeighbor(Node node){
        if(node == node1){
            return node2;
        }
        else{
            return node1;
        }
    }

    public void assignValues(){
        this.node1 = (Node) this.getSource();
        this.node2 = (Node) this.getTarget();
        this.distance = rng.nextInt(MAX_DISTANCE);
    }

    public int getDistance(){
        return distance;
    }

    public Node getNode1(){
        return node1;
    }

    public Node getNode2(){
        return node2;
    }

    @Override
//    public String toString(){
//        return node1 + " <--[" + distance + "]--> " + node2;
//    }
    public String toString(){
        return String.valueOf(distance);
    }
}
