package boruvkas_algorithm;

public class Edge {
    private final Node node1;
    private final Node node2;
    private final int distance;

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
    public String toString(){
        return node1 + " <--[" + distance + "]--> " + node2;
    }
}
