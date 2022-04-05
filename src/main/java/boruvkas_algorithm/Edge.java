package boruvkas_algorithm;

public class Edge {
    private final int node1;
    private final int node2;
    private final int distance;

    public Edge(int node1, int node2, int distance){
        this.node1 = node1;
        this.node2 = node2;
        this.distance = distance;
    }

    public boolean contains(int node){
        return node == node1 || node == node2;
    }

    public int getNeighbor(int node){
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

    public int getNode1(){
        return node1;
    }

    public int getNode2(){
        return node2;
    }

    @Override
    public String toString(){
        return node1 + " <--[" + distance + "]--> " + node2;
    }
}
