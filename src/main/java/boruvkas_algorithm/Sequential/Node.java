package boruvkas_algorithm.Sequential;


import boruvkas_algorithm.Sequential.Component;

public class Node {
    private final int id;
    public static int globalId = 0;
    private Component component;
//    private final Set<Edge> neighbors;

    public Node(int id){
        this.id = id;
    }

    public Node(int id, Component component){
        this.id = id;
        this.component = component;
    }

    public void setComponent(Component component) {
        this.component = component;
    }

    public Component getComponent(){
        return component;
    }

    public int getId(){
        return id;
    }

    public String toString(){
        return String.valueOf(id);
    }
}
