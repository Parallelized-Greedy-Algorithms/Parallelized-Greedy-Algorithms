package boruvkas_algorithm;

import java.util.Set;

public class Node {
    private final int id;
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
