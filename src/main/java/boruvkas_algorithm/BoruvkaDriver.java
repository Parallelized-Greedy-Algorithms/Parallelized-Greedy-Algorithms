package boruvkas_algorithm;

import java.util.Random;
import java.util.Set;

public class BoruvkaDriver {
    static Random rng = new Random(7);

    public static int[][] createEdges(int numNodes){
        int[][] edges = new int[numNodes][numNodes];

        for(int i = 0; i < numNodes; i++){
            for(int j = 0; j < i; j++){
                int num = rng.nextInt(1, 100);
                edges[i][j] = num;
                edges[i][i-j] = num;
            }
        }

        return edges;
    }

    public static void printGraph(int[][] edges){
        for(int i=0; i < edges.length; i++){
            for(int j=0; j < i; j++){
                System.out.println(i + " (" + edges[i][j] + ") " + j);
            }
        }

    }

    public static void main(String[] args){
        int numNodes = 5;
        int[][] edges = createEdges(numNodes);
        printGraph(edges);

        BoruvkaSequential sequential = new BoruvkaSequential(numNodes, edges);
        Set<Edge> minEdges = sequential.run();
        int sum = 0;
        for(Edge edge: minEdges){
            sum += edge.getDistance();
        }
        System.out.println(minEdges);
        System.out.println(sum);
    }
}
