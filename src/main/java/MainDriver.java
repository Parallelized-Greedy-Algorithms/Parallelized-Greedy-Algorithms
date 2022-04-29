import boruvkas_algorithm.BoruvkaDriver;
import dijkstras_algorithm.DijkstraDriver;

public class MainDriver {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Dijkstra's Algorithm");
        DijkstraDriver.main(new String[0]);

        System.out.println("Boruvka's Algorithm");
        BoruvkaDriver.main(new String[0]);
    }
}
