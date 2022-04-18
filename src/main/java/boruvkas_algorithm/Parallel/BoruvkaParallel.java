package boruvkas_algorithm.Parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class BoruvkaParallel {
    List<Integer> destinations;
    List<Integer> weights;
    List<Integer> firstEdges;
    List<Integer> outDegrees;
    Map<Integer, Integer> nodeMinEdge;
    Map<Integer, Integer> colorsMap;
    Map<Integer, Boolean> flagsMap;

    public BoruvkaParallel(){
        destinations = new ArrayList<>();
        weights = new ArrayList<>();
        firstEdges = new ArrayList<>();
        outDegrees = new ArrayList<>();

        nodeMinEdge = new ConcurrentSkipListMap<>();
        colorsMap = new ConcurrentSkipListMap<>();
        flagsMap = new ConcurrentSkipListMap<>();
    }

    public class Worker implements Runnable{
        private Set<Integer> localNodes;

        public Worker(Set<Integer> localNodes){
            this.localNodes = localNodes;
        }

        @Override
        public void run() {
            // (a) find edge with minimum weight & add it to vertexMinEdge
            while(!localNodes.isEmpty()){
                for(Integer node: localNodes){
                    // iterate over outgoing edges from node
                    int minWeight = Integer.MIN_VALUE;
                    int firstEdgeIndex = firstEdges.get(node);
                    for(int i = firstEdgeIndex; i < firstEdgeIndex + outDegrees.get(node); i++){
                        int weight = weights.get(i);
                        if(weight < minWeight){
                            minWeight = weight;
                        }
                    }
                    nodeMinEdge.put(node, minWeight);
                }
            }

            // (b) Remove mirrored edges from vertexMinEdge
            for(Integer node: localNodes){
                if(node < destinations.get(node) &&
                        // the successor of node is node itself
                        destinations.get(nodeMinEdge.get(destinations.get(nodeMinEdge.get(node)))) == node){
                    nodeMinEdge.put(node, null);
                }
            }

            // (c) initialize and propagate colors (components)
            for(Map.Entry<Integer, Integer> entry: nodeMinEdge.entrySet()){
                Integer node = entry.getKey();
                Integer edge = entry.getValue();
                // if edge is null, node is a representative for the component
                if(edge == null){
                    colorsMap.put(node, node);
                    flagsMap.put(node, true);
                }
                // otherwise, find the representative for the component
                else{
                    colorsMap.put(node, destinations.get(nodeMinEdge.get(node)));
                    flagsMap.put(node, false);
                }
            }

            // (e)


        }
    }
}
