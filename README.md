# About
This project consists of efforts to parallelize two greedy algorithms: Dijkstra's algorithm 
for the shortest path problem and Boruvka's Algorithm for minimum spanning tree.

## Dijkstra's Algorithm
* Features sequential implementation `DijkstraSequential`
* Naive parallel implementation called `NaiveDijkstraParallel`
* Working Dijkstra parallel implementation called `DijkstraParallel`
* Better performing alternative Dijkstra parallel implementation `DijkstraParallel2`

## Boruvka's Algorithm
* Features sequential implementation `BoruvkaSequential`
* Parallelized Boruvka's algorithm `BoruvkaParallel`

# Requirements
- maven 3.8
- java 11
- latexmk

# How to build and run code:
## Build
```
mvn install
java -jar target/parallelized-greedy-algorithms-1.0-SNAPSHOT.jar
```
## Alternatively, run the pre-generated binary
```
java -jar out/artifacts/parallelized_greedy_algorithms_jar/parallelized-greedy-algorithms.jar
```

# How to build report:
```
pdflatex -pdf Report/main.txt
```
