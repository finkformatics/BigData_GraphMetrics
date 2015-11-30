# Graph Metrics & Measures
This application is an accumulation of common graph metrics & measures. 
It utilizes Apache Flink for batch data processing and the Apache Flink Gelly 
extension for graph processing. The metrics & measures include:

* Average degree of vertices
* Global cluster coefficient
* Weakly connected components (sizes)
* Spread attributes of vertices and edges (keys and schemas)
* Spread labels of vertices and edges
* Vertex and edge count

## Installation
See [Apache Flink website](http://flink.apache.org/) for documentation and how 
to setup Apache Flink. Then `git clone` this repository and create a jar of the 
*graphMetrics* project or import it into your favorite IDE (we're using *eclipse*). 
Since there are all dependencies defined as Maven dependencies, you don't have to do 
any further steps for installation.

## Usage
As you can read in the Apache Flink documentation, you can run *jobs* in 
different environment. Thanks to the sophisticated processing Apache Flink 
provides, you don't have to change anything if you want to run the jobs **local**, 
in a **local cluster** or in a **distributed cluster**. You just have to run the 
respective Java classes to start the desired job. The job classes can be located 
in the `de.lwerner.bigdata.graphMetrics` package and are one of the following 
(in the same order as the above list):

* `AverageDegree`
* `ClusterCoefficient`
* `ConnectedComponents`
* `SpreadAttributes`
* `SpreadLabels`
* `VertexEdgeCount`

All of these can be started with the following command line arguments:

```
[-v <arg>] [-e <arg>] [-o <arg>] [-m <arg>]
-v,--vertices <arg>         absolute path to vertices json file
-e,--edges <arg>            absolute path to edges json file
-o,--output <arg>           absolute path to output file
-m,--maxIterations <arg>    max iterations on converging algorithms
```

## Testing

There aren't any test classes by now, but we're looking forward to create them, so you can test our implementations correctly.

## Development

To setup your development environment for this project, just `git clone` this repository and start coding. You can integrate this maven project in your favorite IDE too.

## Known issues
* The local maven execution doesn't work, caused by a NoClassDefFoundError. 
It looks like that's because the scope *provided*, but when we are removing this, 
we get a classloader error. We are trying to correct that.