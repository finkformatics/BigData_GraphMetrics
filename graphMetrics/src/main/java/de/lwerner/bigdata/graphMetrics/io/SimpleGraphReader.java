package de.lwerner.bigdata.graphMetrics.io;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.util.Collector;

/**
 * Simple graph reader for "simple graph" files.
 * These are assumed as CSV files, where just the edges are one record with [TAB] (\t) as delimiter.
 * A record must look like this (assume the 4 spaces as \t):
 * sourceVertexId    targetVertexId
 *
 * @author Lukas Werner
 */
public class SimpleGraphReader extends GraphReader<Long, Long, Long> {

    /**
     * Constructor to set graph file path and environment.
     *
     * @param env flink's execution environment
     * @param graphPath path to graph file
     */
    public SimpleGraphReader(ExecutionEnvironment env, String graphPath) {
        super(env, graphPath);
    }

    @Override
    public Graph<Long, Long, Long> getGraph() {
        Graph g = Graph.fromDataSet(getEdges(), env);
        return g;
    }

    /**
     * Fetch the edges from the input file.
     *
     * @return the edges
     */
    private DataSet<Edge<Long, Long>> getEdges() {
        return env.readTextFile(edgesPath).flatMap(new FlatMapFunction<String, Edge<Long, Long>>() {
            @Override
            public void flatMap(String line, Collector<Edge<Long, Long>> out) throws Exception {
                if (!line.contains("#")) { // Ignore comments in CSV
                    String[] split = line.split("\\t"); // Use TAB as delimiter
                    out.collect(new Edge<>(Long.parseLong(split[0]), Long.parseLong(split[1]), 0L)); // Edge values are 0
                }
            }
        });
    }
}
