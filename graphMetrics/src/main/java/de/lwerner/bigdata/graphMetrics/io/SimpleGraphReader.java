package de.lwerner.bigdata.graphMetrics.io;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.util.Collector;

public class SimpleGraphReader extends GraphReader<Long, Long, Long> {

    public SimpleGraphReader(ExecutionEnvironment env, String graphPath) {
        super(env, graphPath);
    }

    @Override
    public Graph<Long, Long, Long> getGraph() {
        Graph g = Graph.fromDataSet(getEdges(), env);
        return g;
    }

    private DataSet<Edge<Long, Long>> getEdges() {
        return env.readTextFile(edgesPath).flatMap(new FlatMapFunction<String, Edge<Long, Long>>() {
            @Override
            public void flatMap(String line, Collector<Edge<Long, Long>> out) throws Exception {
                if (!line.contains("#")) {
                    String[] split = line.split("\\t");
                    out.collect(new Edge<>(Long.parseLong(split[0]), Long.parseLong(split[1]), 0L));
                }
            }
        });
    }
}
