package de.lwerner.bigdata.graphMetrics.io;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;

import java.io.Serializable;

public abstract class GraphReader<K, VV, EV> implements Serializable {

    protected ExecutionEnvironment env;
    protected String verticesPath;
    protected String edgesPath;

    public GraphReader(ExecutionEnvironment env, String graphPath) {
        this(env, graphPath, graphPath);
    }

    public GraphReader(ExecutionEnvironment env, String verticesPath, String edgesPath) {
        this.env = env;
        this.verticesPath = verticesPath;
        this.edgesPath = edgesPath;
    }

    public abstract Graph<K, VV, EV> getGraph();

}