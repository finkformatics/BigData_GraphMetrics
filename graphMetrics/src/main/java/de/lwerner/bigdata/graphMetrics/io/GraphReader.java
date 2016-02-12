package de.lwerner.bigdata.graphMetrics.io;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;

import java.io.Serializable;

/**
 * Abstract class which represents the base code for a graph input reader.
 * All graph input readers must extend this class as it is used as base class in the algorithms.
 *
 * @param <K> Key type of the graph (mostly Long)
 * @param <VV> Vertex value type of the graph
 * @param <EV> Edge value type of the graph
 *
 * @author Lukas Werner
 */
public abstract class GraphReader<K, VV, EV> implements Serializable {

    /**
     * Apache flink's execution environment
     */
    protected ExecutionEnvironment env;
    /**
     * Path to vertices file
     */
    protected String verticesPath;
    /**
     * Path to edges file (you can also use it as a "graph path")
     */
    protected String edgesPath;

    /**
     * Sets both paths to one "graph path"
     *
     * @param env flink's execution environment
     * @param graphPath path to the graph file
     */
    public GraphReader(ExecutionEnvironment env, String graphPath) {
        this(env, graphPath, graphPath);
    }

    /**
     * Sets the execution environment, the vertices and edges path
     *
     * @param env flink's execution environment
     * @param verticesPath path to the vertices file
     * @param edgesPath path to the edges file
     */
    public GraphReader(ExecutionEnvironment env, String verticesPath, String edgesPath) {
        this.env = env;
        this.verticesPath = verticesPath;
        this.edgesPath = edgesPath;
    }

    /**
     * Use this method to get the generated graph from a graph input reader.
     *
     * @return the generated graph
     */
    public abstract Graph<K, VV, EV> getGraph();

}