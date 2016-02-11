package de.lwerner.bigdata.graphMetrics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import de.lwerner.bigdata.graphMetrics.utils.CommandLineArguments;
import de.lwerner.bigdata.graphMetrics.io.GraphMetricsWriter;

/**
 * Abstract class which should be implemented by all graph algorithms
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 *
 * @param <K> the key type for edge and vertex identifiers
 * @param <VV> the value type for vertices
 * @param <EV> the value type for edges
 */
public abstract class GraphAlgorithm<K, VV, EV> {
	
	/**
	 * Command Line Arguments
	 */
	protected static CommandLineArguments arguments;

	/**
	 * Graph
	 */
	protected Graph<K, VV, EV> graph;
	/**
	 * Context
	 */
	protected ExecutionEnvironment context;

	/**
	 *
	 * 
	 * @param graph the graph
	 * @param context the flink execution environment
	 */
	public GraphAlgorithm(Graph<K, VV, EV> graph, ExecutionEnvironment context) throws Exception {
		this(graph, context, false);
	}
	
	/**
	 *
	 * 
	 * @param graph the graph
	 * @param context the flink execution environment
	 * @param undirected make the edges undirected
	 */
	public GraphAlgorithm(Graph<K, VV, EV> graph, ExecutionEnvironment context, boolean undirected) throws Exception {
		if (undirected) {
			this.graph = graph.getUndirected();
		} else {
			this.graph = graph;
		}
		this.context = context;
	}
	
	/**
	 * Get the vertices
	 * 
	 * @return the DataSet with the vertices
	 */
	public DataSet<Vertex<K, VV>> getVertices() {
		return graph.getVertices();
	}
	
	/**
	 * Get the edges
	 * 
	 * @return the DataSet with the edges
	 */
	public DataSet<Edge<K, EV>> getEdges() {
		return graph.getEdges();
	}
	
	/**
	 * Get the graph
	 * 
	 * @return the Graph generated from vertices and edges
	 */
	public Graph<K, VV, EV> getGraph() {
		return graph;
	}
	
	/**
	 * Get the context of the algorithm
	 * 
	 * @return the ExecutionEnvironment context
	 */
	public ExecutionEnvironment getContext() {
		return context;
	}
	
	/**
	 * Run the algorithm and write output
	 * @throws Exception during run or writeOutput
	 */
	public void runAndWrite() throws Exception {
		run();
		ObjectMapper m = new ObjectMapper();
		JsonNode jsonNode = writeOutput(m);
		GraphMetricsWriter.writeJson(m, jsonNode, arguments.getOutputPath());
	}
	
	/**
	 * Run method to run the algorithm
	 */
	abstract public void run() throws Exception;
	
	/**
	 * Method to writeOutput
	 */
	abstract public JsonNode writeOutput(ObjectMapper m);
	
}