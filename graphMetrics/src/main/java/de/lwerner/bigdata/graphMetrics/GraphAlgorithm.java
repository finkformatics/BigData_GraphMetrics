package de.lwerner.bigdata.graphMetrics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import de.lwerner.bigdata.graphMetrics.utils.CommandLineArguments;
import de.lwerner.bigdata.graphMetrics.utils.GraphMetricsWriter;

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
	 * Vertices
	 */
	protected DataSet<Vertex<K, VV>> vertices;
	/**
	 * Edges
	 */
	protected DataSet<Edge<K, EV>> edges;
	/**
	 * Graph
	 */
	protected Graph<K, VV, EV> graph;
	/**
	 * Context
	 */
	protected ExecutionEnvironment context;

	/**
	 * Creates a graph from vertices and edges
	 * 
	 * @param vertices a Dataset of vertices
	 * @param edges a Dataset of edges
	 * @param context the flink execution environment
	 */
	public GraphAlgorithm(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
		this.graph = Graph.fromDataSet(vertices, edges, context);
		this.context = context;
	}
	
	/**
	 * Get the vertices
	 * 
	 * @return the DataSet with the vertices
	 */
	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}
	
	/**
	 * Get the edges
	 * 
	 * @return the DataSet with the edges
	 */
	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
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
	abstract public JsonNode writeOutput(ObjectMapper m) throws Exception;
	
}