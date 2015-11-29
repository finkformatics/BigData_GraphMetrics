package de.lwerner.bigdata.graphMetrics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.utils.CommandLineArguments;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

/**
 * Apache Flink job for computing vertex and edge count of a given graph
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class VertexEdgeCount {

	/**
	 * Command line arguments
	 */
	private static CommandLineArguments arguments;
	
	/**
	 * The main job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		arguments = ArgumentsParser.parseArguments(VertexEdgeCount.class.getName(), FILENAME_VERTEX_EDGE_COUNT, args);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());
		
		Graph<Long, FoodBrokerVertex, FoodBrokerEdge> graph = Graph.fromDataSet(vertices, edges, env);
		
		long verticesCount = graph.numberOfVertices();
		long edgesCount = graph.numberOfEdges();
		
		System.out.println("Number vertices: " + verticesCount);
		System.out.println("Number edges: " + edgesCount);
	}
	
}