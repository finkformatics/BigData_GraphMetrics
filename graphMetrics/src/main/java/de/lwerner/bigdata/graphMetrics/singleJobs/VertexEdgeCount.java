package de.lwerner.bigdata.graphMetrics.singleJobs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

public class VertexEdgeCount {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env);
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env);
		
		Graph<Long, FoodBrokerVertex, FoodBrokerEdge> graph = Graph.fromDataSet(vertices, edges, env);
		
		long verticesCount = graph.numberOfVertices();
		long edgesCount = graph.numberOfEdges();
		
		System.out.println("Number vertices: " + verticesCount);
		System.out.println("Number edges: " + edgesCount);
	}
	
}