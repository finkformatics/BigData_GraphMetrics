package de.lwerner.bigdata.graphMetrics.examples;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

public class OutgoingEdgesExample {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		List<Vertex<Integer, Integer>> vertexList = new ArrayList<>();
		Vertex<Integer, Integer> v1 = new Vertex<>(1, 1);
		Vertex<Integer, Integer> v2 = new Vertex<>(2, 2);
		Vertex<Integer, Integer> v3 = new Vertex<>(3, 3);
		vertexList.add(v1);
		vertexList.add(v2);
		vertexList.add(v3);
		
		List<Edge<Integer, String>> edgeList = new ArrayList<>();
		edgeList.add(new Edge<>(1, 2, "e1"));
		edgeList.add(new Edge<>(2, 1, "e2"));
		edgeList.add(new Edge<>(2, 3, "e3"));
		edgeList.add(new Edge<>(3, 2, "e4"));
		
		Graph<Integer, Integer, String> graph = Graph.fromCollection(vertexList, edgeList, env);
				
		graph.outDegrees().print();
	}
	
}