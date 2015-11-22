package de.lwerner.bigdata.graphMetrics.examples;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

public class ShortestPathsExample {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		List<Vertex<Long, Double>> vertexList = new ArrayList<>();
		Vertex<Long, Double> v1 = new Vertex<>(1L, 0.0);
		Vertex<Long, Double> v2 = new Vertex<>(2L, 1000.0);
		Vertex<Long, Double> v3 = new Vertex<>(3L, 1000.0);
		Vertex<Long, Double> v4 = new Vertex<>(4L, 1000.0);
		Vertex<Long, Double> v5 = new Vertex<>(5L, 1000.0);
		Vertex<Long, Double> v6 = new Vertex<>(6L, 1000.0);
		vertexList.add(v1);
		vertexList.add(v2);
		vertexList.add(v3);
		vertexList.add(v4);
		vertexList.add(v5);
		vertexList.add(v6);

		List<Edge<Long, Double>> edgeList = new ArrayList<>();
		edgeList.add(new Edge<>(1L, 2L, 1.0));
		edgeList.add(new Edge<>(1L, 5L, 7.0));
		edgeList.add(new Edge<>(2L, 3L, 3.0));
		edgeList.add(new Edge<>(3L, 4L, 8.0));
		edgeList.add(new Edge<>(3L, 5L, 3.0));
		edgeList.add(new Edge<>(3L, 6L, 6.0));
		edgeList.add(new Edge<>(5L, 6L, 1.0));
		edgeList.add(new Edge<>(6L, 4L, 3.0));
		edgeList.add(new Edge<>(6L, 3L, 1.0));

		Graph<Long, Double, Double> graph = Graph.fromCollection(vertexList, edgeList, env);

		// define the maximum number of iterations
		int maxIterations = 10;

		// Execute the vertex-centric iteration
		Graph<Long, Double, Double> result = graph.runVertexCentricIteration(new VertexDistanceUpdater(),
				new MinDistanceMessenger(), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();
		
		singleSourceShortestPaths.print();
	}

	// messaging
	public static final class MinDistanceMessenger extends MessagingFunction<Long, Double, Double, Double> {

		private static final long serialVersionUID = 1L;

		public void sendMessages(Vertex<Long, Double> vertex) {
			for (Edge<Long, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getValue() + edge.getValue());
			}
		}
	}

	// vertex update
	public static final class VertexDistanceUpdater extends VertexUpdateFunction<Long, Double, Double> {

		private static final long serialVersionUID = 1L;

		public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages) {
			Double minDistance = Double.MAX_VALUE;

			for (double msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue() > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}

}