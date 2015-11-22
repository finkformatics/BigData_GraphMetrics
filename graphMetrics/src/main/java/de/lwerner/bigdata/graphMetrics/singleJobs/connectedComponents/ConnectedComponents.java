package de.lwerner.bigdata.graphMetrics.singleJobs.connectedComponents;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;

import de.lwerner.bigdata.graphMetrics.examples.ConnectedComponentsData;

/**
 * Calculates the weakly connected components of a (either undirected or directed) graph.
 * 
 * Solves the problem by using the vertex-centric approach:
 * Each vertex sends its Component-ID (initially their own ID) to all neighboring vertices. 
 * These will take the minimum of the incoming messages and set it as their own new Component-ID.
 * This will be repeated until there is no change anymore (until it converges).
 * 
 * @author Lukas Werner
 */
public class ConnectedComponents {

	/**
	 * The main job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// Get the graph data
		// TODO: Do not use the default data!
		DataSet<Long> defaultVertices = ConnectedComponentsData.getDefaultVertexDataSet(env);
		DataSet<Tuple2<Long, Long>> defaultEdges = ConnectedComponentsData.getDefaultEdgeDataSet(env);

		DataSet<Edge<Long, Long>> undirectedEdges = defaultEdges.flatMap(new UndirectEdge());
		DataSet<Vertex<Long, Long>> vertices = defaultVertices.map(new VertexCreator());

		Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, undirectedEdges, env);

		int maxIterations = 10;

		/*
		 * Each vertex sends its component-id (initially the vertex-id) to all neighboring vertices,
		 * takes the minimum of the incoming component-ids and sets it as its own component-ids.
		 */
		Graph<Long, Long, Long> result = graph.runVertexCentricIteration(new ComponentIDUpdater(),
				new ComponentIDMessenger(), maxIterations);
		
		// The vertices (with the component-id as their value) are the result
		DataSet<Vertex<Long, Long>> resultVertices = result.getVertices();
		
		DataSet<Tuple2<Long, Long>> componentSizes = resultVertices.flatMap(new ComponentIDCounter())
				.groupBy(0).sum(1);
		
		// TODO: Is correct, but the first property of the tuple is arbitrary??
		// DataSet<Tuple2<Long, Long>> biggestComponent = componentSizes.aggregate(Aggregations.MAX, 1);
		
		componentSizes.print();
	}
	
	/**
	 * FlatMapFunction which collects all vertices with a certain component-id and maps it to 1, so we can
	 * calculate the size of the connected components.
	 * 
	 * @author Lukas Werner
	 */
	public static final class ComponentIDCounter implements FlatMapFunction<Vertex<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Vertex<Long, Long> vertex, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(new Tuple2<>(vertex.getValue(), 1L));
		}
		
	}

	/**
	 * MapFunction to create a vertex with an initial component-id by given vertex-id
	 * 
	 * @author Lukas Werner
	 */
	public static final class VertexCreator implements MapFunction<Long, Vertex<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Vertex<Long, Long> map(Long vertexID) throws Exception {
			return new Vertex<>(vertexID, vertexID);
		}

	}

	/**
	 * FlatMapFunction to create the edges from two vertex-ids. Creates each inverted edge too.
	 * 
	 * This FlatMapFunction has two roles: Create edges from given vertex-ids and undirect the graph
	 * 
	 * @author Lukas Werner
	 */
	public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Edge<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<Long, Long> vertexIDs, Collector<Edge<Long, Long>> out) throws Exception {
			out.collect(new Edge<>(vertexIDs.f0, vertexIDs.f1, 1L));
			out.collect(new Edge<>(vertexIDs.f1, vertexIDs.f0, 1L));
		}

	}
	
	/**
	 * The messenging component of the vertex-centric approach.
	 * 
	 * Send your own value (component-id) to all neighboring vertices.
	 * 
	 * @author Lukas Werner
	 */
	public static final class ComponentIDMessenger extends MessagingFunction<Long, Long, Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public void sendMessages(Vertex<Long, Long> vertex) throws Exception {
			for (Edge<Long, Long> edge: getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getValue());
			}
		}
		
	}
	
	/**
	 * The receiving component of the vertex-centric approach (Updater).
	 * 
	 * Take the minimum of the incoming component-ids and set it as your own.
	 * 
	 * @author Lukas Werner
	 */
	public static final class ComponentIDUpdater extends VertexUpdateFunction<Long, Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) throws Exception {
			Long minValue = vertex.getValue();

			for (long msg : inMessages) {
				if (msg < minValue) {
					minValue = msg;
				}
			}

			if (vertex.getValue() > minValue) {
				setNewVertexValue(minValue);
			}
		}
		
	}

}