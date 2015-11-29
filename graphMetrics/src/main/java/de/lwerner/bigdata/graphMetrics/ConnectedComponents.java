package de.lwerner.bigdata.graphMetrics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.utils.CommandLineArguments;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

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
		arguments = ArgumentsParser.parseArguments(ConnectedComponents.class.getName(), FILENAME_CONNECTED_COMPONENTS, args);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// Get the graph data
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());

		DataSet<Edge<Long, FoodBrokerEdge>> undirectedEdges = edges.flatMap(new UndirectEdge());
		DataSet<Vertex<Long, Long>> verticesWithComponentIDs = vertices.map(new VertexCreator());

		Graph<Long, Long, FoodBrokerEdge> graph = Graph.fromDataSet(verticesWithComponentIDs, undirectedEdges, env);

		int maxIterations = 10;

		/*
		 * Each vertex sends its component-id (initially the vertex-id) to all neighboring vertices,
		 * takes the minimum of the incoming component-ids and sets it as its own component-ids.
		 */
		Graph<Long, Long, FoodBrokerEdge> result = graph.runVertexCentricIteration(new ComponentIDUpdater(),
				new ComponentIDMessenger(), maxIterations);
		
		// The vertices (with the component-id as their value) are the result
		DataSet<Vertex<Long, Long>> resultVertices = result.getVertices();
		
		resultVertices.print();
		DataSet<Tuple2<Long, Long>> componentSizes = resultVertices.flatMap(new ComponentIDCounter())
				.groupBy(0).sum(1);
		
		// TODO: Is correct, but the first property of the tuple is arbitrary??
		DataSet<Tuple2<Long, Long>> biggestComponent = componentSizes.aggregate(Aggregations.MAX, 1);
		
		componentSizes.print();
		
		biggestComponent.print();
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
	public static final class VertexCreator implements MapFunction<Vertex<Long, FoodBrokerVertex>, Vertex<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Vertex<Long, Long> map(Vertex<Long, FoodBrokerVertex> vertex) throws Exception {
			return new Vertex<>(vertex.getId(), vertex.getId());
		}

	}

	/**
	 * FlatMapFunction to create the edges from two vertex-ids. Creates each inverted edge too.
	 * 
	 * This FlatMapFunction has two roles: Create edges from given vertex-ids and undirect the graph
	 * 
	 * @author Lukas Werner
	 */
	public static final class UndirectEdge implements FlatMapFunction<Edge<Long, FoodBrokerEdge>, Edge<Long, FoodBrokerEdge>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Edge<Long, FoodBrokerEdge> edge, Collector<Edge<Long, FoodBrokerEdge>> out) throws Exception {
			out.collect(edge);
			out.collect(new Edge<>(edge.getTarget(), edge.getSource(), edge.getValue()));
		}

	}
	
	/**
	 * The messenging component of the vertex-centric approach.
	 * 
	 * Send your own value (component-id) to all neighboring vertices.
	 * 
	 * @author Lukas Werner
	 */
	public static final class ComponentIDMessenger extends MessagingFunction<Long, Long, Long, FoodBrokerEdge> {

		private static final long serialVersionUID = 1L;

		@Override
		public void sendMessages(Vertex<Long, Long> vertex) throws Exception {
			for (Edge<Long, FoodBrokerEdge> edge: getEdges()) {
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