package de.lwerner.bigdata.graphMetrics;

import org.apache.commons.cli.ParseException;
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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.utils.CommandLineArguments;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

import java.util.List;

/**
 * Calculates the weakly connected components of a (either undirected or directed) graph.
 * 
 * Solves the problem by using the vertex-centric approach:
 * Each vertex sends its Component-ID (initially their own ID) to all neighboring vertices. 
 * These will take the minimum of the incoming messages and set it as their own new Component-ID.
 * This will be repeated until there is no change anymore (until it converges).
 * 
 * @author Lukas Werner
 * @author Toni Pohl
 */
public class ConnectedComponents<K extends Number, VV, EV> extends GraphAlgorithm<K, VV, EV> {

	private List<Tuple2<K, Long>> componentSizesList;
	private Long biggestSize;

	public ConnectedComponents(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		super(vertices, edges, context);
	}

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
		try {
			arguments = ArgumentsParser.parseArguments(AverageDegree.class.getName(), FILENAME_AVERAGE_DEGREE, args);
		} catch (IllegalArgumentException | ParseException e) {
			e.printStackTrace();
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// Get the graph data
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());
		
		try {
			new ConnectedComponents<Long, FoodBrokerVertex, FoodBrokerEdge>(vertices, edges, env).runAndWrite();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * FlatMapFunction which collects all vertices with a certain component-id and maps it to 1, so we can
	 * calculate the size of the connected components.
	 * 
	 * @author Lukas Werner
	 */
	public final class ComponentIDCounter implements FlatMapFunction<Vertex<K, K>, Tuple2<K, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Vertex<K, K> vertex, Collector<Tuple2<K, Long>> out) throws Exception {
			out.collect(new Tuple2<>(vertex.getValue(), 1L));
		}
		
	}

	/**
	 * MapFunction to create a vertex with an initial component-id by given vertex-id
	 * 
	 * @author Lukas Werner
	 */
	public final class VertexCreator implements MapFunction<Vertex<K, VV>, Vertex<K, K>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Vertex<K, K> map(Vertex<K, VV> vertex) throws Exception {
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
	public final class UndirectEdge implements FlatMapFunction<Edge<K, EV>, Edge<K, EV>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out) throws Exception {
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
	public final class ComponentIDMessenger extends MessagingFunction<K, K, K, EV> {

		private static final long serialVersionUID = 1L;

		@Override
		public void sendMessages(Vertex<K, K> vertex) throws Exception {
			for (Edge<K, EV> edge: getEdges()) {
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
	public final class ComponentIDUpdater extends VertexUpdateFunction<K, K, K> {

		private static final long serialVersionUID = 1L;

		@Override
		public void updateVertex(Vertex<K, K> vertex, MessageIterator<K> inMessages) throws Exception {
			K minValue = vertex.getValue();

			for (K msg : inMessages) {
				if (msg.longValue() < minValue.longValue()) {
					minValue = msg;
				}
			}

			if (vertex.getValue().longValue() > minValue.longValue()) {
				setNewVertexValue(minValue);
			}
		}
		
	}

	@Override
	public void run() throws Exception {
		DataSet<Edge<K, EV>> undirectedEdges = edges.flatMap(new UndirectEdge());
		DataSet<Vertex<K, K>> verticesWithComponentIDs = vertices.map(new VertexCreator());

		Graph<K, K, EV> graph = Graph.fromDataSet(verticesWithComponentIDs, undirectedEdges, context);

		int maxIterations = 10;

		/*
		 * Each vertex sends its component-id (initially the vertex-id) to all neighboring vertices,
		 * takes the minimum of the incoming component-ids and sets it as its own component-ids.
		 */
		Graph<K, K, EV> result = graph.runVertexCentricIteration(new ComponentIDUpdater(),
				new ComponentIDMessenger(), maxIterations);
		
		// The vertices (with the component-id as their value) are the result
		DataSet<Vertex<K, K>> resultVertices = result.getVertices();
		
		resultVertices.print();
		DataSet<Tuple2<K, Long>> componentSizes = resultVertices.flatMap(new ComponentIDCounter())
				.groupBy(0).sum(1);
		componentSizesList = componentSizes.collect();
		
		// TODO: Is correct, but the first property of the tuple is arbitrary??
		DataSet<Tuple2<K, Long>> biggestComponent = componentSizes.aggregate(Aggregations.MAX, 1);
		biggestSize = biggestComponent.collect().get(0).f1;
	}

	@Override
	public JsonNode writeOutput(ObjectMapper m) throws Exception {
		ObjectNode connectedComponentsObject = m.createObjectNode();
		connectedComponentsObject.put("biggestSize", biggestSize);
		ArrayNode connectedComponents = connectedComponentsObject.putArray("connectedComponentSizes");
		for (Tuple2<K, Long> component: componentSizesList) {
			connectedComponents.add(component.f1);
		}
		return connectedComponentsObject;
	}

}