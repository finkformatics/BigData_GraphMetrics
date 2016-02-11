package de.lwerner.bigdata.graphMetrics;

import de.lwerner.bigdata.graphMetrics.io.GraphReader;
import de.lwerner.bigdata.graphMetrics.io.SimpleGraphReader;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.io.FoodBrokerGraphReader;

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

	public ConnectedComponents(Graph<K, VV, EV> graph, ExecutionEnvironment context) throws Exception {
		super(graph, context, true);
	}
	
	/**
	 * The main job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			arguments = ArgumentsParser.parseArguments(AverageDegree.class.getName(), FILENAME_CONNECTED_COMPONENTS, args);
		} catch (IllegalArgumentException | ParseException e) {
			e.printStackTrace();
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		GraphReader reader = null;
		String readerType = arguments.getReader();
		switch (readerType) {
			case "simple":
				reader = new SimpleGraphReader(env, arguments.getGraphPath());
				break;
			case "food":
				reader = new FoodBrokerGraphReader(env, arguments.getVerticesPath(), arguments.getEdgesPath());
				break;
			default:
				throw new IllegalArgumentException("Invalid reader type!");
		}

		new ConnectedComponents<>(reader.getGraph(), env).runAndWrite();
	}
	
	/**
	 * FlatMapFunction which collects all vertices with a certain component-id and maps it to 1, so we can
	 * calculate the size of the connected components.
	 * 
	 * @author Lukas Werner
	 */
	public final class ComponentIDCounter implements FlatMapFunction<Vertex<K, K>, Tuple2<K, Long>> {
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
		@Override
		public Vertex<K, K> map(Vertex<K, VV> vertex) throws Exception {
			return new Vertex<>(vertex.getId(), vertex.getId());
		}
	}

	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	@ForwardedFieldsFirst("f1->f1")
	@ForwardedFieldsSecond("f1->f0")
	public final class NeighborWithComponentIDJoin implements JoinFunction<Vertex<K, K>, Edge<K, EV>, Vertex<K, K>> {
		@Override
		public Vertex<K, K> join(Vertex<K, K> vertexWithComponent, Edge<K, EV> edge) {
			return new Vertex<>(edge.f1, vertexWithComponent.f1);
		}
	}
	
	@ForwardedFieldsFirst("*")
	public final class ComponentIdFilter implements FlatJoinFunction<Vertex<K, K>, Vertex<K, K>, Vertex<K, K>> {
		@Override
		public void join(Vertex<K, K> candidate, Vertex<K, K> old, Collector<Vertex<K, K>> out) {
			if (candidate.f1.longValue() < old.f1.longValue()) {
				out.collect(candidate);
			}
		}
	}

	@Override
	public void run() throws Exception {
		DataSet<Vertex<K, K>> verticesWithComponentIDs = getVertices().map(new VertexCreator());

		DeltaIteration<Vertex<K, K>, Vertex<K, K>> iteration = 
				verticesWithComponentIDs.iterateDelta(verticesWithComponentIDs, arguments.getMaxIterations(), 0);
		
		DataSet<Vertex<K, K>> changes = iteration.getWorkset()
				.join(getEdges())
				.where(0)
				.equalTo(0)
				.with(new NeighborWithComponentIDJoin())
				.groupBy(0)
				.aggregate(Aggregations.MIN, 1)
				.join(iteration.getSolutionSet())
				.where(0)
				.equalTo(0)
				.with(new ComponentIdFilter());
		
		DataSet<Tuple2<K, Long>> result = iteration
				.closeWith(changes, changes)
				.flatMap(new ComponentIDCounter())
				.groupBy(0)
				.sum(1);

		componentSizesList = result.collect();
		
		DataSet<Tuple2<K, Long>> biggestComponent = result.aggregate(Aggregations.MAX, 1);
		biggestSize = biggestComponent
				.collect()
				.get(0)
				.f1;
	}

	@Override
	public JsonNode writeOutput(ObjectMapper m) {
		ObjectNode connectedComponentsObject = m.createObjectNode();
		connectedComponentsObject.put("biggestSize", biggestSize);
		ArrayNode connectedComponents = connectedComponentsObject.putArray("connectedComponentSizes");
		for (Tuple2<K, Long> component: componentSizesList) {
			connectedComponents.add(component.f1);
		}
		return connectedComponentsObject;
	}

}