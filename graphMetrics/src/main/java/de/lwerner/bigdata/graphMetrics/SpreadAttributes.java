package de.lwerner.bigdata.graphMetrics;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
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
import de.lwerner.bigdata.graphMetrics.utils.GraphMetricsWriter;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

/**
 * Job to count attributes of vertices and edges Count different patterns of
 * attributes
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class SpreadAttributes<K, VV extends FoodBrokerVertex, EV extends FoodBrokerEdge>
		extends GraphAlgorithm<K, VV, EV> {

	DataSet<Tuple2<String, Integer>> attributesCount;
	DataSet<Tuple2<String, Integer>> vertexAttributeSchemasCount;
	DataSet<Tuple2<String, Integer>> edgeAttributeSchemasCount;
	

	public SpreadAttributes(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		super(vertices, edges, context);
	}

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

		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env,
				arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());

		new SpreadAttributes<Long, FoodBrokerVertex, FoodBrokerEdge>(vertices, edges, env).runAndWrite();

		
	}
	
	/**
	 * FlatMapFunction to get the vertex attributes
	 * 
	 * @author Toni Pohl
	 */
	public final class VertexAttributes implements FlatMapFunction<Vertex<K, VV>, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Vertex<K, VV> in, Collector<Tuple2<String, Integer>> out) throws Exception {
			Iterator<String> atts = in.getValue().getData().getFieldNames();

			while (atts.hasNext()) {
				out.collect(new Tuple2<String, Integer>(atts.next(), 1));
			}	
		}
	}
	
	/**
	 * FlatMapFunction to get the edge attributes
	 * 
	 * @author Toni Pohl
	 */
	public final class EdgeAttributes implements FlatMapFunction<Edge<K, EV>, Tuple2<String, Integer>> {
		
		private static final long serialVersionUID = 1L;

		public void flatMap(Edge<K, EV> in, Collector<Tuple2<String, Integer>> out) throws Exception {
			Iterator<String> atts = in.getValue().getData().getFieldNames();

			while (atts.hasNext()) {
				out.collect(new Tuple2<String, Integer>(atts.next(), 1));
			}
		}
	}
	
	/**
	 * FlatMapFunction to get a schema of all keys in a vertex
	 * 
	 * @author Toni Pohl
	 */
	public final class VertexKeySchema implements FlatMapFunction<Vertex<K, VV>, Tuple2<String, Integer>> {
		
		private static final long serialVersionUID = 1L;

		public void flatMap(Vertex<K, VV> in, Collector<Tuple2<String, Integer>> out)
				throws Exception {
			Iterator<String> atts = in.getValue().getData().getFieldNames();
			StringBuilder sb = new StringBuilder();
			while (atts.hasNext()) {
				sb.append(atts.next()).append(" ");
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}

			out.collect(new Tuple2<String, Integer>(sb.toString(), 1));
		}
	}
	
	/**
	 * FlatMapFunction to get a schema of all keys in a edge
	 * 
	 * @author Toni Pohl
	 */
	public final class EdgeKeySchema implements FlatMapFunction<Edge<K, EV>, Tuple2<String, Integer>> {
		
		private static final long serialVersionUID = 1L;

		public void flatMap(Edge<K, EV> in, Collector<Tuple2<String, Integer>> out)
				throws Exception {
			Iterator<String> atts = in.getValue().getData().getFieldNames();
			StringBuilder sb = new StringBuilder();
			while (atts.hasNext()) {
				sb.append(atts.next()).append(" ");
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}

			out.collect(new Tuple2<String, Integer>(sb.toString(), 1));
		}
	}

	@Override
	public void run() throws Exception {
		// Attributes of vertices
		DataSet<Tuple2<String, Integer>> attributesVertices = vertices.flatMap(new VertexAttributes()).groupBy(0).sum(1);

		// Attributes of edges
		DataSet<Tuple2<String, Integer>> attributesEdges = edges.flatMap(new EdgeAttributes()).groupBy(0).sum(1);

		// Union attributes
		attributesCount = attributesVertices.union(attributesEdges).groupBy(0).sum(1);

		// Number of keys vertices
		vertexAttributeSchemasCount = vertices.flatMap(new VertexKeySchema()).groupBy(0).sum(1);

		// Number of keys edges
		edgeAttributeSchemasCount = edges.flatMap(new EdgeKeySchema()).groupBy(0).sum(1);
	}

	@Override
	public JsonNode writeOutput(ObjectMapper m) throws Exception {
		List<Tuple2<String, Integer>> attributesCountList = attributesCount.collect();

		ObjectNode attributesObject = m.createObjectNode();
		ArrayNode attributesCountArray = attributesObject.putArray("attributesCount");
		for (Tuple2<String, Integer> attributeCount : attributesCountList) {
			ObjectNode attributeCountObject = attributesCountArray.addObject();
			attributeCountObject.put("attribute", attributeCount.f0);
			attributeCountObject.put("count", attributeCount.f1);
		}

		List<Tuple2<String, Integer>> vertexAttributeSchemasCountList = vertexAttributeSchemasCount.collect();

		ArrayNode vertexAttributeSchemasCountArray = attributesObject.putArray("vertexAttributeSchemasCount");
		for (Tuple2<String, Integer> schemaCount : vertexAttributeSchemasCountList) {
			ObjectNode schemaCountObject = vertexAttributeSchemasCountArray.addObject();
			schemaCountObject.put("schema", schemaCount.f0);
			schemaCountObject.put("count", schemaCount.f1);
		}
		
		List<Tuple2<String, Integer>> edgeAttributeSchemasCountList = edgeAttributeSchemasCount.collect();

		ArrayNode edgeAttributeSchemasCountArray = attributesObject.putArray("edgeAttributeSchemasCount");
		for (Tuple2<String, Integer> schemaCount : edgeAttributeSchemasCountList) {
			ObjectNode schemaCountObject = edgeAttributeSchemasCountArray.addObject();
			schemaCountObject.put("schema", schemaCount.f0);
			schemaCountObject.put("count", schemaCount.f1);
		}

		return attributesObject;
	}
}