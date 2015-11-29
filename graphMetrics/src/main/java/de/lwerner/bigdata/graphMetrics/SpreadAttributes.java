package de.lwerner.bigdata.graphMetrics;

import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
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
 * Job to count attributes of vertices and edges
 * Count different patterns of attributes
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class SpreadAttributes {
	
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
		arguments = ArgumentsParser.parseArguments(SpreadAttributes.class.getName(), FILENAME_SPREAD_ATTRIBUTES, args);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());
		
		// Attributes of vertices
		DataSet<Tuple2<String, Integer>> attributesVertices
			= vertices.flatMap(new FlatMapFunction<Vertex<Long, FoodBrokerVertex>, Tuple2<String, Integer>>() {
	
				private static final long serialVersionUID = 1L;
	
				public void flatMap(Vertex<Long, FoodBrokerVertex> in, Collector<Tuple2<String, Integer>> out) throws Exception {
					Iterator<String> atts = in.getValue().getData().getFieldNames();
					
					while (atts.hasNext()) {
						out.collect(new Tuple2<String, Integer>(atts.next(), 1));
					}
				}
		})
		.groupBy(0)
		.sum(1);
		
		
		// Attributes of edges
		DataSet<Tuple2<String, Integer>> attributesEdges
			= edges.flatMap(new FlatMapFunction<Edge<Long,FoodBrokerEdge>, Tuple2<String, Integer>>() {
				
				private static final long serialVersionUID = 1L;
	
				public void flatMap(Edge<Long, FoodBrokerEdge> in, Collector<Tuple2<String, Integer>> out) throws Exception {
					Iterator<String> atts = in.getValue().getData().getFieldNames();
					
					while (atts.hasNext()) {
						out.collect(new Tuple2<String, Integer>(atts.next(), 1));
					}
				}
		})
		.groupBy(0)
		.sum(1);
		
		DataSet<Tuple2<String, Integer>> attributesCount = attributesVertices.union(attributesEdges).groupBy(0).sum(1);
		List<Tuple2<String, Integer>> attributesCountList = attributesCount.collect();
		
		ObjectMapper m = new ObjectMapper();
		ObjectNode attributesObject = m.createObjectNode();
		ArrayNode attributesCountArray = attributesObject.putArray("attributesCount");
		for (Tuple2<String, Integer> attributeCount: attributesCountList) {
			ObjectNode attributeCountObject = attributesCountArray.addObject();
			attributeCountObject.put("attribute", attributeCount.f0);
			attributeCountObject.put("count", attributeCount.f1);
		}
		
		// Number of keys vertices
		DataSet<Tuple2<String, Integer>> vertexAttributeSchemasCount = vertices.flatMap(new FlatMapFunction<Vertex<Long,FoodBrokerVertex>, Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			public void flatMap(Vertex<Long, FoodBrokerVertex> in, Collector<Tuple2<String, Integer>> out)
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
		})
		.groupBy(0)
		.sum(1);
		List<Tuple2<String, Integer>> vertexAttributeSchemasCountList = vertexAttributeSchemasCount.collect();
		
		ArrayNode vertexAttributeSchemasCountArray = attributesObject.putArray("vertexAttributeSchemasCount");
		for (Tuple2<String, Integer> schemaCount: vertexAttributeSchemasCountList) {
			ObjectNode schemaCountObject = vertexAttributeSchemasCountArray.addObject();
			schemaCountObject.put("schema", schemaCount.f0);
			schemaCountObject.put("count", schemaCount.f1);
		}
		
		// Number of keys edges		
		DataSet<Tuple2<String, Integer>> edgeAttributeSchemasCount = edges.flatMap(new FlatMapFunction<Edge<Long,FoodBrokerEdge>, Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			public void flatMap(Edge<Long, FoodBrokerEdge> in, Collector<Tuple2<String, Integer>> out)
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
		})
		.groupBy(0)
		.sum(1);
		
		List<Tuple2<String, Integer>> edgeAttributeSchemasCountList = edgeAttributeSchemasCount.collect();
		
		ArrayNode edgeAttributeSchemasCountArray = attributesObject.putArray("edgeAttributeSchemasCount");
		for (Tuple2<String, Integer> schemaCount: edgeAttributeSchemasCountList) {
			ObjectNode schemaCountObject = edgeAttributeSchemasCountArray.addObject();
			schemaCountObject.put("schema", schemaCount.f0);
			schemaCountObject.put("count", schemaCount.f1);
		}
		
		GraphMetricsWriter.writeJson(m, attributesObject, arguments.getOutputPath());
	}
	
}