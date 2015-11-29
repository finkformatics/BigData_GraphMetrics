package de.lwerner.bigdata.graphMetrics;

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

import java.util.List;

/**
 * Job to count the label key of vertices and edges.
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class SpreadLabels {
	
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
		arguments = ArgumentsParser.parseArguments(SpreadLabels.class.getName(), FILENAME_SPREAD_LABELS, args);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());
				
		DataSet<Tuple2<String, Integer>> verticesLabelCount 
			= vertices.flatMap(new FlatMapFunction<Vertex<Long, FoodBrokerVertex>, Tuple2<String, Integer>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void flatMap(Vertex<Long, FoodBrokerVertex> in, Collector<Tuple2<String, Integer>> out)
						throws Exception {
					out.collect(new Tuple2<String, Integer>(in.f1.getMeta().get("label").toString(), 1));
				}
		})
		.groupBy(0)
		.sum(1);
		
		DataSet<Tuple2<String, Integer>> edgesLabelCount 
			= edges.flatMap(new FlatMapFunction<Edge<Long, FoodBrokerEdge>, Tuple2<String, Integer>>() {
	
				private static final long serialVersionUID = 1L;

				@Override
				public void flatMap(Edge<Long, FoodBrokerEdge> in, Collector<Tuple2<String, Integer>> out)
						throws Exception {
					out.collect(new Tuple2<String, Integer>(in.f2.getMeta().get("label").toString(), 1));
				}
		})
		.groupBy(0)
		.sum(1);
		
		DataSet<Tuple2<String, Integer>> spreadLabels = verticesLabelCount.union(edgesLabelCount).groupBy(0).sum(1);
		List<Tuple2<String, Integer>> spreadLabelsList = spreadLabels.collect();
		
		ObjectMapper m = new ObjectMapper();
		ObjectNode spreadLabelsObject = m.createObjectNode();
		ArrayNode spreadLabelsArray = spreadLabelsObject.putArray("spreadLabels");
		for (Tuple2<String, Integer> labelCount: spreadLabelsList) {
			ObjectNode labelCountObject = spreadLabelsArray.addObject();
			labelCountObject.put("label", labelCount.f0);
			labelCountObject.put("count", labelCount.f1);
		}
		
		GraphMetricsWriter.writeJson(m, spreadLabelsObject, arguments.getOutputPath());
	}
	
}