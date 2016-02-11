package de.lwerner.bigdata.graphMetrics;

import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.io.FoodBrokerGraphReader;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

import java.util.LinkedList;
import java.util.List;

/**
 * Job to count the label key of vertices and edges.
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class SpreadLabels<K, VV extends FoodBrokerVertex, EV extends FoodBrokerEdge> extends GraphAlgorithm<K, VV, EV> {
	DataSet<Tuple2<String, Integer>> verticesLabelCount;
	DataSet<Tuple2<String, Integer>> spreadLabels;
	DataSet<Tuple2<String, Integer>> edgesLabelCount;
	List<Tuple2<String, Integer>> spreadLabelsList = new LinkedList<>();

	public DataSet<Tuple2<String, Integer>> getVerticesLabelCount() {
		return verticesLabelCount;
	}

	public DataSet<Tuple2<String, Integer>> getEdgesLabelCount() {
		return edgesLabelCount;
	}
	
	public SpreadLabels(Graph<K, VV, EV> graph, ExecutionEnvironment context) throws Exception {
		super(graph, context);
	}

	/**
	 * The main job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			arguments = ArgumentsParser.parseArguments(AverageDegree.class.getName(), FILENAME_SPREAD_LABELS, args);
		} catch (IllegalArgumentException | ParseException e) {
			e.printStackTrace();
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		FoodBrokerGraphReader reader = new FoodBrokerGraphReader(env, arguments.getVerticesPath(), arguments.getEdgesPath());
		new SpreadLabels<>(reader.getGraph(), env).runAndWrite();
	}

	@Override
	public void run() throws Exception {
		verticesLabelCount = getVertices()
				.flatMap(new FlatMapFunction<Vertex<K, VV>, Tuple2<String, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Vertex<K, VV> in, Collector<Tuple2<String, Integer>> out)
							throws Exception {
						out.collect(new Tuple2<>(in.f1.getMeta().get("label").toString(), 1));
					}
				}).groupBy(0).sum(1);

		edgesLabelCount = getEdges()
				.flatMap(new FlatMapFunction<Edge<K, EV>, Tuple2<String, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Edge<K, EV> in, Collector<Tuple2<String, Integer>> out)
							throws Exception {
						out.collect(new Tuple2<>(in.f2.getMeta().get("label").toString(), 1));
					}
				}).groupBy(0).sum(1);

		spreadLabels = verticesLabelCount.union(edgesLabelCount).groupBy(0).sum(1);
		spreadLabelsList = spreadLabels.collect();
	}

	@Override
	public JsonNode writeOutput(ObjectMapper m) {
		ObjectNode spreadLabelsObject = m.createObjectNode();
		ArrayNode spreadLabelsArray = spreadLabelsObject.putArray("spreadLabels");
		for (Tuple2<String, Integer> labelCount : spreadLabelsList) {
			ObjectNode labelCountObject = spreadLabelsArray.addObject();
			labelCountObject.put("label", labelCount.f0);
			labelCountObject.put("count", labelCount.f1);
		}
		return spreadLabelsObject;
	}

}