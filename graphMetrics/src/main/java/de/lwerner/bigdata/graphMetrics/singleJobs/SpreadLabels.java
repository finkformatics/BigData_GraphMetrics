package de.lwerner.bigdata.graphMetrics.singleJobs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.utils.CommandLineArguments;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

/**
 * Job to count the label key of vertices and edges.
 * 
 * @author Toni Pohl
 */
public class SpreadLabels {
	
	private static CommandLineArguments arguments;

	public static void main(String[] args) throws Exception {
		arguments = ArgumentsParser.parseArguments(SpreadLabels.class.getName(), args);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getNodesPath());
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
		
		verticesLabelCount.union(edgesLabelCount).groupBy(0).sum(1).print();
	}
	
}