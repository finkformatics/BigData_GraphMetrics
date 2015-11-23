package de.lwerner.bigdata.graphMetrics.singleJobs.spreadAttributes;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

/**
 * Job to count attributes of vertices and edges
 * Count different patterns of attributes
 * 
 * @author Toni Pohl
 */
public class SpreadAttributes {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env);
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env);
		
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
		
		attributesVertices.union(attributesEdges).groupBy(0).sum(1).print();
		
		// Number of keys vertices
		vertices.flatMap(new FlatMapFunction<Vertex<Long,FoodBrokerVertex>, Tuple2<String, Integer>>() {

			public void flatMap(Vertex<Long, FoodBrokerVertex> in, Collector<Tuple2<String, Integer>> out)
					throws Exception {
				Iterator<String> atts = in.getValue().getData().getFieldNames();
				String str = "";
				
				while (atts.hasNext()) {
					str += atts.next() + " ";
				}
				
				out.collect(new Tuple2<String, Integer>(str, 1));
			
			}
		})
		.groupBy(0)
		.sum(1)
		.print();
		
		// Number of keys edges		
		edges.flatMap(new FlatMapFunction<Edge<Long,FoodBrokerEdge>, Tuple2<String, Integer>>() {

			public void flatMap(Edge<Long, FoodBrokerEdge> in, Collector<Tuple2<String, Integer>> out)
					throws Exception {
				Iterator<String> atts = in.getValue().getData().getFieldNames();
				String str = "";
				
				while (atts.hasNext()) {
					str += atts.next() + " ";
				}
				
				out.collect(new Tuple2<String, Integer>(str, 1));
			
			}
		})
		.groupBy(0)
		.sum(1)
		.print();
	}
	
}