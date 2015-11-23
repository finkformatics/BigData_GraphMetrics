package de.lwerner.bigdata.graphMetrics.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.codehaus.jackson.map.ObjectMapper;

import de.lwerner.bigdata.graphMetrics.models.*;

/**
 * Reads the json files from the FoodBroker data generator
 * 
 * @author Toni Pohl
 */
public class FoodBrokerReader {
	
	private final static ObjectMapper mapper = new ObjectMapper();
	
	/**
	 * Reads the vertices as a DataSet<>
	 * @param env
	 * @return
	 */
	public static DataSet<Vertex<Long, FoodBrokerVertex>> getVertices(ExecutionEnvironment env, String nodesPath) {		
		return env.readTextFile(nodesPath).map(new MapFunction<String, Vertex<Long, FoodBrokerVertex>>() {
			private static final long serialVersionUID = 1L;

			public Vertex<Long, FoodBrokerVertex> map(String arg0) throws Exception {
				
				FoodBrokerVertex vertex = mapper.readValue(arg0, FoodBrokerVertex.class);
				return new Vertex<Long, FoodBrokerVertex>(vertex.getId(), vertex);
			}
		});
	}
	
	/**
	 * Reads the edges as a DataSet<>
	 * @param env
	 * @return
	 */
	public static DataSet<Edge<Long, FoodBrokerEdge>> getEdges(ExecutionEnvironment env, String edgesPath) {		
		return env.readTextFile(edgesPath).map(new MapFunction<String, Edge<Long, FoodBrokerEdge>>() {
			private static final long serialVersionUID = 1L;

			public Edge<Long, FoodBrokerEdge> map(String arg0) throws Exception {
				
				FoodBrokerEdge edge = mapper.readValue(arg0, FoodBrokerEdge.class);
				return new Edge<Long, FoodBrokerEdge>(edge.getSource(), edge.getTarget(), edge);
			}
		});
	}
}
