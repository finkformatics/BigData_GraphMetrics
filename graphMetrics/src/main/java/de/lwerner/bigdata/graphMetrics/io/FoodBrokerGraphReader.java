package de.lwerner.bigdata.graphMetrics.io;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.codehaus.jackson.map.ObjectMapper;

import de.lwerner.bigdata.graphMetrics.models.*;

/**
 * Reads the json files from the FoodBroker data generator
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class FoodBrokerGraphReader extends GraphReader<Long, FoodBrokerVertex, FoodBrokerEdge> {
	
	private static final ObjectMapper mapper = new ObjectMapper();

	public FoodBrokerGraphReader(ExecutionEnvironment env, String verticesPath, String edgesPath) {
		super(env, verticesPath, edgesPath);
	}

    @Override
    public Graph<Long, FoodBrokerVertex, FoodBrokerEdge> getGraph() {
        return Graph.fromDataSet(getVertices(), getEdges(), env);
    }

	private DataSet<Vertex<Long, FoodBrokerVertex>> getVertices() {
		return env.readTextFile(verticesPath).map(new MapFunction<String, Vertex<Long, FoodBrokerVertex>>() {
            @Override
            public Vertex<Long, FoodBrokerVertex> map(String line) throws Exception {
                FoodBrokerVertex vertex = mapper.readValue(line, FoodBrokerVertex.class);
                return new Vertex<>(vertex.getId(), vertex);
            }
        });
	}
	
	private DataSet<Edge<Long, FoodBrokerEdge>> getEdges() {
		return env.readTextFile(edgesPath).map(new MapFunction<String, Edge<Long, FoodBrokerEdge>>() {
            @Override
            public Edge<Long, FoodBrokerEdge> map(String line) throws Exception {
                FoodBrokerEdge edge = mapper.readValue(line, FoodBrokerEdge.class);
                return new Edge<>(edge.getSource(), edge.getTarget(), edge);
            }
        });
	}
}
