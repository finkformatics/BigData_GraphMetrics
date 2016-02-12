package de.lwerner.bigdata.graphMetrics.io;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.codehaus.jackson.map.ObjectMapper;

import de.lwerner.bigdata.graphMetrics.models.*;

/**
 * Reads the json files from the FoodBroker data generator.
 *
 * Vertices and Edges here are rich attributed records. One vertex has the format:
 * {"data":{},"meta":{"system":"ERP","label":"Employee","quality":"good"},"id":1}
 * An edge:
 * {"data":{},"meta":{"label":"sentTo"},"id":1189,"source":1188,"target":1102}
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class FoodBrokerGraphReader extends GraphReader<Long, FoodBrokerVertex, FoodBrokerEdge> {

    /**
     * Jackson ObjectMapper
     */
	private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Sets the environment and the vertices and edges file path
     *
     * @param env flink's execution environment
     * @param verticesPath path to vertices file
     * @param edgesPath path to edges file
     */
	public FoodBrokerGraphReader(ExecutionEnvironment env, String verticesPath, String edgesPath) {
		super(env, verticesPath, edgesPath);
	}

    @Override
    public Graph<Long, FoodBrokerVertex, FoodBrokerEdge> getGraph() {
        return Graph.fromDataSet(getVertices(), getEdges(), env);
    }

    /**
     * Fetch the vertices of a JSON file
     *
     * @return the vertices
     */
	private DataSet<Vertex<Long, FoodBrokerVertex>> getVertices() {
		return env.readTextFile(verticesPath).map(new MapFunction<String, Vertex<Long, FoodBrokerVertex>>() {
            @Override
            public Vertex<Long, FoodBrokerVertex> map(String line) throws Exception {
                FoodBrokerVertex vertex = mapper.readValue(line, FoodBrokerVertex.class); // Mapping
                return new Vertex<>(vertex.getId(), vertex);
            }
        });
	}

    /**
     * Fetch the edges of a JSON file
     *
     * @return the edges
     */
	private DataSet<Edge<Long, FoodBrokerEdge>> getEdges() {
		return env.readTextFile(edgesPath).map(new MapFunction<String, Edge<Long, FoodBrokerEdge>>() {
            @Override
            public Edge<Long, FoodBrokerEdge> map(String line) throws Exception {
                FoodBrokerEdge edge = mapper.readValue(line, FoodBrokerEdge.class); // Mapping
                return new Edge<>(edge.getSource(), edge.getTarget(), edge);
            }
        });
	}
}
