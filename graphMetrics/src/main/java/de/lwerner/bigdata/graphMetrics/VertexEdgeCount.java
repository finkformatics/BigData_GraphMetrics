package de.lwerner.bigdata.graphMetrics;

import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

/**
 * Apache Flink job for computing vertex and edge count of a given graph
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class VertexEdgeCount<K extends Number, VV, EV> extends GraphAlgorithm<K, VV, EV> {
	private long verticesCount = 0;
	private long edgesCount = 0;

	public VertexEdgeCount(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		super(vertices, edges, context);
	}
	
	public long getVerticesCount() {
		return verticesCount;
	}

	public long getEdgesCount() {
		return edgesCount;
	}

	/**
	 * The main job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			arguments = ArgumentsParser.parseArguments(AverageDegree.class.getName(), FILENAME_VERTEX_EDGE_COUNT, args);
		} catch (IllegalArgumentException | ParseException e) {
			e.printStackTrace();
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());
		
		new VertexEdgeCount<Long, FoodBrokerVertex, FoodBrokerEdge>(vertices, edges, env).runAndWrite();
		
	}

	@Override
	public void run() throws Exception {
		Graph<K, VV, EV> graph = getGraph();
		
		verticesCount = graph.numberOfVertices();
		edgesCount = graph.numberOfEdges();		
	}

	@Override
	public JsonNode writeOutput(ObjectMapper m) throws Exception {
		ObjectNode countNode = m.createObjectNode();
		countNode.put("vertexCount", verticesCount);
		countNode.put("edgeCount", edgesCount);
		return countNode;
	}
	
}