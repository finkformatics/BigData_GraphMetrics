package de.lwerner.bigdata.graphMetrics;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the abstract GraphAlgorithm class
 * 
 * @author Toni Pohl
 */
public class GraphAlgorithmTest {
	private GraphAlgorithm<Integer, Integer, Integer> algo;
	private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	private DataSet<Vertex<Integer, Integer>> vertices;
	private DataSet<Edge<Integer, Integer>> edges;
	
	@Before
	public void setup() {
		List<Vertex<Integer, Integer>> vs = new ArrayList<>();
		List<Edge<Integer, Integer>> es = new ArrayList<>();

		vs.add(new Vertex<Integer, Integer>(1, 1));
		vs.add(new Vertex<Integer, Integer>(2, 2));
		
		es.add(new Edge<Integer, Integer>(1, 2, 1));

		try {
			algo = new GraphAlgorithm<Integer, Integer, Integer>(Graph.fromCollection(vs, es, env), env) {
				@Override
				public void run() {
					// do nothing
				}

				@Override
				public JsonNode writeOutput(ObjectMapper m) {
					// do nothing
					return null;
				}
			};
		} catch (Exception e) {
			Assert.fail("Exception during run: " + e.getMessage());
		}
	}
	
	@Test
	public void  evaluateGetter() {
		Assert.assertEquals(vertices, algo.getVertices());
		Assert.assertEquals(edges, algo.getEdges());
		Assert.assertEquals(env, algo.getContext());
	}
}
