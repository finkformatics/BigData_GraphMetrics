package de.lwerner.bigdata.graphMetrics;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterCoefficientTest {
	ClusterCoefficient<Integer, Integer, Integer> algo;
	
	@Before
	public void setup() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		List<Vertex<Integer, Integer>> vs = new ArrayList<>();
		vs.add(new Vertex<>(1, 0));
		vs.add(new Vertex<>(2, 0));
		vs.add(new Vertex<>(3, 0));
		vs.add(new Vertex<>(4, 0));
		vs.add(new Vertex<>(5, 0));
		
		List<Edge<Integer, Integer>> es= new ArrayList<>();
		es.add(new Edge<Integer, Integer>(1, 2, 0));
		es.add(new Edge<Integer, Integer>(3, 1, 0));
		es.add(new Edge<Integer, Integer>(2, 3, 0));
		es.add(new Edge<Integer, Integer>(2, 4, 0));
		es.add(new Edge<Integer, Integer>(4, 5, 0));
		
		try {
			algo = new ClusterCoefficient<>(Graph.fromCollection(vs, es, env),
					env,
					true);
			algo.run();
		} catch (Exception e) {
			Assert.fail("Exception during setup: " + e.getMessage());
		}
	}
	
	@Test
	public void evaluateClusteringCoefficient() {
		Assert.assertEquals(0.5, algo.getGlobalClusterCoefficient(), 0.01);
	}

}
