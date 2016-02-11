package de.lwerner.bigdata.graphMetrics;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

/**
 * Test the AverageDegree algorithm
 * 
 * @author Toni Pohl
 */
public class AverageDegreeTest {
	AverageDegree<Integer, Integer, Integer> algo;
	
	@Before
	public void setup() {
		List<Vertex<Integer, Integer>> vs = new ArrayList<>();
		List<Edge<Integer, Integer>> es = new ArrayList<>();

		for (int i = 1; i < 6; i++) {
			vs.add(new Vertex<Integer, Integer>(i, i));
		}
			
		/*
		 * Graph:
		 *    5 - 1 - 2
		 *     \  |  /   
		 *        3
		 *        |
		 *        4
		 */
		es.add(new Edge<Integer, Integer>(1, 2, 1));
		es.add(new Edge<Integer, Integer>(2, 1, 2));
		es.add(new Edge<Integer, Integer>(1, 3, 1));
		es.add(new Edge<Integer, Integer>(3, 1, 3));
		es.add(new Edge<Integer, Integer>(1, 5, 1));
		es.add(new Edge<Integer, Integer>(5, 1, 5));
		es.add(new Edge<Integer, Integer>(2, 3, 2));
		es.add(new Edge<Integer, Integer>(3, 2, 3));
		es.add(new Edge<Integer, Integer>(3, 4, 3));
		es.add(new Edge<Integer, Integer>(4, 3, 4));
		es.add(new Edge<Integer, Integer>(3, 5, 3));
		es.add(new Edge<Integer, Integer>(5, 3, 5));

		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			algo = new AverageDegree<Integer, Integer, Integer>(Graph.fromCollection(vs, es, env), env);
		} catch (Exception e) {
			Assert.fail("Exception during run: " + e.getMessage());
		}
	}
	
	@Test
	public void evaluateDegree() {
		try {
			algo.run();
		} catch (Exception e) {
			Assert.fail("Exception during run: " + e.getMessage());
		}
		Assert.assertEquals(2.4, algo.getAverageDegree(), 0.1);
	}
}
