package de.lwerner.bigdata.graphMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;

public class SpreadLabelsTest {
	SpreadLabels<Integer, FoodBrokerVertex, FoodBrokerEdge> algo;
	ObjectMapper mapper = new ObjectMapper();
	
	@Before
	public void setup() {
		mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		List<Vertex<Integer, FoodBrokerVertex>> vs = new ArrayList<>();
		List<Edge<Integer, FoodBrokerEdge>> es = new ArrayList<>();
		
		for (int i = 1; i < 6; i++) {
			FoodBrokerVertex vertex = null;
			try {
				vertex = mapper.readValue("{'meta' : {'label': 'Label " + (i % 3) + "' }}", FoodBrokerVertex.class);
			} catch (IOException e) {
				Assert.fail("Exception during setup: " + e.getMessage());
			}
			vs.add(new Vertex<>(i, vertex));
		}
		
		for (int i = 1; i < 11; i++) {
			FoodBrokerEdge edge = null;
			try {
				edge = mapper.readValue("{'meta': {'label': 'Label " + (i % 3) + "' }}", FoodBrokerEdge.class);
			} catch (IOException e) {
				Assert.fail("Exception during setup: " + e.getMessage());
			}
			es.add(new Edge<>(i, (i % 10) + 1, edge));
		}
		
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			algo = new SpreadLabels<>(Graph.fromCollection(vs, es, env), env);
			algo.run();
		} catch (Exception e) {
			Assert.fail("Exception during run: " + e.getMessage());
		}
	}
	
	@Test
	public void evaluateVertexLabels() {
		List<Tuple2<String, Integer>> labelCount = null;
		try {
			labelCount = algo.getVerticesLabelCount().collect();
		} catch (Exception e) {
			Assert.fail("Exception during evaluation: " + e.getMessage());
		}
		
		Assert.assertEquals(3, labelCount.size());
		Assert.assertEquals(2, labelCount.get(0).f1.intValue());
		Assert.assertEquals(1, labelCount.get(1).f1.intValue());
		Assert.assertEquals(2, labelCount.get(2).f1.intValue());
	}
	
	@Test
	public void evaluateEdgeLabels() {
		List<Tuple2<String, Integer>> labelCount = null;
		try {
			labelCount = algo.getEdgesLabelCount().collect();
		} catch (Exception e) {
			Assert.fail("Exception during evaluation: " + e.getMessage());
		}
		
		Assert.assertEquals(3, labelCount.size());
		Assert.assertEquals(4, labelCount.get(0).f1.intValue());
		Assert.assertEquals(3, labelCount.get(1).f1.intValue());
		Assert.assertEquals(3, labelCount.get(2).f1.intValue());
	}
}
