package de.lwerner.bigdata.graphMetrics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.TriangleEnumerator.Triad;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.utils.CommandLineArguments;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;
import de.lwerner.bigdata.graphMetrics.utils.GraphMetricsWriter;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

/**
 * Calculates the global clustering coefficient of a graph.
 * 
 * Solves the problem by counting the triads and the triple of the graph.
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class ClusterCoefficient {
	
	/**
	 * Command line arguments
	 */
	private static CommandLineArguments arguments;

	/**
	 * The main job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		arguments = ArgumentsParser.parseArguments(ConnectedComponents.class.getName(), FILENAME_CLUSTER_COEFFICIENT, args);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env, arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = FoodBrokerReader.getEdges(env, arguments.getEdgesPath());
		
		DataSet<Edge<Long, FoodBrokerEdge>> undirectedEdges = edges.flatMap(new UndirectedEdge());
		
		
		Graph<Long, FoodBrokerVertex, FoodBrokerEdge> g = Graph.fromDataSet(vertices, undirectedEdges, env);
		
		DataSet<Edge<Long, FoodBrokerEdge>> edgesById = edges.map(new EdgesIdMapFunction());
		
		DataSet<Triad<Long>> triads = edgesById
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new TriadBuilder())
			.join(edgesById)
			.where(Triad.V2, Triad.V3)
			.equalTo(0, 1)
			.with(new JoinFunction<Triad<Long>, Edge<Long, FoodBrokerEdge>, Triad<Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Triad<Long> join(Triad<Long> first, Edge<Long, FoodBrokerEdge> second) throws Exception {
					return first;
				}
			})
			.distinct();
		
		long triadsCount = triads.count();
		
		DataSet<Tuple2<Long, Long>> triple = g.outDegrees().map(new TriplePerVertixMapFunction()).sum(1);
		long tripleCount = triple.collect().get(0).f1;
		double globalClusterCoefficient = triadsCount * 3 / (double) tripleCount;
		
		ObjectMapper m = new ObjectMapper();
		ObjectNode clusterCoefficientObject = m.createObjectNode();
		clusterCoefficientObject.put("globalClusterCoefficient", globalClusterCoefficient);
		
		GraphMetricsWriter.writeJson(m, clusterCoefficientObject, arguments.getOutputPath());
	}
	
	/**
	 * FlatMapFunction which created undirected edges from edges
	 * 
	 * @author Toni Pohl
	 */
	private static class UndirectedEdge implements FlatMapFunction<Edge<Long,FoodBrokerEdge>, Edge<Long, FoodBrokerEdge>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Edge<Long, FoodBrokerEdge> edge, Collector<Edge<Long, FoodBrokerEdge>> out) throws Exception {
			out.collect(edge);
			out.collect(new Edge<>(edge.getTarget(), edge.getSource(), edge.getValue()));
		}
	}
	
	/**
	 * MapFunction which creates edges where src ID is bigger than target ID
	 * 
	 * @author Toni Pohl
	 */
	private static class EdgesIdMapFunction implements MapFunction<Edge<Long,FoodBrokerEdge>, Edge<Long, FoodBrokerEdge>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Edge<Long, FoodBrokerEdge> map(Edge<Long, FoodBrokerEdge> value) throws Exception {
			long srcV = value.getSource();
			long trgV = value.getTarget();
			
			if (srcV > trgV) {
				value.setSource(trgV);
				value.setTarget(srcV);
			}
			
			return value;
		}
		
	}
	
	/**
	 * MapFunction which calculates the number of triples per vertex.
	 * 
	 * @author Toni Pohl
	 */
	private static class TriplePerVertixMapFunction implements MapFunction<Tuple2<Long,Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
			long sum = 0;
			for (int i = 1; i < value.f1; i++) {
				sum += i;
			}
			return new Tuple2<Long, Long>(value.f0, sum);
		}
		
	}
	
	/**
	 * GroupReduceFunction which creates all triads for the clustering coefficient.
	 * 
	 * @author Toni Pohl
	 */
	private static class TriadBuilder implements GroupReduceFunction<Edge<Long, FoodBrokerEdge>, Triad<Long>> {
		
		private static final long serialVersionUID = 1L;
		private final List<Long> vertices = new ArrayList<Long>();
		private final Triad<Long> outTriad = new Triad<Long>();
		
		@Override
		public void reduce(Iterable<Edge<Long,FoodBrokerEdge>> edgesIter, Collector<Triad<Long>> out) throws Exception {
			
			final Iterator<Edge<Long, FoodBrokerEdge>> edges = edgesIter.iterator();
			
			// clear vertex list
			vertices.clear();
			
			// read first edge
			Edge<Long, FoodBrokerEdge> firstEdge = edges.next();
			outTriad.setFirstVertex(firstEdge.getSource());
			vertices.add(firstEdge.getTarget());
			
			// build and emit triads
			while (edges.hasNext()) {
				Long higherVertexId = edges.next().getTarget();
				
				// combine vertex with all previously read vertices
				for (Long lowerVertexId : vertices) {
					outTriad.setSecondVertex(lowerVertexId);
					outTriad.setThirdVertex(higherVertexId);
					out.collect(outTriad);
				}
				vertices.add(higherVertexId);
			}
		}
	}
}