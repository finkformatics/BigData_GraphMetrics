package de.lwerner.bigdata.graphMetrics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.ParseException;
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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import de.lwerner.bigdata.graphMetrics.models.FoodBrokerEdge;
import de.lwerner.bigdata.graphMetrics.models.FoodBrokerVertex;
import de.lwerner.bigdata.graphMetrics.utils.ArgumentsParser;
import de.lwerner.bigdata.graphMetrics.utils.FoodBrokerReader;

import static de.lwerner.bigdata.graphMetrics.utils.GraphMetricsConstants.*;

/**
 * Calculates the global clustering coefficient of a graph.
 * 
 * Solves the problem by counting the triads and the triple of the graph.
 * 
 * @author Toni Pohl
 * @author Lukas Werner
 */
public class ClusterCoefficient<K extends Number, VV, EV> extends GraphAlgorithm<K, VV, EV> {

	double globalClusterCoefficient;
	
	public ClusterCoefficient(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges,
			ExecutionEnvironment context) {
		super(vertices, edges, context);
	}
	
	public ClusterCoefficient(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges,
			ExecutionEnvironment context, boolean undirected) {
		super(vertices, edges, context, undirected);
	}

	/**
	 * The main job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			arguments = ArgumentsParser.parseArguments(AverageDegree.class.getName(), FILENAME_AVERAGE_DEGREE, args);
		} catch (IllegalArgumentException | ParseException e) {
			e.printStackTrace();
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Vertex<Long, FoodBrokerVertex>> vertices = FoodBrokerReader.getVertices(env,
				arguments.getVerticesPath());
		DataSet<Edge<Long, FoodBrokerEdge>> edges = 
				FoodBrokerReader.getEdges(env, arguments.getEdgesPath());

		new ClusterCoefficient<Long, FoodBrokerVertex, FoodBrokerEdge>(vertices, edges, env, true).runAndWrite();	
	}

	public double getGlobalClusterCoefficient() {
		return globalClusterCoefficient;
	}

	/**
	 * MapFunction which creates edges where src ID is bigger than target ID
	 * 
	 * @author Toni Pohl
	 */
	public final class EdgesIdMapFunction implements MapFunction<Edge<K, EV>, Edge<K, EV>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Edge<K, EV> map(Edge<K, EV> value) throws Exception {
			K srcV = value.getSource();
			K trgV = value.getTarget();

			if (srcV.longValue() > trgV.longValue()) {
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
	public final class TriplePerVertixMapFunction implements MapFunction<Tuple2<K, Long>, Tuple2<K, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<K, Long> map(Tuple2<K, Long> value) throws Exception {
			long sum = 0;
			for (int i = 1; i < value.f1; i++) {
				sum += i;
			}
			return new Tuple2<K, Long>(value.f0, sum);
		}

	}

	/**
	 * GroupReduceFunction which creates all triads for the clustering
	 * coefficient.
	 * 
	 * @author Toni Pohl
	 */
	public final class TriadBuilder implements GroupReduceFunction<Edge<K, EV>, Triad<K>> {

		private static final long serialVersionUID = 1L;
		private final List<K> vertices = new ArrayList<K>();
		private final Triad<K> outTriad = new Triad<K>();

		@Override
		public void reduce(Iterable<Edge<K, EV>> edgesIter, Collector<Triad<K>> out)
				throws Exception {

			final Iterator<Edge<K, EV>> edges = edgesIter.iterator();

			// clear vertex list
			vertices.clear();

			// read first edge
			Edge<K, EV> firstEdge = edges.next();
			outTriad.setFirstVertex(firstEdge.getSource());
			vertices.add(firstEdge.getTarget());

			// build and emit triads
			while (edges.hasNext()) {
				Edge<K, EV> nextEdge = edges.next();
				K higherVertexId = nextEdge.getTarget();
				
				// combine vertex with all previously read vertices
				for (K lowerVertexId : vertices) {
					outTriad.setSecondVertex(lowerVertexId);
					outTriad.setThirdVertex(higherVertexId);
					out.collect(outTriad);
				}
				vertices.add(higherVertexId);
			}
		}
	}

	@Override
	public void run() throws Exception {
		Graph<K, VV, EV> g = getGraph();

		DataSet<Edge<K, EV>> edgesById = edges
				.map(new EdgesIdMapFunction());

		DataSet<Triad<K>> triads = edgesById.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup(new TriadBuilder())
				.join(edgesById).where(Triad.V2, Triad.V3).equalTo(0, 1)
				.with(new JoinFunction<Triad<K>, Edge<K, EV>, Triad<K>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Triad<K> join(Triad<K> first, Edge<K, EV> second) throws Exception {
						return first;
					}
				})
				.distinct();

		long triadsCount = triads.count();

		DataSet<Tuple2<K, Long>> triple = g.outDegrees().map(new TriplePerVertixMapFunction()).sum(1);
		long tripleCount = triple.collect().get(0).f1;
		globalClusterCoefficient = triadsCount * 3 / (double) tripleCount;

	}

	@Override
	public JsonNode writeOutput(ObjectMapper m) throws Exception {
		ObjectNode clusterCoefficientObject = m.createObjectNode();
		clusterCoefficientObject.put("globalClusterCoefficient", globalClusterCoefficient);
		
		return clusterCoefficientObject;
	}
}