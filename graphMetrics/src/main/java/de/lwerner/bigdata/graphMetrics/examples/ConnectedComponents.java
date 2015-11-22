package de.lwerner.bigdata.graphMetrics.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ConnectedComponents implements ProgramDescription {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String... args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read vertex and edge data
		DataSet<Long> vertices = getVertexDataSet(env);
		// TODO: Was geschieht hier mit dem Collector?
		DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env).flatMap(new UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());

		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = verticesWithInitialId
				.iterateDelta(verticesWithInitialId, maxIterations, 0);

		// apply the step logic: join with the edges, select the minimum
		// neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges).where(0).equalTo(0)
				.with(new NeighborWithComponentIDJoin()).groupBy(0).aggregate(Aggregations.MIN, 1)
				.join(iteration.getSolutionSet()).where(0).equalTo(0).with(new ComponentIdFilter());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

		// emit result
		if (fileOutput) {
			result.writeAsCsv(outputPath, "\n", " ");
			// execute program
			env.execute("Connected Components Example");
		} else {
			result.print();
		}
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Function that turns a value into a 2-tuple where both fields are that
	 * value.
	 */
	@ForwardedFields("*->f0")
	public static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<T, T> map(T vertex) {
			return new Tuple2<T, T>(vertex, vertex);
		}
		
	}

	/**
	 * Undirected edges by emitting for each input edge the input edges itself
	 * and an inverted version.
	 */
	public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
		}
		
	}

	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the
	 * current component that a vertex is associated with, with a
	 * (Source-Vertex-ID, Target-VertexID) edge. The function produces a
	 * (Target-vertex-ID, Component-ID) pair.
	 */
	@ForwardedFieldsFirst("f1->f1")
	@ForwardedFieldsSecond("f1->f0")
	public static final class NeighborWithComponentIDJoin
			implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	
	}

	@ForwardedFieldsFirst("*")
	public static final class ComponentIdFilter
			implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
		
	}

	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String verticesPath = null;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] programArguments) {
		if (programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (programArguments.length == 4) {
				verticesPath = programArguments[0];
				edgesPath = programArguments[1];
				outputPath = programArguments[2];
				maxIterations = Integer.parseInt(programArguments[3]);
			} else {
				System.err.println(
						"Usage: ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>");
				return false;
			}
		} else {
			System.out.println(
					"Executing Connected Components example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println(
					"  Usage: ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>");
		}
		return true;
	}

	private static DataSet<Long> getVertexDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(verticesPath).types(Long.class).map(new MapFunction<Tuple1<Long>, Long>() {

				private static final long serialVersionUID = 1L;

				public Long map(Tuple1<Long> value) {
					return value.f0;
				}
			});
		} else {
			return ConnectedComponentsData.getDefaultVertexDataSet(env);
		}
	}

	private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesPath).fieldDelimiter(" ").types(Long.class, Long.class);
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}

}