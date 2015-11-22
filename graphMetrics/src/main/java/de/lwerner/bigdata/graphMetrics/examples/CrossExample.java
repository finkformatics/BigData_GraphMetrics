package de.lwerner.bigdata.graphMetrics.examples;

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class CrossExample {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		List<Integer> numbersList = new LinkedList<>();
		numbersList.add(1);
		numbersList.add(2);
		numbersList.add(3);
		numbersList.add(4);
		numbersList.add(5);
		List<Character> charactersList = new LinkedList<>();
		charactersList.add('a');
		charactersList.add('b');
		charactersList.add('c');
		charactersList.add('d');
		charactersList.add('e');
		
		DataSet<Integer> dataSet1 = env.fromCollection(numbersList);
		DataSet<Character> dataSet2 = env.fromCollection(charactersList);
		
		DataSet<Tuple2<Integer, Character>> result = dataSet1.cross(dataSet2);
		
		result.print();
	}
	
}