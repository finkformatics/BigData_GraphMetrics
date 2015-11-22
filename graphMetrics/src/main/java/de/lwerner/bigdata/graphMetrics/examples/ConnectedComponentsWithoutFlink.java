package de.lwerner.bigdata.graphMetrics.examples;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class ConnectedComponentsWithoutFlink {
	
	private static final LinkedList<Set<Edge>> connectedComponents = new LinkedList<>();

	public static void main(String[] args) {
		// Create vertices
		Vertex v1 = new Vertex(1);
		Vertex v2 = new Vertex(2);
		Vertex v3 = new Vertex(3);
		Vertex v4 = new Vertex(4);
		Vertex v5 = new Vertex(5);
		Vertex v6 = new Vertex(6);
		Vertex v7 = new Vertex(7);
		Vertex v8 = new Vertex(8);
		Vertex v9 = new Vertex(9);
		Vertex v10 = new Vertex(10);
		Vertex v11 = new Vertex(11);
		Vertex v12 = new Vertex(12);
		Vertex v13 = new Vertex(13);
		Vertex v14 = new Vertex(14);
		Vertex v15 = new Vertex(15);
		Vertex v16 = new Vertex(16);
		
		// Add edges
		v1.outgoingEdges.add(new Edge(v1, v2));
		v1.outgoingEdges.add(new Edge(v1, v15));
		v2.outgoingEdges.add(new Edge(v2, v3));
		v2.outgoingEdges.add(new Edge(v2, v4));
		v3.outgoingEdges.add(new Edge(v3, v5));
		v5.outgoingEdges.add(new Edge(v5, v11));
		v6.outgoingEdges.add(new Edge(v6, v7));
		v8.outgoingEdges.add(new Edge(v8, v9));
		v8.outgoingEdges.add(new Edge(v8, v10));
		v9.outgoingEdges.add(new Edge(v9, v14));
		v10.outgoingEdges.add(new Edge(v10, v13));
		v11.outgoingEdges.add(new Edge(v11, v12));
		v13.outgoingEdges.add(new Edge(v13, v14));
		v16.outgoingEdges.add(new Edge(v16, v1));
		
		// Collect vertices
		Vertex[] vertices = {
			v1, v2, v3, v4, v5, v6, v7, v8,
			v9, v10, v11, v12, v13, v14, v15, v16
		};
		
		// Create undirected graph
		undirectEdges(vertices);
		
		// Find connected components
		for (Vertex v: vertices) {
			Set<Edge> edgesCollector = new HashSet<>();
			findConnectedComponents(v, v, edgesCollector);
			connectedComponents.add(edgesCollector);
		}
		
		for (Set<Edge> connectedComponent: connectedComponents) {
			for (Edge e: connectedComponent) {
				System.out.println(e);
			}
		}
	}
	
	private static void findConnectedComponents(Vertex startVertex, Vertex v, Set<Edge> edgesCollector) {
		if (v.foundAlready) {
			return;
		}
		edgesCollector.add(new Edge(startVertex, v));
		v.foundAlready = true;
		for (Edge e: v.outgoingEdges) {
			findConnectedComponents(startVertex, e.v2, edgesCollector);
		}
	}

	private static void undirectEdges(Vertex[] vertices) {
		for (Vertex v: vertices) {
			for (Edge e: v.outgoingEdges) {
				e.v2.outgoingEdges.add(new Edge(e.v2, e.v1));
			}
		}
	}
	
	private static class Vertex {
		
		public int label;
		public Set<Edge> outgoingEdges = new HashSet<>();
		public boolean foundAlready;
		
		public Vertex(int label) {
			this.label = label;
		}
		
		@Override
		public String toString() {
			return String.format("%d", label);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + label;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Vertex other = (Vertex) obj;
			if (label != other.label)
				return false;
			return true;
		}
		
	}
	
	private static class Edge {
		
		public Vertex v1;
		public Vertex v2;
		
		public Edge(Vertex v1, Vertex v2) {
			this.v1 = v1;
			this.v2 = v2;
		}
		
		@Override
		public String toString() {
			return String.format("(%s, %s)", v1, v2);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((v1 == null) ? 0 : v1.hashCode());
			result = prime * result + ((v2 == null) ? 0 : v2.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Edge other = (Edge) obj;
			if (v1 == null) {
				if (other.v1 != null)
					return false;
			} else if (!v1.equals(other.v1))
				return false;
			if (v2 == null) {
				if (other.v2 != null)
					return false;
			} else if (!v2.equals(other.v2))
				return false;
			return true;
		}
		
	}
	
}