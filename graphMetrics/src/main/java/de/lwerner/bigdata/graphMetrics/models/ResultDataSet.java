package de.lwerner.bigdata.graphMetrics.models;

import java.util.HashMap;
import java.util.LinkedList;

public class ResultDataSet {

	private long vertexCount;
	private long edgeCount;
	private double averageDegree;
	private HashMap<String, Long> spreadLabels;
	// TODO: Maybe use a class "AttributeSchema"?
	private HashMap<String, Long> spreadAttributeKeys;
	private HashMap<LinkedList<String>, Long> spreadAttributeSchemas;
	private double globalClusterCoefficient;
	private LinkedList<Long> connectedComponentSizes;
	private double diameter;
	
	public ResultDataSet() {
		// Empty constructor
	}
	
	public synchronized long getVertexCount() {
		return vertexCount;
	}
	
	public synchronized void setVertexCount(long vertexCount) {
		this.vertexCount = vertexCount;
	}
	
	public synchronized long getEdgeCount() {
		return edgeCount;
	}
	
	public synchronized void setEdgeCount(long edgeCount) {
		this.edgeCount = edgeCount;
	}
	
	public synchronized double getAverageDegree() {
		return averageDegree;
	}
	
	public synchronized void setAverageDegree(double averageDegree) {
		this.averageDegree = averageDegree;
	}
	
	public synchronized HashMap<String, Long> getSpreadLabels() {
		return spreadLabels;
	}
	
	public synchronized void setSpreadLabels(HashMap<String, Long> spreadLabels) {
		this.spreadLabels = spreadLabels;
	}
	
	public synchronized HashMap<String, Long> getSpreadAttributeKeys() {
		return spreadAttributeKeys;
	}
	
	public synchronized void setSpreadAttributeKeys(HashMap<String, Long> spreadAttributeKeys) {
		this.spreadAttributeKeys = spreadAttributeKeys;
	}
	
	public synchronized HashMap<LinkedList<String>, Long> getSpreadAttributeSchemas() {
		return spreadAttributeSchemas;
	}
	
	public synchronized void setSpreadAttributeSchemas(HashMap<LinkedList<String>, Long> spreadAttributeSchemas) {
		this.spreadAttributeSchemas = spreadAttributeSchemas;
	}
	
	public synchronized double getGlobalClusterCoefficient() {
		return globalClusterCoefficient;
	}
	
	public synchronized void setGlobalClusterCoefficient(double globalClusterCoefficient) {
		this.globalClusterCoefficient = globalClusterCoefficient;
	}
	
	public synchronized LinkedList<Long> getConnectedComponentSizes() {
		return connectedComponentSizes;
	}
	
	public synchronized void setConnectedComponentSizes(LinkedList<Long> connectedComponentSizes) {
		this.connectedComponentSizes = connectedComponentSizes;
	}
	
	public synchronized double getDiameter() {
		return diameter;
	}
	
	public synchronized void setDiameter(double diameter) {
		this.diameter = diameter;
	}
	
}