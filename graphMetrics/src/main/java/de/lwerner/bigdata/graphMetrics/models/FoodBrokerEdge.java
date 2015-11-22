package de.lwerner.bigdata.graphMetrics.models;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Model class for FoodBrokerEdge
 * Used by jackson to read the json as a object.
 * 
 * @author Toni Pohl
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class FoodBrokerEdge {
	private long id;
	private long source;
	private long target;
	private JsonNode data;
	private JsonNode meta;
	
	public long getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}

	public long getSource() {
		return source;
	}

	public void setSource(int source) {
		this.source = source;
	}

	public long getTarget() {
		return target;
	}

	public void setTarget(int target) {
		this.target = target;
	}

	public JsonNode getData() {
		return data;
	}

	public void setData(JsonNode data) {
		this.data = data;
	}

	public JsonNode getMeta() {
		return meta;
	}

	public void setMeta(JsonNode meta) {
		this.meta = meta;
	}
}
