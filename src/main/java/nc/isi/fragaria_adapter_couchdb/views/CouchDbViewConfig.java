package nc.isi.fragaria_adapter_couchdb.views;

import nc.isi.fragaria_adapter_rewrite.entities.views.ViewConfig;

public class CouchDbViewConfig implements ViewConfig {
	private static final String MAP = "function(doc) %s";
	private static final String REDUCE = "function (key, values, rereduce) %s";
	private String map;
	private String reduce;
	private final String name;

	public CouchDbViewConfig(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setBody(String name, String value) {
		switch (name) {
		case "map":
			setMapBody(value);
			break;
		case "reduce":
			setReduceBody(value);
			break;
		default:
			throw new IllegalArgumentException(String.format(
					"nom inconnu %s seuls map et reduce sont autorisés", name));
		}
	}

	public String getMap() {
		return map;
	}

	public String getReduce() {
		return reduce;
	}

	public void setMapBody(String map) {
		setMap(String.format(MAP, map));
	}

	public CouchDbViewConfig setMap(String map) {
		this.map = map;
		return this;
	}

	public void setReduceBody(String reduce) {
		setReduce(String.format(REDUCE, reduce));
	}

	public CouchDbViewConfig setReduce(String reduce) {
		this.reduce = reduce;
		return this;
	}

}
