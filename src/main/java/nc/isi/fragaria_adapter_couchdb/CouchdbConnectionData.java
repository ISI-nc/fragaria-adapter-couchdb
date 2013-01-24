package nc.isi.fragaria_adapter_couchdb;

import java.net.MalformedURLException;
import java.net.URL;

import nc.isi.fragaria_adapter_rewrite.resources.ConnectionData;

public class CouchdbConnectionData implements ConnectionData {
	private final URL url;
	private final String dbName;

	public CouchdbConnectionData(URL url, String dbName) {
		this.url = url;
		this.dbName = dbName;
	}

	public CouchdbConnectionData(String url, String dbName) {
		try {
			this.url = new URL(url);
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
		this.dbName = dbName;
	}


	public URL getUrl() {
		return url;
	}

	public String getDbName() {
		return dbName;
	}

}
