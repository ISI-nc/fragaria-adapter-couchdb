package nc.isi.fragaria_adapter_couchdb;

import java.util.List;

import junit.framework.TestCase;
import nc.isi.fragaria_adapter_couchdb.CouchdbConnectionData;
import nc.isi.fragaria_adapter_rewrite.resources.DataSourceMetadata;
import nc.isi.fragaria_adapter_rewrite.resources.Datasource;
import nc.isi.fragaria_adapter_rewrite.resources.DatasourceImpl;
import nc.isi.fragaria_adapter_rewrite.resources.yaml.YamlDsLoader;
import nc.isi.fragaria_adapter_rewrite.services.TapestryRegistry;

import com.google.common.collect.Lists;

public class TestYamlDsLoaderForCouchDBConnectionData extends TestCase {

	public void testYamlDsLoader() {
		List<String> pack = Lists.newArrayList();
		pack.add("nc.isi.fragaria_adapter_couchdb");
		YamlDsLoader loader = TapestryRegistry.INSTANCE.getRegistry()
				.getService(YamlDsLoader.class);
		Datasource dsFragaria = new DatasourceImpl("rer-test",
				new DataSourceMetadata("CouchDB", new CouchdbConnectionData(
						"http://localhost:5984/", "rer"), true));
		Datasource loaded = loader.getDs().get("rer-test");
		CouchdbConnectionData dsFragariaConnectionData = (CouchdbConnectionData) dsFragaria
				.getDsMetadata().getConnectionData();
		CouchdbConnectionData loadedConnectionData = (CouchdbConnectionData) loaded
				.getDsMetadata().getConnectionData();
		assertTrue(loaded.getKey().equals(dsFragaria.getKey()));
		assertTrue(loaded.getDsMetadata().getType()
				.equals(dsFragaria.getDsMetadata().getType()));
		assertTrue(loaded.getDsMetadata().getClass() == dsFragaria
				.getDsMetadata().getClass());
		assertTrue(loadedConnectionData.getClass() == dsFragariaConnectionData
				.getClass());
		assertTrue(loadedConnectionData.getDbName().equals(
				dsFragariaConnectionData.getDbName()));
		assertTrue(loadedConnectionData.getUrl().equals(
				dsFragariaConnectionData.getUrl()));
	}
}
