package nc.isi.fragaria_adapter_couchdb.views;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import nc.isi.fragaria_adapter_couchdb.model.QaRegistry;
import nc.isi.fragaria_adapter_couchdb.views.CouchDbViewConfig;
import nc.isi.fragaria_adapter_couchdb.views.CouchDbViewConfigBuilder;
import nc.isi.fragaria_adapter_rewrite.resources.ResourceFinder;

import org.junit.Test;

public class TestCouchDbViewConfigBuilder extends TestCase {
	private final ResourceFinder resourceFinder = QaRegistry.INSTANCE
			.getRegistry().getService(ResourceFinder.class);

	@Test
	public void testBuildFromJsFile() {
		CouchDbViewConfigBuilder builder = new CouchDbViewConfigBuilder();
		List<File> files = new ArrayList<>(
				resourceFinder.getResourcesMatching("test.js"));
		CouchDbViewConfig conf = (CouchDbViewConfig) builder.build("test",
				files.get(0));
		assertTrue("function(doc) {\n if (doc.types.indexOf('nc.isi.fragaria_adapter_couchdb.model.PersonData') >= 0)\n emit(null, doc);\n }\n"
				.replaceAll(" ", "").equals(conf.getMap().replaceAll(" ", "")));
		assertTrue("function (key, values, rereduce) {\n emit(doc);\n }\n"
				.replaceAll(" ", "").equals(
						conf.getReduce().replaceAll(" ", "")));
	}

}
