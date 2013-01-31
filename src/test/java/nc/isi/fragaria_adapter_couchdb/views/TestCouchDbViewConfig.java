package nc.isi.fragaria_adapter_couchdb.views;

import static junit.framework.Assert.assertTrue;

import org.ektorp.support.DesignDocument.View;
import org.junit.Test;

public class TestCouchDbViewConfig {
	private static final String EMIT_ABC = "function(doc) {\n if (doc.types.indexOf('nc.isi.fragaria_adapter_couchdb.model.PersonData') >= 0)\n emit([a, b, c], doc);\n }\n";

	@Test
	public void testEquals() {
		View view = new View(EMIT_ABC);
		CouchDbViewConfig couchDbViewConfig = new CouchDbViewConfig("abc")
				.setMap(EMIT_ABC);
		assertTrue(couchDbViewConfig.equals(view));
	}

}
