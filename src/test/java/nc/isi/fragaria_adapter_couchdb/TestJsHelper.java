package nc.isi.fragaria_adapter_couchdb;

import static junit.framework.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

public class TestJsHelper {

	private static final String EMIT_ABC = "function(doc) {\n if (doc.types.indexOf('nc.isi.fragaria_adapter_couchdb.model.PersonData') >= 0)\n emit([a, b, c], doc);\n }\n";
	private static final String EMIT_NULL = "function(doc) {\n if (doc.types.indexOf('nc.isi.fragaria_adapter_couchdb.model.PersonData') >= 0)\n emit(null, doc);\n }\n";

	@Test
	public void testGetEmitProperties() {
		Collection<String> properties = JsHelper.getEmitKeys(EMIT_NULL);
		assertTrue(properties.size() == 1);
		assertTrue(properties.contains("null"));
		properties = JsHelper.getEmitKeys(EMIT_ABC);
		assertTrue(properties.size() == 3);
		assertTrue(properties.contains("a"));
		assertTrue(properties.contains("b"));
		assertTrue(properties.contains("c"));

	}

	@Test
	public void testReplaceEmitProperties() {
		String map = new String(EMIT_NULL);
		map = JsHelper.replaceEmitKeys(map, Arrays.asList("a", "b", "c"));
		System.out.println(map);
		assertTrue(map.equals(EMIT_ABC));
		map = JsHelper.replaceEmitKeys(map, Arrays.asList("null"));
		System.out.println(map);
		assertTrue(map.equals(EMIT_NULL));
	}
}
