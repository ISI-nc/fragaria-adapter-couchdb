package nc.isi.fragaria_adapter_couchdb;

import java.util.Arrays;
import java.util.Collection;

public class JsHelper {
	private static final String EMIT_TOKEN = "emit";
	private static final String EMIT_FORMAT = EMIT_TOKEN + "(%s);";
	private static final char COMA_SEPARATOR = ',';
	private static final char OPEN_TABLE_SEPARATOR = '[';
	private static final char CLOSE_TABLE_SEPARATOR = ']';
	private static final char OPEN_PARENTHESES_SEPARATOR = '(';
	private static final char CLOSE_PARENTHESES_SEPARATOR = ')';

	private JsHelper() {

	}

	public static Collection<String> getEmitKeys(String map) {
		String source = map;
		source = source.substring(source.indexOf(EMIT_TOKEN));
		if (firstSeparator(source) == OPEN_TABLE_SEPARATOR) {
			source = source.substring(source.indexOf(OPEN_TABLE_SEPARATOR) + 1,
					source.indexOf(CLOSE_TABLE_SEPARATOR));
		} else {
			source = source.substring(
					source.indexOf(OPEN_PARENTHESES_SEPARATOR) + 1,
					source.indexOf(COMA_SEPARATOR));
		}
		String[] properties = source.split(",");
		int i = 0;
		for (String property : properties) {
			properties[i++] = property.trim();
		}
		return Arrays.asList(properties);
	}

	public static String replaceEmitKeys(String map, Collection<String> newKeys) {
		Collection<String> oldKeys = getEmitKeys(map);
		return map.replace(buildString(oldKeys), buildString(newKeys));
	}

	private static String buildString(Collection<String> keys) {
		String build = keys.toString();
		if (keys.size() == 1) {
			build = build.replace("" + OPEN_TABLE_SEPARATOR, "").replace(
					"" + CLOSE_TABLE_SEPARATOR, "");
		}
		return build;
	}

	private static char firstSeparator(String s) {
		int tbsIndex = s.indexOf(OPEN_TABLE_SEPARATOR);
		if (tbsIndex < 0)
			return COMA_SEPARATOR;
		int csIndex = s.indexOf(COMA_SEPARATOR);
		return tbsIndex < csIndex ? OPEN_TABLE_SEPARATOR : COMA_SEPARATOR;
	}

}
