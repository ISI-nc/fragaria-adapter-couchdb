package nc.isi.fragaria_adapter_couchdb;

import java.util.Collection;

public class BadViewException extends RuntimeException {
	private final static String MESSAGE = "View %s doesn't emit keys %s (emit %s)";

	public BadViewException(String viewName, Collection<String> keys,
			Collection<String> emitKeys) {
		super(String.format(MESSAGE, viewName, keys, emitKeys));
	}

}
