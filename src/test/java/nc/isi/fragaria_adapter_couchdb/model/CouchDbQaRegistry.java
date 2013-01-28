package nc.isi.fragaria_adapter_couchdb.model;

import org.apache.tapestry5.ioc.Registry;
import org.apache.tapestry5.ioc.RegistryBuilder;

public enum CouchDbQaRegistry {
	INSTANCE;

	private final Registry registry = RegistryBuilder
			.buildAndStartupRegistry(QaModule.class);

	public Registry getRegistry() {
		return registry;
	}
}
