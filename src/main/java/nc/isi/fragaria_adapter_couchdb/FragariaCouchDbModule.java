package nc.isi.fragaria_adapter_couchdb;

import nc.isi.fragaria_adapter_couchdb.views.CouchDbViewConfigBuilder;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.Adapter;
import nc.isi.fragaria_adapter_rewrite.entities.views.ViewConfigBuilder;
import nc.isi.fragaria_adapter_rewrite.services.FragariaDomainModule;

import org.apache.tapestry5.ioc.Configuration;
import org.apache.tapestry5.ioc.MappedConfiguration;
import org.apache.tapestry5.ioc.ServiceBinder;
import org.apache.tapestry5.ioc.annotations.SubModule;
import org.slf4j.bridge.SLF4JBridgeHandler;

@SubModule(FragariaDomainModule.class)
public class FragariaCouchDbModule {
	static {
		SLF4JBridgeHandler.install();
	}

	public static void bind(ServiceBinder binder) {
		binder.bind(CouchdbSerializer.class, CouchDbSerializerImpl.class);
		binder.bind(CouchDbAdapter.class);
		binder.bind(CouchDbObjectMapperProvider.class,
				CouchDbObjectMapperProviderImpl.class);
		binder.bind(CouchDbViewConfigBuilder.class);
	}

	public void contributeConnectionDataBuilder(
			MappedConfiguration<String, String> configuration) {
		configuration.add("CouchDB", CouchdbConnectionData.class.getName());
	}

	public void contributeAdapterManager(
			MappedConfiguration<String, Adapter> configuration,
			CouchDbAdapter couchDbAdapter) {
		configuration.add("CouchDB", couchDbAdapter);
	}

	public void contributeViewConfigProvider(Configuration<String> configuration) {
		configuration.add(".js");
	}

	public void contributeViewConfigBuilderProvider(
			MappedConfiguration<String, ViewConfigBuilder> configuration,
			CouchDbViewConfigBuilder couchDbViewConfigBuilder) {
		configuration.add(".js", couchDbViewConfigBuilder);
	}

}
