package nc.isi.fragaria_adapter_couchdb;

import nc.isi.fragaria_adapter_rewrite.dao.adapters.Adapter;
import nc.isi.fragaria_adapter_rewrite.services.FragariaDomainModule;

import org.apache.tapestry5.ioc.MappedConfiguration;
import org.apache.tapestry5.ioc.ServiceBinder;
import org.apache.tapestry5.ioc.annotations.SubModule;

@SubModule(FragariaDomainModule.class)
public class FragariaCouchDbModule {

	public static void bind(ServiceBinder binder) {
		binder.bind(CouchDbSerializer.class);
		binder.bind(CouchDbAdapter.class);
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

}
