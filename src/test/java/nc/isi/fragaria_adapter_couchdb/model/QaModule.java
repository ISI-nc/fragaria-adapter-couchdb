package nc.isi.fragaria_adapter_couchdb.model;

import nc.isi.fragaria_adapter_couchdb.FragariaCouchDbModule;
import nc.isi.fragaria_dsloader_yaml.YamlDsLoaderModule;

import org.apache.tapestry5.ioc.Configuration;
import org.apache.tapestry5.ioc.annotations.SubModule;

@SubModule({ FragariaCouchDbModule.class, YamlDsLoaderModule.class })
public class QaModule {

	public void contributeResourceFinder(Configuration<String> configuration) {
		configuration.add("nc.isi.fragaria_adapter_couchdb");
	}

	public void contributeViewInitializer(Configuration<String> configuration) {
		configuration.add("nc.isi.fragaria_adapter_couchdb");
	}

}
