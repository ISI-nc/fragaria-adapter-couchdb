package nc.isi.fragaria_adapter_couchdb;

import java.util.concurrent.ExecutionException;

import nc.isi.fragaria_adapter_rewrite.services.ObjectMapperProviderImpl;
import nc.isi.fragaria_adapter_rewrite.utils.jackson.EntityJacksonModule;

import org.ektorp.CouchDbConnector;
import org.ektorp.impl.jackson.EktorpJacksonModule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CouchDbObjectMapperProviderImpl extends ObjectMapperProviderImpl
		implements CouchDbObjectMapperProvider {
	private final LoadingCache<CouchDbConnector, ObjectMapper> cache = CacheBuilder
			.newBuilder().build(
					new CacheLoader<CouchDbConnector, ObjectMapper>() {

						@Override
						public ObjectMapper load(CouchDbConnector key) {
							ObjectMapper specificOM = new ObjectMapper();
							init(specificOM);
							specificOM.registerModule(new EktorpJacksonModule(
									key, specificOM));
							return specificOM;
						}
					});

	public CouchDbObjectMapperProviderImpl(
			EntityJacksonModule entityJacksonModule) {
		super(entityJacksonModule);
	}

	@Override
	public ObjectMapper createObjectMapper(CouchDbConnector connector) {
		try {
			return cache.get(connector);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

}
