package nc.isi.fragaria_adapter_couchdb;

import nc.isi.fragaria_adapter_rewrite.entities.FragariaObjectMapper;

import org.ektorp.CouchDbConnector;
import org.ektorp.impl.jackson.EktorpJacksonModule;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CouchDbObjectMapperProviderImpl implements
		CouchDbObjectMapperProvider {

	@Override
	public ObjectMapper createObjectMapper(CouchDbConnector connector) {
		ObjectMapper objectMapper = FragariaObjectMapper.INSTANCE.get();
		objectMapper.registerModule(new EktorpJacksonModule(connector,
				objectMapper));
		return objectMapper;
	}

	@Override
	public ObjectMapper createObjectMapper() {
		return FragariaObjectMapper.INSTANCE.get();
	}

}
