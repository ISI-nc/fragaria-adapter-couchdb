package nc.isi.fragaria_adapter_couchdb;

import org.ektorp.CouchDbConnector;
import org.ektorp.impl.ObjectMapperFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface CouchDbObjectMapperProvider extends ObjectMapperFactory {

	public ObjectMapper createObjectMapper(CouchDbConnector connector);

}