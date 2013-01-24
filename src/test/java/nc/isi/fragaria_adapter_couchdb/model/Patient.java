package nc.isi.fragaria_adapter_couchdb.model;

import nc.isi.fragaria_adapter_rewrite.entities.EntityMetadataFactory;
import nc.isi.fragaria_adapter_rewrite.entities.ObjectResolver;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class Patient extends PersonData {

	public Patient(ObjectNode objectNode, ObjectResolver objectResolver,
			EntityMetadataFactory entityMetadataFactory) {
		super(objectNode, objectResolver, entityMetadataFactory);
	}

}
