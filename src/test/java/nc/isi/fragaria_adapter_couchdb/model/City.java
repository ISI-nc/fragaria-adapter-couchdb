package nc.isi.fragaria_adapter_couchdb.model;

import nc.isi.fragaria_adapter_couchdb.model.CityViews.Name;
import nc.isi.fragaria_adapter_rewrite.annotations.DsKey;
import nc.isi.fragaria_adapter_rewrite.entities.AbstractEntity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityMetadataFactory;
import nc.isi.fragaria_adapter_rewrite.entities.ObjectResolver;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.node.ObjectNode;

@DsKey("rer")
public class City extends AbstractEntity {
	public static final String NAME = "name";

	public City(ObjectNode objectNode, ObjectResolver objectResolver,
			EntityMetadataFactory entityMetadataFactory) {
		super(objectNode, objectResolver, entityMetadataFactory);
	}

	public City() {
		super();
	}

	@JsonView(Name.class)
	public String getName() {
		return readProperty(String.class, NAME);
	}

	public void setName(String name) {
		writeProperty(NAME, name);
	}

	@Override
	public String toString() {
		return toJSON().toString();
	}

}
