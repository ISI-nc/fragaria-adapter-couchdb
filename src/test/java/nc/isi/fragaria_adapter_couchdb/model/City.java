package nc.isi.fragaria_adapter_couchdb.model;

import nc.isi.fragaria_adapter_couchdb.TestCouchDbAdapter;
import nc.isi.fragaria_adapter_couchdb.model.CityViews.Name;
import nc.isi.fragaria_adapter_rewrite.annotations.DsKey;
import nc.isi.fragaria_adapter_rewrite.entities.AbstractEntity;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.node.ObjectNode;

@DsKey(TestCouchDbAdapter.DB_NAME)
public class City extends AbstractEntity {
	public static final String NAME = "name";
	public static final String PERSONDATA = "personData";

	public City(ObjectNode node) {
		super(node);
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

	public void setPersonData(PersonData personData) {
		writeProperty(PERSONDATA, personData);
	}

	public PersonData getPersonData() {
		return readProperty(PersonData.class, PERSONDATA);
	}

}
