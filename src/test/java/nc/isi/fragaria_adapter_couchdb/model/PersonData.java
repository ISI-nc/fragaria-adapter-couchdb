package nc.isi.fragaria_adapter_couchdb.model;

import java.util.Arrays;
import java.util.Collection;

import nc.isi.fragaria_adapter_couchdb.TestCouchDbAdapter;
import nc.isi.fragaria_adapter_couchdb.model.CityViews.Name;
import nc.isi.fragaria_adapter_rewrite.annotations.DsKey;
import nc.isi.fragaria_adapter_rewrite.annotations.Embeded;
import nc.isi.fragaria_adapter_rewrite.entities.AbstractEntity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityMetadataFactory;
import nc.isi.fragaria_adapter_rewrite.entities.ObjectResolver;

import com.fasterxml.jackson.databind.node.ObjectNode;

@DsKey(TestCouchDbAdapter.DB_NAME)
public class PersonData extends AbstractEntity {

	public static final String NAME = "name";
	public static final String FIRST_NAME = "firstName";
	public static final String ADRESS = "adress";
	public static final String CITIES = "cities";
	public static final String CITY = "city";

	public PersonData(ObjectNode objectNode, ObjectResolver objectResolver,
			EntityMetadataFactory entityMetadataFactory) {
		super(objectNode, objectResolver, entityMetadataFactory);
	}

	public PersonData() {
		super();
	}

	public String getName() {
		return readProperty(String.class, NAME);
	}

	public void setName(String name) {
		writeProperty(NAME, name);
	}

	public Collection<String> getFirstName() {
		return readCollection(String.class, FIRST_NAME);
	}

	public void setFirstName(String... firstNames) {
		writeProperty(FIRST_NAME, Arrays.asList(firstNames));
	}

	public Adress getAdress() {
		return readProperty(Adress.class, ADRESS);
	}

	public void setAdress(Adress adress) {
		writeProperty(ADRESS, adress);
	}

	@Embeded(Name.class)
	public Collection<City> getCities() {
		return readCollection(City.class, CITIES);
	}

	public void setCities(Collection<City> cities) {
		writeProperty(CITIES, cities);
	}

	public Boolean addCity(City city) {
		return addToCollection(CITIES, city, City.class);
	}

	public Boolean removeCity(City city) {
		return removeFromCollection(CITIES, city, City.class);
	}

	@Embeded(Name.class)
	public City getCity() {
		return readProperty(City.class, CITY);
	}

	public void setCity(City city) {
		writeProperty(CITY, city);
	}

}
