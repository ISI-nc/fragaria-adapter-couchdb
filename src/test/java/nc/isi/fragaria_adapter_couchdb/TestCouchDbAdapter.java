package nc.isi.fragaria_adapter_couchdb;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import nc.isi.fragaria_adapter_couchdb.model.Adress;
import nc.isi.fragaria_adapter_couchdb.model.City;
import nc.isi.fragaria_adapter_couchdb.model.CouchDbQaRegistry;
import nc.isi.fragaria_adapter_couchdb.model.PersonData;
import nc.isi.fragaria_adapter_couchdb.views.CouchDbViewConfig;
import nc.isi.fragaria_adapter_rewrite.dao.ByViewQuery;
import nc.isi.fragaria_adapter_rewrite.dao.Session;
import nc.isi.fragaria_adapter_rewrite.dao.SessionManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.resources.DataSourceProvider;
import nc.isi.fragaria_adapter_rewrite.resources.Datasource;

import org.apache.tapestry5.ioc.Registry;
import org.junit.AfterClass;
import org.junit.Test;

public class TestCouchDbAdapter {
	private static final String SAMPLE_NAME = "Maltat";
	public static final String DB_NAME = "fragaria-adapter-couchdb-test";
	private static final Registry registry = CouchDbQaRegistry.INSTANCE
			.getRegistry();
	private City paris;
	private City londres;
	private City madrid;
	private PersonData person;
	private Session session;

	private void init() {
		SessionManager sessionManager = registry
				.getService(SessionManager.class);
		session = sessionManager.create();
		paris = session.create(City.class);
		paris.setName("Paris");
		londres = session.create(City.class);
		londres.setName("Londres");
		madrid = session.create(City.class);
		madrid.setName("Madrid");
	}

	@Test
	public void testCreate() {
		init();
		person = session.create(PersonData.class);
		person.setName(SAMPLE_NAME);
		person.setFirstName("Justin", "Pierre");
		Adress adress = new Adress();
		adress.setCity(paris);
		adress.setStreet("Champs Elysée");
		person.setAdress(adress);
		System.out.println(person.toJSON());
		person.setCity(londres);
		person.addCity(londres);
		person.addCity(paris);
		person.addCity(madrid);
		person.removeCity(londres);
		System.out.println(person.toJSON());
		session.post();
		// Collection<City> cities = session.get(new ByViewQuery<>(City.class,
		// null).filterBy(City.ID,
		// Arrays.asList(londres.getId(), madrid.getId(), paris.getId())));
		// assertTrue(cities.size() == 3);
		// Collection<PersonData> personDatas = session.get(new ByViewQuery<>(
		// PersonData.class, All.class));
		// PersonData personData = session.getUnique(new ByViewQuery<>(
		// PersonData.class, null).filterBy(PersonData.NAME, SAMPLE_NAME));
		// assertTrue(personData.getName().equals(SAMPLE_NAME));
		// assertTrue(personDatas.size() == 1);
		// for (PersonData temp : personDatas) {
		// System.out.println(temp.getId());
		// System.out.println(temp.getName());
		// System.out.println(temp.getFirstName());
		// System.out.println(temp.getAdress());
		// if (temp.getAdress() != null)
		// System.out.println(temp.getAdress().getCity().getName());
		// System.out.println(temp.getCities());
		// }
		// PersonData fromDB = session.getUnique(new IdQuery<>(PersonData.class,
		// person.getId()));
		// for (City city : fromDB.getCities()) {
		// System.out.println(city.getName());
		// }
	}

	@Test
	public void testGet() {
		String[] firstNames = { "Justin", "Pierre" };
		Session session = registry.getService(SessionManager.class).create();
		PersonData toCreate = session.create(PersonData.class);
		toCreate.setName("Test");
		toCreate.setFirstName("TestFirstName");
		toCreate.setFirstName(firstNames);
		City city = session.create(City.class);
		city.setName("MN");
		City site = session.create(City.class);
		site.setName("KOPETO");
		session.post();
		PersonData user = session.getUnique(new ByViewQuery<>(PersonData.class,
				null).filterBy(PersonData.NAME, "Test").filterBy(
				PersonData.FIRST_NAME, firstNames));
		assertNotNull(user);
		City centre = session.getUnique(new ByViewQuery<>(City.class, null)
				.filterBy(City.NAME,"MN"));
		assertNotNull(centre);
		Collection<City> cities = session.get(new ByViewQuery<>(City.class,
				null).filterBy(City.ID,
				Arrays.asList(centre.getId(), site.getId())));
		assertTrue(cities.size() == 2);
		for (City temp : cities) {
			System.out.println(temp);
		}
	}

	@AfterClass
	public static void close() {
		DataSourceProvider dataSourceProvider = registry
				.getService(DataSourceProvider.class);
		CouchDbAdapter couchDbAdapter = registry
				.getService(CouchDbAdapter.class);
		for (Datasource ds : dataSourceProvider.datasources()) {
			couchDbAdapter.deleteDb(ds);
		}
	}

	@Test
	public void testViewExist() {
		AdapterManager adapterManager = registry
				.getService(AdapterManager.class);
		assertTrue(adapterManager
				.exist(new CouchDbViewConfig("all")
						.setMap("function(doc) { if(doc.types.indexOf('nc.isi.fragaria_adapter_couchdb.model.PersonData')>=0) emit(null, doc);}"),
						PersonData.class));
	}
}
