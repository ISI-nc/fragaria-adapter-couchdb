package nc.isi.fragaria_adapter_couchdb;

import static org.junit.Assert.assertTrue;

import java.util.Collection;

import nc.isi.fragaria_adapter_couchdb.model.Adress;
import nc.isi.fragaria_adapter_couchdb.model.City;
import nc.isi.fragaria_adapter_couchdb.model.CouchDbQaRegistry;
import nc.isi.fragaria_adapter_couchdb.model.PersonData;
import nc.isi.fragaria_adapter_couchdb.views.CouchDbViewConfig;
import nc.isi.fragaria_adapter_rewrite.dao.ByViewQuery;
import nc.isi.fragaria_adapter_rewrite.dao.IdQuery;
import nc.isi.fragaria_adapter_rewrite.dao.Session;
import nc.isi.fragaria_adapter_rewrite.dao.SessionManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.entities.views.GenericQueryViews.All;
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
		person.setCity(londres);
		person.addCity(londres);
		person.addCity(paris);
		person.addCity(madrid);
		person.removeCity(londres);
		session.post();
		Collection<PersonData> personDatas = session.get(new ByViewQuery<>(
				PersonData.class, All.class));
		PersonData personData = session.getUnique(new ByViewQuery<>(
				PersonData.class, null).filterBy(PersonData.NAME, SAMPLE_NAME));
		assertTrue(personData.getName().equals(SAMPLE_NAME));
		assertTrue(personDatas.size() == 1);
		for (PersonData temp : personDatas) {
			System.out.println(temp.getId());
			System.out.println(temp.getName());
			System.out.println(temp.getFirstName());
			System.out.println(temp.getAdress());
			if (temp.getAdress() != null)
				System.out.println(temp.getAdress().getCity().getName());
			System.out.println(temp.getCities());
		}
		PersonData fromDB = session.getUnique(new IdQuery<>(PersonData.class,
				person.getId()));
		for (City city : fromDB.getCities()) {
			System.out.println(city.getName());
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
