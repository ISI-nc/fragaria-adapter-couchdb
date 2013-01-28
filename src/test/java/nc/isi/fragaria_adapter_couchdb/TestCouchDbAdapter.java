package nc.isi.fragaria_adapter_couchdb;

import java.util.Collection;

import junit.framework.TestCase;
import nc.isi.fragaria_adapter_couchdb.CouchDbAdapter;
import nc.isi.fragaria_adapter_couchdb.model.Adress;
import nc.isi.fragaria_adapter_couchdb.model.City;
import nc.isi.fragaria_adapter_couchdb.model.PersonData;
import nc.isi.fragaria_adapter_couchdb.model.QaRegistry;
import nc.isi.fragaria_adapter_couchdb.views.CouchDbViewConfig;
import nc.isi.fragaria_adapter_rewrite.dao.ByViewQuery;
import nc.isi.fragaria_adapter_rewrite.dao.IdQuery;
import nc.isi.fragaria_adapter_rewrite.dao.Session;
import nc.isi.fragaria_adapter_rewrite.dao.SessionManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.entities.views.GenericQueryViews.All;

import org.junit.Test;

public class TestCouchDbAdapter extends TestCase {
	public static final String DB_NAME = "fragaria-adapter-couchdb-test";
	private City paris;
	private City londres;
	private City madrid;
	private PersonData person;
	private Session session;

	private void init() {
		SessionManager sessionManager = QaRegistry.INSTANCE.getRegistry()
				.getService(SessionManager.class);
		session = sessionManager.create();
		paris = session.create(City.class);
		paris.setName("Paris");
		paris.setSession(session);
		londres = session.create(City.class);
		londres.setName("Londres");
		londres.setSession(session);
		madrid = session.create(City.class);
		madrid.setName("Madrid");
		madrid.setSession(session);
	}

	@Test
	public void testCreate() {
		init();
		person = session.create(PersonData.class);
		person.setSession(session);
		person.setName("Maltat");
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
		fromDB.setSession(session);
		for (City city : fromDB.getCities()) {
			System.out.println(city.getRev());
		}
		close();
	}

	private void close() {
		Collection<PersonData> personDatas = session.get(new ByViewQuery<>(
				PersonData.class, All.class));
		session.delete(personDatas);
		Collection<City> cities = session.get(new ByViewQuery<>(City.class,
				All.class));
		session.delete(cities);
		session.post();
		CouchDbAdapter couchDbAdapter = QaRegistry.INSTANCE.getRegistry()
				.getService(CouchDbAdapter.class);
		couchDbAdapter.deleteDb(DB_NAME);
	}

	@Test
	public void testViewExist() {
		AdapterManager adapterManager = QaRegistry.INSTANCE.getRegistry()
				.getService(AdapterManager.class);
		assertTrue(adapterManager
				.exist(new CouchDbViewConfig("all")
						.setMap("function(doc) { if(doc.types.indexOf('nc.isi.fragaria_adapter_couchdb.model.PersonData')>=0) emit(null, doc);}"),
						PersonData.class));
		close();
	}
}
