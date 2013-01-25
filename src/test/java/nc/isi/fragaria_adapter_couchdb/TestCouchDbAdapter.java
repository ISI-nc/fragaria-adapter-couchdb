package nc.isi.fragaria_adapter_couchdb;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;
import nc.isi.fragaria_adapter_couchdb.model.Adress;
import nc.isi.fragaria_adapter_couchdb.model.City;
import nc.isi.fragaria_adapter_couchdb.model.PersonData;
import nc.isi.fragaria_adapter_couchdb.model.QaRegistry;
import nc.isi.fragaria_adapter_rewrite.dao.ByViewQuery;
import nc.isi.fragaria_adapter_rewrite.dao.IdQuery;
import nc.isi.fragaria_adapter_rewrite.dao.Session;
import nc.isi.fragaria_adapter_rewrite.dao.SessionManager;
import nc.isi.fragaria_adapter_rewrite.entities.views.GenericViews.All;

public class TestCouchDbAdapter extends TestCase {

	public void testCreate() {
		SessionManager sessionManager = QaRegistry.INSTANCE.getRegistry()
				.getService(SessionManager.class);
		Session session = sessionManager.create();
		PersonData personData = session.create(PersonData.class);
		personData.setSession(session);
		personData.setName("Maltat");
		personData.setFirstName("Justin", "Pierre");
		Adress adress = new Adress();
		City paris = session.create(City.class);
		paris.setName("Paris");
		paris.setSession(session);
		System.out.println("paris created : " + paris);
		City londres = session.create(City.class);
		londres.setName("Londres");
		londres.setSession(session);
		System.out.println("londres created : " + londres);
		System.out.println("londres id : " + londres.getId());
		System.out.println("paris id : " + paris.getId());
		adress.setCity(paris);
		adress.setStreet("Champs Elysée");
		System.out.println("adress paris id : " + adress.getCity().getId());
		personData.setAdress(adress);
		System.out.println("address associated");
		personData.setCity(londres);
		System.out.println("londres associated : "
				+ personData.getCity().getId());
		City[] cities = { londres, paris };
		personData.setCities(Arrays.asList(cities));
		System.out.println("cities associated");
		System.out.println("person : " + personData.toJSON());
		session.post();
		Collection<PersonData> personDatas = session.get(new ByViewQuery<>(
				PersonData.class, All.class));
		System.out.println(personDatas.size());
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
				personData.getId()));
		fromDB.setSession(session);
		for (City city : fromDB.getCities()) {
			System.out.println(city.getRev());
		}
	}
}
