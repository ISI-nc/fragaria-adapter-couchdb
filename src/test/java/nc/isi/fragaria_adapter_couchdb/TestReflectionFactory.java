package nc.isi.fragaria_adapter_couchdb;

import java.util.Arrays;

import junit.framework.TestCase;
import nc.isi.fragaria_adapter_couchdb.model.QaModule;
import nc.isi.fragaria_adapter_couchdb.model.CouchDbQaRegistry;
import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.services.ReflectionFactory;

import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class TestReflectionFactory extends TestCase {

	@Test
	public void test() {
		Reflections reflections = CouchDbQaRegistry.INSTANCE
				.getRegistry()
				.getService(ReflectionFactory.class)
				.create(Arrays.asList(QaModule.PACKAGE_NAME),
						new SubTypesScanner());
		System.out.println(reflections.getSubTypesOf(Entity.class));
		Reflections reflectionsOriginal = new Reflections(
				ConfigurationBuilder.build(QaModule.PACKAGE_NAME[0],
						new SubTypesScanner()));
		System.out.println(reflectionsOriginal.getSubTypesOf(Entity.class));
		Reflections reflectionsWithouBuilder = new Reflections(
				ConfigurationBuilder.build(ClasspathHelper
						.forPackage(QaModule.PACKAGE_NAME[0])));
		System.out
				.println(reflectionsWithouBuilder.getSubTypesOf(Entity.class));
	}

}
