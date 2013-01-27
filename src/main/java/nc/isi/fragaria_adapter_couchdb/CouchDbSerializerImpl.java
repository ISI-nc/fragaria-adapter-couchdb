package nc.isi.fragaria_adapter_couchdb;

import java.util.Collection;

import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityBuilder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class CouchDbSerializerImpl implements CouchdbSerializer {
	private final EntityBuilder entityBuilder;

	public CouchDbSerializerImpl(EntityBuilder entityBuilder) {
		this.entityBuilder = entityBuilder;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nc.isi.fragaria_adapter_couchdb.CouchdbSerializer#serialize(java.util
	 * .Collection)
	 */

	@Override
	public Collection<ObjectNode> serialize(Collection<Entity> objects) {
		if (objects == null) {
			return null;
		}
		Collection<ObjectNode> collection = Lists.newArrayList();
		for (Entity entity : objects) {
			collection.add(serialize(entity));
		}
		return collection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see nc.isi.fragaria_adapter_couchdb.CouchdbSerializer#serialize(nc.isi.
	 * fragaria_adapter_rewrite.entities.Entity)
	 */

	@Override
	public ObjectNode serialize(Entity object) {
		if (object == null) {
			return null;
		}
		return object.toJSON();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nc.isi.fragaria_adapter_couchdb.CouchdbSerializer#deSerialize(java.util
	 * .Collection, java.lang.Class)
	 */

	@Override
	public <E extends Entity> Collection<E> deSerialize(
			Collection<ObjectNode> objects, Class<E> entityClass) {
		if (objects == null) {
			return null;
		}
		Collection<E> collection = Lists.newArrayList();
		for (ObjectNode objectNode : objects) {
			collection.add(deSerialize(objectNode, entityClass));
		}
		return collection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * nc.isi.fragaria_adapter_couchdb.CouchdbSerializer#deSerialize(com.fasterxml
	 * .jackson.databind.node.ObjectNode, java.lang.Class)
	 */

	@Override
	public <E extends Entity> E deSerialize(ObjectNode objectNode,
			Class<E> entityClass) {
		if (objectNode == null) {
			return null;
		}
		return entityBuilder.build(objectNode, entityClass);
	}

}
