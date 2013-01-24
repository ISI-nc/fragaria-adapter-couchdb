package nc.isi.fragaria_adapter_couchdb;

import java.util.Collection;

import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityBuilder;
import nc.isi.fragaria_adapter_rewrite.services.Serializer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class CouchDbSerializer implements Serializer<ObjectNode> {
	private final EntityBuilder entityBuilder;

	public CouchDbSerializer(EntityBuilder entityBuilder) {
		this.entityBuilder = entityBuilder;
	}

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

	@Override
	public ObjectNode serialize(Entity object) {
		if (object == null) {
			return null;
		}
		return object.toJSON();
	}

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

	@Override
	public <E extends Entity> E deSerialize(ObjectNode objectNode,
			Class<E> entityClass) {
		if (objectNode == null) {
			return null;
		}
		return entityBuilder.build(objectNode, entityClass);
	}

}
