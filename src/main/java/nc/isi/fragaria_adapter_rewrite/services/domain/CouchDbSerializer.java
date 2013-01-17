package nc.isi.fragaria_adapter_rewrite.services.domain;

import java.util.Collection;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class CouchDbSerializer implements Serializer<ObjectNode> {
	private final EntityBuilder entityBuilder;

	public CouchDbSerializer(EntityBuilder entityBuilder) {
		this.entityBuilder = entityBuilder;
	}

	@Override
	public Collection<ObjectNode> serialize(Collection<Entity> objects) {
		Collection<ObjectNode> collection = Lists.newArrayList();
		for (Entity entity : objects) {
			collection.add(entity.toJSON());
		}
		return collection;
	}

	@Override
	public Collection<Entity> deSerialize(Collection<ObjectNode> objects) {
		Collection<Entity> collection = Lists.newArrayList();
		for (ObjectNode objectNode : objects) {
			collection.add(entityBuilder.build(objectNode,
					getEntityType(objectNode)));
		}
		return null;
	}

	private Class<? extends Entity> getEntityType(ObjectNode objectNode) {
		return AbstractEntity.class;
	}

}
