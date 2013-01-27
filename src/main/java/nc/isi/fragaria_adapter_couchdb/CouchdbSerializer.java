package nc.isi.fragaria_adapter_couchdb;

import java.util.Collection;

import nc.isi.fragaria_adapter_rewrite.entities.Entity;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface CouchdbSerializer {

	public Collection<ObjectNode> serialize(Collection<Entity> objects);

	public ObjectNode serialize(Entity object);

	public <E extends Entity> Collection<E> deSerialize(
			Collection<ObjectNode> objects, Class<E> entityClass);

	public <E extends Entity> E deSerialize(ObjectNode objectNode,
			Class<E> entityClass);

}