package nc.isi.fragaria_adapter_couchdb;

import java.io.InputStream;

import nc.isi.fragaria_adapter_rewrite.entities.AbstractEntity;
import nc.isi.fragaria_adapter_rewrite.entities.attachments.EntityAttachment;

public interface CouchDbAttachment<T extends AbstractEntity> extends EntityAttachment<T>{	
	public String getAttachmentId();
	public InputStream getInputStream();
	public String getContentType();
	public Boolean getIsFile();
}
