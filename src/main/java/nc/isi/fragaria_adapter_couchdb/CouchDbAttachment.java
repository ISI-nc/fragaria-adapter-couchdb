package nc.isi.fragaria_adapter_couchdb;

import java.io.InputStream;

import nc.isi.fragaria_adapter_rewrite.entities.attachments.EntityAttachment;

public interface CouchDbAttachment extends EntityAttachment{	
	public String getUrlForInputStream();
	
	public void setUrlForInputStream(String urlForInputStream);

	public InputStream getInputStream();
	
	public void closeInputStream();

}
