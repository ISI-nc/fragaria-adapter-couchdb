function map(doc) {
  if(doc.types.indexOf('nc.isi.fragaria_adapter_couchdb.model.PersonData')>=0)
  	emit(null, doc);
}

function reduce(doc){
	emit(doc);
}