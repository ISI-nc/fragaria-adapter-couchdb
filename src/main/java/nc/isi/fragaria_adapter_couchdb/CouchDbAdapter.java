package nc.isi.fragaria_adapter_couchdb;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.mysema.query.alias.Alias.$;
import static com.mysema.query.alias.Alias.alias;
import static com.mysema.query.collections.MiniApi.from;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import nc.isi.fragaria_adapter_couchdb.views.CouchDbViewConfig;
import nc.isi.fragaria_adapter_rewrite.dao.ByViewQuery;
import nc.isi.fragaria_adapter_rewrite.dao.CollectionQueryResponse;
import nc.isi.fragaria_adapter_rewrite.dao.IdQuery;
import nc.isi.fragaria_adapter_rewrite.dao.Query;
import nc.isi.fragaria_adapter_rewrite.dao.SearchQuery;
import nc.isi.fragaria_adapter_rewrite.dao.UniqueQueryResponse;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.AbstractAdapter;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.Adapter;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.ElasticSearchAdapter;
import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityMetadata;
import nc.isi.fragaria_adapter_rewrite.entities.views.ViewConfig;
import nc.isi.fragaria_adapter_rewrite.enums.State;
import nc.isi.fragaria_adapter_rewrite.resources.DataSourceProvider;
import nc.isi.fragaria_adapter_rewrite.resources.Datasource;

import org.apache.log4j.Logger;
import org.ektorp.BulkDeleteDocument;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DbPath;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.ektorp.support.DesignDocument;
import org.ektorp.support.DesignDocument.View;

import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class CouchDbAdapter extends AbstractAdapter implements Adapter {
	private static final String KEY_PREFIXE = "doc.";
	private static final String BY_TOKEN = "By";
	private static final Logger LOGGER = Logger.getLogger(CouchDbAdapter.class);
	private static final String DESIGN_DOC_PREFIXE = "_design/";
	private static final long MAX_INSTANCE_TIME = 60L;
	private static final long MAX_CONNECTOR = 30L;
	private final DataSourceProvider dataSourceProvider;
	private final CouchdbSerializer serializer;
	private final ElasticSearchAdapter elasticSearchAdapter;
	private final CouchDbObjectMapperProvider objectMapperProvider;
	private final LoadingCache<URL, CouchDbInstance> instanceCache = CacheBuilder
			.newBuilder()
			.expireAfterAccess(MAX_INSTANCE_TIME, TimeUnit.MINUTES)
			.build(new CacheLoader<URL, CouchDbInstance>() {

				@Override
				public CouchDbInstance load(URL key) {
					HttpClient httpClient = new StdHttpClient.Builder()
							.url(key).build();
					return new StdCouchDbInstance(httpClient);
				}

			});
	private final LoadingCache<Datasource, CouchDbConnector> connectors = CacheBuilder
			.newBuilder().expireAfterAccess(MAX_CONNECTOR, TimeUnit.MINUTES)
			.build(new CacheLoader<Datasource, CouchDbConnector>() {

				@Override
				public CouchDbConnector load(Datasource key)
						throws ExecutionException {
					CouchdbConnectionData couchdbConnectionData = getConnectionData(key);
					CouchDbInstance instance = instanceCache
							.get(couchdbConnectionData.getUrl());
					DbPath path = DbPath.fromString(couchdbConnectionData
							.getDbName());
					if (!instance.checkIfDbExists(path))
						instance.createDatabase(path);
					return new StdCouchDbConnector(couchdbConnectionData
							.getDbName(), instance, objectMapperProvider,
							new FragariaStreamingJsonSerializer());
				}

			});

	public CouchDbAdapter(DataSourceProvider dataSourceProvider,
			CouchdbSerializer serializer,
			ElasticSearchAdapter elasticSearchAdapter,
			CouchDbObjectMapperProvider objectMapperProvider) {
		this.serializer = serializer;
		this.elasticSearchAdapter = elasticSearchAdapter;
		this.dataSourceProvider = dataSourceProvider;
		this.objectMapperProvider = objectMapperProvider;
	}

	public <T extends Entity> CollectionQueryResponse<T> executeQuery(
			final Query<T> query) {
		checkNotNull(query);
		if (query instanceof IdQuery) {
			throw new IllegalArgumentException(
					"Impossible de renvoyer une Collection depuis une IdQuery");
		}
		if (query instanceof ByViewQuery) {
			ByViewQuery<T> bVQuery = (ByViewQuery<T>) query;
			ViewQuery vQuery = buildViewQuery(bVQuery);
			LOGGER.debug(vQuery.getKey());
			CollectionQueryResponse<T> response = executeQuery(vQuery,
					bVQuery.getResultType());
			if (bVQuery.getPredicate() == null) {
				return response;
			}
			T entity = alias(query.getResultType());
			return buildQueryResponse(from($(entity), response.getResponse())
					.where(bVQuery.getPredicate()).list($(entity)));
		}
		if (query instanceof SearchQuery) {
			return elasticSearchAdapter.executeQuery((SearchQuery<T>) query);
		}
		throw new IllegalArgumentException(String.format(
				"Type de query inconnu : %s", query.getClass()));
	}

	protected <T extends Entity> ViewQuery buildViewQuery(ByViewQuery<T> bVQuery) {
		checkNotNull(bVQuery);
		EntityMetadata entityMetadata = new EntityMetadata(
				bVQuery.getResultType());
		String viewName = findViewName(bVQuery, entityMetadata);
		ViewQuery vQuery = new ViewQuery().designDocId(
				buildDesignDocId(bVQuery)).viewName(viewName);
		return bVQuery.getFilter().values().isEmpty() ? vQuery : vQuery
				.key(buildKey(bVQuery.getFilter().values()));
	}

	private String buildKey(Collection<Object> values) {
		String result = values.toString();
		if (values.size() == 1) {
			return result.replaceAll("\\[", "").replaceAll("\\]", "");
		}
		return result;
	}

	private String findViewName(ByViewQuery<?> bVQuery,
			EntityMetadata entityMetadata) {
		String viewName = bVQuery.getView().getSimpleName().toLowerCase();
		View view = getView(viewName, entityMetadata);
		Collection<String> keys = bVQuery.getFilter().keySet();
		Collection<String> emitKeys = JsHelper.getEmitKeys(view.getMap());
		if (!emitKeys.containsAll(keys)) {
			if (viewName.contains(BY_TOKEN))
				throw new BadViewException(viewName, keys, emitKeys);
			viewName = findRightView(viewName, view, keys, entityMetadata);
		}
		return viewName;
	}

	private String findRightView(String viewName, View view,
			Collection<String> keys, EntityMetadata entityMetadata) {
		String rightViewName = viewName + BY_TOKEN;
		Collection<String> docKeys = Lists.newArrayList();
		for (String key : keys) {
			rightViewName += key;
			docKeys.add(buildEmitKey(key, entityMetadata));
		}
		CouchDbViewConfig config = new CouchDbViewConfig(rightViewName);
		config.setMap(JsHelper.replaceEmitKeys(view.getMap(), docKeys));
		config.setReduce(view.getReduce());
		if (!exist(config, entityMetadata))
			buildView(config, entityMetadata);
		return rightViewName;
	}

	private String buildEmitKey(String string, EntityMetadata entityMetadata) {
		if (Entity.class.isAssignableFrom(entityMetadata.propertyType(string)))
			return KEY_PREFIXE + string + "."
					+ entityMetadata.getJsonPropertyName(Entity.ID);
		return KEY_PREFIXE + string;
	}

	protected String buildDesignDocId(ByViewQuery<?> bVQuery) {
		checkNotNull(bVQuery);
		return buildDesignDocId(bVQuery.getResultType());
	}

	protected String buildDesignDocId(Class<? extends Entity> entityClass) {
		return DESIGN_DOC_PREFIXE + entityClass.getSimpleName();
	}

	public <T extends Entity> CollectionQueryResponse<T> executeQuery(
			ViewQuery viewQuery, Class<T> type) {
		checkNotNull(viewQuery);
		checkNotNull(type);
		EntityMetadata entityMetadata = new EntityMetadata(type);
		ViewResult result = getConnector(entityMetadata).queryView(viewQuery);
		Collection<T> collection = Lists.newArrayList();
		for (Row row : result) {
			if (row.getValueAsNode() instanceof MissingNode) {
				continue;
			}
			LOGGER.info(row.getValueAsNode());
			collection.add(serializer.deSerialize(
					ObjectNode.class.cast(row.getValueAsNode()), type));
		}
		return buildQueryResponse(collection);
	}

	public <T extends Entity> UniqueQueryResponse<T> executeUniqueQuery(
			String id, Class<T> type) {
		checkNotNull(id);
		checkNotNull(type);
		EntityMetadata entityMetadata = new EntityMetadata(type);
		return buildQueryResponse(getConnector(entityMetadata).get(type, id));
	}

	@Override
	public <T extends Entity> UniqueQueryResponse<T> executeUniqueQuery(
			Query<T> query) {
		checkNotNull(query);
		if (query instanceof IdQuery) {
			return executeUniqueQuery(((IdQuery<T>) query).getId(),
					query.getResultType());
		}
		CollectionQueryResponse<T> response = executeQuery(query);
		checkState(response.getResponse().size() <= 1,
				"La requête a renvoyé trop de résultat : %s",
				response.getResponse());
		return buildQueryResponse(response.getResponse().size() == 0 ? null
				: response.getResponse().iterator().next());
	}

	@Override
	public void post(Entity... entities) {
		LinkedList<Entity> list = new LinkedList<>();
		for (Entity entity : entities) {
			list.addLast(checkNotNull(entity));
		}
		post(list);
	}

	@Override
	public void post(List<Entity> entities) {
		checkNotNull(entities);
		List<Entity> filtered = cleanMultipleEntries(entities);
		Set<CouchDbConnector> connectorsToFlush = Sets.newHashSet();
		for (Entity entity : filtered) {
			CouchDbConnector couchDbConnector = getConnector(entity.metadata());
			if (!connectorsToFlush.contains(couchDbConnector)) {
				connectorsToFlush.add(couchDbConnector);
			}
			couchDbConnector.addToBulkBuffer(deleteIfNeeded(entity));
		}
		for (CouchDbConnector connector : connectorsToFlush) {
			connector.flushBulkBuffer();
		}
		for (Entity entity : filtered) {
			entity.setState(State.COMMITED);
		}
	}

	private List<Entity> cleanMultipleEntries(List<Entity> entities) {
		List<Entity> filtered = new LinkedList<>();
		Multimap<State, Entity> dispatch = HashMultimap.create();
		for (Entity entity : entities) {
			State state = entity.getState();
			if (!dispatch.containsValue(entity)) {
				dispatch.put(state, entity);
				continue;
			}
			State oldState = lookForEntityState(dispatch, entity);
			manage(dispatch, state, oldState, entity);
		}
		for (State state : dispatch.keySet()) {
			filtered.addAll(dispatch.get(state));
		}
		return filtered;
	}

	private void manage(Multimap<State, Entity> dispatch, State state,
			State oldState, Entity entity) {
		switch (state) {
		case MODIFIED:
			switch (oldState) {
			case MODIFIED:
				dispatch.put(oldState, entity);
				break;
			default:
				commitError(entity, oldState, state);
			}
			break;
		case DELETED:
			switch (oldState) {
			case NEW:
				dispatch.remove(oldState, entity);
				break;
			case MODIFIED:
				dispatch.remove(oldState, entity);
				dispatch.put(state, entity);
				break;
			default:
				commitError(entity, oldState, state);
			}
			break;
		case NEW:
			switch (oldState) {
			case NEW:
				dispatch.put(oldState, entity);
				break;

			default:
				commitError(entity, oldState, state);
			}
			break;
		default:
			commitError(entity, oldState, state);
		}

	}

	private State lookForEntityState(Multimap<State, Entity> dispatch,
			Entity entity) {
		for (State state : dispatch.keySet()) {
			if (dispatch.get(state).contains(entity)) {
				return state;
			}
		}
		return null;
	}

	protected CouchDbConnector getConnector(EntityMetadata entityMetadata) {
		checkNotNull(entityMetadata);
		Datasource ds = dataSourceProvider.provide(entityMetadata.getDsKey());
		CouchDbConnector couchDbConnector;
		try {
			couchDbConnector = connectors.get(ds);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
		return couchDbConnector;
	}

	private Object deleteIfNeeded(Entity entity) {
		checkNotNull(entity);
		if (entity.getState() == State.DELETED) {
			return new BulkDeleteDocument(entity.getId().toString(), entity
					.getRev().toString());
		}
		return entity;
	}

	@Override
	public Boolean exist(ViewConfig viewConfig, EntityMetadata entityMetadata) {
		checkCouchdb(viewConfig, entityMetadata);
		CouchDbConnector connector = checkNotNull(getConnector(entityMetadata));
		String designDocId = buildDesignDocId(entityMetadata.getEntityClass());
		if (!connector.contains(designDocId))
			return false;
		DesignDocument designDoc = connector.get(DesignDocument.class,
				designDocId);
		if (designDoc == null)
			return false;
		if (!designDoc.containsView(viewConfig.getName()))
			return false;
		View view = designDoc.get(viewConfig.getName());
		CouchDbViewConfig config = CouchDbViewConfig.class.cast(viewConfig);
		return config.equals(view);
	}

	@Override
	public void buildView(ViewConfig viewConfig, EntityMetadata entityMetadata) {
		checkCouchdb(viewConfig, entityMetadata);
		CouchDbConnector connector = checkNotNull(getConnector(entityMetadata));
		String designDocId = buildDesignDocId(entityMetadata.getEntityClass());
		DesignDocument dd = connector.contains(designDocId) ? connector.get(
				DesignDocument.class, designDocId) : new DesignDocument(
				designDocId);
		addView(viewConfig, dd);
		connector.update(dd);
	}

	protected void checkCouchdb(ViewConfig viewConfig,
			EntityMetadata entityMetadata) {
		checkNotNull(entityMetadata);
		checkNotNull(viewConfig);
		if (!(viewConfig instanceof CouchDbViewConfig))
			throw new IllegalArgumentException(String.format(
					"Seules les %s sont géré par %s", CouchDbViewConfig.class,
					CouchDbAdapter.class));
	}

	public void createDb(Datasource datasource) {
		checkNotNull(datasource);
		try {
			CouchdbConnectionData connectionData = checkNotNull(getConnectionData(datasource));
			instanceCache.get(connectionData.getUrl()).createDatabase(
					connectionData.getDbName());
			if (!dataSourceProvider.datasources().contains(datasource))
				dataSourceProvider.register(datasource);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	public void deleteDb(Datasource datasource) {
		checkNotNull(datasource);
		try {
			CouchdbConnectionData connectionData = checkNotNull(getConnectionData(datasource));
			CouchDbInstance instance = instanceCache.get(connectionData
					.getUrl());
			if (instance.checkIfDbExists(DbPath.fromString(connectionData
					.getDbName()))) {
				instance.deleteDatabase(connectionData.getDbName());
				connectors.invalidate(datasource);
				if (dataSourceProvider.datasources().contains(datasource))
					dataSourceProvider.unregister(datasource);
			}
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	protected DesignDocument getDesignDocument(EntityMetadata entityMetadata) {
		CouchDbConnector connector = checkNotNull(getConnector(entityMetadata));
		String designDocId = buildDesignDocId(entityMetadata.getEntityClass());
		return connector.get(DesignDocument.class, designDocId);
	}

	protected View getView(String name, EntityMetadata entityMetadata) {
		return getDesignDocument(entityMetadata).get(name);
	}

	protected CouchdbConnectionData getConnectionData(Datasource datasource) {
		return (CouchdbConnectionData) datasource.getDsMetadata()
				.getConnectionData();

	}

	protected void addView(ViewConfig viewConfig, DesignDocument dd) {
		checkNotNull(viewConfig);
		checkNotNull(dd);
		CouchDbViewConfig config = CouchDbViewConfig.class.cast(viewConfig);
		if (dd.containsView(viewConfig.getName())) {
			View view = dd.get(viewConfig.getName());
			if (config.equals(view))
				return;
			dd.removeView(viewConfig.getName());
		}
		dd.addView(viewConfig.getName(),
				new View(config.getMap(), config.getReduce()));
	}

}
