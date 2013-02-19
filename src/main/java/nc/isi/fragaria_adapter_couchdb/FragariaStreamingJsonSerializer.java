package nc.isi.fragaria_adapter_couchdb;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.FragariaObjectMapper;
import nc.isi.fragaria_adapter_rewrite.enums.Completion;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.ektorp.impl.BulkOperation;
import org.ektorp.impl.JsonSerializer;
import org.ektorp.util.Exceptions;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

public class FragariaStreamingJsonSerializer implements JsonSerializer {
	private static final Logger LOGGER = Logger
			.getLogger(FragariaStreamingJsonSerializer.class);
	private static ExecutorService singletonExecutorService;
	private final ExecutorService executorService;
	private final ObjectMapper objectMapper = FragariaObjectMapper.INSTANCE
			.get();
	private final JsonFactory jsonFactory = new JsonFactory();

	public FragariaStreamingJsonSerializer() {
		this.executorService = getSingletonExecutorService();
	}

	private static synchronized ExecutorService getSingletonExecutorService() {
		if (singletonExecutorService == null) {
			singletonExecutorService = Executors
					.newCachedThreadPool(new ThreadFactory() {

						private final AtomicInteger threadCount = new AtomicInteger();

						public Thread newThread(Runnable r) {
							Thread t = new Thread(r, String.format(
									"ektorp-doc-writer-thread-%s",
									threadCount.incrementAndGet()));
							t.setDaemon(true);
							return t;
						}

					});
		}
		return singletonExecutorService;
	}

	public BulkOperation createBulkOperation(final Collection<?> objects,
			final boolean allOrNothing) {
		LOGGER.info(String.format("creating bulk operation for %s", objects));
		try {
			final PipedOutputStream out = new PipedOutputStream();
			PipedInputStream in = new PipedInputStream(out);

			Future<?> writeTask = executorService.submit(new Runnable() {

				public void run() {
					try {
						JsonGenerator jg = jsonFactory.createJsonGenerator(out);
						jg.writeStartObject();
						if (allOrNothing) {
							jg.writeBooleanField("all_or_nothing", true);
						}
						jg.writeArrayFieldStart("docs");
						for (Object o : objects) {
							LOGGER.info(String.format("o : %s - json : %s", o,
									toJson(o)));
							jg.writeRawValue(toJson(o));
						}
						jg.writeEndArray();
						jg.writeEndObject();
						jg.flush();
						jg.close();
					} catch (Exception e) {
						throw Throwables.propagate(e);
					} finally {
						IOUtils.closeQuietly(out);
					}

				}
			});

			return new BulkOperation(writeTask, in);
		} catch (IOException e) {
			throw Exceptions.propagate(e);
		}
	}

	public JsonNode toJson(Entity o) {
		LOGGER.info(o.toJSON(Completion.FULL));
		return o.toJSON(Completion.FULL);
	}

	@Override
	public String toJson(Object o) {
		if (o instanceof Entity)
			return toJson((Entity) o).toString();
		try {
			return objectMapper.writeValueAsString(o);
		} catch (JsonProcessingException e) {
			throw Throwables.propagate(e);
		}
	}

}
