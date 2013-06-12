package nc.isi.fragaria_adapter_couchdb.views;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.views.GenericQueryViews.All;
import nc.isi.fragaria_adapter_rewrite.entities.views.QueryView;
import nc.isi.fragaria_adapter_rewrite.entities.views.ViewConfig;
import nc.isi.fragaria_adapter_rewrite.entities.views.ViewConfigBuilder;

import org.apache.log4j.Logger;
import org.mozilla.javascript.CompilerEnvirons;
import org.mozilla.javascript.ErrorReporter;
import org.mozilla.javascript.Parser;
import org.mozilla.javascript.Token;
import org.mozilla.javascript.ast.AstNode;
import org.mozilla.javascript.ast.AstRoot;
import org.mozilla.javascript.ast.FunctionNode;

import com.google.common.base.Throwables;

public class CouchDbViewConfigBuilder implements ViewConfigBuilder {
	private static final Logger LOGGER = Logger
			.getLogger(CouchDbViewConfigBuilder.class);
	private static final String ALL_MAP = "function(doc) {\n if (doc.types.indexOf('%s') >= 0)\n emit(null, doc);\n }\n";
	public static final String JS = ".*\\.js";

	@Override
	public ViewConfig build(String name, String fileName) {
		CouchDbViewConfig config = new CouchDbViewConfig(name);
		try {
			if (fileName.matches(JS)) {
				jsParser(fileName, config);
			} else {
				throw new IllegalArgumentException(String.format(
						"Unknown file type : %s", fileName));
			}
			return config;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void jsParser(String fileName, CouchDbViewConfig config)
			throws IOException {
		AstRoot script = parseJavascript(fileName);
		AstNode node = (AstNode) script.getFirstChild();
		while (node != null) {
			if (node.getType() == Token.FUNCTION) {
				FunctionNode functionNode = (FunctionNode) node;
				config.setBody(functionNode.getFunctionName().toSource(),
						functionNode.getBody().toSource());
			}
			node = (AstNode) node.getNext();
		}
	}

	@Override
	public ViewConfig buildDefault(Class<? extends Entity> entityClass,
			Class<? extends QueryView> view) {
		if (All.class.equals(view))
			return new CouchDbViewConfig(view.getSimpleName().toLowerCase())
					.setMap(String.format(ALL_MAP, entityClass.getName()));
		return null;
	}

	private AstRoot parseJavascript(String fileName) {
		// Try to open a reader to the file supplied...
		AstRoot rootNode = null;
		InputStream stream = null;
		ClassLoader classLoader = Thread.currentThread()
				.getContextClassLoader();
		try {
			stream = classLoader.getResourceAsStream(fileName);
			InputStreamReader inputStreamReader = new InputStreamReader(stream);

			// Setup the compiler environment, error reporter...
			CompilerEnvirons compilerEnv = new CompilerEnvirons();
			ErrorReporter errorReporter = compilerEnv.getErrorReporter();

			// Create an instance of the parser...
			Parser parser = new Parser(compilerEnv, errorReporter);

			// Try to parse the reader...
			rootNode = parser.parse(inputStreamReader, fileName, 1);
		} catch (IOException e) {
			throw Throwables.propagate(e);
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					LOGGER.warn(String.format(
							"erreur lors de la fermeture du fichier %s : %s",
							fileName, e.getMessage()));
				}
			}
		}
		return rootNode;
	}

}
