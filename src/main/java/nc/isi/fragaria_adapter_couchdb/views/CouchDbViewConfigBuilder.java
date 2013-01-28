package nc.isi.fragaria_adapter_couchdb.views;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.views.GenericQueryViews.All;
import nc.isi.fragaria_adapter_rewrite.entities.views.QueryView;
import nc.isi.fragaria_adapter_rewrite.entities.views.ViewConfig;
import nc.isi.fragaria_adapter_rewrite.entities.views.ViewConfigBuilder;

import org.mozilla.javascript.CompilerEnvirons;
import org.mozilla.javascript.ErrorReporter;
import org.mozilla.javascript.Parser;
import org.mozilla.javascript.Token;
import org.mozilla.javascript.ast.AstNode;
import org.mozilla.javascript.ast.AstRoot;
import org.mozilla.javascript.ast.FunctionNode;

public class CouchDbViewConfigBuilder implements ViewConfigBuilder {
	private static final String ALL_MAP = "function(doc) {\n if (doc.types.indexOf('%s') >= 0)\n emit(null, doc);\n }\n";
	public static final String JS = ".*\\.js";

	@Override
	public ViewConfig build(String name, File file) {
		CouchDbViewConfig config = new CouchDbViewConfig(name);
		try {
			if (file.getName().matches(JS)) {
				jsParser(file, config);
			} else {
				throw new IllegalArgumentException(String.format(
						"Unknown file type : %s", file.getName()));
			}
			return config;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void jsParser(File file, CouchDbViewConfig config)
			throws IOException {
		AstRoot script = parseJavascript(file);
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

	private AstRoot parseJavascript(File file) throws IOException {
		// Try to open a reader to the file supplied...
		Reader reader = new FileReader(file);

		// Setup the compiler environment, error reporter...
		CompilerEnvirons compilerEnv = new CompilerEnvirons();
		ErrorReporter errorReporter = compilerEnv.getErrorReporter();

		// Create an instance of the parser...
		Parser parser = new Parser(compilerEnv, errorReporter);

		String sourceURI;

		try {
			sourceURI = file.getCanonicalPath();
		} catch (IOException e) {
			sourceURI = file.toString();
		}

		// Try to parse the reader...
		AstRoot rootNode = parser.parse(reader, sourceURI, 1);
		return rootNode;
	}

}
