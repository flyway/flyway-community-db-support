package org.flywaydb.community.database.duckdb;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

public class DuckDBParser extends Parser {

    protected DuckDBParser(Configuration configuration, ParsingContext parsingContext) {
        super(configuration, parsingContext, 2);
    }
}
