package org.flywaydb.community.database.crate;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

public class CrateParser extends Parser {
    public CrateParser(Configuration configuration, ParsingContext parsingContext) {
        super(configuration, parsingContext, 3);
    }
}
