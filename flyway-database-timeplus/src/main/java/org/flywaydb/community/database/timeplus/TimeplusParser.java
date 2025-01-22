/*-
 * ========================LICENSE_START=================================
 * flyway-database-timeplus
 * ========================================================================
 * Copyright (C) 2010 - 2024 Red Gate Software Ltd
 * ========================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
/*
 * Copyright (C) Red Gate Software Ltd 2010-2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.community.database.timeplus;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TimeplusParser extends Parser {
    private static final String ALTERNATIVE_QUOTE = "$$";

    protected TimeplusParser(Configuration configuration, ParsingContext parsingContext, int peekDepth) {
        super(configuration, parsingContext, peekDepth);
    }

    @Override
    protected boolean isAlternativeStringLiteral(String peek) {
        if (peek.startsWith(ALTERNATIVE_QUOTE)) {
            return true;
        }
        return super.isAlternativeStringLiteral(peek);
    }

    @Override
    protected Token handleAlternativeStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        String alternativeQuoteOpen = ALTERNATIVE_QUOTE;
        String alternativeQuoteEnd = ALTERNATIVE_QUOTE;

        String text;

        reader.swallow(alternativeQuoteOpen.length());
        text = reader.readUntilExcluding(alternativeQuoteOpen, alternativeQuoteEnd);
        reader.swallow(alternativeQuoteEnd.length());

        return new Token(TokenType.STRING, pos, line, col, text, text, context.getParensDepth());
    }

    @Override
    protected boolean isSingleLineComment(String peek, ParserContext context, int col) {
        return peek.startsWith("--") || peek.startsWith("//");
    }
}
