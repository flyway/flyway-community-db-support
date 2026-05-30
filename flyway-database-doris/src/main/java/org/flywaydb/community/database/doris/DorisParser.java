/*-
 * ========================LICENSE_START=================================
 * flyway-database-doris
 * ========================================================================
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
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

package org.flywaydb.community.database.doris;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.ParserContext;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.PeekingReader;
import org.flywaydb.core.internal.parser.StatementType;
import org.flywaydb.core.internal.parser.Token;
import org.flywaydb.core.internal.parser.TokenType;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.Character.isDigit;

public class DorisParser extends Parser {
    private static final char ALTERNATIVE_SINGLE_LINE_COMMENT = '#';

    private static final Pattern STORED_PROGRAM_REGEX = Pattern.compile(
            "^CREATE\\s(((DEFINER\\s(\\w+\\s)?@\\s(\\w+\\s)?)?(PROCEDURE|FUNCTION|EVENT))|TRIGGER)", Pattern.CASE_INSENSITIVE);
    private static final StatementType STORED_PROGRAM_STATEMENT = new StatementType();

    public DorisParser(Configuration configuration, ParsingContext parsingContext) {
        super(configuration, parsingContext, 8);
    }

    @Override
    protected void resetDelimiter(ParserContext context) {
    }

    @Override
    protected Token handleKeyword(PeekingReader reader, ParserContext context, int pos, int line, int col, String keyword) throws IOException {
        if ("DELIMITER".equalsIgnoreCase(keyword)) {
            String text = "";
            while (text.isEmpty()) {
                text = reader.readUntilExcluding('\n', '\r').trim();
                reader.swallow(1);
            }
            return new Token(TokenType.NEW_DELIMITER, pos, line, col, text, text, context.getParensDepth());
        }
        return super.handleKeyword(reader, context, pos, line, col, keyword);
    }

    @Override
    protected char getIdentifierQuote() {
        return '`';
    }

    @Override
    protected char getAlternativeStringLiteralQuote() {
        return '"';
    }

    @Override
    protected boolean isSingleLineComment(String peek, ParserContext context, int col) {
        return super.isSingleLineComment(peek, context, col)
                || (peek.charAt(0) == ALTERNATIVE_SINGLE_LINE_COMMENT && !isDelimiter(peek, context, col, 0));
    }

    @Override
    protected Token handleStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        reader.swallow();
        reader.swallowUntilIncludingWithEscape('\'', true, '\\');
        return new Token(TokenType.STRING, pos, line, col, null, null, context.getParensDepth());
    }

    @Override
    protected Token handleAlternativeStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        reader.swallow();
        reader.swallowUntilIncludingWithEscape('"', true, '\\');
        return new Token(TokenType.STRING, pos, line, col, null, null, context.getParensDepth());
    }

    @Override
    protected Token handleCommentDirective(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        reader.swallow(2);
        String text = reader.readUntilExcluding("*/");
        reader.swallow(2);
        return new Token(TokenType.MULTI_LINE_COMMENT_DIRECTIVE, pos, line, col, text, text, context.getParensDepth());
    }

    @Override
    protected boolean isCommentDirective(String text) {
        return text.length() >= 8
                && text.charAt(0) == '/'
                && text.charAt(1) == '*'
                && text.charAt(2) == '!'
                && isDigit(text.charAt(3))
                && isDigit(text.charAt(4))
                && isDigit(text.charAt(5))
                && isDigit(text.charAt(6))
                && isDigit(text.charAt(7));
    }

    @Override
    protected StatementType detectStatementType(String simplifiedStatement, ParserContext context, PeekingReader reader) {
        if (STORED_PROGRAM_REGEX.matcher(simplifiedStatement).matches()) {
            return STORED_PROGRAM_STATEMENT;
        }
        return super.detectStatementType(simplifiedStatement, context, reader);
    }

    @Override
    protected boolean shouldAdjustBlockDepth(ParserContext context, List<Token> tokens, Token token) {
        return token.getParensDepth() == 0;
    }

    @Override
    protected void adjustBlockDepth(ParserContext context, List<Token> tokens, Token keyword, PeekingReader reader) {
        String keywordText = keyword.getText();
        int parensDepth = keyword.getParensDepth();

        if ("BEGIN".equalsIgnoreCase(keywordText) && context.getStatementType() == STORED_PROGRAM_STATEMENT) {
            context.increaseBlockDepth("");
        }

        if ("CASE".equalsIgnoreCase(keywordText)) {
            if (!lastTokenIs(tokens, parensDepth, "END")) {
                context.increaseBlockDepth("");
            }
        }

        if (context.getBlockDepth() > 0
                && lastTokenIs(tokens, parensDepth, "END")
                && !"IF".equalsIgnoreCase(keywordText)
                && !"LOOP".equalsIgnoreCase(keywordText)
                && !"REPEAT".equalsIgnoreCase(keywordText)
                && !"WHILE".equalsIgnoreCase(keywordText)) {
            context.decreaseBlockDepth();
        }
    }
}
