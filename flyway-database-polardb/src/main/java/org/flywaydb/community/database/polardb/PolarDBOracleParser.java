/*-
 * ========================LICENSE_START=================================
 * flyway-database-polardb
 * ========================================================================
 * Copyright (C) 2010 - 2026 Red Gate Software Ltd
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
package org.flywaydb.community.database.polardb;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.ParserContext;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.parser.PeekingReader;
import org.flywaydb.core.internal.parser.Token;
import org.flywaydb.core.internal.parser.TokenType;
import org.flywaydb.core.internal.parser.StatementType;
import org.flywaydb.database.postgresql.PostgreSQLParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class PolarDBOracleParser extends PostgreSQLParser {
    private static final Pattern PLSQL_PACKAGE_BODY_REGEX =
            Pattern.compile(
                    "^CREATE(\\s*OR\\s*REPLACE)?"
                            + "(\\s*(NON)?EDITIONABLE)?"
                            + "\\s*PACKAGE\\s*BODY\\s*"
                            + "[^\\s]+");
    private static final Pattern PLSQL_PACKAGE_DEFINITION_REGEX =
            Pattern.compile(
                    "^CREATE(\\s*OR\\s*REPLACE)?"
                            + "(\\s*(NON)?EDITIONABLE)?"
                            + "\\s*PACKAGE\\s+"
                            + "(?!BODY(?:\\s|$))[^\\s]+");
    private static final Pattern PLSQL_TYPE_BODY_REGEX =
            Pattern.compile(
                    "^CREATE(\\s+OR\\s+REPLACE)?"
                            + "(\\s+(NON)?EDITIONABLE)?"
                            + "\\s+TYPE\\s+BODY\\s+"
                            + "[^\\s]+");
    private static final Pattern PLSQL_ROUTINE_REGEX =
            Pattern.compile(
                    "^CREATE(\\s+OR\\s+REPLACE)?"
                            + "(\\s+(NON)?EDITIONABLE)?"
                            + "\\s+(FUNCTION(\\s+\\S*)|PROCEDURE)");
    private static final Pattern PLSQL_TRIGGER_REGEX =
            Pattern.compile(
                    "^CREATE(\\s+OR\\s+REPLACE)?"
                            + "(\\s+(NON)?EDITIONABLE)?"
                            + "\\s+TRIGGER");
    private static final Pattern PLSQL_WITH_ROUTINE_REGEX =
            Pattern.compile("^WITH\\s+(FUNCTION|PROCEDURE)");
    private static final Pattern PLSQL_REGEX =
            Pattern.compile(
                    "^CREATE(\\s+OR\\s+REPLACE)?"
                            + "(\\s+(NON)?EDITIONABLE)?"
                            + "\\s+(FUNCTION(\\s+\\S*)|PROCEDURE|TYPE|TRIGGER)");
    private static final StatementType PLSQL_STATEMENT =
            new StatementType();
    private static final StatementType PLSQL_ROUTINE_STATEMENT =
            new StatementType();
    private static final StatementType PLSQL_TRIGGER_STATEMENT =
            new StatementType();
    private static final StatementType PLSQL_WITH_ROUTINE_STATEMENT =
            new StatementType();
    private static final StatementType PLSQL_PACKAGE_STATEMENT =
            new StatementType();
    private static final StatementType PLSQL_PACKAGE_BODY_STATEMENT =
            new StatementType();
    private static final List<String> CONTROL_FLOW_KEYWORDS =
            Arrays.asList("IF", "LOOP", "CASE");
    private final boolean postgreSqlMode;
    private boolean suppressInquiryDirectiveSecondDollar;

    public PolarDBOracleParser(
            Configuration configuration,
            ParsingContext parsingContext,
            boolean postgreSqlMode) {
        super(configuration, parsingContext);
        this.postgreSqlMode = postgreSqlMode;
    }

    @Override
    protected StatementType detectStatementType(
            String simplifiedStatement,
            ParserContext context,
            PeekingReader reader) {
        if (PLSQL_PACKAGE_BODY_REGEX.matcher(simplifiedStatement).matches()) {
            context.increaseBlockDepth("PACKAGE BODY");
            return PLSQL_PACKAGE_BODY_STATEMENT;
        }

        if (PLSQL_PACKAGE_DEFINITION_REGEX
                .matcher(simplifiedStatement)
                .matches()) {
            context.increaseBlockDepth("PACKAGE");
            return PLSQL_PACKAGE_STATEMENT;
        }

        if ("DECLARE".equals(simplifiedStatement)) {
            context.increaseBlockDepth("DECLARE");
            return PLSQL_STATEMENT;
        }

        if ("BEGIN".equals(simplifiedStatement)) {
            context.increaseBlockDepth("BEGIN");
            return PLSQL_STATEMENT;
        }

        if (!postgreSqlMode
                && PLSQL_WITH_ROUTINE_REGEX
                        .matcher(simplifiedStatement)
                        .matches()) {
            // Keep Oracle-style inline function and procedure declarations
            // open until the top-level SELECT so their internal semicolons
            // remain part of one SQL statement.
            context.increaseBlockDepth("WITH ROUTINE");
            // The FUNCTION or PROCEDURE that completed statement detection
            // is not adjusted again, so record the first routine here.
            context.increaseBlockDepth("ROUTINE");
            return PLSQL_WITH_ROUTINE_STATEMENT;
        }

        if (!postgreSqlMode
                && PLSQL_TRIGGER_REGEX
                        .matcher(simplifiedStatement)
                        .matches()) {
            return PLSQL_TRIGGER_STATEMENT;
        }

        if (!postgreSqlMode
                && PLSQL_ROUTINE_REGEX
                        .matcher(simplifiedStatement)
                        .matches()) {
            // PolarDB-O accepts END; without Oracle's trailing slash. Keep
            // the declaration section inside the routine block so its
            // semicolons do not terminate the Flyway statement.
            context.increaseBlockDepth("ROUTINE");
            return PLSQL_ROUTINE_STATEMENT;
        }

        if (PLSQL_REGEX.matcher(simplifiedStatement).matches()) {
            return PLSQL_STATEMENT;
        }

        return super.detectStatementType(
                simplifiedStatement,
                context,
                reader);
    }

    @Override
    protected boolean shouldAdjustBlockDepth(
            ParserContext context,
            List<Token> tokens,
            Token token) {
        if (supportsNestedRoutineDeclarations(context)
                && token.getParensDepth() == 0
                && isRoutineKeyword(token)) {
            return isAtRoutineDeclarationBoundary(tokens, token);
        }

        if (supportsNestedRoutineDeclarations(context)
                && token.getType() == TokenType.DELIMITER
                && token.getParensDepth() == 0
                && "ROUTINE".equals(context.getBlockInitiator())
                && isForwardRoutineDeclaration(tokens)) {
            return true;
        }

        if (context.getStatementType() == PLSQL_WITH_ROUTINE_STATEMENT
                && context.getBlockDepth() == 1
                && token.getParensDepth() == 0
                && "SELECT".equals(token.getText())) {
            return true;
        }

        if (context.getStatementType() == PLSQL_PACKAGE_BODY_STATEMENT
                && (token.getType() == TokenType.EOF
                        || token.getType() == TokenType.DELIMITER)) {
            return true;
        }

        if (token.getParensDepth() == 0
                && isPlSqlLabelKeyword(tokens)) {
            return false;
        }

        Token previousToken =
                getPreviousToken(tokens, token.getParensDepth());
        if (previousToken != null
                && "CASE".equals(token.getText())
                && "FROM".equals(previousToken.getText())) {
            return false;
        }

        return super.shouldAdjustBlockDepth(context, tokens, token);
    }

    @Override
    protected void adjustBlockDepth(
            ParserContext context,
            List<Token> tokens,
            Token keyword,
            PeekingReader reader) {
        if (context.getStatementType() != PLSQL_STATEMENT
                && context.getStatementType()
                        != PLSQL_ROUTINE_STATEMENT
                && context.getStatementType()
                        != PLSQL_TRIGGER_STATEMENT
                && context.getStatementType()
                        != PLSQL_WITH_ROUTINE_STATEMENT
                && context.getStatementType()
                        != PLSQL_PACKAGE_STATEMENT
                && context.getStatementType()
                        != PLSQL_PACKAGE_BODY_STATEMENT) {
            super.adjustBlockDepth(context, tokens, keyword, reader);
            return;
        }

        String keywordText = keyword.getText();
        int parensDepth = keyword.getParensDepth();

        if (lastTokenIs(tokens, parensDepth, "GOTO")) {
            return;
        }

        if (isEndUsedAsIdentifier(context, tokens, keyword)) {
            return;
        }

        if (keyword.getType() == TokenType.DELIMITER
                && "ROUTINE".equals(context.getBlockInitiator())
                && isForwardRoutineDeclaration(tokens)) {
            // A forward declaration has no IS/AS or executable body. Remove
            // the provisional routine level at its terminating semicolon so
            // that a later full definition can establish its own level.
            context.decreaseBlockDepth();
        } else if (context.getStatementType()
                        == PLSQL_WITH_ROUTINE_STATEMENT
                && context.getBlockDepth() == 1
                && "SELECT".equals(keywordText)) {
            context.decreaseBlockDepth();
        } else if (context.getStatementType() == PLSQL_TRIGGER_STATEMENT
                && context.getBlockDepth() == 0
                && "DECLARE".equals(keywordText)) {
            context.increaseBlockDepth("TRIGGER DECLARE");
        } else if (supportsNestedRoutineDeclarations(context)
                && isNestedRoutineDeclaration(
                        context,
                        tokens,
                        keyword)) {
            context.increaseBlockDepth("ROUTINE");
        } else if (context.getStatementType()
                        == PLSQL_TRIGGER_STATEMENT
                && "TRIGGER".equals(keywordText)
                && lastTokenIs(tokens, parensDepth, "COMPOUND")) {
            context.increaseBlockDepth("COMPOUND TRIGGER");
        } else if ("BEGIN".equals(keywordText)
                || (CONTROL_FLOW_KEYWORDS.contains(keywordText)
                        && !precedingEndAttachesToThisKeyword(
                                tokens,
                                parensDepth,
                                context,
                                keyword))
                || (context.getBlockDepth() == 0
                        && (doTokensMatchPattern(
                                        tokens,
                                        keyword,
                                        PLSQL_PACKAGE_BODY_REGEX)
                                || doTokensMatchPattern(
                                        tokens,
                                        keyword,
                                        PLSQL_PACKAGE_DEFINITION_REGEX)
                                || doTokensMatchPattern(
                                        tokens,
                                        keyword,
                                        PLSQL_TYPE_BODY_REGEX)))) {
            context.increaseBlockDepth(keywordText);
        } else if ("END".equals(keywordText)
                && !(context.getStatementType()
                                == PLSQL_PACKAGE_BODY_STATEMENT
                        && context.getBlockDepth() == 1)) {
            context.decreaseBlockDepth();
            // Cursor queries and declaration initializers may contain SQL
            // CASE expressions. Their END closes CASE, not the surrounding
            // PL/SQL routine or declaration wrapper.
            boolean closedExecutableBody = "BEGIN".equals(
                    context.getLastClosedBlockInitiator());
            if (closedExecutableBody
                    && ((context.getBlockDepth() == 1
                            && "DECLARE".equals(
                                    context.getBlockInitiator()))
                    || (supportsNestedRoutineDeclarations(context)
                            && "ROUTINE".equals(
                                    context.getBlockInitiator()))
                    || (context.getStatementType()
                                    == PLSQL_TRIGGER_STATEMENT
                            && "TRIGGER DECLARE".equals(
                                    context.getBlockInitiator())))) {
                context.decreaseBlockDepth();
            }
        }

        if (context.getStatementType()
                        == PLSQL_PACKAGE_BODY_STATEMENT
                && (keyword.getType() == TokenType.EOF
                        || keyword.getType() == TokenType.DELIMITER)
                && context.getBlockDepth() == 1
                && (isNamedPackageTerminator(tokens)
                        || "/".equals(keyword.getText()))) {
            context.decreaseBlockDepth();
        }
    }

    private boolean isNestedRoutineDeclaration(
            ParserContext context,
            List<Token> tokens,
            Token keyword) {
        if (!isRoutineKeyword(keyword)) {
            return false;
        }

        // An anonymous DECLARE block has no top-level routine keyword. A
        // routine keyword at a declaration boundary is therefore local and
        // must get its own block level before its BEGIN/END body is parsed.
        if (context.getStatementType() == PLSQL_STATEMENT) {
            return true;
        }

        if (context.getStatementType() == PLSQL_TRIGGER_STATEMENT
                || context.getStatementType()
                        == PLSQL_WITH_ROUTINE_STATEMENT) {
            return true;
        }

        int routineKeywordCount = 0;
        boolean currentKeywordRecorded = false;
        for (Token token : tokens) {
            if (token == keyword) {
                currentKeywordRecorded = true;
            }
            if (token.getParensDepth() == 0
                    && isRoutineKeyword(token)) {
                routineKeywordCount++;
            }
        }
        return currentKeywordRecorded
                ? routineKeywordCount > 1
                : routineKeywordCount > 0;
    }

    private boolean supportsNestedRoutineDeclarations(
            ParserContext context) {
        return context.getStatementType() == PLSQL_STATEMENT
                || context.getStatementType()
                        == PLSQL_ROUTINE_STATEMENT
                || context.getStatementType()
                        == PLSQL_TRIGGER_STATEMENT
                || context.getStatementType()
                        == PLSQL_WITH_ROUTINE_STATEMENT;
    }

    private boolean isForwardRoutineDeclaration(List<Token> tokens) {
        int routineIndex = -1;
        for (int i = tokens.size() - 1; i >= 0; i--) {
            Token token = tokens.get(i);
            if (token.getParensDepth() == 0
                    && isRoutineKeyword(token)
                    && isAtRoutineDeclarationBoundary(tokens, token)) {
                routineIndex = i;
                break;
            }
        }

        if (routineIndex < 0) {
            return false;
        }

        for (int i = routineIndex + 1; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.getParensDepth() == 0
                    && ("IS".equals(token.getText())
                            || "AS".equals(token.getText()))) {
                return false;
            }
        }
        return true;
    }

    private boolean isRoutineKeyword(Token token) {
        return "FUNCTION".equals(token.getText())
                || "PROCEDURE".equals(token.getText());
    }

    private boolean isPlSqlLabelKeyword(List<Token> tokens) {
        int firstChevronIndex = previousSignificantTokenIndex(
                tokens,
                tokens.size() - 1);
        int secondChevronIndex = previousSignificantTokenIndex(
                tokens,
                firstChevronIndex - 1);
        return firstChevronIndex >= 0
                && secondChevronIndex >= 0
                && "<".equals(tokens.get(firstChevronIndex).getText())
                && "<".equals(tokens.get(secondChevronIndex).getText());
    }

    private boolean isEndUsedAsIdentifier(
            ParserContext context,
            List<Token> tokens,
            Token keyword) {
        if (!"END".equals(keyword.getText())
                || "CASE".equals(context.getBlockInitiator())) {
            return false;
        }

        int previousIndex = previousSignificantTokenIndex(
                tokens,
                tokens.size() - 1);
        if (previousIndex < 0) {
            return false;
        }

        Token previous = tokens.get(previousIndex);
        return previous.getType() != TokenType.DELIMITER
                && !";".equals(previous.getText());
    }

    private boolean isAtRoutineDeclarationBoundary(
            List<Token> tokens,
            Token keyword) {
        int keywordIndex = -1;
        for (int i = tokens.size() - 1; i >= 0; i--) {
            if (tokens.get(i) == keyword) {
                keywordIndex = i;
                break;
            }
        }

        int previousIndex = previousSignificantTokenIndex(
                tokens,
                keywordIndex >= 0 ? keywordIndex - 1 : tokens.size() - 1);
        if (previousIndex < 0) {
            return false;
        }

        Token previous = tokens.get(previousIndex);
        String previousText = previous.getText();
        return previous.getType() == TokenType.DELIMITER
                || ";".equals(previousText)
                || "AS".equals(previousText)
                || "IS".equals(previousText)
                || "DECLARE".equals(previousText);
    }

    private boolean isNamedPackageTerminator(List<Token> tokens) {
        String packageName = findPackageName(tokens);
        if (packageName == null) {
            return false;
        }

        int endNameIndex =
                previousSignificantTokenIndex(tokens, tokens.size() - 1);
        int endIndex =
                previousSignificantTokenIndex(tokens, endNameIndex - 1);
        if (endNameIndex < 0
                || endIndex < 0
                || !"END".equals(tokens.get(endIndex).getText())) {
            return false;
        }

        return packageName.equalsIgnoreCase(
                normalizeIdentifier(tokens.get(endNameIndex)));
    }

    private String findPackageName(List<Token> tokens) {
        boolean packageKeywordSeen = false;
        String packageName = null;

        for (Token token : tokens) {
            if (token.getParensDepth() != 0
                    || token.getType() == TokenType.COMMENT
                    || token.getType() == TokenType.BLANK_LINES) {
                continue;
            }

            String text = token.getText();
            if (!packageKeywordSeen) {
                if ("PACKAGE".equals(text)) {
                    packageKeywordSeen = true;
                }
                continue;
            }

            if ("AS".equals(text)
                    || "IS".equals(text)
                    || "AUTHID".equals(text)) {
                break;
            }
            if ("BODY".equals(text)) {
                continue;
            }
            if (token.getType() == TokenType.KEYWORD
                    || token.getType() == TokenType.IDENTIFIER) {
                packageName = normalizeIdentifier(token);
            }
        }

        return packageName;
    }

    private int previousSignificantTokenIndex(
            List<Token> tokens,
            int startIndex) {
        for (int i = startIndex; i >= 0; i--) {
            Token token = tokens.get(i);
            if (token.getParensDepth() == 0
                    && token.getType() != TokenType.COMMENT
                    && token.getType() != TokenType.BLANK_LINES) {
                return i;
            }
        }
        return -1;
    }

    private String normalizeIdentifier(Token token) {
        String identifier = token.getRawText() == null
                ? token.getText()
                : token.getRawText();
        int lastDot = identifier.lastIndexOf('.');
        if (lastDot >= 0) {
            identifier = identifier.substring(lastDot + 1);
        }
        if (identifier.length() >= 2
                && identifier.startsWith("\"")
                && identifier.endsWith("\"")) {
            identifier = identifier.substring(
                    1,
                    identifier.length() - 1);
        }
        return identifier;
    }

    private boolean precedingEndAttachesToThisKeyword(
            List<Token> tokens,
            int parensDepth,
            ParserContext context,
            Token keyword) {
        return lastTokenIs(tokens, parensDepth, "END")
                && lastTokenIsOnLine(
                        tokens,
                        parensDepth,
                        keyword.getLine())
                && keyword.getText().equals(
                        context.getLastClosedBlockInitiator());
    }

    @Override
    protected boolean shouldDiscard(
            Token token,
            boolean nonCommentPartSeen) {
        return ("/".equals(token.getText()) && !nonCommentPartSeen)
                || super.shouldDiscard(token, nonCommentPartSeen);
    }

    @Override
    protected boolean isDelimiter(
            String peek,
            ParserContext context,
            int col,
            int colIgnoringWhitespace) {
        if (colIgnoringWhitespace == 1
                && "/".equals(peek.trim())) {
            return true;
        }
        return super.isDelimiter(
                peek,
                context,
                col,
                colIgnoringWhitespace);
    }

    @Override
    protected Token handleDelimiter(
            PeekingReader reader,
            ParserContext context,
            int pos,
            int line,
            int col) throws IOException {
        if (reader.peek('/')) {
            reader.swallow(1);
            return new Token(
                    TokenType.DELIMITER,
                    pos,
                    line,
                    col,
                    "/",
                    "/",
                    context.getParensDepth());
        }
        return super.handleDelimiter(
                reader,
                context,
                pos,
                line,
                col);
    }

    @Override
    protected boolean isAlternativeStringLiteral(String peek) {
        // Oracle inquiry directives such as $$PLSQL_UNIT must not open a
        // PostgreSQL dollar-quoted string. The parser examines each dollar
        // separately, so suppress both characters in Oracle mode.
        if (!postgreSqlMode && suppressInquiryDirectiveSecondDollar) {
            suppressInquiryDirectiveSecondDollar = false;
            if (peek.startsWith("$")) {
                return false;
            }
        }
        if (!postgreSqlMode && peek.startsWith("$$")) {
            suppressInquiryDirectiveSecondDollar = true;
            return false;
        }
        return isOracleQuotedLiteral(peek)
                || ((postgreSqlMode
                                || isTaggedPostgreSqlDollarQuote(peek))
                        && super.isAlternativeStringLiteral(peek));
    }

    @Override
    protected Token handleAlternativeStringLiteral(
            PeekingReader reader,
            ParserContext context,
            int pos,
            int line,
            int col) throws IOException {
        if (!reader.peek("q'") && !reader.peek("Q'")) {
            return super.handleAlternativeStringLiteral(
                    reader,
                    context,
                    pos,
                    line,
                    col);
        }

        reader.swallow(2);
        String closeQuote =
                computeAlternativeCloseQuote((char) reader.read());
        reader.swallowUntilExcluding(closeQuote);
        reader.swallow(closeQuote.length());
        return new Token(
                TokenType.STRING,
                pos,
                line,
                col,
                null,
                null,
                context.getParensDepth());
    }

    @Override
    protected Token handleMultilineComment(
            PeekingReader reader,
            ParserContext context,
            int pos,
            int line,
            int col) throws IOException {
        if (postgreSqlMode) {
            return super.handleMultilineComment(
                    reader,
                    context,
                    pos,
                    line,
                    col);
        }

        reader.swallow(2);
        String text = reader.readUntilExcluding("*/");
        reader.swallow(2);
        return new Token(
                TokenType.COMMENT,
                pos,
                line,
                col,
                text,
                text,
                context.getParensDepth());
    }

    private boolean isOracleQuotedLiteral(String peek) {
        if (peek.length() < 3) {
            return false;
        }
        char firstChar = peek.charAt(0);
        return (firstChar == 'q' || firstChar == 'Q')
                && peek.charAt(1) == '\'';
    }

    private boolean isTaggedPostgreSqlDollarQuote(String peek) {
        if (peek.length() < 2
                || peek.charAt(0) != '$'
                || peek.charAt(1) == '$') {
            return false;
        }
        char firstTagCharacter = peek.charAt(1);
        return firstTagCharacter == '_'
                || Character.isLetter(firstTagCharacter);
    }

    private String computeAlternativeCloseQuote(char specialChar) {
        switch (specialChar) {
            case '[':
                return "]'";
            case '(':
                return ")'";
            case '{':
                return "}'";
            case '<':
                return ">'";
            default:
                return specialChar + "'";
        }
    }
}
