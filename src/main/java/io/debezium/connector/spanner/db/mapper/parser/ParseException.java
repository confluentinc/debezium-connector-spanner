/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper.parser;

/**
 * Exception thrown while parsing json string in DTOs classes
 */
public class ParseException extends RuntimeException {
    public ParseException(String json, Exception ex) {
        super("Error parse string: " + json, ex);
    }

    /**
     * Builds a parse exception carrying only a fixed, non-sensitive reason (no cause), for
     * callers parsing customer data that must not surface the raw content or the parser's cause.
     */
    public ParseException(String reason) {
        super("Error parse string: " + reason);
    }
}
