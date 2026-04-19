package org.example.core.dto;

/** Bound statement ready for execution. */
public record AnalyzedStatement(String tableName, Kind kind, Row row) {
    public enum Kind {
        INSERT,
        SELECT
    }
}
