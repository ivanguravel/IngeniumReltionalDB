package org.example.core.dto;

import java.util.List;

public record ExecutionResult(List<Row> rows, String message) {

    public static ExecutionResult rows(List<Row> rows) {
        return new ExecutionResult(List.copyOf(rows), null);
    }

    public static ExecutionResult message(String message) {
        return new ExecutionResult(List.of(), message);
    }
}
