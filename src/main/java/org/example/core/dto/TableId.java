package org.example.core.dto;

/** Logical table name in the catalog. */
public record TableId(String name) {
    public TableId {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("table name required");
        }
    }
}
