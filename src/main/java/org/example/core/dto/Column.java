package org.example.core.dto;

import org.example.core.schema.ColumnType;

/** Column metadata within a {@link org.example.core.schema.Schema}. */
public record Column(String name, ColumnType type, boolean nullable) {
    public Column {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("column name required");
        }
    }
}
