package org.example.core.schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.example.core.dto.Column;

/**
 * Table schema: ordered columns, primary key column index, optional secondary indexes (column name → column index).
 */
public final class Schema {

    private final List<Column> columns;
    private final int primaryKeyColumnIndex;
    private final Map<String, Integer> secondaryIndexByName;

    public Schema(List<Column> columns, int primaryKeyColumnIndex, Map<String, Integer> secondaryIndexByName) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns required");
        }
        if (primaryKeyColumnIndex < 0 || primaryKeyColumnIndex >= columns.size()) {
            throw new IllegalArgumentException("invalid primaryKeyColumnIndex");
        }
        if (columns.get(primaryKeyColumnIndex).type() != ColumnType.INT64) {
            throw new IllegalArgumentException("primary key must be INT64 in this prototype");
        }
        this.columns = List.copyOf(columns);
        this.primaryKeyColumnIndex = primaryKeyColumnIndex;
        this.secondaryIndexByName =
                secondaryIndexByName == null
                        ? Map.of()
                        : Map.copyOf(secondaryIndexByName);
        for (var e : this.secondaryIndexByName.entrySet()) {
            int idx = e.getValue();
            if (idx < 0 || idx >= this.columns.size() || idx == primaryKeyColumnIndex) {
                throw new IllegalArgumentException("invalid secondary index column: " + e);
            }
        }
    }

    /** Default schema matching {@link org.example.core.dto.Row}: id (PK), text; no secondary indexes. */
    public static Schema defaultRowSchema() {
        return new Schema(
                List.of(
                        new Column("id", ColumnType.INT64, false),
                        new Column("text", ColumnType.STRING, false)),
                0,
                Map.of());
    }

    /** Same as {@link #defaultRowSchema()} plus a unique secondary index on the text column ({@code text_idx}). */
    public static Schema defaultRowSchemaWithTextSecondary() {
        return new Schema(
                List.of(
                        new Column("id", ColumnType.INT64, false),
                        new Column("text", ColumnType.STRING, false)),
                0,
                Map.of("text_idx", 1));
    }

    public List<Column> columns() {
        return columns;
    }

    public int primaryKeyColumnIndex() {
        return primaryKeyColumnIndex;
    }

    /** Secondary index name → 0-based column index (not PK). */
    public Map<String, Integer> secondaryIndexByColumnName() {
        return secondaryIndexByName;
    }

    public Column column(int index) {
        return columns.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema schema = (Schema) o;
        return primaryKeyColumnIndex == schema.primaryKeyColumnIndex
                && Objects.equals(columns, schema.columns)
                && Objects.equals(secondaryIndexByName, schema.secondaryIndexByName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, primaryKeyColumnIndex, secondaryIndexByName);
    }
}
