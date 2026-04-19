package org.example.core.dto;

import java.nio.file.Path;
import java.util.Map;
import org.example.core.schema.Schema;

/** Paths to heap and index files for one table. */
public record TableDescriptor(
        TableId id, Schema schema, Path heapPath, Path primaryIndexPath, Map<String, Path> secondaryIndexPaths) {

    public TableDescriptor {
        secondaryIndexPaths = secondaryIndexPaths == null ? Map.of() : Map.copyOf(secondaryIndexPaths);
    }
}
