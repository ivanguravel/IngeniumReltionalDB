package org.example.core.catalog;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.example.core.dto.TableDescriptor;
import org.example.core.storage.TableStorage;
import org.example.core.storage.impl.PagedHeapTableStorageFactory;

/** In-memory registry of table metadata and paths. */
public final class Catalog {

    private final Map<String, TableDescriptor> tables = new ConcurrentHashMap<>();

    public void register(TableDescriptor descriptor) {
        tables.put(descriptor.id().name(), descriptor);
    }

    public Optional<TableDescriptor> describe(String tableName) {
        return Optional.ofNullable(tables.get(tableName));
    }

    /** Opens heap + indexes for the registered table. */
    public TableStorage openTable(String tableName) throws IOException {
        TableDescriptor d =
                describe(tableName).orElseThrow(() -> new IllegalArgumentException("unknown table: " + tableName));
        return PagedHeapTableStorageFactory.open(d);
    }
}
