package org.example.core.storage;

import java.io.Closeable;
import java.util.Iterator;
import org.example.core.dto.RecordId;
import org.example.core.dto.Row;
import org.example.core.schema.Schema;

/** One physical table: paged heap + indexes (opened via {@link org.example.core.catalog.Catalog}). */
public interface TableStorage extends Closeable {

    Schema schema();

    /** Inserts a row; updates primary and secondary indexes. */
    RecordId insert(Row row);

    Row get(RecordId rid);

    Iterator<Row> scan();
}
