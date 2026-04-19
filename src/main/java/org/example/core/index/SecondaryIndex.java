package org.example.core.index;

import java.io.Closeable;
import java.util.Optional;
import org.example.core.dto.RecordId;

/**
 * Secondary (non-primary) unique index: key bytes point to a heap {@link RecordId}.
 * <p>
 * Duplicate keys are not supported by this interface; use a separate design for non-unique indexes.
 */
public interface SecondaryIndex extends Closeable {

    /**
     * Inserts or replaces the mapping for the given key. The implementation may copy {@code key} bytes.
     */
    void put(byte[] key, RecordId rid);

    /** Returns the RID for {@code key}, or empty if absent. */
    Optional<RecordId> get(byte[] key);

    /** Removes the key if present (unique index). */
    void delete(byte[] key);
}
