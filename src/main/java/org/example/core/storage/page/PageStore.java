package org.example.core.storage.page;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Page-addressed byte storage for one heap table file. Page size is fixed for the lifetime of the store.
 * <p>
 * Page IDs are {@code 0 .. N-1} contiguous; use {@link #allocatePage()} to grow the file. This abstraction
 * is independent of btree4j's internal index pages (see package documentation).
 */
public interface PageStore extends Closeable {

    /** Size of each page in bytes (e.g. 4096). */
    int pageSizeBytes();

    /**
     * Reads a full page into a new heap {@link ByteBuffer} with position 0 and limit {@link #pageSizeBytes()}.
     *
     * @throws IOException if the page is out of range or I/O fails
     */
    ByteBuffer readPage(int pageId) throws IOException;

    /**
     * Writes a full page from {@code data}; {@code data.remaining()} must equal {@link #pageSizeBytes()}.
     * Grows the underlying file if necessary.
     */
    void writePage(int pageId, ByteBuffer data) throws IOException;

    /**
     * Appends one new zero-filled page at the end of the file and returns its id.
     */
    int allocatePage() throws IOException;

    /** Number of pages currently in the file (file length / page size). */
    long pageCount() throws IOException;
}
