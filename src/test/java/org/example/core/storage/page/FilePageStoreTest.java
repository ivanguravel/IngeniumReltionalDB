package org.example.core.storage.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilePageStoreTest {

    private static final int PAGE = 256;

    @Test
    void allocatePagesIncreasesFileAndRoundTripsWrite(@TempDir Path tempDir) throws IOException {
        Path f = tempDir.resolve("heap.pages");
        try (FilePageStore store = new FilePageStore(f, PAGE)) {
            assertEquals(0, store.allocatePage());
            assertEquals(1, store.allocatePage());

            ByteBuffer payload = ByteBuffer.allocate(PAGE);
            payload.put("hello".getBytes(StandardCharsets.UTF_8));
            payload.flip();
            // writePage expects remaining == pageSize; refill to PAGE
            byte[] full = new byte[PAGE];
            System.arraycopy(payload.array(), 0, full, 0, 5);
            store.writePage(0, ByteBuffer.wrap(full));

            ByteBuffer read = store.readPage(0);
            assertEquals(PAGE, read.remaining());
            byte[] head = new byte[5];
            read.get(head);
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), head);
        }
    }

    @Test
    void readMissingPageFails(@TempDir Path tempDir) throws IOException {
        Path f = tempDir.resolve("empty.pages");
        try (FilePageStore store = new FilePageStore(f, PAGE)) {
            assertThrows(IOException.class, () -> store.readPage(0));
        }
    }
}
