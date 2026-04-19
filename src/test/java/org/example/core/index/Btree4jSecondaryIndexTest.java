package org.example.core.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import org.example.core.dto.RecordId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Btree4jSecondaryIndexTest {

    @Test
    void putGetRoundTrip(@TempDir Path tempDir) throws IOException {
        Path idx = tempDir.resolve("test.idx");
        RecordId rid = new RecordId(3, (short) 11);
        byte[] key = "user:42".getBytes(StandardCharsets.UTF_8);

        try (SecondaryIndex index = Btree4jSecondaryIndex.open(idx)) {
            index.put(key, rid);
            Optional<RecordId> got = index.get(key);
            assertTrue(got.isPresent());
            assertEquals(rid, got.get());
        }
    }

    @Test
    void getMissingReturnsEmpty(@TempDir Path tempDir) throws IOException {
        Path idx = tempDir.resolve("empty.idx");
        try (SecondaryIndex index = Btree4jSecondaryIndex.open(idx)) {
            assertTrue(index.get("nope".getBytes(StandardCharsets.UTF_8)).isEmpty());
        }
    }

    @Test
    void deleteRemovesKey(@TempDir Path tempDir) throws IOException {
        Path idx = tempDir.resolve("del.idx");
        byte[] key = "k".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (SecondaryIndex index = Btree4jSecondaryIndex.open(idx)) {
            index.put(key, new RecordId(0, (short) 0));
            assertEquals(Optional.of(new RecordId(0, (short) 0)), index.get(key));
            index.delete(key);
            assertEquals(Optional.empty(), index.get(key));
        }
    }

    @Test
    void persistsAcrossReopen(@TempDir Path tempDir) throws IOException {
        Path idx = tempDir.resolve("persist.idx");
        RecordId rid = new RecordId(1, (short) 0);
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);

        try (SecondaryIndex index = Btree4jSecondaryIndex.open(idx)) {
            index.put(key, rid);
        }

        try (SecondaryIndex index = Btree4jSecondaryIndex.open(idx)) {
            assertEquals(Optional.of(rid), index.get(key));
        }
    }
}
