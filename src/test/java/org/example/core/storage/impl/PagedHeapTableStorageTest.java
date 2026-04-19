package org.example.core.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.Map;
import org.example.core.dto.RecordId;
import org.example.core.dto.Row;
import org.example.core.dto.TableDescriptor;
import org.example.core.dto.TableId;
import org.example.core.schema.Schema;
import org.example.core.storage.DuplicateKeyException;
import org.example.core.storage.TableStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PagedHeapTableStorageTest {

    @Test
    void insertScanAndPkUnique(@TempDir Path tmp) throws Exception {
        Path heap = tmp.resolve("t.heap");
        Path pk = tmp.resolve("t.pk");
        Schema schema = Schema.defaultRowSchema();

        try (TableStorage t =
                PagedHeapTableStorageFactory.open(
                        new TableDescriptor(
                                new TableId("t"),
                                schema,
                                heap,
                                pk,
                                Map.of()))) {

            RecordId r1 = t.insert(new Row(1L, "a"));
            RecordId r2 = t.insert(new Row(2L, "b"));
            assertEquals(new Row(1L, "a"), t.get(r1));
            assertEquals(2, count(t));
            assertThrows(DuplicateKeyException.class, () -> t.insert(new Row(1L, "dup")));
        }
    }

    @Test
    void secondaryIndexUnique(@TempDir Path tmp) throws Exception {
        Path heap = tmp.resolve("u.heap");
        Path pk = tmp.resolve("u.pk");
        Path sec = tmp.resolve("u.sec");
        Schema schema = Schema.defaultRowSchemaWithTextSecondary();

        try (TableStorage t =
                PagedHeapTableStorageFactory.open(
                        new TableDescriptor(
                                new TableId("u"),
                                schema,
                                heap,
                                pk,
                                Map.of("text_idx", sec)))) {

            t.insert(new Row(1L, "x"));
            assertThrows(DuplicateKeyException.class, () -> t.insert(new Row(2L, "x")));
        }
    }

    private static int count(TableStorage t) {
        int n = 0;
        var it = t.scan();
        while (it.hasNext()) {
            it.next();
            n++;
        }
        return n;
    }
}
