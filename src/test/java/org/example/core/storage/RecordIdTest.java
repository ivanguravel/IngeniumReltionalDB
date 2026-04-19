package org.example.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.example.core.dto.RecordId;
import org.junit.jupiter.api.Test;

class RecordIdTest {

    @Test
    void roundTripBytes() {
        RecordId id = new RecordId(42, (short) 7);
        byte[] b = id.toBytes();
        assertEquals(id, RecordId.fromBytes(b));
    }
}
