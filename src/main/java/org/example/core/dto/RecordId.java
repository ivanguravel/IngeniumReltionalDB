package org.example.core.dto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Physical row address in a paged heap: table page number and slot index within that page.
 * <p>
 * Not related to {@link Row} (logical column values); this type is for navigation after an index lookup.
 */
public record RecordId(int pageId, short slot) {

    private static final int BYTES = 6;

    /**
     * Encodes this RID as a fixed 6-byte blob (big-endian): {@code int pageId}, {@code short slot}.
     */
    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(BYTES).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(pageId);
        bb.putShort(slot);
        return bb.array();
    }

    /**
     * Decodes a value produced by {@link #toBytes()}.
     */
    public static RecordId fromBytes(byte[] data) {
        if (data == null || data.length != BYTES) {
            throw new IllegalArgumentException("Expected " + BYTES + " bytes, got " + Arrays.toString(data));
        }
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        return new RecordId(bb.getInt(), bb.getShort());
    }
}
