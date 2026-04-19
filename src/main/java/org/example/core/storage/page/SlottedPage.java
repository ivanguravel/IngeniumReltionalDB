package org.example.core.storage.page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Slotted page: header (16 B), tuple bytes from {@link #HEADER} upward, slot directory (u16 offset, u16 len) from page end downward.
 */
public final class SlottedPage {

    public static final int HEADER = 16;
    private static final int MAGIC = 0x494E4731;

    private SlottedPage() {}

    /** Clears the buffer and writes an empty valid page (full limit for {@link PageStore#writePage}). */
    public static void initEmpty(ByteBuffer page) {
        Arrays.fill(page.array(), 0, page.capacity(), (byte) 0);
        page.order(ByteOrder.BIG_ENDIAN);
        page.putInt(0, MAGIC);
        page.putShort(4, (short) 0);
        page.putShort(6, (short) 0);
        page.putInt(8, HEADER);
        page.putInt(12, 0);
        page.position(0);
        page.limit(page.capacity());
    }

    public static int slotCount(ByteBuffer page) {
        return Short.toUnsignedInt(page.duplicate().order(ByteOrder.BIG_ENDIAN).getShort(4));
    }

    /**
     * Contiguous free bytes available for the next tuple (same bound as in {@link #tryInsert}).
     * Returns 0 if magic is wrong or space is non-positive.
     */
    public static int availableBytes(ByteBuffer page) {
        ByteBuffer p = page.duplicate().order(ByteOrder.BIG_ENDIAN);
        if (p.getInt(0) != MAGIC) {
            return 0;
        }
        int slotCount = Short.toUnsignedInt(p.getShort(4));
        int freeOff = p.getInt(8);
        int pageSize = page.capacity();
        int slotDirStart = pageSize - (slotCount + 1) * 4;
        int avail = slotDirStart - freeOff;
        return Math.max(0, avail);
    }

    public static int tryInsert(ByteBuffer page, byte[] tuple) {
        if (tuple == null || tuple.length > 65535) {
            throw new IllegalArgumentException("invalid tuple length");
        }
        ByteBuffer p = page.duplicate().order(ByteOrder.BIG_ENDIAN);
        if (p.getInt(0) != MAGIC) {
            throw new IllegalStateException("page not initialized; call initEmpty");
        }
        int slotCount = Short.toUnsignedInt(p.getShort(4));
        int freeOff = p.getInt(8);

        int pageSize = page.capacity();
        int slotDirStart = pageSize - (slotCount + 1) * 4;
        if (freeOff + tuple.length > slotDirStart) {
            return -1;
        }

        byte[] arr = page.array();
        System.arraycopy(tuple, 0, arr, freeOff, tuple.length);

        int newSlotIndex = slotCount;
        int slotEntryPos = pageSize - (newSlotIndex + 1) * 4;
        ByteBuffer slot = page.duplicate().order(ByteOrder.BIG_ENDIAN);
        slot.position(slotEntryPos);
        slot.putShort((short) freeOff);
        slot.putShort((short) tuple.length);

        int newFree = freeOff + tuple.length;
        p = page.duplicate().order(ByteOrder.BIG_ENDIAN);
        p.putShort(4, (short) (slotCount + 1));
        p.putInt(8, newFree);

        return newSlotIndex;
    }

    public static byte[] getTuple(ByteBuffer page, int slotIndex) {
        ByteBuffer p = page.duplicate().order(ByteOrder.BIG_ENDIAN);
        if (p.getInt(0) != MAGIC) {
            throw new IllegalStateException("bad page magic");
        }
        int slotCount = Short.toUnsignedInt(p.getShort(4));
        if (slotIndex < 0 || slotIndex >= slotCount) {
            throw new IllegalArgumentException("slot " + slotIndex + " out of 0.." + (slotCount - 1));
        }
        int pageSize = page.capacity();
        int slotEntryPos = pageSize - (slotIndex + 1) * 4;
        p.position(slotEntryPos);
        int off = Short.toUnsignedInt(p.getShort());
        int len = Short.toUnsignedInt(p.getShort());
        byte[] out = new byte[len];
        System.arraycopy(page.array(), off, out, 0, len);
        return out;
    }
}
