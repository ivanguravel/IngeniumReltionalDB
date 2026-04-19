package org.example.core.storage.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FreeSpaceMapTest {

    private static final int PAGE = 256;

    @Test
    void rebuildAndPickPageFindsSpace(@TempDir Path tmp) throws IOException {
        Path f = tmp.resolve("fsm.pages");
        try (FilePageStore store = new FilePageStore(f, PAGE)) {
            int pid = store.allocatePage();
            assertEquals(0, pid);
            ByteBuffer buf = ByteBuffer.allocate(PAGE);
            SlottedPage.initEmpty(buf);
            store.writePage(0, buf);

            FreeSpaceMap map = new FreeSpaceMap();
            map.rebuild(store);
            int avail = SlottedPage.availableBytes(buf);
            assertTrue(avail > 0);
            assertEquals(PAGE - 20, avail);

            assertTrue(map.pickPage(1).isPresent());
            assertEquals(0, map.pickPage(avail).get());
        }
    }

    @Test
    void availableBytesZeroForBadMagic() {
        ByteBuffer buf = ByteBuffer.allocate(PAGE);
        buf.putInt(0, 0);
        assertEquals(0, SlottedPage.availableBytes(buf));
    }
}
