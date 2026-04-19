package org.example.core.storage.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory map: page id → free bytes for next tuple (see {@link SlottedPage#availableBytes}).
 */
public final class FreeSpaceMap {

    private final Map<Integer, Integer> freeByPage = new HashMap<>();

    public boolean isEmpty() {
        return freeByPage.isEmpty();
    }

    public void rebuild(PageStore pages) throws IOException {
        freeByPage.clear();
        long n = pages.pageCount();
        for (int pid = 0; pid < n; pid++) {
            ByteBuffer buf = pages.readPage(pid);
            recordPage(pid, buf);
        }
    }

    public void recordPage(int pageId, ByteBuffer page) {
        freeByPage.put(pageId, SlottedPage.availableBytes(page));
    }

    /** Smallest page id that has at least {@code minBytes} free, if any. */
    public Optional<Integer> pickPage(int minBytes) {
        return freeByPage.entrySet().stream()
                .filter(e -> e.getValue() >= minBytes)
                .map(Map.Entry::getKey)
                .min(Integer::compareTo);
    }
}
