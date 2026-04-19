package org.example.core.storage.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.example.core.dto.TableDescriptor;
import org.example.core.index.Btree4jSecondaryIndex;
import org.example.core.index.SecondaryIndex;
import org.example.core.storage.page.FilePageStore;

public final class PagedHeapTableStorageFactory {

    public static final int DEFAULT_PAGE_SIZE = 512;

    private PagedHeapTableStorageFactory() {}

    public static PagedHeapTableStorage open(TableDescriptor d) throws IOException {
        FilePageStore pages = new FilePageStore(d.heapPath(), DEFAULT_PAGE_SIZE);
        SecondaryIndex primary = Btree4jSecondaryIndex.open(d.primaryIndexPath());
        Map<String, SecondaryIndex> secondaries = new HashMap<>();
        for (var e : d.secondaryIndexPaths().entrySet()) {
            secondaries.put(e.getKey(), Btree4jSecondaryIndex.open(e.getValue()));
        }
        return new PagedHeapTableStorage(d.schema(), pages, primary, secondaries);
    }
}
