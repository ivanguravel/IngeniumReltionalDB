package org.example.core.storage.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.example.core.index.SecondaryIndex;
import org.example.core.schema.Schema;
import org.example.core.storage.DuplicateKeyException;
import org.example.core.dto.RecordId;
import org.example.core.dto.Row;
import org.example.core.storage.StorageException;
import org.example.core.storage.TableStorage;
import org.example.core.storage.TupleCodec;
import org.example.core.storage.page.FreeSpaceMap;
import org.example.core.storage.page.PageStore;
import org.example.core.storage.page.SlottedPage;

/**
 * Slotted heap over {@link PageStore} with unique primary and secondary btree indexes.
 */
public final class PagedHeapTableStorage implements TableStorage {

    private final Schema schema;
    private final PageStore pages;
    private final SecondaryIndex primaryIndex;
    private final Map<String, SecondaryIndex> secondaryIndexes;
    private final FreeSpaceMap freeSpace = new FreeSpaceMap();

    public PagedHeapTableStorage(
            Schema schema,
            PageStore pages,
            SecondaryIndex primaryIndex,
            Map<String, SecondaryIndex> secondaryIndexes) {
        this.schema = Objects.requireNonNull(schema);
        this.pages = Objects.requireNonNull(pages);
        this.primaryIndex = Objects.requireNonNull(primaryIndex);
        this.secondaryIndexes = Map.copyOf(secondaryIndexes);
        for (String name : schema.secondaryIndexByColumnName().keySet()) {
            if (!this.secondaryIndexes.containsKey(name)) {
                throw new IllegalArgumentException("missing SecondaryIndex for: " + name);
            }
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public RecordId insert(Row row) {
        byte[] pk = TupleCodec.primaryKeyBytes(schema, row);
        if (primaryIndex.get(pk).isPresent()) {
            throw new DuplicateKeyException("primary key duplicate");
        }
        for (var e : schema.secondaryIndexByColumnName().entrySet()) {
            int col = e.getValue();
            byte[] sk = TupleCodec.secondaryKeyBytes(schema, row, col);
            SecondaryIndex idx = secondaryIndexes.get(e.getKey());
            if (idx != null && idx.get(sk).isPresent()) {
                throw new DuplicateKeyException("secondary index duplicate: " + e.getKey());
            }
        }

        byte[] tuple = TupleCodec.encodeRow(schema, row);
        try {
            RecordId rid = insertTupleOnly(tuple);
            primaryIndex.put(pk, rid);
            for (var e : schema.secondaryIndexByColumnName().entrySet()) {
                SecondaryIndex idx = secondaryIndexes.get(e.getKey());
                if (idx != null) {
                    byte[] sk = TupleCodec.secondaryKeyBytes(schema, row, e.getValue());
                    idx.put(sk, rid);
                }
            }
            return rid;
        } catch (IOException ex) {
            throw new StorageException("insert failed", ex);
        }
    }

    private void ensureFreeSpaceMap() throws IOException {
        long n = pages.pageCount();
        if (n > 0 && freeSpace.isEmpty()) {
            freeSpace.rebuild(pages);
        }
    }

    private RecordId insertTupleOnly(byte[] tuple) throws IOException {
        int pageSize = pages.pageSizeBytes();
        ensureFreeSpaceMap();

        Optional<Integer> hint = freeSpace.pickPage(tuple.length);
        if (hint.isPresent()) {
            int pid = hint.get();
            ByteBuffer buf = pages.readPage(pid);
            int slot = SlottedPage.tryInsert(buf, tuple);
            if (slot >= 0) {
                pages.writePage(pid, buf);
                freeSpace.recordPage(pid, buf);
                return new RecordId(pid, (short) slot);
            }
        }

        long n = pages.pageCount();
        for (int pid = 0; pid < n; pid++) {
            ByteBuffer buf = pages.readPage(pid);
            int slot = SlottedPage.tryInsert(buf, tuple);
            if (slot >= 0) {
                pages.writePage(pid, buf);
                freeSpace.recordPage(pid, buf);
                return new RecordId(pid, (short) slot);
            }
        }
        int newPid = pages.allocatePage();
        ByteBuffer buf = ByteBuffer.allocate(pageSize);
        SlottedPage.initEmpty(buf);
        int slot = SlottedPage.tryInsert(buf, tuple);
        if (slot < 0) {
            throw new IllegalStateException("tuple larger than one page");
        }
        pages.writePage(newPid, buf);
        freeSpace.recordPage(newPid, buf);
        return new RecordId(newPid, (short) slot);
    }

    @Override
    public Row get(RecordId rid) {
        try {
            ByteBuffer buf = pages.readPage(rid.pageId());
            byte[] t = SlottedPage.getTuple(buf, Short.toUnsignedInt(rid.slot()));
            return TupleCodec.decodeRow(schema, t);
        } catch (IOException e) {
            throw new StorageException("get failed", e);
        }
    }

    @Override
    public Iterator<Row> scan() {
        try {
            List<Row> rows = new ArrayList<>();
            long n = pages.pageCount();
            for (int pid = 0; pid < n; pid++) {
                ByteBuffer buf = pages.readPage(pid);
                int sc = SlottedPage.slotCount(buf);
                for (int s = 0; s < sc; s++) {
                    byte[] t = SlottedPage.getTuple(buf, s);
                    rows.add(TupleCodec.decodeRow(schema, t));
                }
            }
            return rows.iterator();
        } catch (IOException e) {
            throw new StorageException("scan failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        try {
            pages.close();
        } catch (IOException e) {
            first = e;
        }
        try {
            primaryIndex.close();
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        for (SecondaryIndex idx : secondaryIndexes.values()) {
            try {
                idx.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
