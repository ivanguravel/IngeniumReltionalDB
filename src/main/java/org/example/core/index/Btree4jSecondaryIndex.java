package org.example.core.index;

import btree4j.BTreeException;
import btree4j.BTreeIndex;
import btree4j.Value;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import org.example.core.dto.RecordId;
import org.example.core.storage.StorageException;

/**
 * Disk-backed unique secondary index using btree4j's on-disk B+-tree file (separate from heap {@link org.example.core.storage.page.PageStore} pages).
 */
public final class Btree4jSecondaryIndex implements SecondaryIndex {

    private final BTreeIndex btree;

    private Btree4jSecondaryIndex(BTreeIndex btree) {
        this.btree = btree;
    }

    /**
     * Opens or creates a unique index at the given path.
     */
    public static SecondaryIndex open(Path path) {
        try {
            BTreeIndex btree = new BTreeIndex(path.toFile(), false);
            btree.init(false);
            return new Btree4jSecondaryIndex(btree);
        } catch (BTreeException e) {
            throw new StorageException("failed to open btree index: " + path, e);
        }
    }

    @Override
    public void put(byte[] key, RecordId rid) {
        byte[] keyCopy = Arrays.copyOf(key, key.length);
        Value k = new Value(keyCopy);
        Value v = new Value(rid.toBytes());
        try {
            btree.putValue(k, v);
            btree.flush();
        } catch (BTreeException e) {
            throw new StorageException("index put failed", e);
        }
    }

    @Override
    public Optional<RecordId> get(byte[] key) {
        byte[] keyCopy = Arrays.copyOf(key, key.length);
        try {
            byte[] bytes = btree.getValueBytes(new Value(keyCopy));
            if (bytes == null) {
                return Optional.empty();
            }
            return Optional.of(RecordId.fromBytes(bytes));
        } catch (BTreeException e) {
            throw new StorageException("index get failed", e);
        }
    }

    @Override
    public void delete(byte[] key) {
        byte[] keyCopy = Arrays.copyOf(key, key.length);
        try {
            byte[][] removed = btree.remove(new Value(keyCopy));
            if (removed != null && removed.length > 0) {
                btree.flush();
            }
        } catch (BTreeException e) {
            throw new StorageException("index delete failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            btree.flush();
        } catch (BTreeException e) {
            throw new IOException("index flush failed", e);
        }
        try {
            btree.close();
        } catch (BTreeException e) {
            throw new IOException("index close failed", e);
        }
    }
}
