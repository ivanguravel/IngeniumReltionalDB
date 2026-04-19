package org.example.core.storage.page;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Maps {@code pageId} to file range {@code [pageId * pageSize, (pageId + 1) * pageSize)}.
 */
public final class FilePageStore implements PageStore {

    private final RandomAccessFile file;
    private final int pageSize;

    public FilePageStore(Path path, int pageSizeBytes) throws IOException {
        if (pageSizeBytes <= 0) {
            throw new IllegalArgumentException("pageSizeBytes must be positive");
        }
        this.pageSize = pageSizeBytes;
        this.file = new RandomAccessFile(path.toFile(), "rw");
    }

    @Override
    public int pageSizeBytes() {
        return pageSize;
    }

    @Override
    public ByteBuffer readPage(int pageId) throws IOException {
        long offset = (long) pageId * pageSize;
        if (offset < 0 || offset >= file.length()) {
            throw new IOException("page out of range: " + pageId + ", file length=" + file.length());
        }
        ByteBuffer buf = ByteBuffer.allocate(pageSize);
        FileChannel ch = file.getChannel();
        int read = ch.read(buf, offset);
        if (read != pageSize) {
            throw new IOException("short read: " + read + " expected " + pageSize);
        }
        buf.flip();
        return buf;
    }

    @Override
    public void writePage(int pageId, ByteBuffer data) throws IOException {
        if (data.remaining() != pageSize) {
            throw new IllegalArgumentException(
                    "buffer remaining " + data.remaining() + " != page size " + pageSize);
        }
        long offset = (long) pageId * pageSize;
        long required = offset + pageSize;
        if (file.length() < required) {
            file.setLength(required);
        }
        FileChannel ch = file.getChannel();
        data = data.slice();
        int written = ch.write(data, offset);
        if (written != pageSize) {
            throw new IOException("short write: " + written);
        }
    }

    @Override
    public synchronized int allocatePage() throws IOException {
        long len = file.length();
        int nextId = (int) (len / pageSize);
        file.setLength(len + pageSize);
        return nextId;
    }

    @Override
    public long pageCount() throws IOException {
        long len = file.length();
        return len / pageSize;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
