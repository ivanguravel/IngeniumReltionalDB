package org.example.core.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.example.core.dto.Row;
import org.example.core.schema.ColumnType;
import org.example.core.schema.Schema;

/** Encodes/decodes {@link Row} according to a {@link Schema} (prototype: id + text only). */
public final class TupleCodec {

    private TupleCodec() {}

    public static byte[] encodeRow(Schema schema, Row row) {
        if (schema.columns().size() != 2
                || schema.column(0).type() != ColumnType.INT64
                || schema.column(1).type() != ColumnType.STRING) {
            throw new IllegalArgumentException("only two-column INT64+STRING Row schema supported");
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            out.writeLong(row.id());
            byte[] utf = row.text().getBytes(StandardCharsets.UTF_8);
            out.writeInt(utf.length);
            out.write(utf);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new StorageException("encode failed", e);
        }
    }

    public static Row decodeRow(Schema schema, byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            long id = in.readLong();
            int len = in.readInt();
            byte[] utf = new byte[len];
            in.readFully(utf);
            String text = new String(utf, StandardCharsets.UTF_8);
            return new Row(id, text);
        } catch (IOException e) {
            throw new StorageException("decode failed", e);
        }
    }

    public static byte[] primaryKeyBytes(Schema schema, Row row) {
        if (schema.primaryKeyColumnIndex() != 0) {
            throw new IllegalArgumentException("PK bytes only for column 0");
        }
        return ByteBuffer.allocate(8).order(java.nio.ByteOrder.BIG_ENDIAN).putLong(0, row.id()).array();
    }

    /** Key bytes for a secondary column (by index). */
    public static byte[] secondaryKeyBytes(Schema schema, Row row, int columnIndex) {
        if (columnIndex == 1 && schema.column(1).type() == ColumnType.STRING) {
            return row.text().getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("unsupported secondary column index: " + columnIndex);
    }
}
