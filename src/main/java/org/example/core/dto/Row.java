package org.example.core.dto;

/**
 * A logical table row for the minimal heap storage prototype.
 * <p>
 * Field layout is fixed for v1: one 64-bit key and one variable-length string payload.
 */
public record Row(long id, String text) {
    public Row {
        if (text == null) {
            throw new IllegalArgumentException("text must not be null");
        }
    }
}
