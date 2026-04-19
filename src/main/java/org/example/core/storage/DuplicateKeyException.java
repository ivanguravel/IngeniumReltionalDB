package org.example.core.storage;

/** Thrown when inserting a row would violate a unique index (primary or secondary). */
public final class DuplicateKeyException extends RuntimeException {
    public DuplicateKeyException(String message) {
        super(message);
    }
}
