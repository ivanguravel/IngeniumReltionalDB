package org.example.core.dto;

import org.example.core.catalog.Catalog;

/** Runtime catalog access for {@link org.example.core.exec.SqlEngine}. */
public record ExecutionContext(Catalog catalog) {}
