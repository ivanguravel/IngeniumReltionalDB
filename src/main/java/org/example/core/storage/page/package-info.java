/**
 * Paged heap table storage ({@link org.example.core.storage.page.PageStore}).
 * <p>
 * These pages are <strong>not</strong> the same as btree4j's internal B+-tree pages used by
 * {@link org.example.core.index.Btree4jSecondaryIndex}: the index file has its own layout. The link between
 * worlds is {@link org.example.core.dto.RecordId} stored as the index value. Integrating
 * {@link org.example.core.storage.impl.PagedHeapTableStorage} implements heap rows on top of
 * {@link org.example.core.storage.page.PageStore} (slotted pages).
 */
package org.example.core.storage.page;
