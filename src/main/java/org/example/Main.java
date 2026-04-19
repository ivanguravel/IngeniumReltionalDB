package org.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.example.core.catalog.Catalog;
import org.example.core.dto.ExecutionContext;
import org.example.core.dto.ExecutionResult;
import org.example.core.dto.Row;
import org.example.core.dto.TableDescriptor;
import org.example.core.dto.TableId;
import org.example.core.exec.SqlEngine;
import org.example.core.schema.Schema;

public class Main {

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("ingenium-main");
        dir.toFile().deleteOnExit();

        Path heap = dir.resolve("demo.heap");
        Path pk = dir.resolve("demo.pk.idx");

        Schema schema = Schema.defaultRowSchema();
        Catalog catalog = new Catalog();
        catalog.register(new TableDescriptor(new TableId("demo"), schema, heap, pk, Map.of()));

        var sql = new SqlEngine();
        var ctx = new ExecutionContext(catalog);

        ExecutionResult insertRes = sql.execute("INSERT INTO demo VALUES (1, 'hello');", ctx);
        System.out.println("INSERT -> " + insertRes.message());

        ExecutionResult selectRes = sql.execute("SELECT * FROM demo", ctx);
        System.out.println("SELECT rows:");
        for (Row row : selectRes.rows()) {
            System.out.println("  " + row.id() + " | " + row.text());
        }
    }
}
