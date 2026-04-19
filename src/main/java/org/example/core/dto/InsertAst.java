package org.example.core.dto;

public record InsertAst(String tableName, long id, String text) implements Ast {}
