package org.example.core.dto;

/** Root of SQL AST (minimal subset). */
public sealed interface Ast permits InsertAst, SelectAst {}
