# SQL dialect matrix

Golden cases: case1_full_state, case2_sparse, case3_duplicate. Engines: DuckDB 1.4.5, Spark 4.0.4, Trino 483.

`pass` means every golden case returned exactly the expected rows. A file is only required to pass on the engines in its `-- engines:` header; an outcome on another engine is informational and shown in parentheses.

| SQL file | Written for | DuckDB | Spark | Trino |
|---|---|---|---|---|
| sql/dedup.sql | duckdb, spark, trino, athena | pass | pass | pass |
| sql/lag_history.sql | duckdb, spark, trino, athena | pass | pass | pass |
| sql/latest_state.sql | duckdb, spark, trino, athena | pass | pass | pass |
| sql/sparse_current_value.sql | spark, trino, athena | (fail, not claimed: error) | pass | pass |
| sql/sparse_previous_value.sql | spark, trino, athena | (fail, not claimed: error) | pass | pass |
| sql/duckdb/sparse_current_value.sql | duckdb | pass | (fail, not claimed: error) | (fail, not claimed: error) |
| sql/duckdb/sparse_previous_value.sql | duckdb | pass | (fail, not claimed: error) | (fail, not claimed: error) |

## Per case

| SQL file | Engine | case1_full_state | case2_sparse | case3_duplicate |
|---|---|---|---|---|
| sql/dedup.sql | DuckDB | pass | pass | pass |
| sql/dedup.sql | Spark | pass | pass | pass |
| sql/dedup.sql | Trino | pass | pass | pass |
| sql/lag_history.sql | DuckDB | pass | pass | pass |
| sql/lag_history.sql | Spark | pass | pass | pass |
| sql/lag_history.sql | Trino | pass | pass | pass |
| sql/latest_state.sql | DuckDB | pass | pass | pass |
| sql/latest_state.sql | Spark | pass | pass | pass |
| sql/latest_state.sql | Trino | pass | pass | pass |
| sql/sparse_current_value.sql | DuckDB | error: Parser Error: syntax error at or near "IGNORE" | error: Parser Error: syntax error at or near "IGNORE" | error: Parser Error: syntax error at or near "IGNORE" |
| sql/sparse_current_value.sql | Spark | pass | pass | pass |
| sql/sparse_current_value.sql | Trino | pass | pass | pass |
| sql/sparse_previous_value.sql | DuckDB | error: Parser Error: syntax error at or near "IGNORE" | error: Parser Error: syntax error at or near "IGNORE" | error: Parser Error: syntax error at or near "IGNORE" |
| sql/sparse_previous_value.sql | Spark | pass | pass | pass |
| sql/sparse_previous_value.sql | Trino | pass | pass | pass |
| sql/duckdb/sparse_current_value.sql | DuckDB | pass | pass | pass |
| sql/duckdb/sparse_current_value.sql | Spark | error: [PARSE_SYNTAX_ERROR] Syntax error at or near 'IGNORE'. SQLSTATE: 42601 (line 12, pos 19) | error: [PARSE_SYNTAX_ERROR] Syntax error at or near 'IGNORE'. SQLSTATE: 42601 (line 12, pos 19) | error: [PARSE_SYNTAX_ERROR] Syntax error at or near 'IGNORE'. SQLSTATE: 42601 (line 12, pos 19) |
| sql/duckdb/sparse_current_value.sql | Trino | error: TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 12:20: mismatched input 'IGNORE'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', | error: TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 12:20: mismatched input 'IGNORE'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', | error: TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 12:20: mismatched input 'IGNORE'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', |
| sql/duckdb/sparse_previous_value.sql | DuckDB | pass | pass | pass |
| sql/duckdb/sparse_previous_value.sql | Spark | error: [PARSE_SYNTAX_ERROR] Syntax error at or near 'IGNORE'. SQLSTATE: 42601 (line 11, pos 19) | error: [PARSE_SYNTAX_ERROR] Syntax error at or near 'IGNORE'. SQLSTATE: 42601 (line 11, pos 19) | error: [PARSE_SYNTAX_ERROR] Syntax error at or near 'IGNORE'. SQLSTATE: 42601 (line 11, pos 19) |
| sql/duckdb/sparse_previous_value.sql | Trino | error: TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 11:20: mismatched input 'IGNORE'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', | error: TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 11:20: mismatched input 'IGNORE'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', | error: TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message="line 11:20: mismatched input 'IGNORE'. Expecting: '%', '(', ')', '*', '+', ',', '-', '->', '.', '/', |

Every file passed on every engine it claims.
