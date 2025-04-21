#  TODO List

## Catalog & Schema
- [x] Define `Schema` and `Table` with primary key support
- [ ] Primary key constraint enforcement(uniqueness)
- [ ] Add foreign key tracking and validation

## Parser (using sqlglot)
- [x] Parse SQL string into AST
- [ ] Extract SELECT, WHERE, GROUP BY, ORDER BY, HAVING
- [ ] Support WHERE with AND / OR
- [ ] Identify aggregate functions (SUM, MAX, etc.)

## Executor
- [ ] Column projection
- [ ] Row filtering (WHERE)
- [ ] Logical operations (AND, OR)
- [ ] Sorting (ORDER BY)
- [ ] Aggregation (SUM, MAX, MIN)
- [ ] Grouping (GROUP BY)
- [ ] Group filtering (HAVING)

## Expressions & Operators
- [ ] Implement base `Expression` class
- [ ] Binary operators (=, >, <, AND, OR)
- [ ] Aggregate operators (SUM, MAX, etc.)

## Join
- [ ] Support 2-table theta join with ON condition
- [ ] Handle alias and qualified column names (e.g., A.id)

## Testing & Demo
- [ ] Create sample tables and data (e.g., students, enrollments)
- [ ] Write test queries:
  - Simple SELECT + WHERE
  - SELECT + ORDER BY
  - SELECT + GROUP BY + HAVING
  - SELECT with JOIN and condition

## Optional Extensions
- [ ] Add execution plan printing (EXPLAIN)
- [ ] Type checking (integer vs string)
- [ ] Index-aware scan simulation
- [ ] Add error handling for invalid queries



- [ ] 2 table 
- [ ] primary key (2 columns)
- [ ] Conjunctive and disjunctive condition ordering
- [ ] optimizer