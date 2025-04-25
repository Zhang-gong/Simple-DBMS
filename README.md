# Simple-DBMS

## Overview  
Simple-DBMS is a lightweight, Python-based relational database management system that supports core SQL features through a command-line interface. It uses **sqlglot** for parsing SQL into an AST and an **Executor** component to execute the queries.

## Key Features  
- **Schema Definition**: `CREATE TABLE`, `DROP TABLE` with automatic schema persistence.  `INSERT`, `UPDATE`, `DELETE` with primary key uniqueness and foreign key constraint checks.  
- **Query Processing**: `SELECT` with `WHERE` (=, !=, <, <=, >, >=), `ORDER BY`, `GROUP BY`, `HAVING`, `DISTINCT`, and `LIMIT`. 
- **Joins**: 2-table support with a heuristic choice between Nested-Loop and Sort-Merge join strategies. 
- **Condition Reordering**: Conjunctive (AND) and disjunctive (OR) predicates are reordered by estimated cost to improve evaluation efficiency. 
- **Indexes**: B-Tree indexes (BTrees.OOBTree) can be built on any column for fast equality and range scans.
- **Constraints**: Automatic primary key index creation; foreign key enforcement with `RESTRICT` and optional `CASCADE` deletion policies. 

## Dependencies  
- **Python**: recommend 3.11.9 or later 
- **sqlglot** for SQL parsing (`pip install sqlglot`) 
- **BTrees** for B-Tree implementation (`pip install BTrees`) 

## Installation  
Clone the repository:  
   ```bash
    git clone https://github.com/your-username/simple-dbms.git
    cd simple-dbms
