-- Databricks notebook source
declare or replace where_clause string;
declare or replace qry_str string;

-- Retrieve where clause from partitions table
set var where_clause = (
    select where_clause
    from identifier(:tgt_catalog || '.' || :tgt_schema || '.' || :tgt_table || '_partitions')
    where id = :task_id
);

select where_clause;

-- COMMAND ----------

-- Build query string using parameters and where_clause variable
set var qry_str = 
    "insert into identifier(:tgt_catalog || '.' || :tgt_schema || '.' || :tgt_table)"
    "select * from identifier(:src_catalog || '.' || :src_schema || '.' || :src_table)"
    "where " || where_clause;

execute immediate qry_str
using (
    :tgt_catalog AS tgt_catalog,
    :tgt_schema AS tgt_schema,
    :tgt_table AS tgt_table,
    :src_catalog AS src_catalog,
    :src_schema AS src_schema,
    :src_table AS src_table
);