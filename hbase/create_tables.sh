#!/bin/bash

cat create_tables.hbase | hbase shell -n
