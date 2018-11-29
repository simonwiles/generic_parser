#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd $DIR

rm -f output/*

../../generic_parser.py -c config.xml -p People -r Person -i Emp_Id -t template.sql -l file_numbers.csv -d data -o output
