#!/bin/bash

target=$1

rm -r $target/00-Subjects
rm -r $target/01-Aggregate

rm $target/*.txt

echo "All done"
