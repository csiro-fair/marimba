#!/bin/sh
rm -rf $1
mkdir $1
marimba new collection $1 bruvs 
marimba new instrument $1/DR2023-01 bruvs
