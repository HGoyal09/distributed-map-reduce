#!/bin/bash

rm -f mr-out-*

# Building coordinator
go build -o mrcoordinator ../main/mrcoordinator.go

# Building wc.so
go build -buildmode=plugin ../mrapps/wc.go

# Building worker
go build -o mrworker ../main/mrworker.go

./mrcoordinator ./sample-test-*.txt &

sleep 1

./mrworker wc.so
./mrworker wc.so
./mrworker wc.so

# cleanup
rm -f mrcoordinator mrworker wc.so mr-out-*
