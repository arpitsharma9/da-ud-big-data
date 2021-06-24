#!/bin/sh
aws dynamodb update-table --table-name sampledata \
--provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=100

