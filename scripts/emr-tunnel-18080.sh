#!/bin/bash

masterhost=$1

ssh -i ./emr-spark.pem -N -L "18080:${masterhost}:18080" "hadoop@$masterhost"
