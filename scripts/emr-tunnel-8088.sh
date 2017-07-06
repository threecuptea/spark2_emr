#!/bin/bash

masterhost=$1

ssh -i ./emr-spark.pem -N -L "8088:${masterhost}:8088" "hadoop@$masterhost"
