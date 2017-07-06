#!/bin/bash

masterhost=$1

ssh -i ./emr-spark.pem "hadoop@$masterhost" -y
