#!/bin/bash

masterhost=$1

ssh -i [key-pair-pem-path] -N -L "8088:${masterhost}:8088" "hadoop@$masterhost"
