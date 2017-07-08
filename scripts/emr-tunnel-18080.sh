#!/bin/bash

masterhost=$1

ssh -i [key-pair-pem-path] -N -L "18080:${masterhost}:18080" "hadoop@$masterhost"
