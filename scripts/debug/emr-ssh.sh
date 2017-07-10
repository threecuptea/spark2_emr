#!/bin/bash

masterhost=$1

ssh -i [key-pair-pem-path] "hadoop@$masterhost" -y
