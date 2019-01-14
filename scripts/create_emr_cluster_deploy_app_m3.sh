#!/usr/bin/env bash



export AWS_DEFAULT_PROFILE=threecuptea

declare -A APPMAP
APPMAP['flights']='org.freemind.spark.flight.MyFlightSample'
APPMAP['ALS']='org.freemind.spark.sql.MovieLensALS'
APPMAP['ALSCv']='org.freemind.spark.sql.MovieLensALS'

app=$1
id="${app}-$(date +%s)"
bucket=threecuptea-us-west-2
jar=$2
local_working=~/Downloads/emr-spark/$id

s3_app_path=s3://$bucket/$app
s3_jar_path=s3://$bucket/$jar
s3_folder_path=s3://$bucket/$app/$id


aws s3 cp target/scala-2.11/$jar s3://$bucket/

mkdir $local_working

aws emr create-cluster --name $id --release-label emr-5.13.0 --applications Name=Spark --log-uri $s3_folder_path/ \
--ec2-attributes KeyName=emr-spark --use-default-roles \
--instance-groups InstanceGroupType=MASTER,InstanceType=m3.xlarge,InstanceCount=1 \
InstanceGroupType=CORE,InstanceType=m3.2xlarge,InstanceCount=2 \
--configurations file://tuning.json --steps Type=Spark,Name="Spark Program",ActionOnFailure=CANCEL_AND_WAIT,\
Args=[--deploy-mode,cluster,--name,als-m32xlarge-2-3-5-60,--num-executors,6,--executor-cores,5,--executor-memory,6200m,--conf,spark.executor.extraJavaOptions='-XX:ThreadStackSize=2048',--conf,spark.sql.shuffle.partitions=40,--conf,spark.default.parallelism=40,--class,${APPMAP[$app]},$s3_jar_path,$app,$s3_folder_path] | python2.7 scripts/emr_adhoc.py
