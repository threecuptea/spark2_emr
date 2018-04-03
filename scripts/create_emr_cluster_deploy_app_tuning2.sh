#!/usr/bin/env bash



export AWS_DEFAULT_PROFILE=threecuptea

declare -A APPMAP
APPMAP['flights']='org.freemind.spark.flight.MyFlightSample'
APPMAP['recommend']='org.freemind.spark.recommend.MyMovieLensALS'
APPMAP['recommendcv']='org.freemind.spark.recommend.MovieLensALSColdStartCvEmr'

app=$1
id="${app}-$(date +%s)"
bucket=threecuptea-us-west-2
jar=spark2_emr_2.11-1.0.jar
local_working=~/Downloads/emr-spark/$id

s3_app_path=s3://$bucket/$app
s3_jar_path=s3://$bucket/$jar
s3_folder_path=s3://$bucket/$app/$id


aws s3 cp target/scala-2.11/$jar s3://$bucket/

mkdir $local_working

aws emr create-cluster --name $id --release-label emr-5.12.0 --applications Name=Spark --log-uri $s3_folder_path/ \
--ec2-attributes KeyName=emr-spark --instance-type m3.xlarge --instance-count 3 --use-default-roles  \
--configurations file://tuning.json --steps Type=Spark,Name="Spark Program",ActionOnFailure=CANCEL_AND_WAIT,\
Args=[--deploy-mode,cluster,--class,${APPMAP[$app]},$s3_jar_path,$s3_folder_path/] | python2.7 scripts/emr_adhoc.py