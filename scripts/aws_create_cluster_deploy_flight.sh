#!/bin/bash


export AWS_DEFAULT_PROFILE=threecuptea

cd ~/workspace/samples/spark2_emr
sbt package

ts=$(date +%s)
id="flights-$ts"
bucket=threecuptea-us-west-2
app=flights
jar=spark2_emr_2.11-1.0.jar

s3_app_path=s3://$bucket/$app
s3_jar_path=s3://$bucket/$app/$jar
s3_folder_path=s3://$bucket/$app/$id

aws s3 cp target/scala-2.11/$jar $s3_app_path/

mkdir $id
touch $id/EMPTY

aws s3 sync ./$id $s3_folder_path

sleep 5
rm -rf $id

aws emr create-cluster --name $id --release-label emr-5.6.0 --applications Name=Spark --log-uri $s3_folder_path/ \
--ec2-attributes KeyName=emr-spark --instance-type m3.xlarge --instance-count 3 --use-default-roles \
--steps Type=Spark,Name="Spark Program",ActionOnFailure=CANCEL_AND_WAIT,\
Args=[--deploy-mode,cluster,--class,org.freemind.spark.flight.MyFlightSample,$s3_jar_path,$s3_folder_path/]




