#!/bin/bash


export AWS_DEFAULT_PROFILE=threecuptea

cd ~/workspace/samples/spark2_emr
sbt package

aws s3 cp target/scala-2.11/spark2_emr_2.11-1.0.jar s3://threecuptea-us-west-2/my-flights/

ts=1234
id="flights-$ts"
bucket=threecuptea-us-west-2
jar=spark2_emr_2.11-1.0.jar

aws emr create-cluster --name "auto-1234" --release-label emr-5.6.0 --applications Name=Spark --log-uri s3://threecuptea-us-west-2/flights/auto-1234/ \
--ec2-attributes KeyName=emr-spark --instance-type m3.xlarge --instance-count 3 --use-default-roles \
--steps Type=Spark,Name="Spark Program",ActionOnFailure=CANCEL_AND_WAIT,\
Args=[--deploy-mode,cluster,--class,org.freemind.spark.flight.FlightSample,s3://threecuptea-us-west-2/flights/spark2_emr_2.11-1.0.jar,s3://threecuptea-us-west-2/flights/auto-1234/]




