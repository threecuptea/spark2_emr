### spark2_emr collects spark2 projects deployed in AWS EMR cluster.  A lot of companies deployed Spark applications on AWS EMR to take advantage its integrated environments.  The purpose is to be familiar with AWS EMR and futher migrate my spark_tutorial_2 and spark_python_16 projects to EMR.  Automate and run Spark2 applications in real world.
#### The topics include:

1. Flight Sample: this is inpired by the example https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/.  
   I successfully created spark2-EMR cluster and ran flight sample spark application and output results to the 
   designated s3 bucket/ folder.  The take away is that
   
   a) I have to add a spark-application step and configure application locations, spark-submit option and arguments 
      etcs. to run spark-application.      
   b) I need to configure an output location (s3 buck/folder).  It needs to be unique for each run to work.  That 
      requires programming to automate it.      
   c) I can inspect the results in the configured output s3 bucket/folder if running successfully.  However, I have to
      be able to access hadoop resource manager web ui (port 8088) and spark history server (port 18080) to debug. 
      The requires the following a couple of steps:      
      i)  Provision instances with a key pair.      
      ii) EMR use security group ElasticMapReduce-master and ElasticMapReduce-slave to provision master and slave 
          instances. That's not replacable.  However. we can add additional rule to those security group. We have to 
          at least add SSH rule to ElasticMapReduce-master so that we can access the master instance.  For example.
                    
          ssh -i ./emr-spark.pem hadoop@ec2-54-189-237-167.us-west-2.compute.amazonaws.com
    
      iii) Configure SSH local tunneling to port 8088 and 18080 of the master public DNS.  For example
         
         ssh -i ./emr-spark.pem -N -L 8088:ec2-54-189-237-167.us-west-2.compute.amazonaws.com:8088 hadoop@ec2-54-189-237-167.us-west-2.compute.amazonaws.com
         ssh -i ./emr-spark.pem -N -L 18080:ec2-54-189-237-167.us-west-2.compute.amazonaws.com:18080 hadoop@ec2-54-189-237-167.us-west-2.compute.amazonaws.com
         
      iv) In this way, we can access hadoop resource manager and spark history server
         
         http://localhost:8088
         http://localhost:18080
   d) EMR configure hadoop and spark similarly to cloudera.   The applications are in /usr/lib/hadoop, /usr/lib/spark, 
      /usr/lib/hive and configure files are in /etc/hadoop/conf and /etc/spark/conf.  EMR keep all default ports of 
      apache hadoop and apache spark.      
   f) I scp /etc/hadoop/conf and /etc/spark/conf from the master instance and store under flight/etc for references.
      Check spark-default.conf and spark-env.sh to see how EMR does lots of plumbing work.    