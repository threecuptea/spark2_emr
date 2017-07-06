### spark2_emr collects spark2 projects deployed in AWS EMR cluster.  A lot of companies deployed Spark applications on AWS EMR to take advantage its integrated environments.  The purpose is to be familiar with AWS EMR and futher migrate my spark_tutorial_2 and spark_python_16 projects to EMR.  Automate and run Spark2 applications in real world.
#### The topics include:

1. My FlightSample: this is inpired by the example https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/.  
   I successfully created spark2-EMR cluster and ran the flight sample spark application and output results to the 
   designated s3 bucket/ folder.  The take away is that
   
   a) I have to add a spark-application step and configure application locations, spark-submit option and arguments 
      etcs. to run spark-application.      
   b) I need to configure an output location (s3 buck/folder).  It needs to be unique for each run to work.  That 
      requires programming to automate it eventually.      
   c) I can inspect the results in the configured output s3 bucket/folder if running successfully.  However, I have to
      be able to access hadoop resource manager web ui (port:8088) and spark history server web ui (port:18080) to debug. 
      This requires the following a couple of steps:      
      i)  Provision instances with a key pair.      
      ii) EMR uses security group ElasticMapReduce-master and ElasticMapReduce-slave to provision master and slave 
          instances. Those are not replacable.  However. we can add additional rule to those security groups. We have to 
          at least add SSH rule to ElasticMapReduce-master so that we can access the master instance.  
          See http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html. For example.
                    
          ssh -i ./emr-spark.pem hadoop@ec2-54-189-237-167.us-west-2.compute.amazonaws.com
    
      iii)Configure SSH tunnel using local port forwarding to port 8088 and 18080 of the master public DNS.  
      See http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html for other web ui.  -L 
      signifies the use of local port forwarding.   
      see http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-ssh-tunnel-local.html for SSH tunnel. 
      For example (To keep in forwarding state. Stay there once execute them and not to Ctrl-C) 
      
          ssh -i ./emr-spark.pem -N -L 8088:ec2-54-189-237-167.us-west-2.compute.amazonaws.com:8088 hadoop@ec2-54-189-237-167.us-west-2.compute.amazonaws.com
          ssh -i ./emr-spark.pem -N -L 18080:ec2-54-189-237-167.us-west-2.compute.amazonaws.com:18080 hadoop@ec2-54-189-237-167.us-west-2.compute.amazonaws.com
   
      iv) In this way, we can access hadoop resource manager and spark history server
         
         http://localhost:8088
         http://localhost:18080
   d) EMR configure hadoop and spark similarly to cloudera.   The applications are in /usr/lib/hadoop, /usr/lib/spark, 
      /usr/lib/hive and configure files are in /etc/hadoop/conf and /etc/spark/conf.  EMR keep all default ports of 
      apache hadoop and apache spark.      
   f) I scp files under /etc/hadoop/conf and /etc/spark/conf from the master instance and store under flight/etc 
      for references. Check spark-default.conf and spark-env.sh to see how EMR does lots of plumbing work.    
      
   Here is the result of my version of FlightSample. 
          
          
    Job Id    Description                       Duration        Stages: Succeeded/Total	        Tasks Succeeded/Total
        6     csv at FlightSample.scala:57            10 s                3/3                         222/222
        5     csv at FlightSample.scala:53            2 s                 3/3                         222/222
        4     csv at FlightSample.scala:48            4 s                 3/3                         222/222
        3     parquet at FlightSample.scala:43        4 s                 3/3                         222/222
        2     json at FlightSample.scala:38           6 s                 3/3                         222/222
        1     csv at FlightSample.scala:33          1.6 min                 3/3                         222/222
        0     parquet at FlightSample.scala:22        13 s                  1/1                         1/1
      
   I made two major improvement 
   1. I cache the common set.  All output must be >= 2000 year and I only concerns of a few fields
   2. I coalesce to less partitions since we fliter out data < 2000 year. 
      
   That's the following line
        
    val partitions = rawDF.rdd.getNumPartitions
    var adjPartition = partitions - partitions / 3 -1  //expect at least 1/3 < 2000
    val flightDS = rawDF.filter($"year" >= 2000).select($"quarter", $"origin", $"dest", $"depdelay", $"cancelled")
                        .coalesce(adjPartition).cache() 
   
   The performance improves a lot.  It only executes file scan once.  That's in job 1. The rest of jobs re-use cache().           