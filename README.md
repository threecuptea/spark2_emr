### spark2_emr collects spark2 projects deployed in AWS EMR cluster.  A lot of companies deployed Spark applications on AWS EMR to take advantage its integrated environments.  The purpose is to be familiar with AWS EMR and futher migrate my spark_tutorial_2 and spark_python_16 projects to EMR.  Automate and run Spark2 applications in real world.
#### The topics include:

1. MyFlightSample Spark application: this is inpired by the example https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/.  
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
   f) I scp files under /etc/hadoop/conf and /etc/spark/conf from the master instance and store under configure
      for references. Check spark-default.conf and spark-env.sh to see how EMR does lots of plumbing work.    
      
   Check flight/job_metrics file for event-log job performance metrics. 
         
   I made two major improvement compared with the original FlightSample
  
   1. I cache the common set.  All output must be >= 2000 year and I only concerns of a few fields
   2. I coalesce to less partitions since we fliter out data < 2000 year. 
      
   That's the following line
        
        
    val partitions = rawDF.rdd.getNumPartitions
    var adjPartition = partitions - partitions / 3 - 1  //expect at least 1/3 < 2000
    val flightDS = rawDF.filter($"year" >= 2000).select($"quarter", $"origin", $"dest", $"depdelay", $"cancelled")
                        .coalesce(adjPartition).cache() 
   
   The performance improves a lot.  It only executes file scan once.  That's in job 1. The rest of jobs re-use cache(). 
   
2. Automate the creation of EMR Spark cluster and the deployment of FlightSample with aws-cli (A big step).   
   The sample script is in scripts/aws_create_cluster_deploy_flight.sh).  Futthermore, I enhance it to generic 
   (scripts/create_emr_cluster_deploy_app_tuning2.sh) so that I can re-use it in recommend and other projects.  It is followed
   by emr_adhoc.py to retrive the cluster state and download the result output when steps are completed.  This is a 
   simple one for developer.  DevOp use other professional tool like Terraform to accomplish it.    

   There are a couple of key points:
   
   a. I must specify --deploy-mode cluster since my jar file is in S3.  In contrast, my application files must be in 
      a local path on the EMR cluster if I want to use the default deploy-mode: client.   
   b. To help debugging, I have to specify the logging location with --log-uri   
   c. I have to dynamically create working folder in s3 for each run and I use timestamp $(date+%s) to ensure
      its uniqueness.       
   d. I add emr_adhoc.py to retrieve ClusterId from the result json of create-cluster, then use it to query 
      describe-cluster like "aws emr describe-cluster --cluster-id j-3AFOPWLHEWP9H" to get the cluster state in 
      a loop until the cluster reach one of final states.  If the cluster terminates normally, I would syn local 
      working folder with s3 one to get all log and final outputs otherwise I would retrieve the master public dns for 
      the debug reason.  I use python subprocess and json modules to easily achieve this.
      
3. MyMovieLensAls: I migrate MovieLensALS recommendation system of spark_tutorial_2 to EMR

   a. MovieLensALS recommendation system is more a real-world example.   
   b. Make sure create_emr_cluster_deploy_app2.sh can be used in multiple applications deployment and adhoc analysis.   
      I have to adjust steps of create_emr_cluster_deploy_app2.sh when I deploy pyspark applications to EMR.
   c. The result of join is a DataFrame to convert back to type by using as[T] like and also get (A - B) Dataset, we 
      can use except directly rather than drop to rdd then subtract  
   
        val pRatedDS = prDS.join(movieDS, prDS("movieId") === movieDS("id"), "inner").select($"id", $"title", $"genres").as[Movie]
        val pUnratedDS = movieDS.except(pRatedDS).withColumnRenamed("id", "movieId").withColumn("userId", lit(pUserId)) //matches with ALS required fields
   
   d. CSV data source does not support array<string> data type.  I have to use udf
   
        val stringify = udf(vs: Seq[String] => s"""[${vs.mkString(",")}]""")     
      
   f. How to de-duplicate Dataset records     
           
        ds.distinct()
        df.dropDuplicates(col1: String, cols: String*)
        df.dropDuplicates(cols: Array[String])
        df.dropDuplicates(cols: Seq[String])
           
   e. How to generate unique Id, monotonically_increasing_id() is one of sql functions.  It is incremental but
      not continuous
      
        df.withColumn(""uniqueId", monotonically_increasing_id()) 
        val inputRows = df.rdd.zipWithUniqueId.map{
           case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}esc
        spark.createDataFrame(inputRows, StructType(StructField("id", LongType, false) +: df.schema.fields))           