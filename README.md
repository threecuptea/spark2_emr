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
      
3. I finally pushed MovieLensALS and MovieLensALSCv with largest Datset to EMR with manual tuning (
not using maximizeResourceAllocation) and was able to finish MovieLensALS in 15 minutes (shown in AWS console) without any 
failed tasks or failed attempt. The specs and process are as the followings

   a.  I used ml-latest.zip in https://grouplens.org/datasets/movielens/.  That has more than 26 million of rating 
       records and total size is more than 1.09GB
             
   b.  MovieLensALS application is reading both rating and movie data set.  Split rating into training, validation 
       and test data set. Apply ALS process: fit training set, transform validation data set and calculate rmse. 
       I used param grid of 6 (3 reg-params x 2 latent factors) to find the best parameters and model.
       This means 6 full ALS cycles and each cycle running maxIter 20.  I apply the result to test data set to make sure
       the best model is not over-fitting or under-fitting.  Then I refit the best parameters to full rating set.       
       I called the result augmented model.  Then I used augmented model to get recommendation for a test userId=6001.
       There are two approaches and both require join rating and movie partially.  Fonally I stored 
       recommendForAllUsers of 25 movies to file in parquet format.  See the details in MovieLensALS.
       
   c.  The best result (15 min) I got is using 2 m3.2xlarge nodes which has 16vcore and 23040MB disk spaces each.  
       I use the followings
   
   
      --num-executors,6,--executor-cores,5,--executor-memory,6200m, --conf,spark.executor.extraJavaOptions='-XX:ThreadStackSize=2048',--conf,spark.sql.shuffle.partitions=40,--conf,spark.default.parallelism=40
        
   d. Here is how I got here.  I allocate 3 executors to each node and 5 cores per each executor. Therefore, 
      non-driver node use 15 cores in total and driver node use (15 + 1) cores.  The 1 core is for ApplicationMaster to 
      launch the driver. Then I calculate executor memory allocated. I cannot use 23040 / 3 executors / 1.1 
      (0.1 for executor memory overhead) since one of node will be used to AM to launch driver.  I have to subtract
      1.408GB from 23040 first.  I got that number from ResourceManager console. Each one can have 6500MB.  
      I leave a little cushion and chose 6200MB.
      
   e. Now, parallelism is very important. It is suggested each core should take on 2-4 tasks to best utilize its 
      resources. 6 executor x 5 x 2.  I probably can use 60 parallelism.   However, I am familiar with MovieLensALS.  
      The most heavy stage of ALS cycles has 42 tasks.  That's why I chose 40 for both parallelism and shuffle partitions.
      
   f. Prior to this choice,  I tried 3 m3.xlarge which has 8 vcores and 11520MB disk space.  I got 22 minutes from 
      3 executors (1 executor each node) x 5 cores per executor and 23 minutes from 5 executors 
      (2 + 2 + 1, 1 is for the driver node) x 4 cores per executor.  Notice that I used 3 m3.xlarge but 2 m3.2xlarge.  
      No additional cost occure by upgrading to m3.2xlarge. The major improvement is on both most heavy stage of ALS 
      cycle which dropped from 1.6 - 1.9 minute to 55-60 seconds and two joins at final stages due to better computing 
      power. MovieLensALS is CPU dominant application.
      
   g.  I encountered 'OOM' Java Heap space issue in the beginning.  I solved it by using KryoSerializer 
       and persist with StorageLevel.MEMORY_ONLY_SER.  I dropped using Dataset of customized Rating and Movie
       classes and using generic DataFrame instead after knowing DataFrame perform a little better due to no extra 
       encoding and decoding overhead involved.  Also, I have to register special encoder for those customized
       classes for KryoSerializer.  That's unnecessary hassle.  
       
   10. I had failed tasks and failed attempt and even failed job due to failed multiple attempt.  I looked
       into stderr and found stackoverflow is the cause.  Therefore, I added *-XX:ThreadStackSize=2048*.  
       Notice this does not take away space from neither 'yarn.nodemanager.resource.memory-mb' in 
       yarn-site.xml nor executor-memory which is part of the nodemanager memory.
       
   11. I started to pay more attention to resource manager console. Node link page is very helpful.  It tells
       me how much memory and how many cores being allocated for each node.  I found out one alraming fact: 
       yarn only allocates one core for each executor even though I requested 5 cores.  That information does not show 
       in spark-history console.  I googled to find the solution and DominantResourceCalculator comes up.   
       I added the followings to tuning.json which the configuration file I used for AWS deployment       
      
       
        {
           "Classification": "capacity-scheduler",
           "Properties": {
             "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
           }
         }
                                   
   12.  I avoid using AWS 'maximizeResourceAllocation' option. It does not allocate driver node to any 
        executor and waste resources.  However, well distributed load across executors is ideal but not always possible.   
        I found out that tasks of 7 ALS cycles, including the refit to the whole population, load are not well 
        distributed. The majority tasks went to one executor only.   However, loads well-distributed across 
        executors do happen to 2 join operations at final stages.  As Spark document said about 
        'spark.default.parallelism' 
           
           
        Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user.    
    

   13.  For Spark performance tuning and memory optimization, I consulted with the following documents.
        https://spark.apache.org/docs/latest/tuning.html
        http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2, Sandy Ryza.
        http://www.treselle.com/blog/apache-spark-performance-tuning-degree-of-parallelism/, 4 parts series
        https://rea.tech/how-we-optimize-apache-spark-apps/
        
   14.  Spark performance tuning in AWS should be tuned to the nature of application.   MovieLensALS is more computing
        intensive than memory consumption application.  I just scratched the surface and have more improvement to come.
            
   
Other notes
   
   a. CSV data source does not support array<string> data type.  I have to use udf
   
        val stringify = udf(vs: Seq[String] => s"""[${vs.mkString(",")}]""")     
      
   b. How to de-duplicate Dataset records     
           
        ds.distinct()
        df.dropDuplicates(col1: String, cols: String*)
        df.dropDuplicates(cols: Array[String])
        df.dropDuplicates(cols: Seq[String])
           
   c. How to generate unique Id, monotonically_increasing_id() is one of sql functions.  It is incremental but
      not continuous
      
        df.withColumn(""uniqueId", monotonically_increasing_id()) 
        val inputRows = df.rdd.zipWithUniqueId.map{
           case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}esc
        spark.createDataFrame(inputRows, StructType(StructField("id", LongType, false) +: df.schema.fields))           