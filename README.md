### It collects spark2 projects deployed to AWS EMR . Automate the whole process and successfully finished ALS recommendation jobs on 26 million Movielens data in 15 minutes using limited AWS resoures
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
          instances. Those are irreplaceable.  However. we can add additional rule to those security groups. We have to 
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
    Furthermore, I enhance it to be generic 
   (scripts/create_emr_cluster_deploy_app_tuning2.sh) so that I can re-use it in recommend and other projects.  It is 
   followed by emr_adhoc.py to retrieve the cluster state and download the result output when steps are completed.  
    
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
      
3. I finally complete MovieLensALSEmr and MovieLensALSCvEmr jobs on 26 million data set in AWS EMR cluster with manual tuning 
instead of using maximizeResourceAllocation and was able to finish MovieLensALS in 15 minutes (shown in AWS console) 
without any failed tasks or failed attempt. The specs and process are as the followings

   a.  I used ml-latest.zip in https://grouplens.org/datasets/movielens/.  That has more than 26 million of rating 
       records and total size is more than 1.09GB
             
   b.  MovieLensALS application is reading both rating and movie data set.  Split rating into training, validation 
       and test data set. Apply ALS process: fit training set, transform validation data set and calculate RMSE. 
       I used param grid of 6 (3 reg-params x 2 latent factors) to find the best parameters and model.
       This means 6 full ALS cycles and each cycle running maxIter 20.  I apply the result to test data set to make sure
       the best model is not over-fitting or under-fitting.  Then I refit the best parameters to full rating set.       
       I called the result augmented model.  Then I used augmented model to get recommendation for a test userId=6001.
       There are two approaches and both require join with movie partially.  Finally I stored 
       recommendForAllUsers of 25 movies to file in parquet format.  See the details in MovieLensALSEmr.
       
   c.  The best result is 15 min that I got by using 2 m3.2xlarge nodes which has 16vcore and 23040MB memory 
       (I use 'yarn.nodemanager.resource.cpu-vcores' and 'yarn.nodemanager.resource.memory-mb' numbers in yarn-site.xml) each. 
       I use EMR 5.13.0 that comes with Spark 2.3.0.  For some reason, its parallism and the performance is far better 
       than 5.17.0 that comes with Spark 2.3.1. I did try c3.4xlarge which has the same specs.
       of vcore as well as memory but better computing power with cost of 1.5 times. That finish in 11 min 
   
   
          --num-executors,6,--executor-cores,5,--executor-memory,6200m, --conf,spark.executor.extraJavaOptions='-XX:ThreadStackSize=2048',--conf,spark.sql.shuffle.partitions=40,--conf,spark.default.parallelism=40
       
        
   d. Here is how I got here.  I allocate 3 executors per node and 5 cores per executor. Therefore, 
      non-driver node use 15 cores in total and driver node use (15 + 1) cores.  The 1 core is for ApplicationMaster to 
      launch the driver. Then I calculate executor memory allocated. I cannot use 23040 / 3 executors / 1.1 
      (0.1 for executor memory overhead) since one of node will be used for AM to launch the driver.  I have to subtract
      1.408GB from 23.04GB first.  I got 1.408GB from ResourceManager console. Each one can have 6500MB.  
      I leave a little cushion and chose 6200MB.    
      
   e. Now, parallelism is very important. It is suggested each core should take on 2-4 tasks to best utilize its 
      resources. 6 executor x 5 x 2.  I probably can use 60 parallelism.   However, I am familiar with MovieLensALS.  
      The most heavy stage of ALS cycles has 42 stages.  That's why I chose 40 for both parallelism and shuffle partitions.
      I did try 60 parallism and process in 16 minute
      
   f. Prior to this choice,  I tried 3 m3.xlarge which has 8 vcores and 11520MB based upon yarn-site,xml. I got 22 minutes from 
      3 executors (1 executor each node) x 5 cores per executor and 23 minutes from 5 executors 
      (2 + 2 + 1, 1 is for the driver node) x 4 cores per executor.  Notice that I used 3 m3.xlarge but 2 m3.2xlarge.  
      No additional cost occure by upgrading to m3.2xlarge. The major improvements are on most heavy stage of ALS 
      cycle which dropped from 1.6 - 1.9 minute to 55-60 seconds and two joins at final stages due to better computing 
      power. MovieLensALS is CPU dominant application.  c3.4xlarge also has vcore 16 and 23040gb and better computing power.
      It can finish in 11 minute.  It reduces  most heavy stage of ALS cycle to 38-43 sec. 
      However, it is 1.5 times costly; m3.2xlarge: 0.140/hr vs. c3.4xlarge:$0.210/hr.  I got the following information 
      from ResourceManager console when I use m3.2xlarge which make very good use of memory and cores.
 
                        Memory used     Memory avail.  Core used     Core avail.
            driver        21.44GB        1.06GB           16 cores      0 core
            non-driver    20.06GB        2.44GB           15 cores      1 core
      
     
   g. I encountered 'OOM' Java Heap space issue in the beginning.  I solved it by using KryoSerializer 
      and persist with StorageLevel.MEMORY_ONLY_SER.  I dropped using Dataset of customized Rating and Movie
      classes and using generic DataFrame and customized schema instead after knowing DataFrame perform a little better 
      due to no extra encoding and decoding overhead involved.  Also, I have to register special encoder for 
      those customized classes for KryoSerializer.  That's unnecessary hassle.  
       
   h. I had failed tasks and failed attempts and even failed job due to multiple failed attempts.  I looked
      into stderr and found stackoverflow is the cause.  Therefore, I added *-XX:ThreadStackSize=2048*.  
      Notice this does not take away space from neither 'yarn.nodemanager.resource.memory-mb' in 
      yarn-site.xml nor executor-memory which is part of the nodemanager memory. The latter uses heap instead of stack.
       
   i. I started to pay more attention to resource manager console. Node link page is very helpful.  It tells
      me how much memory and how many cores being allocated for each node.  I found out one alarming fact: 
      yarn only allocates one core per executor even though I requested 5 cores.  That information does not show 
      in spark-history console.  I googled to find the solution and DominantResourceCalculator comes up.   
      I added the followings to tuning.json which the configuration file I used for AWS deployment       
      
       
        {
           "Classification": "capacity-scheduler",
           "Properties": {
             "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
           }
         }
                                   
  j.  I avoid using AWS 'maximizeResourceAllocation' option. It does not allocate any executor to the driver node 
      and waste resources.  However, well distributed load across executors is ideal but not always possible.
      I found out that tasks of 7 ALS cycles, including the refit to the whole population, load are not well 
      distributed. However, I can get 3 executor load well-distributed in the dominant node to some extent.  . 
      Loads are well-distributed across executors do happen to 2 join operations at final stages. 
      As Spark document said about 'spark.default.parallelism' 
           
           
        Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user.    
    

  k.  For Spark performance tuning and memory optimization, I consulted with the following documents.
      https://spark.apache.org/docs/latest/tuning.html
      http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2, Sandy Ryza.
      http://www.treselle.com/blog/apache-spark-performance-tuning-degree-of-parallelism/, 4 parts series
      https://rea.tech/how-we-optimize-apache-spark-apps/
        
  l.  Spark performance tuning in AWS should be tuned to the nature of application.   MovieLensALS is more computing
      intensive than memory consumption application.  I just scratched the surface and have more improvement to come.
        
  m.  For Capacity Scheduler,              
        i). Refer to https://community.pivotal.io/s/article/How-to-configure-queues-using-YARN-capacity-scheduler-xml 
           regarding queue hierarchy, queue resource allocation (percentage), permission of submitting application 
           to queue etc.  Two take aways, always set 'hadoop,yarn,mapred,hdfs' to 
           'yarn.scheduler.capacity.root.acl_submit_applications' so those default users can submit jobs; 
           use -Dspark.yarn.queue=<queue-name> to submit Spark job to a specific queue
        ii). Refer to https://hortonworks.com/blog/yarn-capacity-scheduler for minimum user percentage and user limit 
           factor.  Use them to prevent one user from overtake the whole queue. Enabling preemption allows an 
           application to get back their minimum capacity which is being used in another queue as elastic. However, 
           Preemption only work across queues vs. minimum user percentage and user limit factor work within the queue.
        iii). Refer to 
           https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.2/bk_yarn-resource-management/content/ch_capacity_scheduler.html
           for a comprehensive guide.
           a) Elastically allocate resources among queue but limited by maximum.capacity of a queue.         
           b) FIFO or Fair scheduling ordering policy: Fair ordering policy either allocate equal share for all 
              submitted jobs of the queue or use enable-size-based-weight to allocate resource to individual 
              applications based on their size.
           c) DominantResourceCalculator uses Dominant Resource Fairness (DRF) model of resource allocation. For example, 
              if user A runs CPU-heavy tasks and user B runs memory-heavy tasks, the DRF attempts to equalize CPU share
              of user A with the memory share of user B. In this case, the DRF allocates more CPU and less memory 
              to the tasks run by user A, and allocates less CPU and more memory to the tasks run by user B.  It has
              advantages on mixed work load: ex. CPU-constrained Storm on YARN job and memory-constrained MapReduce job.     
            
4. I always think that MovieLensALS should be generic.  I should be able to verify sample data locally before I 
   promote it to EMR.  It's very costly to prematurely promote codes to EMR and encounter Exception.  It will be good
   to consolidate MovieLensALS and MovieLensALSCv both too since a lot of codes are duplicate. I refactor codes to
   make MovieLensALS and MovieLenCommon supporting running it in AWS-EMR as well as locally and support ALS 
   standalone estimator as well as Cross Validator using ALS on 1018-12-31 (A New Year Resolution!!!).
   
   The new command line usage is as the followings:
   
          MovieLensALS [ALS or ALSCv] [s3-output-path] or 
          MovieLensALS [ALS or ALSCv] [local-rating-path] [ocal-movie-path] [local-output-path] 
        
   However, support running sample/ partial data locally does throw a curve ball
   
   a. Original test userId(=6001) is no longer in sample data set.  I have to find a new one(=60015)
   
   b. I have to fix the seed to generate sample data, otherwise I cannot expect designated test userId will be there.
   
   c. 5% sample data set is much smaller than original data set.  The RMSE of the bestModel from ALS on sample data set 
      is only 1%- 3% over the RMSE of the baseline. The RMSE of the bestModel from ALS on original data set (26 million)
      is 23.81% over baseline RMSE.
      
   d. The paramMap obtained from the best model on sample data is (rank= 10, regParam: 0.1) and the paramMap obtained 
      from the best model on the original 26 million data set is (rank= 12, regParam: 0.1)
   
   f. The above suggest that I might not be able to generalize result obtained from sample data set to general 
      population.  That might be misleading.  However, it does help me to detect unexpected exception.
      
   e. Local environment has limited resources: memory and CPU.  That might require additional tuning.  I use the 
      followings to run it locally
      
        $SPARK_HOME/bin/spark-submit --master local[*] --conf spark.sql.shuffle.partitions=10 --conf spark.default.parallelism=10 \
          * --num-executors 1 --executor-cores 5 --executor-memory 2000m --conf spark.executor.extraJavaOptions=-'XX:ThreadStackSize=2048' \
          * --class org.freemind.spark.recommend.MovieLensALS target/scala-2.11/spark2_emr_2.11-1.0.jar \
          * ALS ~/Downloads/ml-latest/ratings-sample.csv.gz ~/Downloads/ml-latest/movies.csv recommend-$(date +%s)
      
      which is very different from what I configured AWS-EMR cluster.
      
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
           case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}}
        spark.createDataFrame(inputRows, StructType(StructField("id", LongType, false) +: df.schema.fields))  
        
   d. To view parguet file
        
        cd ~/Downloads/emr-spark
        hadoop jar ./parquet-tools-1.8.3.jar --help (head display text format vertically)
        hadoop jar ./parquet-tools-1.8.3.jar cat --json \ 
        file:///home/fandev/Downloads/emr-spark/recommend-1539734851/recommendAll/part-00000-c000.snappy.parquet 
              
        
   e. AWS Glue Data Catalog can be used as external Hive metastore for Apache Spark, Apache Hive, and 
      Presto (distriibuted SQL engineer) workloads on Amazon EMR.
      
   f. Ganglia, which provides a view of cluster resources usage, including memory and CPU. Symptoms of 
      an unhealthy state can be: low percentage of CPU usage, a large number of idle CPUs, or memory spikes.   
      
   f. AWS EMR Zeppelin is similar to Jupyter Notebook.
   
   g. AWS instances:        
      General purpose: m3.xlarge (8 cores), m3.2xlarge (16 cores)
      Compute optimized: c3.xlarge, c3.2xlarge(15gb, 8vcore), c3.4xlarge with the same spec. asm3.2xlarge but CPU with
      more computing power, c3.8xlarge
      Memory optimized: r3.xlarge, r3.2xlarge, r3.4xlarge, r3.8xlarge
      
       