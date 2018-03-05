import json
import os
import os.path
import re
import subprocess
import sys
from time import sleep


CLUSTER_FINAL_STATES = ['COMPLETED', 'FAILED', 'TERMINATED', 'TERMINATED_WITH_ERRORS']
if __name__ == '__main__':

    cluster_id = json.load(sys.stdin)['ClusterId']
    print 'ClusterId: %s' % cluster_id
    describe_cluster = 'aws emr describe-cluster --cluster-id %s' % cluster_id
    describe_cluster_arr = describe_cluster.split()
    proc_describe_cluster = subprocess.Popen(describe_cluster_arr, stdout=subprocess.PIPE)
    stdindata, _ = proc_describe_cluster.communicate()
    describe_cluster_json = json.loads(stdindata)
    state = describe_cluster_json['Cluster']['Status']['State']
    prev_state = state
    print 'Initial state: %s' % state
    while True:
        if state in CLUSTER_FINAL_STATES:
            break
        sleep(1)
        proc_describe_cluster = subprocess.Popen(describe_cluster_arr, stdout=subprocess.PIPE)
        stdindata, _ = proc_describe_cluster.communicate()
        describe_cluster_json = json.loads(stdindata)
        state = describe_cluster_json['Cluster']['Status']['State']
        if state != prev_state:
            print 'Current state: %s' % state
            prev_state = state
    if state == 'FAILED' or state == 'TERMINATED_WITH_ERRORS':
        master_pub_dns = describe_cluster_json['Cluster']['MasterPublicDnsName']
        if master_pub_dns is not None:
            print 'Cluster-id: %s - Master DNS: %s FAILED in EMR create/ deploy process, please DEBUG' % (
            cluster_id, master_pub_dns)
        else:
            print 'Cluster-id: %s FAILED in EMR create/ deploy process, please DEBUG' % cluster_id
        sys.exit(-1)
    else:
        log_uri = describe_cluster_json['Cluster']['LogUri']
        # It can have s3n addresss instead of s3://, need to modify then re-join
        len_1 = len(log_uri) - 1
        temp = log_uri[:len_1]
        arr = temp.split('/')
        arr[0] = 's3:'
        s3_working = '/'.join(arr)
        folder = arr[len(arr) - 1]
        local_working = '%s/%s' % ('/home/fandev/Downloads/emr-spark', folder)
        cmd = 'aws s3 sync %s %s' % (s3_working, local_working)
        print 'aws_s3_sync: %s' % cmd
        proc_sync = subprocess.check_call(cmd.split())

