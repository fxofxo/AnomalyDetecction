#/bin/bash
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sleep  3
ssh -i .ssh/clu_rsa w0 "./go.sh "
ssh -i .ssh/clu_rsa w1 "./go.sh "
sleep 2 
ssh -i .ssh/clu_rsa w0 "jps"
