#/bin/sh
gcloud dataproc clusters create spark-clu\
    --project tfm-ts \
    --bucket fxo-bk  \
    --master-machine-type n1-standard-4 \
    --num-workers   2        \
    --worker-machine-type n1-standard-2 \
    --region europe-west1  --initialization-actions gs://dataproc-initialization-actions/jupyter/jupyter.sh

#####On Cluster master
# sudo /opt/conda/bin/pip install https://dist.apache.org/repos/dist/dev/incubator/toree/0.2.0/snapshots/dev1/toree-pip/toree-0.2.0.dev1.tar.gz
# sudo /opt/conda/bin/jupyter toree install --sys-prefix --spark_home=//usr/lib/spark

# The release toree-0.2.0.dev1.tar.gz has a incompatibility error with class asm-3.2.jar, i
# "Exception in thread "main" java.lang.IncompatibleClassChangeError: class org.clapper.classutil.asm.ASMEmptyVisitor has interface org.objectweb.asm.ClassVisitor as super class

# the best solution found was remove asm jar where conflict. (manualy checked)
 #/usr/lib/ hadoop-mapreduce/asm-3.2.jar.ORG
 #/usr/lib/hadoop-mapreduce/lib/asm-3.2.jar.ORG
 #/usr/lib/hadoop-hdfs/lib/asm-3.2.jar.ORG
#/usr/lib/hadoop/lib/asm-3.2.jar.ORG
#/usr/lib/hadoop-yarn/lib/asm-3.2.jar.ORG

## RECOVEREd
gcloud dataproc clusters create spark-test \
        --project tfm-ecg \
       --subnet default \
       --bucket fxo-ecg-buck  \
       --region europe-west1 --zone europe-west1-b \
        --master-machine-type custom-2-6144 \
        --master-boot-disk-size 50 \
        --num-workers 2 \
        --worker-machine-type n1-standard-2 \
        --worker-boot-disk-size 20 \
        --initialization-actions gs://dataproc-initialization-actions/jupyter/jupyter.sh

####################################On Cluster master
---------------------Make jupyter notebook available through ssh
on
/usr/local/bin/launch_jupyter.sh
 change original,
# /opt/conda/bin/jupyter notebook --allow-root --no-browser
/opt/conda/bin/jupyter notebook --allow-root  --notebook-dir=/home/fsainz --port=8888
to
----------------------- Install apache Toree for jupiter scala notebooks

sudo /opt/conda/bin/pip install https://dist.apache.org/repos/dist/dev/incubator/toree/0.2.0-incubating-rc2/toree-pip/toree-0.2.0.tar.gz
sudo /opt/conda/bin/jupyter toree install --sys-prefix --spark_home=//usr/lib/spark

---------------------Install matplot lib

sudo /opt/conda/bin/conda install matplotlib

---------------------- install ssh keys
use facilities provided on web interface GCP/Compute engine/ metadata/ssh keys
Add fsainz@hp key y cluster-kwy

-------------------- Kafka
# install zookeeper on master
#### install kafka on master and workers
### config kafka according config files.

