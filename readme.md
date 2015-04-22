A Python 2/3 wrapper library to access Hadoop WebHDFS REST API

### Requires

Require requests module: http://docs.python-requests.org/

### Installation

    $ pip install webhdfspy

### Documentation

### Hadoop configuration

To enable WebHDFS in Hadoop add this to your $HADOOP_DIR/conf/hdfs-site.xml:

        <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
        </property>  

To enable append on HDFS you need to configure your hdfs-site.xml as follows:

        <property>
            <name>dfs.support.append</name>
            <value>true</value>
        </property>


More about WebHDFS: http://hadoop.apache.org/common/docs/r1.0.0/webhdfs.html
