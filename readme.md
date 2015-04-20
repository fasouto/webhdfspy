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

WebHDFS documentation: http://hadoop.apache.org/common/docs/r1.0.0/webhdfs.html
