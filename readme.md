NOTE: THIS IS A WORK IN PROGRESS AND IT'S NOT SUITABLE FOR PRODUCTION YET
---


pyWebHDFS a wrapper library to access Hadoop WebHDFS REST API

Require requests module: http://docs.python-requests.org/

To enable WebHDFS in Hadoop add this to your $HADOOP_DIR/conf/hdfs-site.xml:

        <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
        </property>  

WebHDFS documentation: http://hadoop.apache.org/common/docs/r1.0.0/webhdfs.html