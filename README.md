Dexter-Hadoop
========

Dexter-hadoop is a small plugin to run [Dexter](http://dexter.isti.cnr.it) annotations in Hadoop MapReduce environment.

1. Configuration: To be able to use this feature, first you have to store the configuration file dexter-conf.xml and the model files of Dexter in your shared locations (e.g. HDFS or HBase file). In dexter-conf.xml, specify the full path of the model with the protocol (file:/// or http:// or hdfs://). For example, 


```
<model>
 <name>en</name>
 <path>hdfs://[YOUR_HADOOP_CLUSTER_HOST]/[PATH-TO-English-model-directory]</path>
</model>
```

where the INPATH is the path in shared locations of your data (e.g. HDFS), OUTPATH is where you want to store the annotation results (CSV format: docid TAB [list of <entity,score> pairs]), and "dexter-conf" is the location of the configuration file dexter-conf.xml. If there is no protocol specified, it will assume the path to be in local file system.


2. Adding parser: For each dataset, it is required the parser, where the text is extracted and pipelined to the map / reduce phase. In Dexter-hadoop, this is done by implementing the interface <code>it.cnr.isti.hpc.dexter.hadoop.AnnotateMapper</code>. 



3. Run: Run the utility <code>it.cnr.isti.hpc.dexter.hadoop.HadoopAnnotation</code>. The program will ask for the following information:

 * The input and output data structures for key/value emitted by the mappers and reducers
 * Format of input and output files stored in a Hadoop cluster (e.g. TextInputFormat, etc.)
 * Class signature of the parser

If you don't know what exact arguments to specify, simply pass on nothing and the program will print out the helper message to instruct. For the convenience, we provide an example application, where the whole Wikipedia dump has been annotated (after stripping down all markups). You can see this example in the "example" source directory.