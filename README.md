H-Dexter
========

H-Dexter is a small plugin to run [Dexter](http://dexter.isti.cnr.it) annotations in Hadoop MapReduce environment.

1. Configuration: To be able to use this feature, first you have to store the configuration file dexter-conf.xml and the model files of Dexter in your shared locations (e.g. HDFS or HBase file). In dexter-conf.xml, please specify the full path of the model with the protocol (file:/// or http:// or hdfs://). For example, 

where the INPATH is the path in shared locations of your data (e.g. HDFS), OUTPATH is where you want to store the annotation results (CSV format: docid TAB [list of <entity,score> pairs]), and "dexter-conf" is the location of the configuration file dexter-conf.xml:

```
<model>
 <name>en</name>
 <path>hdfs://[YOUR_HADOOP_CLUSTER_HOST]/[PATH-TO-English-model-directory]</path>
</model>
```

2. Adding parser: For each dataset, it is required the parser, where the text is extracted and pipelined to the map / reduce phase. In H-Dexter, this is done by implementing the interface <code>it.cnr.isti.hpc.dexter.hadoop.AnnotateMapper</code>. 



3. Run: Run the utility <code>it.cnr.isti.hpc.dexter.hadoop.HadoopAnnotation</code>. The program will ask for the following information:

 * The input and output data structures for key/value emitted by the mappers and reducers
 * Format of input and output files stored in a Hadoop cluster (e.g. TextInputFormat, etc.)
 * Class signature of the parser

If you don't know what exact arguments to specify, simply pass on nothing and the program will print out the helper message to instruct. One example usage is using H-Dexter to annotate the huge corpus StreamCorpus (https://github.com/antoine-tran/SCTools)