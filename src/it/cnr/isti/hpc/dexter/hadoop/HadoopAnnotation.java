package it.cnr.isti.hpc.dexter.hadoop;


import java.io.IOException;

import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tuan.hadoop.conf.JobConfig;

public class HadoopAnnotation extends JobConfig implements Tool {
	
	private static final Logger LOG = LoggerFactory.getLogger(HadoopAnnotation.class);
	
	private static final String DISAMB_OPT = "disamb";
	private static final String DEXTER_CONF_OPT = "dexter";
	private static final String MAPPER_OPT = "mapper";
	private static final String INPUTFORMAT_OPT = "informat";
	private static final String OUTPUTFORMAT_OPT = "outformat";
	private static final String KEYIN_OPT = "inkey";
	private static final String VALUEIN_OPT = "inval";
	private static final String KEYOUT_OPT = "outkey";	
	private static final String VALUEOUT_OPT = "outval";
	
	// private static final IdHelper helper = IdHelperFactory.getStdIdHelper();
	private Options opts;
		
	// Add extra option about:
	// 1. the dexter configuration file path (in local or hdfs),
	// 2. the disambiguator used
	// 3. class of mapper
	// 4. class of input format
	// 5. class of output format
	// 6. class of input key
	// 7. class of input value
	// 8. class of output key
	// 9. class of output value
	@SuppressWarnings("static-access")
	@Override
	public Options options() {
		opts = super.options();
		
		Option nedOpt = OptionBuilder.withArgName("disambiguator").hasArg(true)
				.withDescription("method of disambiguation (Tagme / Wikiminer)")
				.create(DISAMB_OPT);
		opts.addOption(nedOpt);
		
		Option dexterOpt = OptionBuilder.withArgName("dexterconfiguration").hasArg(true)
				.withDescription("path to dexter configuration file")
				.create(DEXTER_CONF_OPT);
		opts.addOption(dexterOpt);
		return opts;
	}

	@SuppressWarnings("unchecked")
	public Job setup(String[] args) throws IOException, ClassNotFoundException {
		parseOtions(args);
		
		String inputFormatClassName = null;
		if (!command.hasOption(INPUTFORMAT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		Class inputFormatClass = Class.forName(inputFormatClassName);
		
		String outputFormatClassname = null;
		if (!command.hasOption(OUTPUTFORMAT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		Class outputFormatClass = Class.forName(outputFormatClassname);
		
		String mapKeyOutClassName = null;
		if (!command.hasOption(KEYIN_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		Class mapKeyOutClass = Class.forName(mapKeyOutClassName);
		
		String mapValOutClassName = null;
		if (!command.hasOption(VALUEIN_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		Class mapValOutClass = Class.forName(mapValOutClassName);
		
		String keyOutClassName = null;
		if (!command.hasOption(KEYOUT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		Class keyOutClass = Class.forName(keyOutClassName);
		
		String valOutClassName = null;
		if (!command.hasOption(VALUEOUT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		Class valOutClass = Class.forName(valOutClassName);
		
		String mapClassName = null;
		if (!command.hasOption(VALUEOUT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		Class mapClass = Class.forName(mapClassName);
		
		Job job = setup(jobName, this.getClass(),
				input, output, inputFormatClass, outputFormatClass,
				mapKeyOutClass, mapValOutClass, keyOutClass, valOutClass,
				mapClass, Reducer.class, reduceNo);
		
		// load extra options into configuration object
		String dexterConf = null;
		if (!command.hasOption(DEXTER_CONF_OPT)) {
			LOG.error("Dexter configuration path missing");
			System.exit(-1);;
		}
		dexterConf = command.getOptionValue(DEXTER_CONF_OPT);
		job.getConfiguration().set(AnnotateMapper.DEXTER_CONF_PATH_HDFS, dexterConf);
		
		String disambiguator = "tagme";
		if (command.hasOption(DISAMB_OPT)) {
			disambiguator = command.getOptionValue(DISAMB_OPT);
		}
		job.getConfiguration().set(AnnotateMapper.DISAMB_HDFS_OPT, disambiguator);
		
		return job;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Job job = setup(args);
		
		// register the filesystem before starting
		registerHDFS();
		
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			LOG.error("Job failed: ", e);
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new HadoopAnnotation(), args);
		} catch (Exception e) {
			LOG.error("FAILED: ", e);
			e.printStackTrace();
		}
	}
}
