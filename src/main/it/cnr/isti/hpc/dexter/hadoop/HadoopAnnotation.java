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
		
		Option mapperOpt = OptionBuilder.withArgName("mapperconfiguration").hasArg(true)
				.withDescription("class of input parser")
				.create(MAPPER_OPT);
		opts.addOption(mapperOpt);
		
		Option inpformatOpt = OptionBuilder.withArgName("inputformat").hasArg(true)
				.withDescription("class of input format")
				.create(INPUTFORMAT_OPT);
		opts.addOption(inpformatOpt);
		
		Option outformatOpt = OptionBuilder.withArgName("outputformat").hasArg(true)
				.withDescription("class of output format")
				.create(OUTPUTFORMAT_OPT);
		opts.addOption(outformatOpt);
		
		Option keyInOpt = OptionBuilder.withArgName("mapperkeyOpt").hasArg(true)
				.withDescription("class of key for mapper")
				.create(KEYIN_OPT);
		opts.addOption(keyInOpt);
		
		Option valInOpt = OptionBuilder.withArgName("mapperValueOpt").hasArg(true)
				.withDescription("class of value for mapper")
				.create(VALUEIN_OPT);
		opts.addOption(valInOpt);
		
		Option keyOutOpt = OptionBuilder.withArgName("keyOutOpt").hasArg(true)
				.withDescription("class of key of output")
				.create(KEYOUT_OPT);
		opts.addOption(keyOutOpt);
		
		Option valOutOpt = OptionBuilder.withArgName("valOutOpt").hasArg(true)
				.withDescription("class of value of output")
				.create(VALUEOUT_OPT);
		opts.addOption(valOutOpt);
		
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
		inputFormatClassName = command.getOptionValue(INPUTFORMAT_OPT);
		Class inputFormatClass = Class.forName(inputFormatClassName);
		
		String outputFormatClassname = null;
		if (!command.hasOption(OUTPUTFORMAT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		outputFormatClassname = command.getOptionValue(OUTPUTFORMAT_OPT);
		Class outputFormatClass = Class.forName(outputFormatClassname);
		
		String mapKeyOutClassName = null;
		if (!command.hasOption(KEYIN_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		mapKeyOutClassName = command.getOptionValue(KEYIN_OPT);
		Class mapKeyOutClass = Class.forName(mapKeyOutClassName);
		
		String mapValOutClassName = null;
		if (!command.hasOption(VALUEIN_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		mapValOutClassName = command.getOptionValue(VALUEIN_OPT);
		Class mapValOutClass = Class.forName(mapValOutClassName);
		
		String keyOutClassName = null;
		if (!command.hasOption(KEYOUT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		keyOutClassName = command.getOptionValue(KEYOUT_OPT);
		Class keyOutClass = Class.forName(keyOutClassName);
		
		String valOutClassName = null;
		if (!command.hasOption(VALUEOUT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		valOutClassName = command.getOptionValue(VALUEOUT_OPT);
		Class valOutClass = Class.forName(valOutClassName);
		
		String mapClassName = null;
		if (!command.hasOption(MAPPER_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
		}
		mapClassName = command.getOptionValue(MAPPER_OPT);
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
		
		System.out.println("Run with jar setting: " + job.getJar());
		
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
