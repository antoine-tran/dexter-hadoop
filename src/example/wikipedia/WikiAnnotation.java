package wikipedia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import tuan.hadoop.io.IntFloatArrayListWritable;
import it.cnr.isti.hpc.dexter.hadoop.AnnotateMapper;
import it.cnr.isti.hpc.dexter.hadoop.HadoopAnnotation;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;

public class WikiAnnotation extends HadoopAnnotation {

	public static final class MyMapper extends AnnotateMapper<LongWritable, WikipediaPage, 
			LongWritable, IntFloatArrayListWritable> {

		@Override
		public LongWritable instantiateKeyOutput() {
			return new LongWritable();
		}

		@Override
		public IntFloatArrayListWritable instantiateValueOutput() {
			return new IntFloatArrayListWritable();
		}

		@Override
		// There is only one field of main content of the article
		public Iterable<String> contents(WikipediaPage value) {
			Set<String> contents = new HashSet<>();
			try {
				contents.add(value.getContent());
			} catch (Exception e) {
				LOG.warn("Error in reading page " + value.getTitle() + ". Skip !");
			}
			return contents;			
		}

		@Override
		// Before annotating, we save the page id from input key to output key to
		// forward it to the reducer, and clean the output value cache
		public void preAnnotations(LongWritable keyIn, WikipediaPage valIn,
				LongWritable keyOut, IntFloatArrayListWritable valOut) {
			keyOut.set(keyIn.get());
			valOut.clear();
		}

		@Override
		public void consumeAnnotation(LongWritable keyOut,
				IntFloatArrayListWritable valOut, AnnotatedSpot spot) {
			valOut.add(spot.getEntity(), (float) spot.getScore());
		}

		@Override
		public boolean postAnnotations(LongWritable keyOut,
				IntFloatArrayListWritable valOut) {
			return !valOut.isEmpty();
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public Job setup(String[] args) throws IOException, ClassNotFoundException {
		Job job = super.setup(args);
		
		// increase heap
		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
		
		return job;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// add extra arguments about the classes
		List<String> newArgs = new ArrayList<>();
		for (String a : args) newArgs.add(a);
		newArgs.add("-informat");
		newArgs.add("edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat");
		newArgs.add("-outformat");
		newArgs.add("org.apache.hadoop.mapreduce.lib.output.TextOutputFormat");
		newArgs.add("-inkey");
		newArgs.add("org.apache.hadoop.io.LongWritable");
		newArgs.add("-inval");
		newArgs.add("edu.umd.cloud9.collection.wikipedia.WikipediaPage");
		newArgs.add("-outkey");
		newArgs.add("org.apache.hadoop.io.Text");
		newArgs.add("-outval");
		newArgs.add("tuan.hadoop.io.IntFloatArrayListWritable");
		newArgs.add("-mapper");
		newArgs.add("wikipedia.WikiAnnotation$MyMapper");
		
		try {
			ToolRunner.run(new WikiAnnotation(), 
					newArgs.toArray(new String[newArgs.size()]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
