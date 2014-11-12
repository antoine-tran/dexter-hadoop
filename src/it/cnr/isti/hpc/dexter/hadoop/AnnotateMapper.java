package it.cnr.isti.hpc.dexter.hadoop;

import it.cnr.isti.hpc.dexter.StandardTagger;
import it.cnr.isti.hpc.dexter.Tagger;
import it.cnr.isti.hpc.dexter.common.Field;
import it.cnr.isti.hpc.dexter.common.FlatDocument;
import it.cnr.isti.hpc.dexter.common.MultifieldDocument;
import it.cnr.isti.hpc.dexter.disambiguation.Disambiguator;
import it.cnr.isti.hpc.dexter.entity.EntityMatch;
import it.cnr.isti.hpc.dexter.entity.EntityMatchList;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedDocument;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;
import it.cnr.isti.hpc.dexter.rest.domain.Tagmeta;
import it.cnr.isti.hpc.dexter.spotter.Spotter;
import it.cnr.isti.hpc.dexter.util.DexterLocalParams;
import it.cnr.isti.hpc.dexter.util.DexterParams;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tuan.terrier.Files;
import tuan.terrier.HadoopDistributedFileSystem;

public abstract class AnnotateMapper<KEYIN,VALIN,KEYOUT,VALOUT> 
		extends Mapper<KEYIN,VALIN,KEYOUT,VALOUT> {
	
	private static Logger LOG = LoggerFactory.getLogger(AnnotateMapper.class);
	
	public static final String DISAMB_HDFS_OPT = "dexter.disambiguator";
	public static final String DEXTER_CONF_PATH_HDFS = "dexter-conf.path";
	
	// Each JVM has only one dexterParam instance
	private DexterParams dexterParams;
	private Spotter s;
	private Disambiguator d;
	private Tagger tagger;
	private boolean addWikinames;
	private Integer entitiesToAnnotate;
	private double minConfidence;

	private String ned;

	private KEYOUT keyOut;
	private VALOUT valOut;
	
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		Configuration conf = context.getConfiguration();

		// register HDFS for each JVM in the datanode
		if (!Files.hasFileSystemScheme(HadoopDistributedFileSystem.HDFS_SCHEME)) {
			Files.addFileSystemCapability(new HadoopDistributedFileSystem(conf));	
		}

		if (dexterParams == null) {
			String dexterConf = System.getProperty("conf");
			if (dexterConf == null) {
				dexterConf = conf.get(DEXTER_CONF_PATH_HDFS);
				System.setProperty("conf", dexterConf);
			}
			dexterParams = DexterParams.getInstance();
		}
		s = dexterParams.getSpotter("wiki-dictionary");
		d = dexterParams.getDisambiguator("wikiminer");

		tagger = new StandardTagger("std", s, d);
		addWikinames = new Boolean("true");

		entitiesToAnnotate = new Integer(50);
		minConfidence = Double.parseDouble("0.1");

		if (ned == null) {
			ned = conf.get(DISAMB_HDFS_OPT);
		}
		
		keyOut = instantiateKeyOutput();
		valOut = instantiateValueOutput();
	}
	
	public abstract KEYOUT instantiateKeyOutput();
	public abstract VALOUT instantiateValueOutput();
	
	@Override
	protected void map(KEYIN key, VALIN item, Context context) 
			throws IOException, InterruptedException {		
		for (String content : contents(item)) {
			MultifieldDocument doc = parseDocument(content, "text");		

			EntityMatchList eml = tagger.tag(new DexterLocalParams(), doc);

			AnnotatedDocument adoc = new AnnotatedDocument(doc);

			
			annotate(adoc, eml, entitiesToAnnotate, addWikinames, minConfidence);
			
			for (AnnotatedSpot spot : adoc.getSpots()) {			
				// neds.add(spot.getEntity(), (float) spot.getScore());
				consumeAnnotation(keyOut, valOut, spot);
			}
			
			context.write(keyOut, valOut);
		}
	}
	
	// Get different texts from a value (e.g. from different fields)
	public abstract Iterable<String> contents(VALIN value);
	
	public abstract void consumeAnnotation(KEYOUT keyOut, VALOUT valOut, AnnotatedSpot spot);
	
	public static MultifieldDocument parseDocument(String text, String format) {
		Tagmeta.DocumentFormat df = Tagmeta.getDocumentFormat(format);
		MultifieldDocument doc = null;
		if (df == Tagmeta.DocumentFormat.TEXT) {
			doc = new FlatDocument(text);
		}
		return doc;
	}
	
	public static void annotate(AnnotatedDocument adoc, EntityMatchList eml,
			int nEntities, boolean addWikiNames, double minConfidence) {
		eml.sort();
		EntityMatchList emlSub = new EntityMatchList();
		int size = Math.min(nEntities, eml.size());
		List<AnnotatedSpot> spots = adoc.getSpots();
		spots.clear();
		for (int i = 0; i < size; i++) {
			EntityMatch em = eml.get(i);
			if (em.getScore() < minConfidence) {
				
				continue;
			}
			emlSub.add(em);
			AnnotatedSpot spot = new AnnotatedSpot(em.getMention(),
					em.getSpotLinkProbability(), em.getStart(), em.getEnd(), em
							.getSpot().getLinkFrequency(), em.getSpot()
							.getFrequency(), em.getId(), em.getFrequency(),
					em.getCommonness(), em.getScore());
			spot.setField(em.getSpot().getField().getName());
			/*if (addWikiNames) {
				spot.setWikiname(helper.getLabel(em.getId()));
			}*/

			spots.add(spot);
		}
		MultifieldDocument annotatedDocument = getAnnotatedDocument(adoc,
				emlSub);
		adoc.setAnnotatedDocument(annotatedDocument);
	}
	
	public static MultifieldDocument getAnnotatedDocument(AnnotatedDocument adoc,
			EntityMatchList eml) {
		Collections.sort(eml, new EntityMatch.SortByPosition());

		Iterator<Field> iterator = adoc.getDocument().getFields();
		MultifieldDocument annotated = new MultifieldDocument();
		while (iterator.hasNext()) {
			int pos = 0;
			StringBuffer sb = new StringBuffer();
			Field field = iterator.next();
			String currentField = field.getName();
			String currentText = field.getValue();

			for (EntityMatch em : eml) {
				if (!em.getSpot().getField().getName().equals(currentField)) {
					continue;
				}
				assert em.getStart() >= 0;
				assert em.getEnd() >= 0;
				try {
					sb.append(currentText.substring(pos, em.getStart()));
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					LOG.warn(
							"error annotating text output of bound for range {} - {} ",
							pos, em.getStart());
					LOG.warn("text: \n\n {}\n\n", currentText);
				}
				// the spot has been normalized, i want to retrieve the real one
				String realSpot = "none";
				try {
					realSpot = currentText
							.substring(em.getStart(), em.getEnd());
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					LOG.warn(
							"error annotating text output of bound for range {} - {} ",
							pos, em.getStart());
					LOG.warn("text: \n\n {}\n\n", currentText);
				}
				sb.append(
						"<a href=\"#\" onmouseover='manage(" + em.getId()
								+ ")' >").append(realSpot).append("</a>");
				pos = em.getEnd();
			}
			if (pos < currentText.length()) {
				try {
					sb.append(currentText.substring(pos));
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					LOG.warn(
							"error annotating text output of bound for range {} - end ",
							pos);
					LOG.warn("text: \n\n {}\n\n", currentText);
				}

			}
			annotated.addField(new Field(field.getName(), sb.toString()));

		}

		return annotated;
	}
}
