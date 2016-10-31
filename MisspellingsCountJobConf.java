import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MisspellingsCountJobConf extends Configured implements Tool {

	
	//Map Class
   static public class CommonlyMisspelledWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
   
	  //final private static LongWritable ONE = new LongWritable(1);
	  //final private static LongWritable ZERO = new LongWritable(0);
	  private LongWritable count = new LongWritable();
	  private Text year = new Text();
	  
	  
	  // Using a list of the most common misspellings from https://en.oxforddictionaries.com/spelling/common-misspellings
	  // we are pretending this is our corpus and counting misspellings and correct spellings to determine a percentage
	  // of the occurrences that commonly misspelled words are indeed misspelled
	  
	  // Interested in whether there is a trend over the years for misspellings of these commonly misspelled words
	  // this is not perfect because some of these words may have changed in usage patterns over time
	  // so for example, if none of these words were used in earlier writing then it might appear that authors
	  // could spell correctly, when they may indeed have just mispelled to the same degree but with different words!
	  
	  // commonly misspelled words
	  private String[] commonly_misspelled_words_array = {"accomodate","accomodation",
			  "acheive","accross","agressive", "agression","apparantly","appearence",
			  "arguement","assasination","basicly","begining","belive",
			  "bizzare","buisness","Carribean","cemetary","chauffer","collegue",
			  "comming","commitee","completly","concious","curiousity","definately",
			  "dilemna","dissapear","dissapoint","ecstacy","embarass","enviroment",
			  "existance","Farenheit","familar","finaly","florescent","foriegn","forseeable",
			  "fourty","foward","freind","futher","jist","glamourous","goverment","gaurd",
			  "happend","harrass","harrassment","honourary","humourous","idiosyncracy",
			  "immediatly","incidently","independant","interupt","irresistable","knowlege",
			  "liase","liason","lollypop","millenium","millenia","Neandertal","neccessary",
			  "noticable","ocassion","occassion","occured","occuring","occurance","occurence",
			  "pavillion","persistant","pharoah","peice","politican","Portugese","posession",
			  "prefered","prefering","propoganda","publically","realy","recieve","refered",
			  "refering","religous","remeber","resistence","sence","seperate","seige",
			  "succesful","supercede","suprise","tatoo","tendancy","therefor","threshhold",
			  "tommorow","tommorrow","tounge","truely","unforseen","unfortunatly","untill",
			  "wierd","whereever","wich"};
	  
	  // the correctly spelled counterparts
	  private String[] correctly_spelled_words_array = {"accommodate", "accommodation", "achieve", 
			  "across", "aggressive", "aggression", "apparently", "appearance", 
				"argument", "assassination", "basically", "beginning", "believe", 
				"bizarre", "business", "Caribbean", "cemetery", "chauffeur", "colleague", 
				"coming", "committee", "completely", "conscious", "curiosity", "definitely", 
				"dilemma", "disappear", "disappoint", "ecstasy", "embarrass", "environment", 
				"existence", "Fahrenheit", "familiar", "finally", "fluorescent", "foreign", "foreseeable", 
				"fourth", "forward", "friend", "father", "jest", "glamorous", "government", "guard", 
				"happened", "harass", "harassment", "honorary", "humorous", "idiosyncrasy", 
				"immediately", "incidentally", "independent", "interrupt", "irresistible", "knowledge", 
				"lease", "liaison", "lollipop", "millennium", "millennia", "Neanderthal", "necessary", 
				"noticeable", "occasion", "occasion", "occurred", "occurring", "occurrence", "occurrence", 
				"pavilion", "persistent", "pharaoh", "piece", "politician", "Portuguese", "possession", 
				"preferred", "preferring", "propaganda", "publicly", "really", "receive", "referred", 
				"referring", "religious", "remember", "resistance", "since", "separate", "siege", 
				"successful", "supersede", "surprise", "tattoo", "tendency", "therefore", "threshold", 
				"tomorrow", "tomorrow", "tongue", "truly", "unforeseen", "unfortunately", "until", 
				"weird", "wherever", "which"};
	  
	  ArrayList<String> commonly_misspelled_words = new ArrayList<String>(
			  Arrays.asList(commonly_misspelled_words_array));
	  
	  ArrayList<String> correctly_spelled_words = new ArrayList<String>(
			  Arrays.asList(correctly_spelled_words_array));
	  
	  @Override
	  protected void map(LongWritable offset, Text text, Context context) 
			  throws IOException, InterruptedException {
	   
		  // Split the ngrams entry up by tab-delimiter
		  String[] values = text.toString().split("\\t+");
		  // get the year value
		  year.set(values[1]); 
		  // get the count value
		  long cnt = Long.parseLong(values[2]);
		  
		  // here we are kind of emitting to the reducer a "encoding" where misspelled
		  // words have negative count and postive count for not misspelled, 
		  // which lets us sum the misspellings but keep
		  // a count of all the words matching our limited corpus for the purposes
		  // of calculating a percentage of mispelled words in the corpus of commonly misspelled words
		  
		  // count the number of mispelled words by year
		  if(this.commonly_misspelled_words.contains(values[0])) {
			  count.set(cnt*-1);
			  context.write(year, count);
		  }
		  
		  // count the number of correctly spelled words by year too, here we emit zero so it doesn't 
		  // contribute to the sum in the reducer, but can be counted still to enable a percentage 
		  // of misspellings to be calculated
		  else if(this.correctly_spelled_words.contains(values[0])) {
			  count.set(cnt);
			  context.write(year, count);
		  }
	  }
   }

   // Reducer
   static public class CommonlyMisspelledWordCountReducer 
	extends Reducer<Text, LongWritable, Text, LongWritable> {
	  private LongWritable rate = new LongWritable();

	  @Override
	  protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
			throws IOException, InterruptedException {
		  
		 long n = 0; // count of the words in our corpus that were misspelled
		 long total = 0; // count of the words in a year that were from our corpus
		 
		 //Calculate count of misspelled words and count of total words in our corpus
		 for (LongWritable count : counts) {
			 long cnt = count.get();
			 long cnt_abs = Math.abs(cnt);
			 // if its a misspelling (encoded as negative count) 
			 // then add it to the misspellings count
			 if(cnt < 0) {
				 n += cnt_abs;
			 }
			 // add everything, misspellings and correct, to the total count
			 total += cnt_abs;
		 }
		 
		 // calculate percentage misspelled (whole percentage) and emit
		 float percent = (float)n / (float)total * (float)100;
		 long pc = (long) Math.round(percent);
		 rate.set(pc);
		 context.write(token, rate);
	  }
   }

   public int run(String[] args) throws Exception {
	  Configuration configuration = getConf();

	  //Initialising Map Reduce Job
	  Job job = new Job(configuration, "Ngrams commonly misspelled word percentage");
	  
	  //Set Map Reduce main jobconf class
	  job.setJarByClass(MisspellingsCountJobConf.class);

	  //Set Mapper class
	  job.setMapperClass(CommonlyMisspelledWordCountMapper.class);
	  
	  //Set Combiner class
	  // cannot use a combiner because reduce function is NOT both commutative and associative
	  
	  //set Reducer class
	  job.setReducerClass(CommonlyMisspelledWordCountReducer.class);
	  
	  // set input class
	  // S3 requires SequenceFileInputFormat 
	  job.setInputFormatClass(SequenceFileInputFormat.class);
	  
	  //set Output Format
	  job.setOutputFormatClass(TextOutputFormat.class);

	  //set Output key class
	  job.setOutputKeyClass(Text.class);
	  
	  //set Output value class
	  job.setOutputValueClass(LongWritable.class);

	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));

	  return job.waitForCompletion(true) ? 0 : -1;
   }

   public static void main(String[] args) throws Exception {
	  System.exit(ToolRunner.run(new MisspellingsCountJobConf(), args));
   }
}

