import java.io.IOException;
import java.util.Random;

import com.hadoop.compression.lzo.LzoCodec;
import com.twitter.elephantbird.examples.proto.Examples;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockRecordWriter;
import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Copyright (C) 2014 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */

public class MRJob {

	public static class LzoMapper
			extends Mapper<LongWritable, ProtobufWritable<Examples.Age>, Text, Text> {

		@Override
		protected void map(LongWritable key, ProtobufWritable<Examples.Age> value, Context context)
				throws IOException, InterruptedException {
			Examples.Age age = value.get();
			System.out.println(age.getName() + "\t" + age.getAge());
			context.write(new Text(age.getName()), new Text(String.valueOf(age.getAge())));
		}

	}

	public static class TextMapper
			extends Mapper<LongWritable, Text, LongWritable, ProtobufWritable<Examples.Age>> {

		ProtobufWritable<Examples.Age> protoWritable = ProtobufWritable.newInstance(Examples.Age.class);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println(key + "\t" + value);

			Examples.Age exage = Examples.Age.newBuilder()
					.setAge((int) key.get())
					.setName(value.toString())
					.build();

			protoWritable.set(exage);
			context.write(key, protoWritable);
		}
	}

	public static void textMapperMain() throws Exception {
		Configuration conf = new Configuration();

		Path opath = new Path("/elephant.proto");
		FileSystem fs = opath.getFileSystem(conf);
		fs.delete(opath, true);

		Job job = new Job(conf);
		job.setJobName("Protobuf Example : Text to LzoB64Line");

		job.setJarByClass(MRJob.class);
		job.setMapperClass(TextMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		LzoProtobufBlockOutputFormat.setClassConf(Examples.Age.class, HadoopCompat.getConfiguration(job));
		job.setOutputFormatClass(LzoProtobufBlockOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/elephant.seq"));
		FileOutputFormat.setOutputPath(job, new Path("/elephant.proto"));

		job.waitForCompletion(true);
	}

	public static void lzoMapperMain() throws Exception {
		Configuration conf = new Configuration();

		Path opath = new Path("/elephant.t");
		FileSystem fs = opath.getFileSystem(conf);
		fs.delete(opath);

		Job job = new Job(conf);
		job.setJobName("Protobuf Example : LzoB64Line to Text");

		job.setJarByClass(MRJob.class);
		job.setMapperClass(LzoMapper.class);
		job.setNumReduceTasks(0);

		// input format is same for both B64Line or block:
		MultiInputFormat.setInputFormatClass(Examples.Age.class, job);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/elephant.proto/part-m-00000.lzo"));
//		FileInputFormat.setInputPaths(job, new Path("/seq"));
		FileOutputFormat.setOutputPath(job, new Path("/elephant.t"));

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {

//		Configuration conf = new Configuration();
//		Path file = new Path("/elephant.proto/part-m-00000.lzo");
//		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);

		writeSomeStuff();
//		textMapperMain();
		lzoMapperMain();
	}

	public static void writeSomeStuff() throws Exception {
		Configuration conf = new Configuration();

//		Path opath = new Path("/elephant.seq");
//		FileSystem fs = opath.getFileSystem(conf);
//		fs.delete(opath);

		FileSystem fs = FileSystem.getLocal(conf);
		Path sf = new Path("/home/asatish/Downloads/sequence-file");
		FSDataOutputStream os = fs.create(sf);


		BinaryBlockWriter<Examples.Age> bw = new ProtobufBlockWriter<Examples.Age>(os, Examples.Age.class);
		LzoProtobufBlockRecordWriter writer = new LzoProtobufBlockRecordWriter<
				Examples.Age, ProtobufWritable<Examples.Age>>(bw);



//		SequenceFile.Writer writer =
//		SequenceFile.createWriter(fs, conf, sf, LongWritable.class, Text.class);
//		SequenceFile.createWriter(conf, os, LongWritable.class, ProtobufWritable.class,
//				SequenceFile.CompressionType.BLOCK, new LzoCodec());

		ProtobufWritable<Examples.Age> protoWritable = ProtobufWritable.newInstance(Examples.Age.class);

		Random generator = new Random();
		String[] strs = new String[] {"arj", "tom", "pya", "slu"};
		for (int i=0; i<1000; i++) {
			String name = strs[generator.nextInt(strs.length)];
			int a = generator.nextInt(10);
			Examples.Age age = Examples.Age.newBuilder()
					.setAge(a)
					.setName(name)
					.build();
//			writer.append(new LongWritable(a), new Text(name));

			protoWritable.set(age);
//			writer.append(new LongWritable(a), protoWritable);
			writer.write(null, protoWritable);
		}

		writer.close(null);
		os.close();
	}

}
