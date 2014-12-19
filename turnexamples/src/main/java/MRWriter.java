import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import com.google.common.collect.Maps;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.examples.proto.Examples;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Copyright (C) 2014 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */

public class MRWriter {

	public static void main(String[] args) throws IOException {

		Random generator = new Random();
		String[] strs = new String[] {"arj", "tom", "pya", "slu"};

		Path lzoPath = new Path("hdfs://localhost:9000/elephant.proto");
		Configuration conf = new Configuration();
		FileSystem fs = lzoPath.getFileSystem(conf);
		FSDataOutputStream fos = fs.create(lzoPath, true);
		LzopCodec codec = new LzopCodec();
		codec.setConf(conf);

		HashMap<String, Integer> map = Maps.newHashMap();
		for (String s: strs) map.put(s, 0);

//		FileOutputStream fos = new FileOutputStream("/home/asatish/Downloads/elephant-proto.pb");
		ProtobufBlockWriter<Examples.Age> pbWriter =
				new ProtobufBlockWriter<Examples.Age>(fos, Examples.Age.class);

		for (int i=0; i<1000; i++) {
			String name = strs[generator.nextInt(strs.length)];
			int a = generator.nextInt(10);
			Examples.Age age = Examples.Age.newBuilder()
					.setAge(a)
					.setName(name)
					.build();
			pbWriter.write(age);
			map.put(name, map.get(name) + a);
		}

		pbWriter.finish();
		pbWriter.close();

		System.out.println(map);
	}

}
