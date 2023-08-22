/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.atguigu;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamExecutionEnvironment env_2 = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
		env.setParallelism(1); //set up parallelism degree
		DataStream<String> stream = env.socketTextStream("Localhost",9999); //construt data stream via localhost:9999,unlimited stream
		DataStream<String> streamSource = env.readTextFile("data/textfile"); //limited boundary stream,red text file line by line
		stream.flatMap(new Tokenizer()).keyBy(r -> r.f0).sum(1).print(); //use flatmap method, self-define keySelector to extract key as stream partition

		DataStreamSource<String> socket = env.socketTextStream("localhost",9999);
		socket
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public void flatMap(String value,Collector<String> out) throws Exception{
						String[] s = value.split(" ");
						for (String word:s){
							out.collect(word);
						}
					}
				})
				.map(new MapFunction<String, Tuple2<String,Integer>>() {
					@Override
					public Tuple2<String, Integer> map(String value) throws Exception {
						return Tuple2.of(value,1);
					}
				})
				.keyBy(0) //此处单词作为key
				.sum(1) //对tuple中位置1的值求和
				.print();
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */
		//stream graph -> job graph -> execution pla
		//get execution plan (can visualize flink plan through Flink Plan Visualizer)
		env.getExecutionPlan();
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
	//implement method is used to realise interface
	public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
			String[] stringList = value.split("\\s");
			for (String s: stringList){
				collector.collect(new Tuple2<>(s,1));
			}
		}
	}
}
