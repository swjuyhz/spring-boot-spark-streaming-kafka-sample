package com.swjuyhz.sample.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

@Configuration
public class SparkConfig {
	@Value("${spring.application.name}")
	private String sparkAppName;
	@Value("${spark.master}")
	private String sparkMasteer;
	@Value("${spark.stream.kafka.durations}")
	private String streamDurationTime;
	@Value("${spark.driver.memory}")
	private String sparkDriverMemory;
	@Value("${spark.worker.memory}")
	private String sparkWorkerMemory;
	@Value("${spark.executor.memory}")
	private String sparkExecutorMemory;
	@Value("${spark.rpc.message.maxSize}")
	private String sparkRpcMessageMaxSize;

	@Bean
	@ConditionalOnMissingBean(SparkConf.class)
	public SparkConf sparkConf() {
		SparkConf conf = new SparkConf()
				.setAppName(sparkAppName)
                .setMaster(sparkMasteer).set("spark.driver.memory",sparkDriverMemory)
                .set("spark.worker.memory",sparkWorkerMemory)//"26g".set("spark.shuffle.memoryFraction","0") //默认0.2
                .set("spark.executor.memory",sparkExecutorMemory)
                .set("spark.rpc.message.maxSize",sparkRpcMessageMaxSize);
		// .setMaster("local[*]");//just use in test
		return conf;
	}

	@Bean
	@ConditionalOnMissingBean(JavaSparkContext.class) //默认： JVM 只允许存在一个sparkcontext
	public JavaSparkContext javaSparkContext(@Autowired SparkConf sparkConf) {
		return new JavaSparkContext(sparkConf);
	}

}
