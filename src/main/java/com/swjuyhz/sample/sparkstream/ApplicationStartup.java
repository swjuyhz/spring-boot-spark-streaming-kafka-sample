package com.swjuyhz.sample.sparkstream;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import com.swjuyhz.sample.sparkstream.executor.SparkKafkaStreamExecutor;
/**
 * spring boot 容器加载完成后执行
 * 启动kafka数据接收和处理
 * @author yonghao.zheng
 *
 */
public class ApplicationStartup implements ApplicationListener<ContextRefreshedEvent> {

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		ApplicationContext ac = event.getApplicationContext();
		SparkKafkaStreamExecutor sparkKafkaStreamExecutor= ac.getBean(SparkKafkaStreamExecutor.class);
		Thread thread = new Thread(sparkKafkaStreamExecutor);
		thread.start();
	}

}
