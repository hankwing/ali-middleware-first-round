package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.components.DSBolt;
import com.alibaba.middleware.components.MergeResultsBolt;
import com.alibaba.middleware.components.PartialResultsBolt;
import com.alibaba.middleware.components.PaySpout;
import com.alibaba.middleware.components.PaySpoutPull;
import com.alibaba.middleware.components.TaobaoSpout;
import com.alibaba.middleware.components.TaobaoSpoutPull;
import com.alibaba.middleware.components.TmallSpout;
import com.alibaba.middleware.components.TmallSpoutPull;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.storm.bolt.RocketMqBolt;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.alibaba.rocketmq.storm.internal.tools.ConfigUtils;
import com.alibaba.rocketmq.storm.spout.BatchMessageSpout;
import com.alibaba.rocketmq.storm.spout.StreamMessageSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包
 * ，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology； 所以这个主类路径一定要正确
 */
public class RaceTopology {

	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		Config mqConfig = ConfigUtils.init(RaceConfig.PROP_FILE_NAME);
		//RocketMQConfig mqConig = (RocketMQConfig) mqConfig.get(ConfigUtils.CONFIG_ROCKETMQ);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(RaceConfig.ComponentSpouts,
				new PaySpout(), RaceConfig.spout_Parallelism_hint);

		builder.setBolt(RaceConfig.ComponentDSBolt,
				new DSBolt(),
				RaceConfig.dsBolt_Parallelism_hint)
				.shuffleGrouping(RaceConfig.ComponentSpouts);
		
		builder.setBolt(RaceConfig.ComponentPartialResultBolt,
				new PartialResultsBolt(),
				RaceConfig.middleBolt_Parallelism_hint)
				.fieldsGrouping(RaceConfig.ComponentDSBolt,
						new Fields("orderID"));

		builder.setBolt(RaceConfig.ComponentResultBolt,
				new MergeResultsBolt(RaceConfig.windowLengthInSeconds),
				RaceConfig.resultBolt_Parallelism_hint).globalGrouping(
				RaceConfig.ComponentPartialResultBolt);
		String topologyName = RaceConfig.JstormTopologyName;

		conf.setNumWorkers(RaceConfig.worker_Number);
		conf.setNumAckers(RaceConfig.ackBolt_Parallelism_hint);
		conf.setMaxSpoutPending(RaceConfig.max_Spout_Pending);

		try {
			//if (args.length > 0) {
				// cluster mode
				StormSubmitter.submitTopology(topologyName, conf,
						builder.createTopology());
			/*} else {
				// local mode
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, conf,
						builder.createTopology());

				Thread.sleep(600000);
				cluster.killTopology(RaceConfig.JstormTopologyName);
				cluster.shutdown();
			}*/

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}