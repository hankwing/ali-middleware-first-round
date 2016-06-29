package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.components.MergeResultsBolt;
import com.alibaba.middleware.components.PartialResultsBolt;
import com.alibaba.middleware.components.PaySpout;
import com.alibaba.middleware.components.TaobaoSpout;
import com.alibaba.middleware.components.TmallSpout;
import com.alibaba.middleware.race.RaceConfig;

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
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(RaceConfig.ComponentTmallSpout, new TmallSpout(),
				RaceConfig.spout_Parallelism_hint);
		builder.setSpout(RaceConfig.ComponentTaobaoSpout, new TaobaoSpout(),
				RaceConfig.spout_Parallelism_hint);
		builder.setSpout(RaceConfig.ComponentPaymentSpout, new PaySpout(),
				RaceConfig.spout_Parallelism_hint);
		builder.setBolt(RaceConfig.ComponentPartialResultBolt,
				new PartialResultsBolt(),
				RaceConfig.middleBolt_Parallelism_hint)
				.fieldsGrouping(RaceConfig.ComponentTmallSpout,
						new Fields("orderID"))
				.fieldsGrouping(RaceConfig.ComponentTaobaoSpout,
						new Fields("orderID"))
				.fieldsGrouping(RaceConfig.ComponentPaymentSpout,
						new Fields("orderID"));
		builder.setBolt(RaceConfig.ComponentResultBolt,
				new MergeResultsBolt(RaceConfig.windowLengthInSeconds),
				RaceConfig.resultBolt_Parallelism_hint).globalGrouping(
				RaceConfig.ComponentPartialResultBolt);
		String topologyName = RaceConfig.JstormTopologyName;

		conf.setNumWorkers(RaceConfig.worker_Number);
		//conf.setNumAckers(RaceConfig.ackBolt_Parallelism_hint);
		conf.setMaxSpoutPending(RaceConfig.max_Spout_Pending);

		try {
			if (args.length > 0) {
				// cluster mode
				StormSubmitter.submitTopology(topologyName, conf,
						builder.createTopology());
			} else {
				// local mode
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, conf,
						builder.createTopology());

				Thread.sleep(600000);
				cluster.killTopology(RaceConfig.JstormTopologyName);
				cluster.shutdown();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}