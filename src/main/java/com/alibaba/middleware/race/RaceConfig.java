package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀43210ehyps
    public static String prex_tmall = "platformTmall_43210ehyps_";
    public static String prex_taobao = "platformTaobao_43210ehyps_";
    public static String prex_pc = "pc_43210ehyps_";
    public static String prex_mobile = "mobile_43210ehyps_";
    public static String prex_ratio = "ratio_43210ehyps_";
    public static String[] componentsIds = {"TmallSpout", "TaobaoSpout","PaymentSpout", 
    	"PartialResultsBolt", "ResultBolt"};

    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "43210ehyps";
    //public static String MetaConsumerGroup = "43210ehyps1007";	// local test!!
    public static String MetaConsumerGroup = "43210ehyps";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    //public static String MqPayTopic = "MiddlewareRaceTestData_Pay_Test3";			//local test!!
    //public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder_Test3";	//local test!!
    //public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder_Test3";//local test!!
    public static String TairConfigServer = "10.101.72.127:5198";
    //public static String TairConfigServer = "192.168.0.237:5198";					// local test!!
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    //public static String TairGroup = "group_1";										// local test!!
    public static Integer TairNamespace = 33404;
    //public static Integer TairNamespace = 0;										// local test!!
    
    public static String MqConfigServer = "192.168.0.17:9876";
    
    public static long emitOnceTime = 100;
    
    public static enum TradeType {
    	Tmall,Taobao,PC,Mobile;
    }
    
    public static String ComponentSpouts = "AllTopicSpouts";
    public static String ComponentTmallSpout = "TmallSpout";
    public static String ComponentTaobaoSpout = "TaobaoSpout";
    public static String ComponentPaymentSpout = "PaymentSpout";
    public static String ComponentPartialResultBolt = "PartialResultBolt";
    public static String ComponentDSBolt = "DSBolt";
    public static String ComponentResultBolt = "ResultBolt";
    
    public static int windowLengthInSeconds = 120;
    public static int emitFrequencyInSeconds = 1;
    
    public static int spout_Parallelism_hint = 1;
    public static int middleBolt_Parallelism_hint = 4;
    public static int dsBolt_Parallelism_hint = 4;
    public static int resultBolt_Parallelism_hint = 1;
    public static int ackBolt_Parallelism_hint = 0;
    public static int worker_Number = 4;
    public static int max_Spout_Pending = 100;
    
    public static int slidingThreshold = 100;
    public static int emit_One_Time = 100;
    public static long orderExpiredMinutes = 30;
    
    public static String PROP_FILE_NAME = "mqspout.default.prop";
    
}
