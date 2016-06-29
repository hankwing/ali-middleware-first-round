package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";
    public static String[] componentsIds = {"TmallSpout", "TaobaoSpout","PaymentSpout", 
    	"PartialResultsBolt", "ResultBolt"};

    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "RUC";  
    //public static String MetaConsumerGroup = "test";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "xxx";
    public static String TairSalveConfigServer = "xxx";
    public static String TairGroup = "xxx";
    public static Integer TairNamespace = 1;
    
    public static String MqConfigServer = "192.168.0.17:9876";
    
    public static long emitOnceTime = 100;
    
    public static enum TradeType {
    	Tmall,Taobao,PC,Mobile;
    }
    
    public static String ComponentTmallSpout = "TmallSpout";
    public static String ComponentTaobaoSpout = "TaobaoSpout";
    public static String ComponentPaymentSpout = "PaymentSpout";
    public static String ComponentPartialResultBolt = "PartialResultBolt";
    public static String ComponentResultBolt = "ResultBolt";
    
    public static int windowLengthInSeconds = 20;
    public static int emitFrequencyInSeconds = 1;
    
    public static int spout_Parallelism_hint = 1;
    public static int middleBolt_Parallelism_hint = 4;
    public static int resultBolt_Parallelism_hint = 1;
    public static int ackBolt_Parallelism_hint = 2;
    public static int worker_Number = 4;
    public static int max_Spout_Pending = 10 * 1024;
    
    public static int slidingThreshold = 10000;
    public static long cleanupIntervel = 60 * 1000;
    public static int emit_One_Time = 100;
    
}
