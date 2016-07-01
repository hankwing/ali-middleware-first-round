
package com.alibaba.middleware.tools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.components.PartialResultsBolt.OrderToBeProcess;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceConfig.TradeType;
import com.alibaba.middleware.race.jstorm.RaceTopology;

public final class SlotBasedCounter implements Serializable {

	private static Logger LOG = LoggerFactory.getLogger(SlotBasedCounter.class);
	private static final long serialVersionUID = -6690136329401787197L;
	private final Map<Long, PartialResult> timeToResults = new HashMap<Long, PartialResult>();
	private List<Long> times = null;
	private int numSlots = 0;

	public SlotBasedCounter(int numSlots) {
		if (numSlots <= 0) {
			throw new IllegalArgumentException(
					"Number of slots must be greater than zero (you requested "
							+ numSlots + ")");
		}
		times = new ArrayList<Long>();
		this.numSlots = numSlots;
	}

	/**
	 * if it is need to advance the window , return true
	 * @param time
	 * @param type
	 * @param trade
	 * @return
	 */
	public void incrementTrade(Long time, TradeType type,double trade) {

		PartialResult partial = timeToResults.get(time);
		if (partial == null) {
			Long mtime = getMinimumTime();
			if( time < mtime && times.size() >= numSlots ) {
				LOG.info("need to increase window size!!!!!!!!!!!!!");
				return;
			}
			times.add(time);
			times.sort(null);
			partial = new PartialResult(time);
			timeToResults.put(time, partial);
		}
		switch( type) {
		case Tmall:
			partial.tmallTrade += trade;
			break;
		case Taobao:
			partial.taobaoTrade += trade;
			break;
		case PC:
			partial.PC += trade;
			break;
		case Mobile:
			partial.mobile += trade;
			break;
		}
		
	}
	
	public boolean isNeedSlid() {
		if( times.size() > numSlots ) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public Long getMinimumTime() {
		Long mTime = 0L;
		if( times.size() > 0) {
			mTime = times.get(0);
		}
		return mTime;
	}
	
	/**
	 * when slide window, we need to add the old 
	 * values of PC & Mobile to other window
	 * @return
	 */
	public PartialResult getPartialResult( Map<Long, Map<Long,OrderToBeProcess>> ordersToBeProcess) {

		PartialResult result = null;
		//LOG.info("sliding times:" + times.get(0));
		Long mTime = getMinimumTime();
		int errorOrders = ordersToBeProcess.containsKey(mTime) ?
				ordersToBeProcess.get(mTime).size() : 1;

		// return the minimum time result
		if( errorOrders < RaceConfig.slidingThreshold ) {
			// can emit the result
			LOG.info("emit partial result,remaining error orders:{}, time:{}", errorOrders, mTime);
			result = timeToResults.get(mTime);
			wipeSlot(mTime);
			
		}
		else {
			LOG.error("too much orders waiting to be processed!!!!:" + errorOrders);
		}	
		return result;
	}
	
	/**
	 * when cleanup, return all remainder results
	 * @return
	 */
	public List<PartialResult> getRemainders() {
		List<PartialResult> remainders = new ArrayList<PartialResult>();
		for( Long time : times) {
			remainders.add( timeToResults.get(time));
			wipeSlot(time);
		}
		return remainders;
	}

	/**
	 * Reset the slot count of any tracked objects to zero for the given slot.
	 *
	 * @param slot
	 */
	public void wipeSlot(Long time) {
		PartialResult temp = timeToResults.remove(time);
		PartialResult needToModify = timeToResults.get(times.get(1));
		needToModify.mobile += temp.mobile;
		needToModify.PC += temp.PC;
		times.remove(time);
	}

}
