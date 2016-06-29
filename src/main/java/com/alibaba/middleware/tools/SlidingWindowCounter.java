
package com.alibaba.middleware.tools;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.components.PartialResultsBolt.OrderToBeProcess;
import com.alibaba.middleware.race.RaceConfig.TradeType;

public final class SlidingWindowCounter implements Serializable {

	private static final long serialVersionUID = -2645063988768785810L;

	private SlotBasedCounter objCounter;
	private int windowLengthInSlots;

	public SlidingWindowCounter(int windowLengthInSlots) {
		if (windowLengthInSlots < 2) {
			throw new IllegalArgumentException(
					"Window length in slots must be at least two (you requested "
							+ windowLengthInSlots + ")");
		}
		this.windowLengthInSlots = windowLengthInSlots;
		this.objCounter = new SlotBasedCounter(this.windowLengthInSlots);
	}

	/**
	 * if advance window, return partial result and sent to next bolts
	 * 
	 * @param time
	 * @param type
	 * @param trade
	 * @return
	 */
	public void incrementCount(Long time, TradeType type, double trade) {
		objCounter.incrementTrade(time, type, trade);
	}
	
	public boolean isNeedSlide() {
		return objCounter.isNeedSlid();
	}
	
	public PartialResult getSlidingPartialResult( 
			Map<Long, Map<Long,OrderToBeProcess>> ordersToBeProcess) {
		return objCounter.getPartialResult( ordersToBeProcess);
	}
	
	public List<PartialResult> getRemainResults() {
		return objCounter.getRemainders();
	}

}
