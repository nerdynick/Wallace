package com.rebelai.wallace.health;

import com.codahale.metrics.health.HealthCheck;

public abstract class TimeHealthCheck extends HealthCheck {
	protected abstract long lastTime();
	protected abstract long maxDiff();
	
	@Override
	protected Result check() throws Exception {
		long dif = System.currentTimeMillis()-lastTime();
		
		if(dif > maxDiff()){
			return Result.unhealthy("Max time has passed of "+ Long.toString(dif) +"ms");
		}
		return Result.healthy("Time passed so far is "+ Long.toString(dif) +"ms");
	}
}
