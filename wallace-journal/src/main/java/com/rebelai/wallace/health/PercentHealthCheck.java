package com.rebelai.wallace.health;

import com.codahale.metrics.health.HealthCheck;

public abstract class PercentHealthCheck extends HealthCheck {
	
	protected abstract int percent();
	protected abstract int threshold();
	
	@Override
	protected Result check() throws Exception {
		int percent = percent();
		if(percent > threshold()){
			return Result.unhealthy("Threshold of "+Integer.toString(threshold())+"% passed with "+Integer.toString(percent)+"% used");
		}
		return Result.healthy("Threshold of "+Integer.toString(threshold())+"% good with only "+Integer.toString(percent)+"% used");
	}

}
