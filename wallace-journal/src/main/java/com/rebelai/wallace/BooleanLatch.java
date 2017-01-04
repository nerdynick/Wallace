package com.rebelai.wallace;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public class BooleanLatch {
	private static class Sync extends AbstractQueuedSynchronizer {
		private static final long serialVersionUID = -1629133695635492479L;
		
		public Sync(){
			this.setState(0);
		}

		boolean isSignalled() {
			return getState() == 0;
		}
		
		protected boolean tryAcquire(int ignore) {
	        if(this.compareAndSetState(0, 1)){
	        	return true;
	        }
	        return false;
	    }

		protected int tryAcquireShared(int ignore) {
			return isSignalled() ? 1 : -1;
		}

		protected boolean tryReleaseShared(int ignore) {
			setState(0);
			return true;
		}
	}

	private final Sync sync = new Sync();

	public boolean getState() {
		return !sync.isSignalled();
	}
	
	public boolean tryAquire(){
		return sync.tryAcquire(1);
	}

	public void release() {
		sync.releaseShared(0);
	}

	public void await() throws InterruptedException {
		sync.acquireSharedInterruptibly(0);
	}

	public void await(long timeout, java.util.concurrent.TimeUnit unit) throws InterruptedException {
		sync.tryAcquireSharedNanos(0, unit.toNanos(timeout));
	}
}
