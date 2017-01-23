package com.rebelai.wallace;

public class JournalSegmentStats {
	public final long readOffset;
	public final int msgsRead;
	public final int msgsTotal;
	public final boolean isReadOpen;
	public final boolean isWriteOpen;
	
	public JournalSegmentStats(final long readOffset, final int msgsRead, final int msgsTotal, final boolean isReadOpen, final boolean isWriteOpen){
		this.readOffset = readOffset;
		this.msgsRead = msgsRead;
		this.msgsTotal = msgsTotal;
		this.isReadOpen = isReadOpen;
		this.isWriteOpen = isWriteOpen;
	}
	
	@Override
	public String toString(){
		StringBuilder b = new StringBuilder();
		b.append("ReadOffset=").append(this.readOffset);
		b.append(" Msgs=").append(this.msgsRead).append("/").append(this.msgsTotal);
		b.append(" ReadingOpen=").append(this.isReadOpen);
		b.append(" WritingOpen=").append(this.isWriteOpen);
		
		return b.toString();
	}
}
