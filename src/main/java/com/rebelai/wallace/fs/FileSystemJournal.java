package com.rebelai.wallace.fs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.rebelai.wallace.Journal;

public class FileSystemJournal implements Journal {
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemJournal.class);
	
	private static final String extention = ".jrnl";
	private static final int headerByteSize = 4; //We only store the length as long in the headers
	
	final Path journalPath;
	protected final long maxSegmentSize;
	protected final int maxWriteQueueSize;
	private final Writer journalWriter;
	
	public FileSystemJournal(final Path journalDirectoryPath) throws IOException{
		//Defaults to 1GB
		this(journalDirectoryPath, 1024*1024*1000, 10000);
	}
	
	public FileSystemJournal(final Path journalDirectoryPath, final long maxSegmentSize, final int maxWriteQueueSize) throws IOException{
		this.journalPath = journalDirectoryPath;
		this.maxSegmentSize = maxSegmentSize;
		this.maxWriteQueueSize = maxWriteQueueSize;
		
		if (!Files.exists(journalDirectoryPath)) {
            throw new IOException(journalDirectoryPath + " directory doesn't exist.");            
        }
        
        if (!Files.isDirectory(journalDirectoryPath)) {
            throw new IOException(journalDirectoryPath + " is not directory.");
        }

        if (!Files.isWritable(journalDirectoryPath)) {
            throw new IOException(journalDirectoryPath + " directory is not writeable.");
        }
        
        this.journalWriter = new Writer(this, maxWriteQueueSize, maxSegmentSize);
	}
	
	public Path getEarliestJournal(){
		final TreeSet<String> journals = this.getAllJournals();
		if(!Iterables.isEmpty(journals)){
			return journalPath.resolve(journals.first());
		}
		
		return null;
	}
	
	public Path getLatestJournal(){
		final TreeSet<String> journals = this.getAllJournals();
		if(!Iterables.isEmpty(journals)){
			return journalPath.resolve(journals.last());
		}
		
		return null;
	}
	
	protected Path newJournal(){
		return journalPath.resolve(Long.toString(System.currentTimeMillis())+FileSystemJournal.extention);
	}
	
	public TreeSet<String> getAllJournals(){
		final String[] journalPaths = journalPath.toFile().list(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(FileSystemJournal.extention);
            }
        });
		
		final TreeSet<String> journals = new TreeSet<>();
		if(journalPaths != null){
			for(String j: journalPaths){
				if(!Strings.isNullOrEmpty(j)){
					journals.add(j);
				}
			}
		}
		
		return journals;
	}
	
	public boolean isClosed(){
		return this.journalWriter.isClosed();
	}

	@Override
	public void close() throws IOException {
		this.journalWriter.close();
	}
	
	public void write(final byte[] bytes) throws IOException{
		this.write(bytes, 0, bytes.length);
	}
	public void write(final byte[] bytes, int offset, int length) throws IOException{
		ByteBuffer b = ByteBuffer.allocate(headerByteSize+length);
		b.clear();
		b.putInt(length);
		b.put(bytes, offset, length);
		b.flip();
		this.journalWriter.write(b);
	}
	public void write(final ByteBuffer buffer) throws IOException{
		ByteBuffer b = ByteBuffer.allocate(headerByteSize+buffer.remaining());
		b.clear();
		b.putInt(buffer.remaining());
		b.put(buffer);
		b.flip();
		this.journalWriter.write(b);
	}

}