package com.rebelai.wallace.channel;

import java.nio.channels.AsynchronousChannel;

import com.rebelai.wallace.JournalSegment;

public interface AsyncJournalSegment<T extends AsynchronousChannel> extends JournalSegment<T> {
}
