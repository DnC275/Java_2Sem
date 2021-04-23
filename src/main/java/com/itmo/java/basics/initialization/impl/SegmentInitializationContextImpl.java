package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import lombok.Builder;

import java.nio.file.Path;
import java.nio.file.Paths;

@Builder
public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private Integer currentSize;
    private SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this(segmentName, Paths.get(tablePath.toString(), segmentName), currentSize, new SegmentIndex());
    }

    @Override
    public String getSegmentName() {
        return this.segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return this.segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return this.index;
    }

    @Override
    public long getCurrentSize() {
        return this.currentSize;
    }
}
