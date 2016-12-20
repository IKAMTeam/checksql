package com.onevizion.checksql;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.AbstractMatcherFilter;
import ch.qos.logback.core.spi.FilterReply;

public class CheckSqlLogFilter extends AbstractMatcherFilter<ILoggingEvent> {

    private Marker marker = null;

    @Override
    public void start() {
        if (null != this.marker)
            super.start();
        else
            addError("Marker is not set");
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
        if (!isStarted()) {
            return FilterReply.NEUTRAL;
        } else if (marker == null) {
            return onMismatch;
        } else if (marker.equals(event.getMarker())) {
            return onMatch;
        }
        return onMismatch;
    }

    public void setMarker(String markerStr) {
        if (null != markerStr)
            marker = MarkerFactory.getMarker(markerStr);
    }

}
