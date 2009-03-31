package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogEntry {
	private final static SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm");

	private Date date;
	private String logLevel;
	private String className;
	private String body;

	public LogEntry(String recordEntry) throws ParseException {
		String dStr = recordEntry.substring(0, 23);
		date = sdf.parse(dStr);
		int start = 24;
		int idx = recordEntry.indexOf(' ', start);
		logLevel = recordEntry.substring(start, idx);
		start = idx + 1;
		idx = recordEntry.indexOf(' ', start);
		className = recordEntry.substring(start, idx - 1);
		body = recordEntry.substring(idx + 1);
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getLogLevel() {
		return logLevel;
	}

	public String getClassName() {
		return className;
	}

	public String getBody() {
		return body;
	}
}
