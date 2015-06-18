/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILogReader;
import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.LogRecord;
import edu.uci.ics.asterix.common.transactions.MutableLong;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class LogReader implements ILogReader {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(LogReader.class.getName());
    private final LogManager logMgr;
    private final long logFileSize;
    private final int logPageSize;
    private final MutableLong flushLSN;
    private final boolean isRecoveryMode;
    private final ByteBuffer readBuffer;
    private final ILogRecord logRecord;
    private long readLSN;
    private long bufferBeginLSN;
    private long fileBeginLSN;
    private IFileHandle logFile;
    private IIOManager ioManager;

    private enum ReturnState {
        FLUSH,
        EOF
    };

    public LogReader(LogManager logMgr, long logFileSize, int logPageSize, MutableLong flushLSN, boolean isRecoveryMode) {
        this.logMgr = logMgr;
        this.logFileSize = logFileSize;
        this.logPageSize = logPageSize;
        this.flushLSN = flushLSN;
        this.isRecoveryMode = isRecoveryMode;
        this.readBuffer = ByteBuffer.allocate(logPageSize);
        this.logRecord = new LogRecord();
        this.ioManager = logMgr.ioManager;
    }

    @Override
    public void initializeScan(long beginLSN) throws ACIDException {
        readLSN = beginLSN;
        if (waitForFlushOrReturnIfEOF() == ReturnState.EOF) {
            return;
        }
        getFileChannel();
        readPage();
    }

    //for scanning
    @Override
    public ILogRecord next() throws ACIDException {
        if (waitForFlushOrReturnIfEOF() == ReturnState.EOF) {
            return null;
        }
        if (readBuffer.position() == readBuffer.limit() || !logRecord.readLogRecord(readBuffer)) {
            readNextPage();
            if (!logRecord.readLogRecord(readBuffer)) {
                throw new IllegalStateException();
            }
        }
        logRecord.setLSN(readLSN);
        readLSN += logRecord.getLogSize();
        return logRecord;
    }

    private ReturnState waitForFlushOrReturnIfEOF() {
        synchronized (flushLSN) {
            while (readLSN >= flushLSN.get()) {
                if (isRecoveryMode) {
                    return ReturnState.EOF;
                }
                try {
                    if (IS_DEBUG_MODE) {
                        LOGGER.info("waitForFlushOrReturnIfEOF()| flushLSN: " + flushLSN.get() + ", readLSN: "
                                + readLSN);
                    }
                    flushLSN.wait();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
            return ReturnState.FLUSH;
        }
    }

    private void readNextPage() throws ACIDException {
        try {
            if (readLSN % logFileSize == ioManager.getSize(logFile)) {
                ioManager.close(logFile);
                readLSN += logFileSize - (readLSN % logFileSize);
                getFileChannel();
            }
            readPage();
        } catch (IOException e) {
            throw new ACIDException(e);
        }
    }

    private void readPage() throws ACIDException {
        try {
            ioManager.syncRead(logFile, ((long) (readLSN % logFileSize)), readBuffer);
        }catch(HyracksDataException e){
            throw new ACIDException(e);
        }
    }

    //for random reading
    @Override
    public ILogRecord read(long LSN) throws ACIDException {
        readLSN = LSN;
        synchronized (flushLSN) {
            while (readLSN >= flushLSN.get()) {
                try {
                    flushLSN.wait();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
        try {
            if (logFile == null) {
                getFileChannel();
                readPage();
            } else if (readLSN < fileBeginLSN || readLSN >= fileBeginLSN + ioManager.getSize(logFile)) {
                ioManager.close(logFile);
                getFileChannel();
                readPage();
            } else if (readLSN < bufferBeginLSN || readLSN >= bufferBeginLSN + readBuffer.limit()) {
                readPage();
            } else {
                readBuffer.position((int) (readLSN - bufferBeginLSN));
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
        if (!logRecord.readLogRecord(readBuffer)) {
            readNextPage();
            if (!logRecord.readLogRecord(readBuffer)) {
                throw new IllegalStateException();
            }
        }
        logRecord.setLSN(readLSN);
        readLSN += logRecord.getLogSize();
        return logRecord;
    }

    private void getFileChannel() throws ACIDException {
        logFile = logMgr.getLogFile(readLSN, false);
        fileBeginLSN = readLSN;
    }

    @Override
    public void close() throws ACIDException {
        try {
            if (logFile != null) {
                ioManager.close(logFile);
            }
        } catch (IOException e) {
            throw new ACIDException(e);
        }
    }
}
