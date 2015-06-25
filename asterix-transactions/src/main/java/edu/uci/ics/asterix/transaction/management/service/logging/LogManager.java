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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILogManager;
import edu.uci.ics.asterix.common.transactions.ILogReader;
import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.LogManagerProperties;
import edu.uci.ics.asterix.common.transactions.LogType;
import edu.uci.ics.asterix.common.transactions.MutableLong;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

public class LogManager implements ILogManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;// true
    private static final Logger LOGGER = Logger.getLogger(LogManager.class.getName());
    private final TransactionSubsystem txnSubsystem;

    private final LogManagerProperties logManagerProperties;
    private final long logFileSize;
    private final int logPageSize;
    private final int numLogPages;
    private final String logDir;
    private final String logFilePrefix;
    private final MutableLong flushLSN;
    public final IIOManager ioManager;
    private LinkedBlockingQueue<LogPage> emptyQ;
    private LinkedBlockingQueue<LogPage> flushQ;
    private final AtomicLong appendLSN;
    private IFileHandle currentLogFile;
    private LogPage appendPage;
    private LogFlusher logFlusher;
    private Future<Object> futureLogFlusher;

    public LogManager(TransactionSubsystem txnSubsystem) throws ACIDException {
        this.txnSubsystem = txnSubsystem;

        ioManager = this.txnSubsystem.getAsterixAppRuntimeContextProvider().getIOManager();
        logManagerProperties = new LogManagerProperties(this.txnSubsystem.getTransactionProperties(),
                this.txnSubsystem.getId());
        logFileSize = logManagerProperties.getLogPartitionSize();
        logPageSize = logManagerProperties.getLogPageSize();
        numLogPages = logManagerProperties.getNumLogPages();
        logDir = logManagerProperties.getLogDir();
        logFilePrefix = logManagerProperties.getLogFilePrefix();
        flushLSN = new MutableLong();
        appendLSN = new AtomicLong();
        initializeLogManager(0);
    }

    private void initializeLogManager(long nextLogFileId) {
        emptyQ = new LinkedBlockingQueue<LogPage>(numLogPages);
        flushQ = new LinkedBlockingQueue<LogPage>(numLogPages);
        for (int i = 0; i < numLogPages; i++) {
            emptyQ.offer(new LogPage(txnSubsystem, logPageSize, flushLSN, ioManager));
        }
        appendLSN.set(initializeLogAnchor(nextLogFileId));
        flushLSN.set(appendLSN.get());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("LogManager starts logging in LSN: " + appendLSN);
        }
        currentLogFile = getLogFile(appendLSN.get(), false);
        getAndInitNewPage();
        logFlusher = new LogFlusher(this, emptyQ, flushQ);
        futureLogFlusher = txnSubsystem.getAsterixAppRuntimeContextProvider().getThreadExecutor().submit(logFlusher);
    }
    
    @Override
    public void log(ILogRecord logRecord) throws ACIDException {
        if (logRecord.getLogSize() > logPageSize) {
            throw new IllegalStateException();
        }

        syncLog(logRecord);
        
        if ((logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT)
                && !logRecord.isFlushed()) {
            synchronized (logRecord) {
                while (!logRecord.isFlushed()) {
                    try {
                        logRecord.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }
        }
    }

    private synchronized void syncLog(ILogRecord logRecord) throws ACIDException {
        ITransactionContext txnCtx = null;
        
        if(logRecord.getLogType() != LogType.FLUSH)
        {       
            txnCtx = logRecord.getTxnCtx();
            if (txnCtx.getTxnState() == ITransactionManager.ABORTED && logRecord.getLogType() != LogType.ABORT) {
                throw new ACIDException("Aborted job(" + txnCtx.getJobId() + ") tried to write non-abort type log record.");
            }

        }
        if (getLogFileOffset(appendLSN.get()) + logRecord.getLogSize() > logFileSize) {
            prepareNextLogFile();
            appendPage.isFull(true);
            getAndInitNewPage();
        } else if (!appendPage.hasSpace(logRecord.getLogSize())) {
            appendPage.isFull(true);
            getAndInitNewPage();
        }
        if (logRecord.getLogType() == LogType.UPDATE) {
            logRecord.setPrevLSN(txnCtx.getLastLSN());
        }
        appendPage.append(logRecord, appendLSN.get());
        
        if(logRecord.getLogType() == LogType.FLUSH)
        {
            logRecord.setLSN(appendLSN.get());
        }
        appendLSN.addAndGet(logRecord.getLogSize());
    }

    private void getAndInitNewPage() {
        appendPage = null;
        while (appendPage == null) {
            try {
                appendPage = emptyQ.take();
            } catch (InterruptedException e) {
                //ignore
            }
        }
        appendPage.reset();
        appendPage.setFileHandle(currentLogFile);
        flushQ.offer(appendPage);
    }

    private void prepareNextLogFile() {
        appendLSN.addAndGet(logFileSize - getLogFileOffset(appendLSN.get()));
        currentLogFile = getLogFile(appendLSN.get(), true);
        appendPage.isLastPage(true);
        //[Notice]
        //the current log file channel is closed if 
        //LogPage.flush() completely flush the last page of the file.
    }

    @Override
    public ILogReader getLogReader(boolean isRecoveryMode) {
        return new LogReader(this, logFileSize, logPageSize, flushLSN, isRecoveryMode);
    }

    public LogManagerProperties getLogManagerProperties() {
        return logManagerProperties;
    }

    public TransactionSubsystem getTransactionSubsystem() {
        return txnSubsystem;
    }

    @Override
    public long getAppendLSN() {
        return appendLSN.get();
    }

    @Override
    public void start() {
        // no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        terminateLogFlusher();
        if (dumpState) {
            dumpState(os);
        }
    }

    @Override
    public void dumpState(OutputStream os) {
        // #. dump Configurable Variables
        dumpConfVars(os);

        // #. dump LSNInfo
        dumpLSNInfo(os);
    }

    private void dumpConfVars(OutputStream os) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("\n>>dump_begin\t>>----- [ConfVars] -----");
            sb.append(logManagerProperties.toString());
            sb.append("\n>>dump_end\t>>----- [ConfVars] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            // ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }

    private void dumpLSNInfo(OutputStream os) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("\n>>dump_begin\t>>----- [LSNInfo] -----");
            sb.append("\nappendLsn: " + appendLSN);
            sb.append("\nflushLsn: " + flushLSN.get());
            sb.append("\n>>dump_end\t>>----- [LSNInfo] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            // ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }

    public MutableLong getFlushLSN() {
        return flushLSN;
    }

    private long initializeLogAnchor(long nextLogFileId) {
        long fileId = 0;
        long offset = 0;
        FileReference fileLogDir = new FileReference(logDir, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        try {
            if (ioManager.exists(fileLogDir)) {
                List<Long> logFileIds = getLogFileIds();
                if (logFileIds == null) {
                    fileId = nextLogFileId;
                    FileReference newFile = new FileReference(getLogFilePath(fileId), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
//                    ioManager.mkdirs(newFile);
                    IFileHandle touch = ioManager.open(newFile, IIOManager.FileReadWriteMode.READ_WRITE, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    ioManager.close(touch);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("created a log file: " + getLogFilePath(fileId));
                    }
                } else {
                    fileId = logFileIds.get(logFileIds.size() - 1);
                    FileReference logFile = new FileReference(getLogFilePath(fileId), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                    IFileHandle logHandle = ioManager.open(logFile, IIOManager.FileReadWriteMode.READ_ONLY, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    offset = ioManager.getSize(logHandle);
                    ioManager.close(logHandle);
                }
            } else {
                fileId = nextLogFileId;
                ioManager.mkdirs(new FileReference(logDir, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL));
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("created the log directory: " + logManagerProperties.getLogDir());
                }
                FileReference newFile = new FileReference(getLogFilePath(fileId), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
//                ioManager.mkdirs(newFile);
                IFileHandle touch = ioManager.open(newFile, IIOManager.FileReadWriteMode.READ_WRITE, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                ioManager.close(touch);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("created a log file: " + getLogFilePath(fileId));
                }
            }
        } catch (IOException ioe) {
            throw new IllegalStateException("Failed to initialize the log anchor", ioe);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("log file Id: " + fileId + ", offset: " + offset);
        }
        return logFileSize * fileId + offset;
    }

    public void renewLogFiles() throws IOException {
        terminateLogFlusher();
        long lastMaxLogFileId = deleteAllLogFiles();
        initializeLogManager(lastMaxLogFileId + 1);
    }

    public void deleteOldLogFiles(long checkpointLSN) throws HyracksDataException {

        Long checkpointLSNLogFileID = getLogFileId(checkpointLSN);
        List<Long> logFileIds = getLogFileIds();
        for (Long id : logFileIds) {
            if(id < checkpointLSNLogFileID)
            {
                FileReference file = new FileReference(getLogFilePath(id), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                if (!ioManager.delete(file)) {
                    throw new IllegalStateException("Failed to delete a file: " + file.getPath());
                }
            }
        }
    }
    
    private void terminateLogFlusher() {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Terminating LogFlusher thread ...");
        }
        logFlusher.terminate();
        try {
            futureLogFlusher.get();
        } catch (ExecutionException | InterruptedException e) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("---------- warning(begin): LogFlusher thread is terminated abnormally --------");
                e.printStackTrace();
                LOGGER.info("---------- warning(end)  : LogFlusher thread is terminated abnormally --------");
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("LogFlusher thread is terminated.");
        }
    }

    private long deleteAllLogFiles() throws HyracksDataException {
        if (currentLogFile != null) {
            try {
                ioManager.close(currentLogFile);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to close a fileChannel of a log file");
            }
        }
        List<Long> logFileIds = getLogFileIds();
        for (Long id : logFileIds) {
            FileReference file = new FileReference(getLogFilePath(id), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            if (!ioManager.delete(file)) {
                throw new IllegalStateException("Failed to delete a file: " + file.getPath());
            }
        }
        return logFileIds.get(logFileIds.size() - 1);
    }

    private List<Long> getLogFileIds() throws HyracksDataException {
        FileReference fileLogDir = new FileReference(logDir, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        String[] logFileNames;
        List<Long> logFileIds = null;
        if (ioManager.exists(fileLogDir)) {
            logFileNames = ioManager.listFiles(fileLogDir, new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name.startsWith(logFilePrefix)) {
                        return true;
                    }
                    return false;
                }
            });
            if (logFileNames != null && logFileNames.length != 0) {
                logFileIds = new ArrayList<Long>();
                for (String fileName : logFileNames) {
                    logFileIds.add(Long.parseLong(fileName.substring(logFilePrefix.length() + 1)));
                }
                Collections.sort(logFileIds, new Comparator<Long>() {
                    @Override
                    public int compare(Long arg0, Long arg1) {
                        return arg0.compareTo(arg1);
                    }
                });
            }
        }
        return logFileIds;
    }

    public String getLogFilePath(long fileId) {
        return logDir + File.separator + logFilePrefix + "_" + fileId;
    }

    public long getLogFileOffset(long lsn) {
        return lsn % logFileSize;
    }

    public long getLogFileId(long lsn) {
        return lsn / logFileSize;
    }

    public IFileHandle getLogFile(long lsn, boolean create) {
        IFileHandle handle = null;
        try {
            long fileId = getLogFileId(lsn);
            String logFilePath = getLogFilePath(fileId);
            FileReference file = new FileReference(logFilePath, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            if (create) {
                if (!ioManager.mkdirs(file)) {
                    throw new IllegalStateException();
                }
            } else {
                if (!ioManager.exists(file)) {
                    throw new IllegalStateException();
                }
            }
            handle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_WRITE, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return handle;
    }

    public long getReadableSmallestLSN() throws HyracksDataException {
        List<Long> logFileIds = getLogFileIds();
        return logFileIds.get(0) * logFileSize;
    }
}

class LogFlusher implements Callable<Boolean> {
    private static final Logger LOGGER = Logger.getLogger(LogFlusher.class.getName());
    private final static LogPage POISON_PILL = new LogPage(null, ILogRecord.JOB_TERMINATE_LOG_SIZE, null, null);
    private final LogManager logMgr;//for debugging
    private final LinkedBlockingQueue<LogPage> emptyQ;
    private final LinkedBlockingQueue<LogPage> flushQ;
    private LogPage flushPage;
    private final AtomicBoolean isStarted;
    private final AtomicBoolean terminateFlag;
    private final IIOManager ioManager;

    public LogFlusher(LogManager logMgr, LinkedBlockingQueue<LogPage> emptyQ, LinkedBlockingQueue<LogPage> flushQ) {
        this.logMgr = logMgr;
        this.ioManager = logMgr.ioManager;
        this.emptyQ = emptyQ;
        this.flushQ = flushQ;
        flushPage = null;
        isStarted = new AtomicBoolean(false);
        terminateFlag = new AtomicBoolean(false);

    }

    public void terminate() {
        //make sure the LogFlusher thread started before terminating it.
        synchronized (isStarted) {
            while (!isStarted.get()) {
                try {
                    isStarted.wait();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }

        terminateFlag.set(true);
        if (flushPage != null) {
            synchronized (flushPage) {
                flushPage.isStop(true);
                flushPage.notify();
            }
        }
        //[Notice]
        //The return value doesn't need to be checked
        //since terminateFlag will trigger termination if the flushQ is full.
        flushQ.offer(POISON_PILL);
    }

    @Override
    public Boolean call() {
        synchronized (isStarted) {
            isStarted.set(true);
            isStarted.notify();
        }
        try {
            while (true) {
                flushPage = null;
                try {
                    flushPage = flushQ.take();
                    if (flushPage == POISON_PILL || terminateFlag.get()) {
                        return true;
                    }
                } catch (InterruptedException e) {
                    if (flushPage == null) {
                        continue;
                    }
                }
                flushPage.flush();
                emptyQ.offer(flushPage);
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("-------------------------------------------------------------------------");
                LOGGER.info("LogFlusher is terminating abnormally. System is in unusalbe state.");
                LOGGER.info("-------------------------------------------------------------------------");
            }
            e.printStackTrace();
            throw e;
        }
    }
}