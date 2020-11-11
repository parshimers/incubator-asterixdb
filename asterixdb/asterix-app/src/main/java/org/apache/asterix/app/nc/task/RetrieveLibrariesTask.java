package org.apache.asterix.app.nc.task;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.hyracks.api.util.IoUtil.flushDirectory;

public class RetrieveLibrariesTask implements INCLifecycleTask  {
    private static final Logger LOGGER = LogManager.getLogger();
    private final URI libraryURI;
    private String authToken;
    private byte[] copyBuffer;

    public RetrieveLibrariesTask(URI libraryURI, String authToken){
        this.libraryURI = libraryURI;
        this.authToken = authToken;
    }

    private void download(INcApplicationContext appCtx, FileReference targetFile) throws HyracksException {
        try {
            targetFile.getFile().createNewFile();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        IIOManager ioManager = appCtx.getIoManager();
        IFileHandle fHandle = ioManager.open(targetFile, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        try {
            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            try {
                // retry 10 times at maximum for downloading binaries
                HttpGet request = new HttpGet(libraryURI);
                request.setHeader(HttpHeaders.AUTHORIZATION, authToken);
                int tried = 0;
                Exception trace = null;
                while (tried < 30) {
                    tried++;
                    CloseableHttpResponse response = null;
                    try {
                        response = httpClient.execute(request);
                        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                            throw new IOException("Http Error: " + response.getStatusLine().getStatusCode());
                        }
                        HttpEntity e = response.getEntity();
                        if (e == null) {
                            throw new IOException("No response");
                        }
                        WritableByteChannel outChannel = ioManager.newWritableChannel(fHandle);
                        OutputStream outStream = Channels.newOutputStream(outChannel);
                        e.writeTo(outStream);
                        outStream.flush();
                        ioManager.sync(fHandle, true);
                        return;
                    } catch (IOException e) {
                        LOGGER.error("Unable to download library", e);
                        trace = e;
                        try {
                            ioManager.truncate(fHandle, 0);
                        } catch (IOException e2) {
                            throw HyracksDataException.create(e2);
                        }
                    } finally {
                        if (response != null) {
                            try {
                                response.close();
                            } catch (IOException e) {
                                LOGGER.warn("Failed to close", e);
                            }
                        }
                    }
                }

                throw HyracksDataException.create(trace);
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    LOGGER.warn("Failed to close", e);
                }
            }
        } finally {
            try {
                ioManager.close(fHandle);
            } catch (HyracksDataException e) {
                LOGGER.warn("Failed to close", e);
            }
        }
    }


    private void unzip(FileReference sourceFile, INcApplicationContext appCtx) throws IOException {
        boolean logTraceEnabled = LOGGER.isTraceEnabled();
        Set<Path> newDirs = new HashSet<>();
        Path outputDirPath = appCtx.getLibraryManager().getStorageDir().getFile().toPath().toAbsolutePath().normalize();
        try (ZipFile zipFile = new ZipFile(sourceFile.getFile())) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.isDirectory()) {
                    continue;
                }
                Path entryOutputPath = outputDirPath.resolve(entry.getName()).toAbsolutePath().normalize();
                if (!entryOutputPath.startsWith(outputDirPath)) {
                    throw new IOException("Malformed ZIP archive: " + entry.getName());
                }
                Path entryOutputDir = entryOutputPath.getParent();
                Files.createDirectories(entryOutputDir);
                // remember new directories so we can flush them later
                for (Path p = entryOutputDir; !p.equals(outputDirPath); p = p.getParent()) {
                    newDirs.add(p);
                }
                try (InputStream in = zipFile.getInputStream(entry)) {
                    FileReference entryOutputFileRef =
                            appCtx.getIoManager().resolveAbsolutePath(entryOutputPath.toString());
                    if (logTraceEnabled) {
                        LOGGER.trace("Extracting file {}", entryOutputFileRef);
                    }
                    writeAndForce(entryOutputFileRef, in, appCtx.getIoManager());
                }
            }
        }
        for (Path newDir : newDirs) {
            flushDirectory(newDir);
        }
    }

    private void writeAndForce(FileReference outputFile, InputStream dataStream, IIOManager ioManager) throws IOException {
        outputFile.getFile().createNewFile();
        IFileHandle fHandle = ioManager.open(outputFile, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        try {
            WritableByteChannel outChannel = ioManager.newWritableChannel(fHandle);
            OutputStream outputStream = Channels.newOutputStream(outChannel);
            IOUtils.copyLarge(dataStream, outputStream, getCopyBuffer());
            outputStream.flush();
            ioManager.sync(fHandle, true);
        } finally {
            ioManager.close(fHandle);
        }
    }

    private byte[] getCopyBuffer() {
        if (copyBuffer == null) {
            copyBuffer = new byte[4096];
        }
        return copyBuffer;
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) {
        INcApplicationContext appContext = (INcApplicationContext) cs.getApplicationContext();
        FileReference targetFile = appContext.getLibraryManager().getStorageDir().getChild("replicate.zip");
        try {
            download(appContext, targetFile);
            unzip(targetFile,appContext);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\" }";
    }
}
