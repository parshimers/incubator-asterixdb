package org.apache.asterix.app.nc.task;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class RetrieveLibrariesTask implements INCLifecycleTask  {
    private static final Logger LOGGER = LogManager.getLogger();
    private final URI libraryURI;
    private String authToken;

    public RetrieveLibrariesTask(URI libraryURI, String authToken){
        this.libraryURI = libraryURI;
        this.authToken = authToken;
    }

    private void download(INcApplicationContext appCtx) throws HyracksException {
        File targetFile = appCtx.getLibraryManager().getLibraryDir();
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
    @Override
    public void perform(CcId ccId, IControllerService cs) {
        INcApplicationContext appContext = (INcApplicationContext) cs.getApplicationContext();
        download();
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\" }";
    }
}
