package org.apache.asterix.test.podman;

import java.io.IOException;
import java.net.URI;

import org.apache.asterix.app.external.IExternalUDFLibrarian;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PodmanUDFLibrarian implements IExternalUDFLibrarian {
    final GenericContainer<?> asterix;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public PodmanUDFLibrarian(GenericContainer asterix){
        OBJECT_MAPPER.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),true);
        this.asterix = asterix;
    }

    @Override
    public void install(URI path, String type, String libPath, Pair<String, String> credentials) throws Exception {
        Container.ExecResult curlResult = null;
        int retryCt = 0;
        while(retryCt < 3){
            try{
                curlResult = asterix.execInContainer("curl", "--no-progress-meter", "-X", "POST", "-u", credentials.first + ":" + credentials.second, "-F", "data=@" + "/var/tmp/asterix-app/" + libPath, "-F", "type=" + type, "http://localhost:19004" + path.getRawPath());
                handleResponse(curlResult);
                return;
            } catch(RuntimeException e){
                retryCt++;
                if(retryCt > 3) throw e;
            }
        }
    }

    @Override
    public void uninstall(URI path, Pair<String, String> credentials) throws IOException, AsterixException {
        try {
            Container.ExecResult curlResult = asterix.execInContainer("curl", "-X", "DELETE", "-u", credentials.first+":"+credentials.second, "http://localhost:19004"+path.getPath());
            handleResponse(curlResult);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void handleResponse(Container.ExecResult result) throws AsterixException, JsonProcessingException {
        if(result.getExitCode() != 0){
            throw new AsterixException(result.getStderr());
        }
        JsonNode resp = OBJECT_MAPPER.readTree(result.getStdout().replace('\0',' '));
        if(resp.has("error")){
            throw new AsterixException(resp.get("error").toString());
        }
        return;
    }
}
