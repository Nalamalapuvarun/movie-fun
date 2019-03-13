package org.superbiz.moviefun.blobstore;

import org.apache.tika.Tika;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

import static java.lang.ClassLoader.getSystemResource;

public class FileStore implements BlobStore {

    @Override
    public void put(Blob blob) throws IOException {
        File targetFile = new File(blob.getName());
        targetFile.delete();
        targetFile.getParentFile().mkdirs();
        targetFile.createNewFile();
        java.nio.file.Files.copy(blob.getInputStream(),targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public Optional<Blob> get(String name) throws IOException, URISyntaxException {
        File coverFile = new File(name);
        Path coverFilePath;
        if (coverFile.exists()) {
            coverFilePath = coverFile.toPath();
        } else {
            coverFilePath = Paths.get(getSystemResource("default-cover.jpg").toURI());
        }
        InputStream inputStream = Files.newInputStream(coverFilePath);
        return Optional.ofNullable(new Blob(name,inputStream,new Tika().detect(coverFilePath)));
    }

    @Override
    public void deleteAll() {

    }


}
