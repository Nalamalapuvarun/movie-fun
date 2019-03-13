package org.superbiz.moviefun;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.tika.Tika;
import org.apache.tika.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.superbiz.moviefun.blobstore.Blob;
import org.superbiz.moviefun.blobstore.BlobStore;

import java.io.ByteArrayInputStream;
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

public class S3Store implements BlobStore {
    private AmazonS3Client s3Client;
    private String photoStorageBucket;

    public S3Store(AmazonS3Client s3Client, String photoStorageBucket) {
        this.s3Client = s3Client;
        this.photoStorageBucket = photoStorageBucket;
    }


    @Override
    public void put(Blob blob) throws IOException {
        //FixMe: do we have to copy the input stream to a file and then later store that in S3 Bucket
//        File file=new File(blob.getName());
//        java.nio.file.Files.copy(blob.getInputStream(),file.toPath(), StandardCopyOption.REPLACE_EXISTING);
        //FIXME: is this what we have to be doing?
        ObjectMetadata objectMetadata = new ObjectMetadata();
        s3Client.putObject(photoStorageBucket,blob.getName(),blob.getInputStream(),objectMetadata);
        //s3Client.putObject(photoStorageBucket,blob.getName(),file);

    }

    @Override
    public Optional<Blob> get(String name) throws IOException, URISyntaxException {
//        File coverFile = new File(name);
//        Path coverFilePath;
//        if (coverFile.exists()) {
//            coverFilePath = coverFile.toPath();
//        } else {
//            coverFilePath = Paths.get(getSystemResource("default-cover.jpg").toURI());
//        }
//        InputStream inputStream = Files.newInputStream(coverFilePath);
//        return Optional.ofNullable(new Blob(name,inputStream,new Tika().detect(coverFilePath)));

       if(!s3Client.doesObjectExist(photoStorageBucket,name)) {
           return Optional.empty();
       }
        InputStream fromS3 =s3Client.getObject(photoStorageBucket,name).getObjectContent();
        byte[] imageBytes = IOUtils.toByteArray(fromS3);
        InputStream input= new ByteArrayInputStream(imageBytes);
       // FIXME: Check if this is correct and determine the contentType
        return Optional.ofNullable(new Blob(name,input,new Tika().detect(imageBytes)));

    }

    @Override
    public void deleteAll() {

    }
}
