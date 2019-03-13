package org.superbiz.moviefun.albums;

import org.apache.tika.Tika;
import org.apache.tika.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.superbiz.moviefun.blobstore.Blob;
import org.superbiz.moviefun.blobstore.BlobStore;
import org.superbiz.moviefun.blobstore.FileStore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static java.lang.ClassLoader.getSystemResource;
import static java.lang.String.format;
import static java.nio.file.Files.readAllBytes;

@Controller
@RequestMapping("/albums")
public class AlbumsController {

    private final AlbumsBean albumsBean;
    @Autowired
    private BlobStore blobStore;

    public AlbumsController(AlbumsBean albumsBean) {
        this.albumsBean = albumsBean;
    }


    @GetMapping
    public String index(Map<String, Object> model) {
        model.put("albums", albumsBean.getAlbums());
        return "albums";
    }

    @GetMapping("/{albumId}")
    public String details(@PathVariable long albumId, Map<String, Object> model) {
        model.put("album", albumsBean.find(albumId));
        return "albumDetails";
    }

    @PostMapping("/{albumId}/cover")
    public String uploadCover(@PathVariable long albumId, @RequestParam("file") MultipartFile uploadedFile) throws IOException {
        blobStore.put(new Blob(getCoverFileName(albumId),uploadedFile.getInputStream(),uploadedFile.getContentType()));
        return format("redirect:/albums/%d", albumId);
    }

    @GetMapping("/{albumId}/cover")
    public HttpEntity<byte[]> getCover(@PathVariable long albumId) throws IOException, URISyntaxException {
       // Path coverFilePath = getExistingCoverPath(albumId);
        Optional<Blob> blob=blobStore.get(getCoverFileName(albumId));

        if(!blob.isPresent()){
            ClassLoader classLoader = AlbumsController.class.getClassLoader();
            InputStream inputStream= classLoader.getResourceAsStream("default-cover.jpg");
            byte[] imagesBytes = IOUtils.toByteArray(inputStream);
            System.out.println("In Not Present default" + imagesBytes.length);
            HttpHeaders headers=createImageHttpHeaders("image/jpg",imagesBytes);
            return new HttpEntity<>(imagesBytes, headers);
        }else{
            Blob b=blobStore.get(getCoverFileName(albumId)).get();
            //byte[] imageBytes = readAllBytes(coverFilePath);
            byte[] imageBytes = IOUtils.toByteArray(b.getInputStream());
            System.out.println(b.getContentType());
            HttpHeaders headers = createImageHttpHeaders(b.contentType, imageBytes);
            return new HttpEntity<>(imageBytes, headers);
        }
    }

    private HttpHeaders createImageHttpHeaders(String contentType, byte[] imageBytes) throws IOException {
//        String contentType = new Tika().detect(coverFilePath);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType(contentType));
        headers.setContentLength(imageBytes.length);
        return headers;
    }

//    private File getCoverFile(@PathVariable long albumId) {
//        String coverFileName = format("covers/%d", albumId);
//        return new File(coverFileName);
//    }
    private String getCoverFileName(@PathVariable long albumId) {
        String coverFileName = format("covers/%d", albumId);
        return coverFileName;
    }

//    private Path getExistingCoverPath(@PathVariable long albumId) throws URISyntaxException {
//        File coverFile = getCoverFile(albumId);
//        Path coverFilePath;
//
//        if (coverFile.exists()) {
//            coverFilePath = coverFile.toPath();
//        } else {
//            coverFilePath = Paths.get(getSystemResource("default-cover.jpg").toURI());
//        }
//
//        return coverFilePath;
//    }
}
//    private void saveUploadToFile(@RequestParam("file") MultipartFile uploadedFile, File targetFile) throws IOException {
//        targetFile.delete();
//        targetFile.getParentFile().mkdirs();
//        targetFile.createNewFile();
//
//        try (FileOutputStream outputStream = new FileOutputStream(targetFile)) {
//            outputStream.write(uploadedFile.getBytes());
//        }
//    }