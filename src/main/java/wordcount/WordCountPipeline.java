//package wordcount;
//
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.io.Compression;
//import org.apache.beam.sdk.io.FileIO;
//import org.apache.beam.sdk.io.TextIO;
//import org.apache.beam.sdk.io.fs.MatchResult;
//import org.apache.beam.sdk.options.PipelineOptions;
//import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.transforms.*;
//import org.apache.beam.sdk.values.KV;
//import org.apache.beam.sdk.values.PCollection;
//
//import java.io.BufferedInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.nio.channels.Channels;
//import java.nio.channels.SeekableByteChannel;
//import java.nio.channels.WritableByteChannel;
//import java.util.Arrays;
//import java.util.List;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipInputStream;
//
//import static org.apache.beam.sdk.io.Compression.ZIP;
//
//public class WordCountPipeline {
//    public static class UnzipFN extends DoFn<String,Long>{
//        private long filesUnzipped=0;
//        @ProcessElement
//        public void processElement(ProcessContext c){
//            String p = c.element();
//            GcsUtilFactory factory = new GcsUtilFactory();
//            GcsUtil u = factory.create(c.getPipelineOptions());
//            byte[] buffer = new byte[100000000];
//            try{
//                SeekableByteChannel sek = u.open(GcsPath.fromUri(p));
//                InputStream is = Channels.newInputStream(sek);
//                BufferedInputStream bis = new BufferedInputStream(is);
//                ZipInputStream zis = new ZipInputStream(bis);
//                ZipEntry ze = zis.getNextEntry();
//                while(ze!=null){
//                    LOG.info("Unzipping File {}",ze.getName());
//                    WritableByteChannel wri = u.create(GcsPath.fromUri("gs://bucket_location/" + ze.getName()), getType(ze.getName()));
//                    OutputStream os = Channels.newOutputStream(wri);
//                    int len;
//                    while((len=zis.read(buffer))>0){
//                        os.write(buffer,0,len);
//                    }
//                    os.close();
//                    filesUnzipped++;
//                    ze=zis.getNextEntry();
//
//
//                }
//                zis.closeEntry();
//                zis.close();
//
//            }
//            catch(Exception e){
//                e.printStackTrace();
//            }
//            c.output(filesUnzipped);
//            System.out.println(filesUnzipped+"FilesUnzipped");
//            LOG.info("FilesUnzipped");
//        }
//
//        private String getType(String fName){
//            if(fName.endsWith(".zip")){
//                return "application/x-zip-compressed";
//            }
//            else {
//                return "text/plain";
//            }
//        }
//    }
//
//    public static void main(String... args) {
//        PipelineOptions options = PipelineOptionsFactory.create();
//        Pipeline pipeline = Pipeline.create(options);
////
////        PCollection<String> lines = pipeline.apply("read from file", TextIO.read().from("sample.txt"));
////        PCollection<List<String>> wordsperline = lines.apply(MapElements.via(new SimpleFunction<String, List<String>>() {
////            @Override
////            public List<String> apply(String input) {
////                return Arrays.asList(input.split(" "));
////            }
////        }));
////
////        PCollection<String> words = wordsperline.apply(Flatten.<String>iterables());
////
////        PCollection<KV<String, Long>> wordCount = words.apply(Count.<String>perElement());
////
////        wordCount.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
////            @Override
////            public String apply(KV<String, Long> input) {
////                return String.format("%s: %d", input.getKey(), input.getValue());
////            }
////        })).apply(TextIO.write().to("sampleoutput").withoutSharding());
//
//        PCollection<FileIO.ReadableFile> filesAndContents = pipeline
//                .apply(FileIO.match().filepattern("Archive.zip"))
//                .apply(FileIO.readMatches().withCompression(ZIP))
//
//                .apply(ParDo.of(new UnzipFn()));
//
//
//
//
//
//        pipeline.run().waitUntilFinish();
//
//    }
//
//
//}
