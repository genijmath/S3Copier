package s3copy;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import java.io.IOException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

/**
 * Copyright 2014 Yevgen Yampolskiy
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class SimpleCopier {
    void run(String[] args) throws IOException {
        SCCmdLine cmdline = new SCCmdLine();
        SCCmdLine.Settings settings = cmdline.parse(args);

        String[] components = settings.input.split("/");
        String bucket = components[0];
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i < components.length; i++){
            sb.append(components[i]);
            sb.append("/");
        }
        sb.deleteCharAt(sb.length()-1);

        AmazonS3 s3client = new AmazonS3Client(new BasicAWSCredentials(settings.access_key_id, settings.secret_access_key));
        System.out.println("bucket: " + bucket);
        System.out.println("value: " + sb.toString());
        S3Object obj = s3client.getObject(bucket, sb.toString());
        InputStream is = obj.getObjectContent();

        OutputStream out = null;
        if (!settings.isURI()){
            //local copy
            out =  new FileOutputStream(settings.output);
        }else{
            Configuration conf = new Configuration();
            if (settings.conf != null){
                File _conf = new File(settings.conf);
                if (_conf.exists()){
                    if (_conf.isDirectory()){
                        conf.addResource(new Path(new File(settings.conf, "core-site.xml").getAbsolutePath()));
                    }else{
                        conf.addResource(new Path(settings.conf));
                    }
                }
            }

            FileSystem fs = FileSystem.get(conf);
            out = fs.create(new Path(settings.output));
        }
        IOUtils.copyLarge(is, out);
        out.close();
    }
    
    
    public static void main(String[] args) throws Exception {
        new SimpleCopier().run(args);
    }
}
