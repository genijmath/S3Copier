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


package s3copy;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;

public class SCCmdLine {
    private static Log log = LogFactory.getLog(SCCmdLine.class);
    

    static class Settings{
        static class SettingsException extends RuntimeException{
            public SettingsException(String s) {
                super(s);
            }
        }
        
        String conf;
        String access_key_id;
        String secret_access_key;
        String input;
        String output;
        private boolean is_uri;
        
        private Settings(){}
        
        static Settings getInstance(String conf, String access_key_id, String secret_access_key, String input, String output){
            Settings settings = new Settings();
            settings.is_uri = (output.contains(":"));
            if (settings.is_uri && conf == null)
                throw new SettingsException("Hadoop configuration is missing, specify --conf option"); 
            settings.conf = conf;
            settings.access_key_id = access_key_id;
            settings.secret_access_key = secret_access_key;
            settings.input = input;
            settings.output = output;
            
            if (settings.access_key_id == null)
                throw new SettingsException("AWS access key id is not provided");
            if (settings.secret_access_key == null)
                throw new SettingsException("AWS secret access key is not provided");
            if (input == null || input.trim().length() == 0)
                throw new SettingsException("Input folder is blank?");
            if (output == null || output.trim().length() == 0)
                throw new SettingsException("Output folder is blank?");
            

            if (input.startsWith("/")){
                throw new SettingsException("input string must start with bucket name (no /)");
            }
            
            String[] components = settings.input.split("/");
            if (components.length < 2){
                throw new SettingsException("Input path must use '/' as a separator with bucket name being the root element");
            }
            
            return settings;
        }
        
        boolean isURI(){
            return is_uri;
        }
    }

    private static final String cmdLineSyntax = "s3copy.SimpleCopier " +
            "[-c path2cfg] [-k key] [-s secret]  <input> <output>\n"; 
    private static final String header = ""; 
    private static final String footer =
                "\nThe following environmental variables provide default values for " + 
                "--conf/--access-key-id/--secret-access-key: " + 
                "HADOOP_CONF_DIR/AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY";

    Settings parse(String[] args){
        CommandLineParser parser = new GnuParser();

        // create the Options
        Options options = new Options();
        
        options.addOption(
                OptionBuilder.withLongOpt("conf")
                        .withDescription("Hadoop configuration files location;" +
                                "required if URI is used as an output")
                        .hasArg()
                        .withArgName("PATH")
                        .create('c'));
        options.addOption(
                OptionBuilder.withLongOpt("access-key-id")
                        .withDescription( "AWS Access key ID;" +
                                "required if AWS_ACCESS_KEY_ID environmental variable is not set" )
                        .hasArg()
                        .withArgName("KEY_ID")
                        .create('k'));

        options.addOption(
                OptionBuilder.withLongOpt("secret-access-key")
                        .withDescription( "AWS secret access key" )
                        .hasArg()
                        .withArgName("SECRET")
                        .create('s'));

        String conf = null;
        String access_key = null;
        String secret_access_key = null;
        String[] args_left = null;
        
        try {
            // parse the command line arguments
            CommandLine cmdline = parser.parse( options, args );

            args_left = cmdline.getArgs();
            if (args_left.length != 2){
                throw new ParseException("Must provide two required argument: <input> and <output>\nFound: " + Arrays.toString(args_left));
            }
            
            if (args_left[0] == null || args_left[0].trim().length() == 0)
                throw new ParseException("<input> is blank?");
            
            if (args_left[1] == null || args_left[1].trim().length() == 0)
                throw new ParseException("<output> is blank?");            
            
            if (cmdline.hasOption("conf"))
                conf = cmdline.getOptionValue("conf");
            if (cmdline.hasOption("access-key-id"))
                access_key = cmdline.getOptionValue("access-key-id");
            if (cmdline.hasOption("secret-access-key"))
                secret_access_key = cmdline.getOptionValue("secret-access-key");

            if (conf == null)
                conf = getenv("HADOOP_CONF_DIR");
            if (access_key == null)
                access_key = getenv("AWS_ACCESS_KEY_ID");
            if (secret_access_key == null)
                secret_access_key = getenv("AWS_SECRET_ACCESS_KEY");

            if (access_key == null)
                throw new ParseException("--access-key-id must be provided, or AWS_ACCESS_KEY_ID must be set");

            if (secret_access_key == null)
                throw new ParseException("--secret-access-key must be provided, or AWS_SECRET_ACCESS_KEY must be set");            
        }
        catch( ParseException exp ) {
            getErrStream().println( "Incorrect usage:\n" + exp.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            PrintWriter pw = new PrintWriter(getOutStream());
            formatter.printHelp(pw, 80, cmdLineSyntax, header,options, 3, 3, footer);
            pw.flush();
            pw.close();
            
            quit(1);
        }

        args_left[0] = args_left[0].trim();
        args_left[1] = args_left[1].trim();
        
        log.debug("--conf: " + conf);
        log.debug("input: " + args_left[0]);
        log.debug("output: " + args_left[1]);
        return Settings.getInstance(conf, access_key, secret_access_key, args_left[0], args_left[1]);
    }

    void quit(int ec) {
        System.exit(ec);
    }
    
    PrintStream getOutStream(){
        return System.out;
    }

    PrintStream getErrStream(){
        return System.err;
    }

    String getenv(String key) {
        return System.getenv(key);
    }


}

