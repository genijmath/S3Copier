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

import junit.framework.Assert;
import org.junit.Test;


public class SCCmdLineTest {
    @Test
    public void testParseBad() throws Exception {
        try{
            new SCCmdLine(){
                @Override
                void quit(int ec){
                    System.out.println("EC: " + ec);
                    Assert.assertEquals(ec, 1);
                }
            }.parse(new String[]{"output"});
        }catch (ArrayIndexOutOfBoundsException ex){//IntelliJ marks this test as 'failed' by no reason
        }
    }

    @Test
    public void testParseGood() throws Exception {
        SCCmdLine.Settings settings = new SCCmdLine().parse(new String[]{
                "-c", "hadoop",
                "-k", "key1",
                "-s", "pw",
                "in",
                "out"});
        
        Assert.assertEquals(settings.access_key_id, "key1");
        Assert.assertEquals(settings.secret_access_key, "pw");
        Assert.assertEquals(settings.conf, "hadoop");
        Assert.assertEquals(settings.input, "in");
        Assert.assertEquals(settings.output, "out");
        Assert.assertFalse(settings.isURI());

        settings = new SCCmdLine().parse(new String[]{
                "-c", "hadoop",
                "-k", "key1",
                "-s", "pw",
                "in",
                "hdfs://out"});

        Assert.assertEquals(settings.access_key_id, "key1");
        Assert.assertEquals(settings.secret_access_key, "pw");
        Assert.assertEquals(settings.conf, "hadoop");
        Assert.assertEquals(settings.input, "in");
        Assert.assertEquals(settings.output, "hdfs://out");
        Assert.assertTrue(settings.isURI());
        
    }
    
}
