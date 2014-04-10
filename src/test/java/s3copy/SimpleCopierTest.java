package s3copy;

import org.junit.Test;

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
public class SimpleCopierTest {
    @Test
    public void testRun01() throws Exception {
        SimpleCopier sc = new SimpleCopier();
        sc.run(new String[]{
                "yevgen.us.data/test/maxim.jpg", "local.jpg"
        });
    }

    @Test
    public void testRun02() throws Exception {
        SimpleCopier sc = new SimpleCopier();
        sc.run(new String[]{"--conf", "/etc/hadoop/conf",
                "yevgen.us.data/test/maxim.jpg", "hdfs:///user/yevgen/Data/maxim.jpg"
        });
    }

}
