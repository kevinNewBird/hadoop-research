package com.pomelo.mapjoiner;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * description：map阶段同时处理person.txt和address.txt数据
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2024/12/17 22:48
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final String template = "id={0},name={1},age={2},address={3}";

    Map<String, String> addressMap = new HashMap<>();

    private Text outputKey = new Text();

    /**
     * 缓存数据(在map方法调用前执行)
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 1.获取缓存文件的路径
        URI[] cacheFiles = context.getCacheFiles();
        if (null == cacheFiles) {
            return;
        }
        for (URI file : cacheFiles) {
            BufferedReader br = new BufferedReader(new FileReader(new File(file.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = StringUtils.split(line, ",");
                addressMap.put(words[0], words[1]);
            }
            br.close();
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // person.txt行数据：1,张三,18
        String line = value.toString();
        String[] words = StringUtils.split(line, ",");
        String id = words[0];
        String name = words[1];
        Integer age = Integer.valueOf(words[2]);
        String address = addressMap.get(id);

        if (StringUtils.isNoneBlank(address)) {
            outputKey.set(MessageFormat.format(template, id, name, age, address));
            context.write(outputKey, NullWritable.get());
        }
    }


}
