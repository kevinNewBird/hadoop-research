# 0.说明
在MapReduce中，压缩是一项常见的优化技术，用于减少数据在存储和传输过程中所占用的空间。<br/>
通过对输入、中间和输出数据进行压缩，可以有效降低存储成本、减少网络传输开销，并提高整体计算效率。<br/>
MapReduce中支持的常见的压缩算法如下：

| **压缩格式** | **算法实现** | **扩展名**   | **压缩比率** | **速度** | **可切分** | **Native** | **描述**                                               |
| ------------ | ------------ | ------------ | ------------ | -------- | ---------- | ---------- | ------------------------------------------------------ |
| bzip2        | bzip2        | .bz2         | 最高         | 最慢     | Yes        | Yes        | 压缩率最高、压缩/解压缩效率最慢                        |
| deflate      | deflate      | .deflate     | 高           | 慢       | No         | No         | 标准压缩算法                                           |
| gzip         | deflate      | .gz          | 高           | 慢       | No         | Yes        | 相比deflate增加文件头、尾，压缩比较高，压缩/解压效率慢 |
| snappy       | snappy       | .snappy      | 低           | 快       | No         | Yes        | 压缩率较低，压缩/解压缩效率最快                        |
| lzo          | lzo          | .lzo_deflate | 低           | 快       | Yes        | No         | 压缩率较低，压缩/解压缩效率最快                        |
| lz4          | lz4          | .lz4         | 最低         | 最快     | No         | No         | 压缩率较低，压缩/解压缩效率最快                        |

以上各种压缩格式中，压缩比和压缩性能比较如下：

* 压缩比率对比: bzip2 > gzip > snappy > lzo > lz4，bzip2压缩比可以达到8:1;gzip压缩比可以达到5比1;lzo可以达到3:1。
* 压缩性能对比：lz4 > lzo > snappy > gzip>bzip2 ，lzo压缩速度可达约50M/s,解压速度可达约70M/s;gzip速度约为20M/s,解压速度约为60M/s;bzip2压缩速度约为2.5M/s，解压速度约为9.5M/s。

在MapReduce中使用压缩格式有如下建议：

1) MapReduce中读取数据后在mapper中可以输出压缩格式中间结果数据，常用的压缩格式是snappy和lzo格式，这两种格式压缩/解压缩比例相对较快，Reduce端输出结果数据也可以输出压缩格式数据，如果输出数据量小，也可以选择snappy/lzo格式，如果输出数据结果较大，可以选择bzip2压缩格式。

2) 如果MapReduce输出的结果作为下一个MapReduce的输入，还需要考虑对应的压缩格式是否支持Split。

3) MapReduce中对于运算密集型Job建议少用压缩，对于IO密集型操作，建议可以使用压缩。
# 1.数据压缩使用

不同的压缩/解压缩算法在Hadoop中使用时需要有对应的编/解码器类，这样在Hadoop中就可以进行数据的压缩和解压缩操作，如下是每种压缩算法在Hadoop中对应的编码/解码类：

* bzip2编码/解码器类：org.apache.hadoop.io.compress.BZip2Codec
* DEFLATE编码/解码器类： org.apache.hadoop.io.compress.DefaultCodec
* gzip编码/解码器类： org.apache.hadoop.io.compress.GzipCodec
* Snappy编码/解码器类： org.apache.hadoop.io.compress.SnappyCodec
* LZO编码/解码器类： com.hadoop.compression.lzo.LzopCodec
* Lz4编码/解码器类：org.apache.hadoop.io.compress.Lz4Codec

# 3.数据压缩使用
在MapReduce中mapper端和reduce端写出数据都支持压缩格式，在Mapper端写出压缩数据需要在Driver代码中设置map端的压缩格式：

```
//1.获取配置信息及job对象
Configuration conf = new Configuration();
// 开启map端输出压缩
conf.setBoolean("mapreduce.map.output.compress", true);

// 设置map端输出压缩方式
//conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
//conf.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);
//conf.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, DefaultCodec.class);
//conf.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);
//conf.setClass("mapreduce.map.output.compress.codec", LzopCodec.class, LzoCodec.class);
conf.setClass("mapreduce.map.output.compress.codec", Lz4Codec.class, CompressionCodec.class);

Job job = Job.getInstance(conf);
... ...
```

以上conf.setClass(name,theClass,xface)三个参数分别表示压缩格式配置参数名称、压缩格式类名、压缩格式类名父接口。

在Reducer端写出压缩数据同样需要在Driver代码中设置reduce端的压缩格式：

```
// 设置reduce端输出压缩开启
FileOutputFormat.setCompressOutput(job, true);

// 设置压缩的方式
//FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
//FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);
```

需要注意的是Lzo压缩格式需要在Hadoop中安装对应的lzo本地库，否则不能使用。

下面以Snappy压缩格式为例来演示MapReduce中如何使用压缩格式。如下是WordCount代码，MapReduce读取HDFS中的数据文件进行统计后输出压缩格式，Mapper端和Reducer端都设置为Snappy格式。

1) WordCount案例中Mapper和Reducer端代码都不变

2) 在Driver中开启Mapper端和Reducer端压缩配置并指定对应的压缩格式