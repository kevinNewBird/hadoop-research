# 说明
自定义输出格式。<br/>
自定义OutputFormat及使用自定义输出格式步骤如下：
- 1).自定义类继承FileOutputFormat并实现getRecordWriter方法

- 2).在getRecordWriter方法中返回自定义RecordWriter类，该类需要集成RecordWriter对象实现对应的数据写出逻辑。

- 3).在Driver中设置“job.setOutputFormatClass(YourOutputFormat.class)”使用自定义outputFormat。