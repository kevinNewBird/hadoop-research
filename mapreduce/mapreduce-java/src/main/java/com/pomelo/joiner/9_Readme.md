# 说明
join操作，注意其是在reduce阶段做的join。这个过程涉及到数据的Shuffle，与之对应的还有map阶段的join，其效率要更高<br/>
person.txt 记录了用户的基本信息：

```
1,张三,18
2,李四,19
3,王五,20
4,马六,21
5,田七,22
6,高八,23
```

address.txt记录了用户的地址信息：

```
1,北京
2,上海
3,深圳
4,广州
5,天津
7,杭州
```

现在需要通过mapReduce输出如下Join结果数据：

```
id='1', name='张三', age=18, address='北京'
id='2', name='李四', age=19, address='上海'
id='3', name='王五', age=20, address='深圳'
id='4', name='马六', age=21, address='广州'
id='5', name='田七', age=22, address='天津'
```

要想通过MapReduce实现以上需求，那么这里涉及到MapReduce读取多个源头文件问题，在MapReduce编程中Driver支持设置一个MapReduce Job设置多个Mapper，通过“MultipleInputs.addInputPath（...）”进行设置,方式如下:

```
MultipleInputs.addInputPath(job, new Path("data/person.txt"), TextInputFormat.class, PersonInfoMapper.class);
MultipleInputs.addInputPath(job, new Path("data/address.txt"), TextInputFormat.class, AddressMapper.class);
```

MultipleInputs.addInputPath(...)参数解释如下：

* job: 表示当前的MapReduce作业。
* new Path("data/person.txt"): 这是要处理的输入路径，指定了输入文件所在的位置，这里是"data/person.txt"。
* TextInputFormat.class: 表示输入数据的格式，这里使用的是TextInputFormat，它将每行文本作为一个输入记录。
* PersonInfoMapper.class: 表示对应的Mapper类，即处理"data/person.txt"输入路径的数据时使用的Mapper类。

在MapReduce中实现Join的思路：设置两个Mapper分别加载两个文件数据，每个Mapper中输出K,V格式数据作为Reducer端的输入，这就要求两个Mapper输出的看，V格式完全相同，否则代码无法执行；这样就可以在Reduce端中获取两个文件中相同key的数据，进而按照相同key数据把对应的Join结果组织出来。最后在Driver中通过MultipleInputs.addInputPath(...)来设置当前Job对应多个Mapper输入即可。