# 说明
map join操作，注意其是在map阶段做的join。reduce的join涉及到数据的Shuffle，我们也可以在Mapper端完成数据的Join以提高效率，<br/>
这个思路是首先将一个文件读取并缓存，然后每个MapTask执行时，读取另一个文件中的每行数据后可以从缓存中查询是否有匹配的相同数据，进而形成Join结果。<br/>

以上这个过程就需要在Driver中设置“job.addCacheFile(...)”来执行要缓存的数据文件，并在Mapper端实现setup方法，在该方法中获取缓存文件并缓存，<br/>
这样只需要有mapper就可以完成数据的Join，并不需要Reducer。<br/>
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

现在需要通过Mapper整合数据：

```
id=1,name=张三,age=18,address=北京
id=2,name=李四,age=19,address=上海
id=3,name=王五,age=20,address=深圳
id=4,name=马六,age=21,address=广州
id=5,name=田七,age=22,address=天津
```