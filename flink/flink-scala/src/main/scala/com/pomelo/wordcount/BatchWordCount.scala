package com.pomelo.wordcount

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * description：flink 批数据 wordcount
 *
 * @author zhaosong
 * @company 北京海量数据有限公司
 * @date 2024/12/18 23:11
 * @version 1.0
 */
object BatchWordCount {

  def main(args: Array[String]): Unit = {
    //1.准备环境，注意是Scala中对应的Flink环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.导入隐式转换，使用Scala API 时需要隐式转换来推断函数操作后的类型
    import org.apache.flink.api.scala._

    //3.读取数据文件
    val linesDS: DataSet[String] = env.readTextFile("./data/words.txt")

    //4.切分单词
    val wordsDs: DataSet[String] = linesDS.flatMap(line => {
      line.split(" ")
    })

    // 5.对数据进行计数、分组、聚合统计
    wordsDs.map(word=>{(word, 1)})
      .groupBy(0)
      .sum(1)
      .print()

  }

}
