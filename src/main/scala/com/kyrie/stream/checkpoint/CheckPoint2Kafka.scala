package com.kyrie.stream.checkpoint

/**
 * demo: Flink+Kafka 如何实现端到端的 exactly-once 语义
 * 实现数字相加的状态一致性
 */
object CheckPoint2Kafka {

  """
    |端到端的状态一致性的实现，需要每一个组件都实现，
    |内部 —— 利用 checkpoint 机制，把状态存盘，发生故障的时候可以恢复，保证内部的状态一致性
    |
    |source —— kafka consumer 作为 source，可以将偏移量保存下来，如果后
    |续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，
    |保证一致性
    | sink —— kafka producer 作为 sink，采用两阶段提交 sink，需要实现一个
    |TwoPhaseCommitSinkFunction
    |
    |
    |""".stripMargin

}
