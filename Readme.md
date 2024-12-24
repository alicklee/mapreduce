# 模型定义
定一个map函数：每个map都会处理结构、大小相同的初始数据块，经过处理后生成很多中间数据块
定一个reduce函数：reduce节点接收数据，对中间数据进行整合，通过相应的处理生成一个最终结果

# MapReduce 的功能实现步骤 
1. 有一个待处理的大数据，被划分为大小相同的数据块，以及相应的作业程序
2. 系统中有一个负责调度的主节点master以及数据处理的（map，reduce）的工作节点
3. 用户提交任务到master节点
4. master节点为该任务寻找和匹配可用的map节点，并将程序（map处理程序）传给map
5. master节点为该任务寻找和匹配可以用的reduce节点，并将程序（reduce处理程序）传给reduce
6. master节点启动每一个map节点的执行程序，读取数据并计算
7. map节点生成中间结果，告诉master节点任务完成，以及处理完成之后的结果数据储存位置
8. master等待所有map节点处理完，然后启动reduce节点。
9. reduce获取到中间结果位置信息，读取数据。
10. reduce将计算结果汇总，得到最终结果。

# 实现流程
1. 顺序实现
2. 并发实现
