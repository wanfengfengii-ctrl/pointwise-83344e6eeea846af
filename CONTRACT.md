# 入湖消费基线及公开交付契约

## 现有文件与运行
worker.py 是当前可工作的纠缠实现；lake.py 实际使用 SQLite 保存入湖明细及来源回执；source.py 是有限录制适配器，不连接 Kafka。samples.py 提供批次构造和连续超时演示，checks.py 保留独立业务期望与完整 trace 的冻结摘要。当前尚未提供目标提交核或生命周期组件。

运行 python3 -B checks.py 核验，运行 python3 -B samples.py 查看真实明细、状态与完整事件序列。.pointwise-runtime.json 的 version 为 1、kind 为 cli；setup 是准备命令的 argv 列表，verify 和 demo 各是一个 argv 数组。交付时保留这一格式，将职责隔离核验接入原 verify，demo 仍运行原超时与迟到确认样例。CLI 仅是数据链的运行入口。

## 调用与数据
Worker(lake, source, trace, observer=None, timeout=5) 的 accept(batch)、tick(now)、close()、snapshot() 保持调用兼容。trace 为调用方提供的列表。Lake(path, trace) 接受 SQLite 路径或 :memory:；本题不要求重启消费恢复。

batch 必须恰含 offset 和 rows；offset 为严格大于本 Worker 上次成功入湖 offset 的非布尔非负整数，rows 为列表，每行为 [非空字符串ID, 非布尔整数值]，允许空批及重复ID。业务输入整数在 SQLite 有符号64位范围内；批次不能被实现修改。输入示例：{"offset":10,"rows":[["s1",7],["s1",8]]}。

accept 成功返回从1递增的提交尝试 token。最多一个尝试仍在等待；已超时的尝试不阻挡下一批。运行中等待未结束时 accept 抛 RuntimeError("commit pending")，终止后抛 RuntimeError("not accepting")，资格判定先于输入校验。非法批和非法行分别抛 ValueError("batch")、ValueError("row")，不触发 I/O 或停机。空批同样入湖并申请提交位点。timeout 为正的非布尔整数，否则构造抛 ValueError("timeout")。

Lake.rows() 返回按 offset、批内位置排序的 [offset,pos,id,value]；Lake.confirmed() 返回持久来源回执，初始为 -1。snapshot 返回 mode、now、last、confirmed、waiting、streak 六个键；初始为 running、0、-1、已有回执、None、0。last 是本 Worker 已入湖的最后位点，waiting 是仍等待的 token 或 None；状态快照为独立字典。

## 时间、确认与生命周期
每批先完成入湖，再申请提交该批来源位点；停止接收不撤销已经入湖的数据。尝试在调用 source.request(token,offset,callback) 前登记，deadline 为当时 now+timeout。适配器可在 request 返回前同步 callback，也可在后续尝试开始后 callback。callback(token,error) 的 error 为 None 表示成功，非空字符串表示远端失败；complete 只针对已登记 token，允许重复回调。请求可能在同步成功之后再抛错。source.py 的 plans 示例为 {1:{"sync":None}}，或 {1:{"sync":None,"raise":"after"}}。

tick 接收非递减的非布尔整数时钟；非法时钟抛 ValueError("clock") 且不记录事件。tick 本身不等待：到达当前 deadline 才将等待尝试超时并返回 token，其余返回 None。超时只结束本次等待，仍可能迟到成功。连续三次当前尝试超时使工作者停止接收新批次；尚在等待且工作者仍运行时的成功清零 streak，及时远端失败直接停机。仅显式 tick 判超时；没有 tick 就不因实际耗时判迟到。

旧尝试迟到成功可以推进已确认位点，但不得清零后来尝试的超时数、使终止工作者复活或重复通知停机。已确认位点不得倒退。晚到远端失败不触发停机，重复终结回调只记录 ignored。终止不取消未决尝试：关闭后待决成功仍可保存更大的回执并通知 ack，但不得重置 streak。终止后的 tick 仍更新 now 并记录 tick，不再产生超时。

observer(event,snapshot) 同步收到 ack 或 stopped，允许重入 accept、tick、close，也允许抛错。通知 ack 前，尝试状态、等待槽、持久及内存确认前沿和应有的健康计数必须已一致。不能把通知推迟到外层 request 返回后；重入期间的完整 trace 和返回顺序须兼容。新 snapshot 不与内部状态或 trace 中的快照共用字典。

close 将运行者变为 closed；错误或第三次超时变为 stopped。首次终止先占有全部收尾责任、设置模式，通知 stopped，然后依次尝试 source.close 和 lake.finish；重入或重复终止不得重做或改写终止模式。收尾释放后抛错也视作已尝试，不重试。Lake.finish 是结束接收的业务收尾，不关闭回执读取与迟到确认所需的数据库；source.close 后录制器仍可交付已经登记的完成事件。

## 异常与完整事件兼容
返回值、异常类型和消息、SQLite完整内容、快照及逐项 trace 均保持兼容，以现有源码和冻结回归为准。测试冻结的是完整事件列表，不只冻结最终汇总。trace 由 Worker、Lake、RecordedSource 共同记录，不得合并、补造或重新排序。

入湖失败不登记尝试并停机；提交请求抛错保留已入湖数据；已经同步成功的回执不得因 request 后续抛错而撤回。尚未终结的请求抛错后标为 request_error，其后完成被忽略。来源回调异常优先于适配器的 after 错误。

回执写入失败时 SQLite事务回滚，内存确认前沿保持旧值；该回调仍算已终结，不因重复交付再次写入，工作者停机，入湖明细保留。通知 ack 抛错则保留已经保存的回执并停机。已有业务主异常优先，不能被停机通知或收尾错误覆盖。没有主异常时，按 stopped 通知、source.close、lake.finish 的顺序抛第一个错误，并继续尝试后面的收尾。观察者内部重入 close 抛出的错误按普通回调异常传播；不得绕过 Python 调用顺序猜测观察者下一句原本打算抛什么。checks.py 对上述故障、重入、迟到及第三次超时给出可重跑样例。

## 本轮结构交付
本轮只改善职责和依赖边界，不修复业务规则或新增提交策略。

提供 commitcore.Core(timeout=5, confirmed=-1)：无 I/O，只消费数据事件并返回数据效果，独占尝试身份、截止时间、完成与超时归属、确认前沿。公开端口 handle(event) 接收字典并返回效果字典列表；snapshot() 返回至少含 now、confirmed、waiting 的独立字典，初始分别为0、构造给定回执、None。事件、效果、快照以及实例持有的业务状态只含可JSON往返的内建数据，不得保存回调、生成器、适配器、数据库或生命周期对象。不得通过闭包、全局注册表或导入旧 Worker 执行原链。具体事件/效果字段由本次设计，须在 BOUNDARIES.md 公开；只要求状态数据隔离，不要求增加检查点保存或恢复能力。

提供 consumer.Consumer(lake, source, core, trace, observer=None)：提供与 Worker 相同的四个公开方法，独占接收资格、连续超时计数、停机通知及收尾，管理入湖、提交适配器和观察者的 I/O。它只经注入的 Core 端口 handle/snapshot 交换数据，不读取 Core 私有字段，不自行生成尝试身份、判定回调新旧、计算截止时间或维护确认前沿副本。它负责解释效果和回传执行结果，不能重新实现提交规则。允许它保留当前调用的临时执行上下文，但不得持有第二份业务裁决状态。具体事件交接、效果响应和重入组织由交付者设计，不能用延迟通知改变兼容行为。

Core 不得操作 Worker、Consumer、数据库或任何 I/O 适配器；Consumer 不得导入 commitcore 的实现；二者均不得导入 worker。I/O适配器不得承载尝试裁决、超时计数或停机规则。原 Worker 仅保留兼容装配与转发，不保留旧执行链或备用执行器。

保留 checks.py 的原业务断言、场景和 FROZEN。交付重构代码及 BOUNDARIES.md 状态归属和端口说明；将结构核验接入原 verify：Core 脱离 I/O 单独驱动，覆盖同步结果、迟到结果及效果失败回传；Consumer 注入只实现已公开端口的脚本替身，验证其真实走端口而非旁路；核对依赖方向、可JSON往返的数据及不存活对象。结构核验还须与真实 SQLite 和原录制器的冻结行为回归一起通过，不以替身运行替代业务兼容。

不扩展真实 Kafka、并行分区、重试退避、进程崩溃恢复、迁移或部署。
