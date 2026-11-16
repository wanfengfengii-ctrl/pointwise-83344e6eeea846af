# 职责边界与状态归属

本轮只拆职责和依赖方向，业务规则、异常、trace 与 SQLite 内容均未改动。

## 文件与依赖方向

| 文件 | 角色 | 导入 |
| --- | --- | --- |
| `commitcore.py` | 无 I/O 提交核 `Core` | 无 |
| `consumer.py` | 消费生命周期 `Consumer` | 无（只使用注入端口的 `handle`/`snapshot`） |
| `worker.py` | 兼容装配与转发 | `commitcore`、`consumer` |
| `lake.py` / `source.py` | I/O 适配器 | 仅 `sqlite3`（lake） |
| `checks.py` | 原业务冻结回归 + 结构核验 | 装配件与 `boundaries` |
| `boundaries.py` | 结构核验（Core 单独驱动、Consumer 端口替身、依赖与隔离） | 允许使用全部组件 |

```text
Worker ──装配──> Consumer ──端口(handle/snapshot)──> Core
   │                 │
   └────────────────> Lake / RecordedSource / observer（I/O 与用户回调）
```

- `Core` 不导入任何模块，不接触 Worker、Consumer、数据库或适配器。
- `Consumer` 不导入 `commitcore`，不导入 `worker`，不读取 Core 私有字段。
- `worker.py` 只做参数校验后的装配与四个方法转发，不保留旧 `_done`/`_finish`/
  `_attempts` 等执行链。
- `Lake`/`RecordedSource` 不含尝试裁决、超时计数或停机规则。

## 状态归属

| 状态 | 唯一所有者 | 说明 |
| --- | --- | --- |
| 尝试身份（token 序列） | `Core` | `_next`，从 1 递增 |
| 每次尝试的 offset / deadline / 终态 | `Core` | `_attempts` |
| 当前等待槽 `waiting` | `Core` | 生命周期只读 `snapshot()["waiting"]` 做接收资格判断 |
| 时钟 `now` | `Core` | 由 `tick` 事件推进；非法时钟不记录事件 |
| 确认前沿 `confirmed` | `Core` | 仅在 `effect_result(confirm, ok=True)` 后推进，且不回退 |
| 接收资格 / 模式 `mode` | `Consumer` | running / closed / stopped |
| 连续超时计数 `streak` | `Consumer` | 由 `timeout` 效果驱动，只有当前尝试的 `health` 效果清零 |
| 最后入湖位点 `last` | `Consumer` | 入湖成功后更新 |
| 收尾责任 `_finished`、收尾顺序 | `Consumer` | 先占有再通知，重入不重复 |
| 停机通知与观察者调用 | `Consumer` | `notify` 效果触发 |
| 入湖、回执、source 请求/关闭 | `Consumer`（经适配器） | Core 不感知 |
| 当前调用临时上下文（effects 列表） | `Consumer` | 不落为字段，不构成第二份裁决状态 |

`Consumer` 不维护确认前沿副本：每次 `snapshot()` 都从 `core.snapshot()` 取
`now`/`confirmed`/`waiting`；它只在执行 `confirm` 效果后把成功/失败回传，
由 Core 决定前沿。

## Core 端口：事件与效果

`Core(timeout=5, confirmed=-1)`，`timeout` 非正整数抛 `ValueError("timeout")`。
`handle(event) -> [effect, ...]`，输入不被修改；`snapshot() -> {"now", "confirmed", "waiting"}`，
每次返回独立字典。所有事件、效果、快照及实例字段均为可 JSON 往返的内建数据。

### 事件

| 事件 | 字段 | 含义 |
| --- | --- | --- |
| `submit` | `offset: int` | 数据已入湖，申请提交该位点 |
| `request_failed` | `token: int` | 适配器 `request` 抛错 |
| `complete` | `token`, `error`(None/str), `active`(bool) | 来源回调；`active` 表示生命周期仍运行 |
| `effect_result` | `effect:"confirm"`, `token`, `offset`, `ok: bool` | 回执写入的成败回传 |
| `tick` | `now: int`, `active: bool` | 时钟推进；非法时钟抛 `ValueError("clock")` |

### 效果

| 效果 | 字段 | 生命周期应做的事 |
| --- | --- | --- |
| `attempt` | `token`, `offset`, `deadline` | 记录 `attempt` trace、调用 `source.request` |
| `rejected` | `reason` | 仅当仍有等待尝试；防御分支 |
| `ignored` | `token` | 记录 `ignored` trace |
| `confirm` | `token`, `offset` | `lake.confirm`，然后回传 `effect_result(ok=...)` |
| `health` | `token`, `streak:0` | 清零 streak、记录 `health` trace |
| `notify` | `token`, `event:"ack"` | 同步通知观察者 |
| `remote_failure` | `token` | 按远端失败停机 |
| `timeout` | `token` | streak 加一、记 `timeout` trace，第三次停机 |

效果按列表顺序执行；`confirm` 在执行 `effect_result` 前不会推进 Core 前沿。
迟到成功只产生 `confirm`+`notify`，不产生 `health`，因此不清零后来尝试的超时数。
迟到失败不产生 `remote_failure`。

## 兼容性要点

- `Worker(lake, source, trace, observer=None, timeout=5)` 与
  `accept/tick/close/snapshot` 签名、返回值、异常消息不变。
- 同步回调仍在 `source.request` 返回前完成通知，收尾也在同一栈内完成；
  无延迟通知、无备用执行链。
- 收尾顺序仍为 `stopped` 通知 → `source.close` → `lake.finish`，首个错误优先，
  后续动作继续尝试；已有主异常优先于收尾错误。
- `checks.py` 的 12 份冻结完整 trace 摘要保持不变；结构核验附加在同一 `verify`
  之后运行。
- `samples.py` 未改动，继续演示连续三次超时停止以及关闭后的迟到确认。
