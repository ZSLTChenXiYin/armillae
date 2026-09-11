# RFC 0004：Rig 流式完成事实校验

> 状态：Accepted
> 日期：2026-09-11
> 来源：全 Provider 结构化结果的真实 HTTP 分片回归
> 相关规范：[LLM Bridge Spec 7.2.1](../specs/llm-bridge.md)

## 问题与证据

输入已经是完整、符合 Schema 的 `{"answer":"你好"}`，但 HTTP body 在没有任何 Provider
结束事件时直接 EOF。结构化 JSON 校验成功并不证明 Provider 正常结束。

新增回归 `providers::structured_tests::all_supported_streams_fail_on_interruption_and_drop_without_drain`
使用真实 Rig Provider Client 和逐字节 SSE/NDJSON 传输，刻意不发送 finish_reason、[DONE]、
Anthropic message_delta/message_stop 或 Ollama done=true。该测试保持非 ignored；失败必须阻止
全量验收，不能通过改用非法 JSON、要求 Usage 非零或忽略失败使其通过。

Rig 0.41.0 的 `src/providers/internal/openai_chat_completions_compatible.rs:362` 将
`StreamEnded` 当作正常退出，随后在 389 行附近无条件产生 FinalResponse；其公开
`StreamingCompletionResponse` 仅保留 Usage。Anthropic `src/providers/anthropic/streaming.rs`
289–359 行也会在循环结束后无条件产生 FinalResponse，其公开终端类型同样不保留 stop reason。
现有 Armillae StreamState 能拒绝“没有 Rig Final 的结束”，却无法从这些已经丢失区分信息的
typed Final 中判断是 Provider 完成还是 EOF。仅修改最终 JSON 校验器不能可靠修复。

实际矩阵结果：openai、openai-compatible 的两种模式，以及 deepseek、minimax、moonshot 的
JsonObjectValidated 和 anthropic 的 NativeStrict，共 8 个组合出现 EOF 假成功；Ollama 两种
模式均能拒绝无 done=true 的 EOF。七个入口的 drop 取消检查均通过。不能以 Ollama 通过
替代其余入口，也不能以未支持模式的预检拒绝代替支持模式的真实流式验证。

## 目标

- 全七个 Provider 配置入口、两种结构化模式的 streaming 保持同一成功标准。
- 只有真实 Provider 完成事实且 JSON/Schema 校验通过才能发出 ResponseCompleted。
- 已知截断、拒绝、HTTP/解析中断均为错误；不按 JSON 形状或 token 计数猜测完成原因。
- 保持一次 Model Call、原生增量、内容索引/ID、Usage 和 drop 取消；不后台重试/排空。

## 已接受方案：升级 Rig 0.42.0 原生 typed Driver

用户于 2026-09-11 在完成升级调研后明确授权直接升级 0.42.0。原先有限 HTTP 完成事实观察层不采用。

在 armillae-llm-rig 内以私有 Driver 封装全部七个配置入口的 raw_completion/raw_stream。
Rig 继续独占请求执行与 SSE/NDJSON 解析；Armillae 消费已解析的原生类型，以自己的转换器保留
Provider 完成原因、内容顺序、ToolCall ID、Usage 与 ProviderData。不得采用 Rig 根据输出
ToolCall 推断后的 finish reason，不把 Rig 类型暴露到其他 crate。

非流式在消费原生响应前提取事实，再复用 Rig 的内容转换。流式在 Rig 解析后将原生终端事实
放入请求私有的 typed 槽位，再复用 Rig 公共内容聚合器；向聚合器提供不含结束原因的内部
终端，Armillae 最终使用槽位中的原生事实，不采用 Rig 推断的原因，也不将整份原生响应
序列化到 Value 后再解码。

原生 ToolInputEnd 的 Anthropic 工具 ID 只存在于不公开读取的 StreamPartId 中，直接消费 raw
事件不足以恢复它。因此保留既有的 Rig 内容聚合与 Armillae 事件聚合两层，不能宣称降低了
聚合内存开销；不从 Debug 或序列化相关键推导 Provider ID。typed 槽位只在终端写入/取出，
不跨请求共享，不记录原始内容。这个实现修正了调研中可省去 Rig 聚合器的初步判断。

每个请求只执行一次调用；首次流错误即终止，不排空、不自动重试。EOF 缺少原生 Final 时
返回 StreamInterrupted；重复终端或其后内容均拒绝；Final 仍须经过结束原因和最终 JSON/Schema
校验才可发出成功事件。已由 Rig parser 消费并过滤的 wire 数据不是 Adapter 可观察事件。

迁移范围包含所有七个 Provider 入口、complete/stream 与两种结构化模式；不支持原生严格
Schema 的服务仍显式拒绝，不将 JSON Object 模式冒充 NativeStrict。保持公共协议与安全边界。

## 证据与验收

独立 P0 探针对 0.42.0 执行七个入口 × 正常、EOF、length、未知原因共 28 个逐字节场景通过；
它只证明候选终端语义，不替代 Armillae 全量回归。直接升级编译失败，须完成 Driver、容器、
流事件迁移与共享转换合约后才能宣称离线完成。

必须覆盖合法 JSON 后 EOF、正常结束、显式 length/refusal、Usage 前后顺序、未知/损坏事件、
UTF-8 任意分片、多 ToolCall 交错、唯一完成/错误、取消与单次 HTTP 请求。随后运行 fmt、
Clippy 与全部相关离线测试；授权 Live 矩阵单独记录，缺少真实证据时不得宣称 Live 完成。

## 取舍

升级需要一次 Adapter 迁移，但避免长期维护第二套流式字节解析逻辑。私有 Driver 只负责一次
调用与类型隔离，不演变为 SDK 或重试层。额外 HTTP 观察层不采用。
