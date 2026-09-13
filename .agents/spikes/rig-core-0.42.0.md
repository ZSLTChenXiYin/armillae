# rig-core 0.42.0 迁移与完成事实 Spike

> 日期：2026-09-11
> 结论：离线验证通过；Live 未执行
> 依赖：`rig-core = "=0.42.0"`（生产与 dev）；Rust 1.98.0
> 决策：[RFC 0004](../rfcs/0004-stream-terminal-evidence.md)

## 范围与结果

全部七个配置入口统一升级：OpenAI、OpenAI-compatible、DeepSeek、MiniMax、Moonshot、
Anthropic、Ollama。通过真实 Rig Client 与 Mock HTTP 的 complete/stream 请求映射、解析与
共享 Bridge 合约。两种 Structured 模式按能力矩阵验收：支持的模式实际执行并校验结果，
不支持模式在发送前拒绝；拒绝不等于服务端获得 NativeStrict 能力。

- 0.41 的合法 JSON 后 EOF 假完成缺陷已修复；缺少 Provider 终端返回 StreamInterrupted。
- 正常结果、无效 JSON、Schema 不匹配、length/refusal/未知结束原因覆盖两种调用方式；
  两种模式共十个受支持 Provider/模式组合。
- 逐字节 HTTP body 覆盖 SSE、NDJSON、JSON token 与中文 UTF-8 边界。
- 传输错误与损坏帧在全七个入口各受支持模式下只报告一次错误，不产生成功事件，不重发
  请求；取消释放底层 body，不后台排空。
- 原生 Stop 不因输出 ToolCall 改成 ToolCall；重复终端、终端后内容拒绝。
- 多工具交错、稳定 ID、ToolResult 回放、同 Provider reasoning 与签名、Usage 前后顺序沿用
  共享回归；0.42 新暴露的 Anthropic 未知事件保留为 ProviderEvent。
- ToolResult.name 和 Provider 关联身份从先前 Assistant ToolCall 恢复；Ollama 不再用工具名
  改写 canonical ID，保留原有孤立 ToolResult 预检。

## 接入与性能取舍

私有 RigDriver 隔离原生响应类型。非流式先读取原生事实，再消费响应复用 Rig 内容转换；不
采用 Rig 的输出推断原因。流式先将原生 Terminal 放入每请求 typed 槽位，Rig 公共聚合器
负责内容生命周期与 Provider ID 恢复，Armillae 负责公共事件和最终结果校验。

Anthropic 原生 ToolInputEnd 将 Provider ID 放在不公开读取的 StreamPartId 中；只使用 raw
事件无法安全恢复。不能把 Debug 字符串或不透明键当作 Provider ID。因此保留既有 Rig 与
Armillae 双层内容聚合，修正调研阶段省去 Rig 聚合器的设想。终端槽位只写入和取出一次，
避免完整响应转 Value 再解码；没有新增 HTTP 观察层、字节解析器或自动重试。

本次没有性能基准，不声明零开销或内存降低。终端槽位增加请求级分配与同步；最终 JSON/
Schema 校验仍需完整输出。远端是否停止计算也不能由客户端 drop 证明。

## 可复现验证

```sh
rtk cargo fmt --all -- --check
rtk cargo clippy -p armillae-core -p armillae-llm -p armillae-llm-rig -p armillae-tools --all-targets --all-features --offline -- -D warnings
rtk cargo test -p armillae-core -p armillae-llm -p armillae-llm-rig -p armillae-tools --all-targets --all-features --offline
rtk cargo test -p armillae-core -p armillae-llm -p armillae-llm-rig -p armillae-tools --doc --all-features --offline
rtk cargo doc -p armillae-core -p armillae-llm -p armillae-llm-rig -p armillae-tools --no-deps --all-features --offline
rtk git diff --check
```

相关全部目标测试：162 通过、0 失败、33 ignored（28 项结构化 Live + 5 项原有 Live）；
其中 Adapter 单元/矩阵测试 92 项、迁移后的 P0 测试 7 项。示例编译通过；格式、Clippy、
文档测试和 API 文档构建通过。Live 未执行，不作全量真实服务兼容或发布就绪声明。

主要回归源：`src/providers/structured_tests.rs`、`src/stream.rs`、各 Provider 测试与
`tests/p0_spike.rs`（相对 `crates/armillae-llm-rig`）。原有 0.41 Spike 保留历史证据；当前
测试文件已经迁移到 0.42，复现旧行为须使用旧版本提交，不能拿当前运行结果证明旧版本。

## 公共接口与安全审计

升级未改变 Armillae 公共协议、配置字段或 Provider 能力声明；RigBridge 保持 factory-only
构造，私有 RigDriver 约束不穿透其他 crate。生产错误不新增 unwrap/expect，不输出原始
终端内容或凭证。更新现有 README 的版本和未知事件描述，不创建独立稳定用户指南。
