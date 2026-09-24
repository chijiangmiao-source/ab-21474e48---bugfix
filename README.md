# 低温阵列扫描监控

低温阵列扫描持续产生稀疏校正帧。本应用保证复核员在网络抖动或浏览器休眠后，
仍看到**同一代采集**的连续累计曲线：不漏帧、不重放、不混入已重开的采集。

## 功能与保证

- **代（generation）**：页面可新开一代采集，每代有唯一代号（如 `gen-1a2b3c4d`），
  序号从 1 严格递增。新代开启后旧代立即隔离：旧代追加返回 `410`，旧代事件流收到
  `superseded` 后关闭，旧连接的迟到事件被客户端双重门卫（会话令牌 + 代号）丢弃。
- **幂等追加**：按操作标识追加含多通道增量的帧。后端在一次 SQLite 事务内完成
  幂等校验、序号分配、快照（高水位 + 各通道累计值）更新与压缩：
  - 相同操作标识 + 相同参数重试 → 返回原序号（`200 replayed`），不产生新事件；
  - 相同操作标识 + 不同参数 → 稳定拒绝（`409 operation_conflict`），不产生事件；
  - 并发重复追加只落一帧（写串行化 + 唯一约束）。
- **事件流（SSE）**：浏览器先收到带高水位的 `snapshot`，再消费增量 `frame`。
  重连携带代号与游标（`?cursor=N` 或 `Last-Event-ID`）。服务端仅保留**最近 32 帧**；
  游标过旧时发送 `reset` 事件，客户端原子替换全部状态，累计值与逐帧求和一致。
- **明确状态**：连接中 / 已连接 / 重连中 / 序号缺口（自动带游标修复）/
  代际不符（已忽略）/ 不可恢复（409、410、404）均显示在页面状态徽标与事件日志。

## 运行（Docker）

```bash
docker compose up --build app          # 默认宿主机端口 8080
HOST_PORT=9000 docker compose up app   # 自定义宿主机端口
```

容器就绪后：

- 监看页：`http://localhost:8080/`
- 健康检查：`http://localhost:8080/health`

## 复核（verify 单次服务）

```bash
docker compose run --rm verify
# 或
docker compose up --abort-on-container-exit --exit-code-from verify verify
```

verify 依次执行并以退出码报告结果（0 通过 / 非 0 失败）：

1. **代码测试** `python -m unittest discover -s tests -v` —— 持久层单元测试 +
   HTTP/SSE 集成测试（幂等、冲突、压缩、并发、代际隔离、重置等 17 项）；
2. **前端构建检查** `node frontend/build.mjs --check` —— JS 语法编译校验 +
   资源引用与 dist 产物完整性；
3. **API/HTTP 冒烟** `python verify/smoke.py` —— 对 `app` 服务的端到端检查
   （健康、监看页、幂等重试、异参拒绝、并发只落一帧、快照→增量、游标补发、
   过旧重置且累计值一致、旧代隔离）。

## 本地开发（无 Docker）

```bash
node frontend/build.mjs                # 构建前端到 frontend/dist
python3 app/server.py                  # PORT=8080 DB_PATH=:memory: 可覆盖
sh verify/run_verify.sh                # 需另起终端先启动服务，APP_URL 可覆盖
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/api/acquisitions` | 新开一代采集，返回 `{code}` |
| POST | `/api/acquisitions/{code}/frames` | 追加帧 `{operationId, deltas}`；`201` 新帧 / `200` 幂等重放 / `409` 异参复用 / `410` 代已关闭 |
| GET | `/api/acquisitions/{code}/stream?cursor=N` | SSE：`snapshot` → `frame`* → （`reset` / `superseded`） |

## 结构

```
app/server.py      HTTP + SSE 服务（Python 标准库）
app/store.py       SQLite 持久层：单事务追加、幂等、快照、压缩（保留 32 帧）
frontend/src/      监看页源码（原生 JS + EventSource）
frontend/build.mjs 零依赖前端构建/检查脚本
tests/             单元 + 集成测试
verify/            冒烟脚本与复核入口 run_verify.sh
Dockerfile         应用镜像（构建期完成前端构建）
docker-compose.yml app + verify 编排，HOST_PORT 可配
```
