# 🪝 Webhook 事件回放实验室（Webhook Replay Lab）

一个可用 Docker 一键启动的 Webhook 事件回放/调试沙盒：导入 JSON 事件 → 配置目标 URL 与 HMAC 密钥 →
按顺序发送并自动重试 → 在时间轴中观察每一次请求、签名、幂等键与响应。

技术栈：**FastAPI + SQLite（WAL）+ httpx + 原生 HTML/CSS/JS**，无构建步骤。

---

## 快速开始（一条命令）

```bash
docker compose up --build -d
```

打开浏览器：

| 用途 | 地址 |
| --- | --- |
| 实验台 Web UI | http://localhost:8000/ |
| 交互式 API 文档 | http://localhost:8000/docs |
| 内置接收器收到的请求 | http://localhost:8000/recv/logs |
| 健康检查 | http://localhost:8000/healthz |

> 端口：宿主 **8000** → 容器 **8000**（在 `docker-compose.yml` 的 `ports` 中修改）。
> SQLite 数据保存在命名卷 `replay-data`（容器内 `/data/webhook_replay.db`），重启不丢数据。

不用 Compose 也行：

```bash
docker build -t webhook-replay-lab .
docker run -d -p 8000:8000 -v replay-data:/data webhook-replay-lab
```

本地直接运行（需要 Python 3.11）：

```bash
pip install -r requirements.txt
REPLAY_DB=./replay.db uvicorn app.main:app --reload --port 8000
```

---

## 功能

- **导入事件**：粘贴或上传 `.json` 文件；接受单个对象、数组或 `{"events": [...]}`；
  用对象中的 `event_id`（或 `id`）作为主键，缺失时自动生成；重复 ID 覆盖并重置状态。
- **目标配置**：本地目标 URL、HMAC 密钥、最大重试次数（0–10）、请求超时。
- **顺序投递**：单后台工作线程 + FIFO 队列，严格按事件入库顺序逐条发送。
- **自动签名**：每个请求带 Stripe 风格的 `X-Webhook-Signature: t=<unix>,v1=<hmac_sha256>`，
  另有 `X-Webhook-Event`、`X-Webhook-Timestamp`。
- **幂等键**：`Idempotency-Key` / `X-Idempotency-Key` 为
  `sha256("<event_id>#replay=<n>")`，**同一次重放内的所有重试共享同一个键**，手动重放则换一个新键。
- **指数退避重试**：超时、连接错误、408、429、5xx 自动重试，
  退避 `0.5s → 1s → 2s …`（带 50–100% 抖动，上限 30s）；4xx 等不可重试错误立即判定失败。
- **时间轴**：每次尝试记录 HTTP 状态、耗时、结果（已送达/可重试/不可重试）与响应摘要（前 1000 字符）。
- **事件详情**：按事件 ID 查看原始载荷、幂等键、每一轮的完整尝试记录。
- **手动重放**：对任意单条失败（或成功）事件一键重新发送；也可批量重放全部 / 仅失败事件。
- **清理历史**：一键删除全部事件与尝试记录（配置保留）。
- **快照备份 / 原子恢复**：一键把 `settings`、`events`、`attempts` 导出为固定字段顺序的
  JSONL 压缩包（`.zip`），包内 `manifest.json` 记录每个文件的行数、字节数、SHA-256 与
  数据库版本；恢复前完整校验 manifest、SHA-256、字段顺序、事件载荷可解析性与
  attempts→events 外键，**任一错误都拒绝写入**，校验通过后在单事务中整体替换数据，
  **当前投递配置始终保留**（快照里的设置仅存档不应用）。
- **内置测试接收器**：`POST /recv`，开箱即可联调，并支持故障注入（见下文）。

---

## 30 秒体验流程

1. `docker compose up --build -d` 后打开 http://localhost:8000/
2. 在「2 · 导入事件」点 **填充示例 → 解析并入库**（目标 URL 默认就指向内置接收器 `http://127.0.0.1:8000/recv`）。
3. 点 **▶ 按顺序发送全部**，观察三个事件依次变绿，时间轴出现 3 条 200 记录。
4. 把目标 URL 改成故障注入地址，制造「前 2 次 500、之后成功」，再重放失败事件：

   ```
   http://127.0.0.1:8000/recv?fail=500&times=2
   ```

   还可以用 `?sleep=8`（配合较小超时）制造超时重试。
5. 在时间轴中可以看到同一轮重试共享幂等键、退避后重试、最终成功的完整过程。
6. 在「5 · 实验数据快照备份 / 恢复」点 **⬇ 下载快照**，随后可改配置、清历史，
   再上传该 zip 一键还原事件与尝试记录（页面会弹出恢复结果摘要；当前配置不会被覆盖）。

> 向容器外宿主机上的服务发送时，URL 主机用 `host.docker.internal`（Linux 上需
> `docker run --add-host=host.docker.internal:host-gateway`，或直接使用宿主机内网 IP）。

---

## 示例请求（curl）

```bash
# 1) 导入事件
curl -s -X POST http://localhost:8000/api/events/import \
  -H 'Content-Type: application/json' \
  --data @examples/events.json

# 2) 配置目标（内置接收器，前两次返回 503 以演示重试）
curl -s -X POST http://localhost:8000/api/settings \
  -H 'Content-Type: application/json' \
  -d '{"target_url":"http://127.0.0.1:8000/recv?fail=503&times=2",
       "hmac_secret":"shhh","max_retries":3,"timeout_seconds":5.0}'

# 3) 按顺序投递全部事件
curl -s -X POST http://localhost:8000/api/replay -d '{}'

# 4) 查看状态（每 2~3 秒轮询即可）
curl -s http://localhost:8000/api/status | python -m json.tool

# 5) 查看某条事件的原始载荷与每次尝试
curl -s http://localhost:8000/api/events/evt-002 | python -m json.tool

# 6) 手动重放单条失败事件
curl -s -X POST http://localhost:8000/api/events/evt-002/replay

# 7) 仅重放所有失败事件
curl -s -X POST http://localhost:8000/api/replay \
  -H 'Content-Type: application/json' -d '{"only_failed":true}'

# 8) 查看内置接收器实际收到的内容（含签名与幂等键）
curl -s http://localhost:8000/recv/logs | python -m json.tool

# 9) 清理全部历史记录
curl -s -X DELETE http://localhost:8000/api/events

# 10) 下载实验数据快照（JSONL 压缩包）
curl -sOJ http://localhost:8000/api/backup        # 得到 webhook-replay-snapshot-<UTC时间>.zip

# 11) 校验并原子恢复（任何校验错误返回 422，数据一行都不会改）
curl -s -X POST http://localhost:8000/api/restore \
  -H 'Content-Type: application/zip' \
  --data-binary @webhook-replay-snapshot-20260922T120000Z.zip | python -m json.tool
```

### 快照包格式

`GET /api/backup` 返回一个 zip，成员固定且各文件均为 UTF-8 JSONL（每行一条记录，尾随换行），
字段顺序固定：

```
manifest.json     # 格式标识、format_version、db_version、创建时间、每个文件的行数/字节数/SHA-256
settings.jsonl    # id,target_url,hmac_secret,max_retries,timeout_seconds（仅存档，恢复时不应用）
events.jsonl      # event_id,seq,payload,status,created_at,updated_at
attempts.jsonl    # id,event_id,attempt_no,replay_no,idem_key,request_at,response_at,
                  # http_status,outcome,elapsed_ms,req_headers,resp_excerpt
```

恢复的校验规则（全部通过才会写入，单事务提交，失败整体回滚）：

1. 必须是合法 zip，且**只**包含上述 4 个成员；
2. manifest 的 `format` / `format_version` / `db_version` 与当前服务匹配；
3. 每个 JSONL 的 SHA-256、行数、字节数与 manifest 一致；
4. 每行字段集合与顺序必须与约定完全一致，类型合法（status/outcome 枚举、HTTP 状态码范围等）；
5. 每个事件的 `payload` 必须是可解析的 JSON 对象；
6. 每条 attempt 的 `event_id` 必须能在快照的 events 中找到（外键）；
7. 投递进行中（队列非空或有 inflight 事件）返回 `409`，拒绝恢复。

恢复后快照中 `queued/running` 的事件会归一化为 `pending`（与重启语义一致），
attempts 的自增 id 原样保留，后续新尝试从 `MAX(id)+1` 继续编号。

### 接收方如何验签（Python 示例）

签名内容为 `<unix时间戳>.<紧凑排序JSON>` 的 HMAC-SHA256，头部形如 `t=1727000000,v1=ab12…`：

```python
import hmac, hashlib, json, time

def verify(raw_body: bytes, secret: str, header: str, tolerance=300) -> bool:
    parts = dict(p.split("=", 1) for p in header.split(","))
    ts, sig = int(parts["t"]), parts["v1"]
    if abs(time.time() - ts) > tolerance:
        return False
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + raw_body,
                        hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)
```

---

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/settings` | 读取当前配置 |
| POST | `/api/settings` | 更新目标 URL / HMAC / 重试次数 / 超时 |
| POST | `/api/events/import` | 导入（或覆盖）JSON 事件 |
| GET | `/api/events` | 事件列表（含状态、尝试次数） |
| GET | `/api/events/{event_id}` | 事件详情：原始载荷 + 全部尝试记录 |
| DELETE | `/api/events` | 清理全部历史 |
| POST | `/api/replay` | 顺序入队全部（`{"only_failed":true}` 仅失败的） |
| POST | `/api/events/{event_id}/replay` | 手动重放单条事件 |
| GET | `/api/timeline` | 最近 200 条尝试的时间轴 |
| GET | `/api/status` | 队列深度、各状态计数 |
| GET | `/api/backup` | 下载快照（`settings/events/attempts` 的 JSONL 压缩包 + manifest） |
| POST | `/api/restore` | 上传快照：全量校验后单事务原子恢复，保留当前配置（422 校验失败 / 409 投递中） |
| POST | `/recv?fail=500&times=2&sleep=1` | 内置接收器（可注入故障/延迟） |
| GET / DELETE | `/recv/logs` | 接收器收到的请求日志 / 清空 |

---

## 目录结构

```
.
├── app/
│   ├── main.py          # FastAPI 路由（配置/事件/重放/时间轴/备份恢复）+ 静态页面
│   ├── dispatcher.py    # FIFO 工作线程、退避重试、签名/幂等头注入
│   ├── db.py            # SQLite 持久层（settings / events / attempts + 快照导出/单事务替换）
│   ├── snapshot.py      # JSONL 快照打包、manifest 与全量恢复校验
│   ├── signing.py       # HMAC 签名与幂等键工具
│   ├── receiver.py      # 内置测试接收器（支持 fail/sleep 故障注入）
│   ├── models.py        # Pydantic 模型
│   └── static/          # 单页实验台（index.html / app.css / app.js）
├── examples/events.json # 可直接导入的示例事件
├── smoke_test.py        # TestClient 端到端冒烟测试（含备份/恢复用例）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 说明与限制

- 面向**本地实验/教学**：单进程、单工作线程、内存队列；清空或重启容器时进行中的事件会回到 `pending`。
- 快照恢复同样遵循该语义：快照里 `queued/running` 的事件恢复后变为 `pending`。恢复会覆盖事件与尝试
  数据（单事务、原子），但不会改动当前 `settings`；投递进行中拒绝恢复（409）。
- 响应体仅保留前 1000 字符作为摘要；完整数据请在你的接收端记录。
- 生产级 Webhook 网关还需要多副本、持久化队列、死信队列与告警——本项目刻意保持简单，方便阅读与改造。
