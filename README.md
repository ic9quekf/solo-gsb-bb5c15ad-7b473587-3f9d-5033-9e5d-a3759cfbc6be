# 🪝 Webhook 事件回放实验室（Webhook Replay Lab）

一个可用 Docker 一键启动的 Webhook 事件回放/调试沙盒：导入 JSON 事件 → 配置 1～5 个目标 URL 与各自的
HMAC 密钥 → 按顺序**多目标扇出**发送（每个事件为每个目标分别签名、独立投递与重试）并在时间轴中观察
每一次请求、签名、幂等键与响应。

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
- **两种目标模式**：
  - **单目标（兼容模式）**：沿用 `/api/settings` 的单个目标 URL、HMAC 密钥、重试次数与超时；
    v1 的 API、数据库与快照完全兼容。
  - **多目标扇出**：`POST /api/targets` 维护 **2～5 个带名称的目标**，每个目标独立配置
    URL、HMAC 密钥、最大重试次数与超时。
- **扇出语义**：批量重放时每个事件**为每个目标分别签名**（该目标自己的密钥）、独立投递、
  独立指数退避重试、独立记录 attempts 与 per-target delivery 状态；请求额外带
  `X-Webhook-Target: <目标名>` 头。**仅当所有目标都成功**，事件才标记为 `success`；
  部分失败时事件为 `failed`，但每个目标各自的状态保留在 `deliveries` 中。
- **只重试失败目标**：对失败事件调用 `POST /api/events/{id}/replay`（`{"scope":"failed"}`）
  或批量 `POST /api/replay {"only_failed":true}`，只会重投尚未成功的目标（含事件成功后新增的
  目标）；已成功的目标不会被重复打扰。`scope:"all"` 则对所有目标重新开一轮投递。
- **目标配置**：本地目标 URL、HMAC 密钥、最大重试次数（0–10）、请求超时。
- **顺序投递**：单后台工作线程 + FIFO 队列，严格按事件入库顺序逐条发送。
- **自动签名**：每个请求带 Stripe 风格的 `X-Webhook-Signature: t=<unix>,v1=<hmac_sha256>`，
  另有 `X-Webhook-Event`、`X-Webhook-Timestamp`；多目标模式下额外带 `X-Webhook-Target`。
- **幂等键**：单目标模式为 `sha256("<event_id>#replay=<n>")`；多目标模式为
  `sha256("<event_id>#target=<目标名>#replay=<n>")`，即**每个 (事件, 目标) 组合一轮投递内
  的所有重试共享一个键**，不同目标/不同重放轮次各自不同；手动重放则换一个新键。
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
2. 在「2 · 导入事件」点 **填充示例 → 解析并入库**（单目标 URL 默认就指向内置接收器 `http://127.0.0.1:8000/recv`）。
3. 点 **▶ 按顺序扇出发送全部**，观察三个事件依次变绿，时间轴出现 3 条 200 记录。
4. 切到「**多目标扇出（2–5 个命名目标）**」标签，配 2～3 个目标，其中一个填故障注入地址
   制造「前 2 次 500、之后成功」：

   ```
   http://127.0.0.1:8000/recv?fail=500&times=2
   ```

   再点发送全部：每个事件对每个目标分别签名、独立退避重试；事件列表「各目标结果」列显示
   每个目标的彩色徽标。还可以用 `?sleep=8`（配合较小超时）制造某一个目标的超时重试。
5. 制造一次部分失败后（例如某目标返回 422），事件保持红色失败、但好目标仍为绿色；对该事件点
   **只补失败目标**（或「仅重放失败事件」），修好的目标补投成功后事件整体变绿，已成功目标不重发。
6. 在时间轴中可以看到目标名、同一轮各目标独立的幂等键、退避后重试、最终成功的完整过程。
7. 在「5 · 实验数据快照备份 / 恢复」点 **⬇ 下载快照**，随后可改配置、清历史，
   再上传该 zip 一键还原目标组、事件、各目标投递与尝试记录（页面会弹出恢复结果摘要；
   当前单目标配置不会被覆盖）。

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

# 8) 查看内置接收器实际收到的内容（含签名、目标名与幂等键）
curl -s http://localhost:8000/recv/logs | python -m json.tool

# 8a) 切换到多目标扇出：2～5 个带名称的目标（各自 URL/HMAC/重试/超时）
curl -s -X POST http://localhost:8000/api/targets \
  -H 'Content-Type: application/json' \
  -d '{"targets":[
        {"name":"billing","url":"http://127.0.0.1:8000/recv?fail=503&times=2",
         "hmac_secret":"sec-bill","max_retries":3,"timeout_seconds":5.0},
        {"name":"audit","url":"http://127.0.0.1:8000/recv",
         "hmac_secret":"sec-audit","max_retries":1,"timeout_seconds":5.0}]}'

# 8b) 部分失败后，只对尚未成功的目标补投（已成功目标不会重复投递）
curl -s -X POST http://localhost:8000/api/events/evt-002/replay \
  -H 'Content-Type: application/json' -d '{"scope":"failed"}'

# 8c) 查看每个事件 × 每个目标的投递状态
curl -s http://localhost:8000/api/deliveries | python -m json.tool

# 8d) 删除命名目标组，回到单目标兼容模式
curl -s -X DELETE http://localhost:8000/api/targets

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
字段顺序固定（v2 格式，共 6 个成员）：

```
manifest.json      # 格式标识、format_version、db_version、创建时间、每个文件的行数/字节数/SHA-256
settings.jsonl     # id,target_url,hmac_secret,max_retries,timeout_seconds（仅存档，恢复时不应用）
targets.jsonl      # id,name,url,hmac_secret,max_retries,timeout_seconds,created_at,updated_at（0 行=单目标模式）
events.jsonl       # event_id,seq,payload,status,created_at,updated_at
deliveries.jsonl   # event_id,target_id,status,replay_no,attempts_count,
                   # last_http_status,last_outcome,last_attempt_at,updated_at
attempts.jsonl     # id,event_id,target_id,attempt_no,replay_no,idem_key,request_at,response_at,
                   # http_status,outcome,elapsed_ms,req_headers,resp_excerpt（target_id 为 null=单目标）
```

恢复时**单目标 `settings` 始终保留**，命名目标组 `targets` 与各目标投递状态 `deliveries` 随快照
一起原子恢复；**旧版 v1 快照（4 个成员、attempts 无 target_id）仍可恢复**，读入时自动迁移为
attempts 的 `target_id=null`、无 targets/deliveries 的单目标形态。

恢复的校验规则（全部通过才会写入，单事务提交，失败整体回滚）：

1. 必须是合法 zip；v2 只包含上述 6 个成员，v1 只包含 4 个旧成员；
2. manifest 的 `format` / `format_version` / `db_version` 与格式匹配（v2↔v2，v1↔v1 快照）；
3. 每个 JSONL 的 SHA-256、行数、字节数与 manifest 一致；
4. 每行字段集合与顺序必须与约定完全一致，类型合法（status/outcome 枚举、HTTP 状态码范围等）；
5. 每个事件的 `payload` 必须是可解析的 JSON 对象；
6. targets 的 id/name 唯一、数量 ≤5、URL 合法；deliveries/attempts 的
   `event_id`/`target_id` 外键都必须存在（单目标 attempts 的 `target_id` 必须为 null）；
7. 投递进行中（队列非空或有 inflight 事件）返回 `409`，拒绝恢复；修改目标组同理。

恢复后快照中 `queued/running` 的事件与 delivery 会归一化为 `pending`（与重启语义一致），
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
| GET | `/api/settings` | 读取单目标兼容配置 |
| POST | `/api/settings` | 更新单目标 URL / HMAC / 重试次数 / 超时 |
| GET | `/api/targets` | 读取目标模式（`single`/`fanout`）与命名目标列表 |
| POST | `/api/targets` | 整体替换命名目标组（必须 2～5 个，名称唯一；投递中返回 409） |
| DELETE | `/api/targets` | 删除命名目标组，回到单目标模式（级联清除其 attempts/deliveries） |
| GET | `/api/deliveries?event_id=` | 每个 (事件, 目标) 的投递状态（可选按事件过滤） |
| POST | `/api/events/import` | 导入（或覆盖）JSON 事件 |
| GET | `/api/events` | 事件列表（含状态、尝试次数） |
| GET | `/api/events/{event_id}` | 事件详情：原始载荷 + 各目标投递 + 全部尝试记录 |
| DELETE | `/api/events` | 清理全部历史 |
| POST | `/api/replay` | 顺序入队全部（`{"only_failed":true}` 仅失败事件且只补投失败目标） |
| POST | `/api/events/{event_id}/replay` | 重放单条事件，body `{"scope":"all"}`（默认）或 `"failed"` |
| GET | `/api/timeline` | 最近 200 条尝试的时间轴（含目标名） |
| GET | `/api/status` | 队列深度、事件/目标投递各状态计数、当前模式 |
| GET | `/api/backup` | 下载快照 v2（settings/targets/events/deliveries/attempts 的 JSONL 压缩包） |
| POST | `/api/restore` | 上传快照（v2/v1 均可）：全量校验后单事务原子恢复，保留当前单目标配置（422/409） |
| POST | `/recv?fail=500&times=2&sleep=1` | 内置接收器（可注入故障/延迟，日志含目标名） |
| GET / DELETE | `/recv/logs` | 接收器收到的请求日志 / 清空 |

---

## 目录结构

```
.
├── app/
│   ├── main.py          # FastAPI 路由（配置/目标组/事件/扇出重放/时间轴/备份恢复）+ 静态页面
│   ├── dispatcher.py    # FIFO 工作线程、每目标并发扇出、各自退避重试、签名/幂等头注入
│   ├── db.py            # SQLite 持久层（settings/targets/events/deliveries/attempts + v1→v2 迁移）
│   ├── snapshot.py      # JSONL 快照打包（v2）、manifest 与全量恢复校验（可读 v1）
│   ├── signing.py       # HMAC 签名与（按目标的）幂等键工具
│   ├── receiver.py      # 内置测试接收器（支持 fail/sleep 故障注入，日志含目标名）
│   ├── models.py        # Pydantic 模型
│   └── static/          # 单页实验台（index.html / app.css / app.js）
├── examples/events.json # 可直接导入的示例事件
├── smoke_test.py        # TestClient 端到端冒烟测试（含备份/恢复用例）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 说明与限制

- 面向**本地实验/教学**：单进程、单工作线程、内存队列；事件严格按入库顺序逐条处理，
  但**一个事件对各目标的投递是并发扇出**、彼此独立重试。清空或重启容器时进行中的事件/投递
  会回到 `pending`。
- 命名目标组按**名称**匹配做原地更新（id 保持稳定）；删除某个目标会级联清除它的 attempts 与
  deliveries。投递进行中拒绝修改目标组（409）。
- 未配置命名目标时完全等同旧版单目标行为：attempts 的 `target_id` 为 null、幂等键用旧格式、
  不写 deliveries；旧 v1 数据库启动时自动迁移到 v2（加列 + 建表），旧 v1 快照可直接恢复。
- 快照恢复同样遵循重启语义：快照里 `queued/running` 的事件与 delivery 恢复后变为 `pending`。
  恢复在单事务内原子覆盖 targets/events/deliveries/attempts，但不会改动当前单目标 `settings`。
- 响应体仅保留前 1000 字符作为摘要；完整数据请在你的接收端记录。
- 生产级 Webhook 网关还需要多副本、持久化队列、死信队列与告警——本项目刻意保持简单，方便阅读与改造。
