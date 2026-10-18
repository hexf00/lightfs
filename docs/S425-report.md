# S4.25 交付报告:挂载写有界内存 + 真实文件系统验收

任务书六节全部执行完毕。最终 HEAD = `f8be410`(vnext 分支已推送,未合 main,等评审)。

## 一、最终 SHA 与摘要

S4.25 共 13 个提交(基线 5d8ca74 → f8be410):

| 提交 | 内容 |
|---|---|
| 486c253 | §一 空文件完整性校验 + staging 有界准入 |
| beb8601 | §三 写会话整文件 Vec→磁盘 spool(服务端 WriteHandle + 客户端 ClientWriteSession 同换 WriteSpool) |
| a1cdd8c | §三 mount-gate 脚本 + client_staging 转发 |
| b3e7382 | 真实挂载 bring-up 修复①②(路径锚 getattr / root 0o755) |
| 5c7a0ac | 修复④ OBJECT_MISSING 独立线上码 |
| 7691fc7 | 修复⑤ Client 覆写 commit_write_merged(线上挂载写提交合并) |
| 34c46ba | 修复③⑥ 适配器自管 ino 分配器 + by-id 读解析真 node id |
| 5dac211 | gate E 相位只等 dd worker |
| b3d93f6 | fuse read 传参 u64(Linux fuse feature 编译修复) |
| 2c48d84 | winfsp truncate 诊断日志 |
| f43e19a | 修复⑦ 零长读永不触内容 + 客户端 staging 惰性化(性能回归根修) |
| 9ca88f0 | 修复⑧ WriteSpool::truncate 跨 EOF 脏区保留前缀 |
| f8be410 | s425fail 测试专属 staging 根(修 9ca88f0 的编译破损) |

测试状态:lib 188 全绿;MinIO 集成 25/25 绿;win-gnu 交叉 `cargo check --tests` 零 error。

## 二、空文件边界测试结果

- `s424_empty_file_and_dropped_reader_cleanup`(S4.24 已有,继续绿):已提交空文件有真实 52 字节对象,整文件读现在**抓取并完整校验**该对象——外部结果不变(空),损坏无处藏身。
- `zero_length_read_needs_no_content`(本轮新增,绿):create-never-written 文件(无对象)在 `read(path,0,0)`、`open_read(path,0,0)`、`open_read_by_id(id,0,0)` 三入口一律返回空;非零长度读保持 ObjectMissing。这是 WinFsp A4 缺陷的契约面钉死。
- 整文件流式读 `read_stream_by_id` **有意不短路**:commit merge 依赖 ObjectMissing 判定"无 base",这是契约不是缺陷。

## 三、staging 预算与并发 RSS 实测

Linux FUSE gate(ECS 原生挂载,内核 3.10)实测:

- 64 MiB 挂载写:mount 进程 RSS 峰值 **2668 KB**(hwm 3008 KB);256 MiB 挂载写:**2112 KB**——与会话 spool 有界,与文件大小无关。
- server 进程 RSS 全程有界(SIGKILL 恢复 + 数据存活验证通过)。
- 准入语义:`budget_bytes` 超限 = 命名错误 CAPACITY(ENOSPC 语义),不驱逐、不自旋、不降级;`s425_write_session_refuses_past_budget_without_corruption` 与 `s425_staging_budget_exhaustion_is_named_and_leaves_no_residue` 钉死。
- 默认 `budget_bytes` = 2 GiB/进程;`[staging].dir` 配置项本轮接线(此前有文档无消费点)。

## 四、WinFsp / FUSE 精确证据

### Linux FUSE gate(ECS 原生,`scripts/s425-mount-gate.sh`)

`S4.25 MOUNT GATE: PASSED (A,B,C,D,E,F)`——A 基础读写、B 挂载写链路、C 64MiB RSS、D 256MiB RSS、E 并发 dd、F SIGKILL 恢复+数据存活。内核 3.10 + fuser 0.18(minfuse 对照例 9/9 证 fuser 无辜)。

### Windows CI 时间线(windows-ci.yml,mount-smoke job)

| HEAD | 结果 | 证据 |
|---|---|---|
| 5d8ca74(S4.24 基线) | 红 | `Access denied at lightfs_accept.rs:436`(create_dir_all)——**回归早于 S4.25**,由 ci-bisect 分支单证(run 39104234726) |
| 5dac211 | 红 | create_dir_all 已通(0o755 修复生效),**A1–A3 全绿**;A4 `set_len` ENOENT(accept.rs:484) |
| 2c48d84(诊断) | 红 | `truncate /sm/a4.bin -> 8192` + `truncate commit failed: object missing: file has no content yet`——根因定死 |
| f43e19a | 红 | **A4 绿**(8195 字节字节精确);A5 shrink+append 字节 0 = base 0(WriteSpool::truncate 丢整条跨 EOF 脏区) |
| 9ca88f0 系 | 红 | straddle 修复后测试编译破损(教训见下) |
| f8be410 | **待本轮** | straddle 修复+编译修复后派发,结果见下节 |

### 本轮八层根因(按发现顺序)

1. **目录/根 getattr 用 node id 锚**:vnext 目录无 file id,根树每次提交 COW 重写换 id——getattr 改路径锚。
2. **root mode 0o555**:`default_permissions`(与 WinFsp)拒绝挂载内一切创建——改 0o755(同治 Windows create_dir_all ACCESS_DENIED)。
3. **ino==node.id 撞车**:两张表各自独立 rowid 空间,内核 ino 不能当 node id——适配器自管单调 ino 分配器 + node_ids 双向映射。
4. **ObjectMissing 线上码塌缩 NOT_FOUND**:"无 base"语义在线上消失——拆出独立 `OBJECT_MISSING`(404)。
5. **trait 默认 commit_write_merged 无 ObjectMissing 臂**:Client 未覆写,真实挂载首 flush 必败(进程内测试全绿的经典盲区)——Client 覆写,base 缺失=空 base 合并。
6. **read/truncate 把 ino 当 node id**:改经 node_id_of 解析真 id。
7. **零长读要求内容**:对已建未写文件 `read(path,0,0)` 炸 ObjectMissing——零长读一律空(POSIX),三入口短路;非零读契约不动。
8. **WriteSpool::truncate 丢跨 EOF 脏区**:`retain(|(_,e)| e<=new)` 把 straddling 区间整条丢弃(注释宣称的前缀裁剪从未实现),overlay_base 用 base 零覆盖活脏字节——改 `retain(|r| r.0 < new_size)` + 端点截断。**FUSE 同病**,只是从未有套件在 Linux 上驱动过该序列。

### 过程事故(如实记录)

- 推送 5dac211 后才发现 Linux fuse feature 编译破损(cast 修复在工作树未提交)——fuse.rs 被 `cfg(target_os="linux")` 门控,mac 检查永远编不到它;交叉/容器构建才是真验证。
- 9ca88f0 推送了测试编译破损:套件输出经 `| tail` 管道吞掉 cargo 退出码,链式命令带病提交(与 S4.22 教训同源,再次写入纪律)。
- colima virtiofs 两次呈现陈旧源码(0o755/OBJECT_MISSING"部署了但没生效")——弃 bind-mount,改 tar+docker cp;QEMU cc 链接器随机 SIGSEGV 用 docker exec -d 分离重试。
- 3.10 内核尸体挂载:SIGKILL 后 mount 在 /proc/mounts 永驻,同点重挂级联失败;stat -f 不触发 root getattr 造成假阳性。

## 五、五轮性能与已知资源限制

方法:本机 MinIO,每轮 300 小文件 shell-RPC 写 + 300 读,base(5d8ca74)与 cand 各五轮,单轮单进程调用(60s 纪律)。

| 侧 | write ms/op(五轮) | read ms/op(五轮) |
|---|---|---|
| base 5d8ca74 | 8.5 / 8.95 / 10.14 / 10.55 / 10.77 | 6.21 / 5.99 / 6.99 / 7.36 / 7.50 |
| cand 修复前 | 21.44 / 21.55 | 17.93 / 17.89 |
| cand f8be410 | 12.4 / 10.98 / 11.45 / 12.5 / 11.14 | 7.92 / 7.54 / 7.18 / 7.87 / 7.36 |

- **修复前回归 2.2–3×**:根因 = `Client::new` 急切 `StagingBudget::open`,而 open 每次全量 sweep 临时根目录——每个 CLI 进程(每个 op)付 13ms(裸 stat 3ms→16ms 交叉实验单证责任在 shell 客户端)。修复 = 客户端预算惰性化(首会话才开)+ `open_fresh` 免 sweep(孤儿目录由共享同一 temp root 的 server boot sweep 回收)。
- **修复后中位写 11.45 vs 10.14、读 7.54 vs 6.99**(+13%/+8%):base 自身轮间漂移即 8.5→10.77(27%),cand 在漂移带内;残余 ~1ms/op 是 §三 会话 spool 建文件的契约成本(有界内存付在应付处)。
- 已知限制:小文件一进程一 spool 文件的生命周期成本;大文件挂载写有界(第三节 RSS);掉电持久性**未验证**(沿用历轮口径);D 阶段 client-local 缓存与本轮无交叠。

## 六、未验证故障与唯一推荐下一项

未验证:掉电(无持久队列/无 fsync 闸断言);Windows mount-smoke 完整绿仍待本轮 CI 落定;A13 之后的 accept 套件相位(B/C/D——rename/unlink/并发)在 Windows 上的表现取决于本轮是否走到。

**唯一推荐下一项:把 accept 套件(A1–A13)跑上 Linux FUSE gate。** 理由:本轮两个最深的缺陷(⑤ 首 flush、⑧ straddle)都是 accept 套件在 Windows 首次触达、FUSE 侧共病却从未暴露——gate 的 A–F 是自建相位,覆盖不了状态机边界。跑通一次的成本(一台有 /dev/fuse 的 Linux + 现成 lightfs_accept.rs)远低于再来一轮 Windows 日志考古。
