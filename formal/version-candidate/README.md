# VersionCandidate — S4.16 候选生命周期形式化模型

小型伴生模型:只覆盖 S4.16 新增的逐文件 version-key 候选机制,不复制
`formal/pack-lifecycle`(Pack/Batch-Pack 的 Building 行生命周期照旧由
该模型承保)。Strict 与 Write-Back 共用一台机器是**有意为之**:S4.16 的
交付就是三条逐文件 PUT 站点(严格流式 / 严格缓冲会话 / WB drain)接入
同一套共享契约。

## 运行(java 在 ~/tools/jre21/.../bin,jar 复用 pack-lifecycle 的)

```bash
cd formal/version-candidate
J=~/tools/jre21/jdk-21.0.12.1+1-jre/Contents/Home/bin/java
CP=../pack-lifecycle/tla2tools.jar

# 1) safety
$J -cp $CP tlc2.TLC -cleanup -config VersionCandidate.cfg VersionCandidate.tla
# 2) liveness(带公平性)
$J -cp $CP tlc2.TLC -cleanup -config VersionCandidate-liveness.cfg VersionCandidate.tla
# 3) 动作覆盖门:每个在册动作第二列(总触发数)必须 > 0
$J -cp $CP tlc2.TLC -coverage 1 -cleanup -config VersionCandidate.cfg VersionCandidate.tla
# 4) 非空/可达 probe:以下三个都应当报 VIOLATED(期望失败)
$J -cp $CP tlc2.TLC -cleanup -config ProbeIntent.cfg ProbeIntent.tla      # NotIntentDone 被违反 => Done 可达
$J -cp $CP tlc2.TLC -cleanup -config ProbeLive.cfg ProbeLive.tla          # NotLive 被违反     => live 可达
$J -cp $CP tlc2.TLC -cleanup -config VacuityCheck.cfg VacuityCheck.tla    # NegIntentCompletes 被违反
```

`*_TTrace_*` 是 TLC 反例产物,已进 `.gitignore`(S4.12.1 二度误推教训)。

## 本轮 TLC 结果(2026-10-17)

| 检查 | 结果 |
|---|---|
| safety(4 不变量) | No error has been found(28 distinct states) |
| liveness(3 性质 + FairSpec) | No error has been found(91 states) |
| 动作覆盖门 | 14/14 在册动作总触发 > 0;SweepLiveOwned **有意移出**(见下) |
| 非空 probe ×3 | 全部如期 VIOLATED(Done 可达 / live 可达 / FairSpec 非空) |

开发期抓到并修掉的两个**模型错误**(非实现缺陷):①Register 缺 key
唯一性守卫 → 同 key 二次注册 + CAS 造出 live∧Tracked 假反例;②sweep
带 phase 门 → Crash/Recover 交替取消 ENABLED 期打爆 WF(S4.12 同款教
训)。处置:①Register 收紧为只从 pristine key 状态出发;②sweep/gc 动
作去 phase 门并在头注声明"锁互斥委托给 fslock/open_maintenance 的
Rust 测试"。

## 明确不声称(证明边界)

- 不建模任何具体 S3 错误码:failProne 是抽象故障期,PUT/DELETE 失败后
  远端状态在模型里就是自由的两态(与 trait 契约"Err 后 UNSPECIFIED"
  一致);HEAD 只在健康期可判定。
- 不声称掉电耐久:Crash 只翻 phase,所有持久行原样存活——这建模的是
  "已提交事务在进程死亡后可见"(SQLite 同步=FULL 的义务,由 S4.4 三道
  闸与文件库测试承保),不是断电。
- 不建模锁互斥、备份保护过滤(上游原样)、SQLite 事务自身的原子性
  (S4.2.1 纪律:事务粒度=单动作,是真 SQLite 的实现义务)。

## 模型性质 → Rust → SQLite 事务 → 故障测试 映射

| TLA+ 元素 | Rust 对应 | SQLite 承载 | 故障测试 |
|---|---|---|---|
| `Register`(PUT 前,pristine 守卫=key 唯一) | `register_version_candidate`(core.rs 两条路径 + wb.rs drain_task,PUT 前 `?` 即abort) | `INSERT OR REPLACE INTO version_candidates`(独立行,每 attempt 一行) | w7/c12(崩溃前后行仍在) |
| `PutOk` / `PutUncertain`(Err 后远端自由) | `put` / `put_stream` 返回值;trait 契约 Err=UNSPECIFIED | —(数据面) | CandidateFaultObjects.uncertain_put(真写后报错) |
| `CasCommit`(live+消费候选=单动作) | `commit_content_cas` 成功臂 | 同一 Immediate 事务内 `DELETE FROM version_candidates WHERE key=?1`(§3.2) | c3 / c11(文件库重开验证) |
| `CleanupConfirmed`(确认删除才删行) | `cleanup_version_candidate_after_failed_put`(仅 delete=Ok 才 drop 行) | `DELETE FROM version_candidates` | c1(确认)/c8(不确定则留行) |
| `CleanupUncertain`(行保留) | 同 helper 的 `Err` 臂 | 行不动 | c2/c4/w5(delete 注入 5xx) |
| `SweepIntentEnsured`(intent+删行=单动作) | `resolve_version_candidates` → `retire_version_candidate_to_gc` | 同一事务:`INSERT OR IGNORE INTO gc_deletions` + 删候选行(§3.4 无窗) | c2/c9/w7/w10 |
| (模型外)LiveOwned 分支 | 同 retire 的 live 判定 | 同事务 `NOT EXISTS(nodes WHERE object_key=?)` | s416_c14(守卫钉死,模型按 key 唯一性抽象排除) |
| `SweepConfirmedAbsent` | sweep HEAD ObjectMissing 臂 | `drop_version_candidate` | c9(b 键)/c8 收敛 |
| HEAD 不可判定=无动作(行保留) | sweep 其他 Err 臂 fail-open 保留 | 行不动 | c9(c 键,恢复后转 intent) |
| `GcDrain`(要求 ¬live) | `gc_execute_deletions`(protected 过滤上游) | `gc_claim/complete/fail_deletions` | c2/c6/w5/w10(gc 真删远端) |
| `Crash`(只翻 phase) | 进程退出;行在 SQLite | WAL+FULL 落盘义务(S4.4) | **c12 真子进程 exit(9)**(远端字节已落+候选仍在) |
| `ObjectTracked` 不变量 | —(机制整体保证) | 候选表+intent 表+nodes.object_key 三态互斥 | 十场景断言合集 |
| `LiveSafe`/`IntentSound` | retire 单事务内 live 复查;CAS 先消费 | 同事务谓词 | c4(胜者 key 不入队)/w7 |
| `CandidateResolves`/`IntentCompletes` | 启动 sweep(三入口)+gc executor | 两表联动 | w7/w10(恢复→sweep→gc 全链) |
| `TerminalTrichotomy` | — | — | c9(三判定各一) |

## FairSpec 假设(活性承重点)

- `<>[] StorageHealthy`:故障期最终永久结束(pack 模型同款稳定性假设)。
- WF_vars(Sweep*/GcDrain/Recover):sweep 与 gc 动作无 phase 门,ENABLED
  期不被 Crash/Recover 交替取消,弱公平即够。
- 真实对应:启动 sweep 挂在三引擎入口(build_core);gc 由运维周期触发
  (S3.4 语义),没有常驻 worker——模型假设与该部署事实一致。
