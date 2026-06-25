# mongogo

[![Go Reference](https://pkg.go.dev/badge/github.com/callmejoker9527/mongogo.svg)](https://pkg.go.dev/github.com/callmejoker9527/mongogo)
[![Go 版本](https://img.shields.io/badge/go-1.21+-blue.svg)](https://golang.org/dl/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`mongogo` 是一个 Go 语言 MongoDB 驱动，在官方 [mongo-driver v2](https://github.com/mongodb/mongo-go-driver) 之上提供与 **mgo 兼容的 API**，支持 **MongoDB 4.0 至 8.x** 的全部特性。

如果你有使用 [go-mgo/mgo](https://github.com/go-mgo/mgo) 或 [globalsign/mgo](https://github.com/globalsign/mgo) 的历史代码，`mongogo` 可以让你以极少的改动完成迁移，同时获得对现代 MongoDB 特性的完整支持。

---

## 功能特性

- ✅ **兼容 mgo API** — `Session`、`Database`、`Collection`、`Query`、`Iter`、`Bulk`、`Pipe`、`GridFS`
- ✅ **底层使用 mongo-driver v2** — MongoDB 官方 Go 驱动
- ✅ **支持 MongoDB 4.0–8.x**
- ✅ **Change Streams 变更流** — `Session.Watch`、`Database.Watch`、`Collection.Watch`
- ✅ **时序集合** — MongoDB 5.0+
- ✅ **读偏好模式** — Primary、Secondary、Nearest 等
- ✅ **写关注** — Acknowledged、Majority、Unacknowledged
- ✅ **GridFS** — 文件上传/下载及元数据管理
- ✅ **Bulk 批量操作** — 有序和无序
- ✅ **聚合管道** — 支持 AllowDiskUse、Collation 等
- ✅ **全文搜索** — `$text` 搜索通过 Pipe 支持

---

## 安装

```bash
go get github.com/callmejoker9527/mongogo
```

要求 Go 1.21+。

---

## 快速上手

```go
package main

import (
    "fmt"
    "log"

    "github.com/callmejoker9527/mongogo/mongogo"
    "go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
    // 连接数据库
    session, err := mongogo.Dial("mongodb://localhost:27017")
    if err != nil {
        log.Fatal(err)
    }
    defer session.Close()

    // 获取集合
    col := session.DB("mydb").C("users")

    // 插入文档
    err = col.Insert(bson.D{
        {Key: "name", Value: "Alice"},
        {Key: "age", Value: 30},
    })
    if err != nil {
        log.Fatal(err)
    }

    // 查询单个文档
    var result bson.M
    err = col.Find(bson.D{{Key: "name", Value: "Alice"}}).One(&result)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result)
}
```

---

## API 文档

### 连接数据库

```go
// 最简连接
session, err := mongogo.Dial("localhost:27017")

// 完整 URI（含认证、副本集）
session, err = mongogo.Dial("mongodb://user:pass@host1:27017,host2:27017/mydb?replicaSet=rs0")

// 带超时的连接
session, err = mongogo.DialWithTimeout("localhost:27017", 5*time.Second)

// 通过 DialInfo 精细控制
session, err = mongogo.DialWithInfo(&mongogo.DialInfo{
    Addrs:          []string{"localhost:27017"},
    Database:       "mydb",
    Username:       "user",
    Password:       "pass",
    Timeout:        10 * time.Second,
    ReplicaSetName: "rs0",
    PoolLimit:      100,           // 连接池上限
    MinPoolSize:    5,             // 连接池下限
    AppName:        "my-app",
    // TLSConfig: &tls.Config{...}, // 启用 TLS
})
```

---

### Session（会话）

会话是所有操作的入口，封装了连接池和全局配置。

```go
// 读偏好模式
session.SetMode(mongogo.Primary, true)           // 强一致，走主节点（默认）
session.SetMode(mongogo.PrimaryPreferred, true)  // 优先主节点
session.SetMode(mongogo.Secondary, true)         // 走从节点
session.SetMode(mongogo.SecondaryPreferred, true)// 优先从节点
session.SetMode(mongogo.Nearest, true)           // 最近节点
session.SetMode(mongogo.Monotonic, true)         // 单调一致性
session.SetMode(mongogo.Eventual, true)          // 最终一致性

// 写关注
session.SetSafe(&mongogo.Safe{W: 1, J: true})          // 写入主节点并刷盘
session.SetSafe(&mongogo.Safe{WMode: "majority"})       // 多数节点确认
session.SetSafe(mongogo.WriteConcernMajority())         // 同上（快捷方式）
session.SetSafe(nil)                                    // 不等待确认（fire-and-forget）

// 超时与连接池
session.SetSocketTimeout(30 * time.Second)
session.SetSyncTimeout(1 * time.Minute)
session.SetPoolLimit(200)

// 克隆会话（共享连接池，继承配置）
clone := session.Clone()
// 复制会话（同 Clone）
copy := session.Copy()
// 新建会话（共享连接池，重置配置）
fresh := session.New()

// 服务器信息
names, err := session.DatabaseNames()
info, err  := session.BuildInfo()  // 返回 MongoDB 版本等信息
err         = session.Ping()

// 运维操作（需要 admin 权限）
err = session.FsyncLock()    // 刷盘并锁定写入（备份用）
err = session.FsyncUnlock()  // 解锁
err = session.Fsync(false)   // 仅刷盘，不锁定

// 监控当前操作
ops, err := session.CurrentOp()
err       = session.KillOp(opID)

// 全局 Change Stream（需要副本集或分片集群，MongoDB 4.0+）
cs, err := session.Watch(nil)
defer cs.Close()
var event mongogo.ChangeStreamEvent
for cs.Next(&event) {
    fmt.Println(event.OperationType, event.Namespace.Collection)
}
```

---

### Database（数据库）

```go
db := session.DB("mydb")

// 基础操作
names, err := db.CollectionNames()
err         = db.DropDatabase()
err         = db.Run(bson.D{{Key: "ping", Value: 1}}, &result)

// GridFS
gfs := db.GridFS("fs")  // "fs" 是存储桶名称前缀

// 创建普通集合
err = db.CreateCollection("logs")

// 创建带 Capped 上限的集合
err = db.CreateCollectionWithOpts("events", &mongogo.CreateCollectionOptions{
    Capped:   true,
    MaxBytes: 100 * 1024 * 1024, // 100MB 上限
    MaxDocs:  10000,             // 最多 10000 条
})

// 创建时序集合（MongoDB 5.0+）
err = db.CreateCollectionWithOpts("metrics", &mongogo.CreateCollectionOptions{
    TimeSeries: &mongogo.TimeSeriesOptions{
        TimeField:   "timestamp", // 时间字段（必填）
        MetaField:   "sensor",    // 元数据字段（可选）
        Granularity: "seconds",   // 粒度：seconds / minutes / hours
    },
})

// 创建带验证规则的集合
err = db.CreateCollectionWithOpts("orders", &mongogo.CreateCollectionOptions{
    Validator: bson.D{{Key: "$jsonSchema", Value: bson.M{
        "required": bson.A{"name", "price"},
    }}},
    ValidationLevel:  "strict",  // strict / moderate / off
    ValidationAction: "error",   // error / warn
})

// 创建视图（MongoDB 3.4+）
err = db.CreateView("active_users", "users", bson.A{
    bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
})

// 用户管理
err = db.AddUser("readonly_user", "password", true)   // readOnly=true
err = db.AddUser("rw_user", "password", false)
err = db.RemoveUser("readonly_user")

// 数据库 Change Stream（MongoDB 4.0+）
cs, err := db.Watch(nil)
// 仅监听 insert 事件
cs, err  = db.Watch(bson.A{
    bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}},
})

// 查询集合规格（含时序、验证规则等详细信息）
specs, err := db.ListCollectionSpecs(nil)
```

---

### Collection（集合）

```go
col := db.C("users")

// ——— 插入 ———
err = col.Insert(doc)
err = col.Insert(doc1, doc2, doc3)  // 批量插入

// ——— 更新 ———
err         = col.Update(selector, update)    // 更新第一条匹配文档
err         = col.UpdateId(id, update)
info, err  := col.UpdateAll(selector, update) // 更新所有匹配文档
info, err   = col.Upsert(selector, update)    // 不存在则插入
info, err   = col.UpsertId(id, update)

// ——— 删除 ———
err         = col.Remove(selector)    // 删除第一条匹配文档
err         = col.RemoveId(id)
info, err   = col.RemoveAll(selector) // 删除所有匹配文档

// ——— 计数 ———
n, err := col.Count()             // 精确计数（扫描）
n, err  = col.EstimatedCount()    // 快速估算（使用元数据，MongoDB 4.0+）

// ——— FindAndModify（原子操作）———
info, err = col.Apply(mongogo.Change{
    Update:    bson.D{{Key: "$inc", Value: bson.D{{Key: "seq", Value: 1}}}},
    ReturnNew: true,   // 返回修改后的文档
    Upsert:    true,   // 不存在则插入
}, &result)

// 删除并返回旧文档
info, err = col.Find(selector).Apply(mongogo.Change{Remove: true}, &result)

// ——— 索引管理 ———
// 创建单个索引
err = col.EnsureIndex(mongogo.Index{
    Key:         []string{"email"},     // 普通升序
    Unique:      true,
    Name:        "email_unique",
    ExpireAfter: 24 * time.Hour,        // TTL 索引
})
// 复合索引（- 前缀表示降序）
err = col.EnsureIndexKey("name", "-created_at")
// 批量创建（效率更高）
err = col.CreateIndexes([]mongogo.Index{
    {Key: []string{"email"}, Unique: true},
    {Key: []string{"name", "-score"}},
    {Key: []string{"$text:content"}, DefaultLanguage: "english"}, // 全文索引
})
// 删除索引
err = col.DropIndex("email", "name")
err = col.DropIndexName("email_unique")
err = col.DropAllIndexes()
// 查看索引
indexes, err := col.Indexes()
// 重建索引
err = col.ReIndex()

// ——— 集合管理 ———
err = col.Rename("new_collection_name")
err = col.DropCollection()
stats, err := col.Stats()

// ——— 聚合 ———
pipe := col.Pipe(bson.A{
    bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
    bson.D{{Key: "$group", Value: bson.D{
        {Key: "_id", Value: "$city"},
        {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
    }}},
    bson.D{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
})
pipe.AllowDiskUse()   // 允许写临时文件（大数据集）
pipe.Batch(500)
err = pipe.All(&results)
err = pipe.One(&result)
iter := pipe.Iter()
var plan bson.M
err = pipe.Explain(&plan)

// ——— Bulk 批量操作 ———
bulk := col.Bulk()
bulk.Unordered()                         // 无序模式（遇错继续）
bulk.Insert(doc1, doc2, doc3)
bulk.Update(sel1, upd1, sel2, upd2)      // 成对传入：selector, update
bulk.UpdateAll(sel1, upd1)               // 更新所有匹配项
bulk.Upsert(sel1, upd1)
bulk.Remove(sel1, sel2)
bulk.RemoveAll(sel1)
result, err := bulk.Run()
// result.Matched / Modified / Inserted / Upserted / Deleted

// ——— Change Stream（MongoDB 3.6+，需要副本集）———
cs, err := col.Watch(nil)
// 指定过滤管道
cs, err  = col.Watch(bson.A{
    bson.D{{Key: "$match", Value: bson.D{
        {Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update"}}}},
    }}},
})
```

---

### Query（查询）

```go
// 构建查询
query := col.Find(bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: 18}}}})
query  = col.FindId(id)

// ——— 链式修饰符 ———
query.Sort("name", "-created_at")           // 排序（- 前缀为降序）
query.Select(bson.M{"name": 1, "email": 1}) // 投影（指定返回字段）
query.Skip(20).Limit(10)                    // 分页
query.Hint("email")                         // 指定索引
query.Hint("-name", "age")                  // 复合索引 hint
query.Batch(100)                            // 游标批次大小
query.Comment("debug: user list query")     // 查询注释（便于 profiling）
query.NoCursorTimeout()                     // 禁用游标超时
query.AllowDiskUse()                        // 允许磁盘（大结果集，MongoDB 4.4+）
query.Collation(&mongogo.Collation{
    Locale:   "zh",       // 中文排序
    Strength: 2,          // 忽略大小写
})
query.Where("this.score > this.minScore")   // $where JavaScript 条件（慎用）

// ——— 执行查询 ———
err = query.One(&result)    // 返回第一条，无结果返回 ErrNotFound
err = query.All(&results)   // 返回全部（结果集不宜过大）
n, err := query.Count()     // 计数

// 去重
var cities []string
err = query.Distinct("city", &cities)

// ——— 游标迭代 ———
iter := query.Iter()
var doc bson.M
for iter.Next(&doc) {
    fmt.Println(doc)
}
if err := iter.Err(); err != nil {
    log.Fatal(err)
}
iter.Close()

// 可追加游标（Tailable，用于 Capped 集合）
iter = col.Find(nil).Tail(5 * time.Second)

// ——— FindAndModify ———
info, err := query.Sort("-_id").Apply(mongogo.Change{
    Update:    bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "done"}}}},
    ReturnNew: true,
}, &result)

// ——— 执行计划 ———
var plan bson.M
err = query.Explain(&plan)
// 三种详细度
err = query.ExplainVerbosity("queryPlanner", &plan)      // 仅计划
err = query.ExplainVerbosity("executionStats", &plan)    // 执行统计（默认）
err = query.ExplainVerbosity("allPlansExecution", &plan) // 全部候选计划
```

---

### Iter（迭代器）

```go
iter := col.Find(nil).Sort("_id").Iter()

var doc bson.M
for iter.Next(&doc) {
    fmt.Println(doc)
}

// 错误检查
if err := iter.Err(); err != nil {
    log.Fatal(err)
}
iter.Close()

// 状态判断
if iter.Done() { ... }         // 游标是否已耗尽
if iter.Timeout() { ... }      // 是否超时

// 获取原始 BSON（适合高性能场景，避免二次解码）
rawBytes := iter.Data()        // 当前文档的原始 BSON 字节（需在 Next 后调用）
rawBytes  = iter.RawNext()     // 推进游标并返回原始 BSON
```

---

### GridFS（文件存储）

```go
gfs := db.GridFS("fs")  // "fs" 为存储桶前缀，默认即可

// ——— 上传文件 ———
file, err := gfs.Create("avatar.png")
file.SetMeta(bson.M{
    "userId":      "user123",
    "contentType": "image/png",
    "tags":        []string{"avatar", "profile"},
})
_, err = io.Copy(file, imageReader)
err    = file.Close()  // 必须调用 Close 才会真正写入

// 从 io.Reader 直接上传（不需要手动 Close）
fileId, err := gfs.UploadFromStream("report.pdf", pdfReader)

// ——— 下载文件 ———
// 按文件名（最新版本）
file, err = gfs.Open("avatar.png")
_, err    = io.Copy(w, file)
file.Close()

// 按 ID
file, err = gfs.OpenId(fileId)

// 直接写入 io.Writer
n, err := gfs.DownloadToStream("report.pdf", writer)
n, err  = gfs.DownloadToStreamByID(fileId, writer)

// ——— 文件元数据访问 ———
fmt.Println(file.Id())          // 文件 _id
fmt.Println(file.Name())        // 文件名
fmt.Println(file.Size())        // 文件大小（字节）
fmt.Println(file.UploadDate())  // 上传时间
fmt.Println(file.ContentType()) // 内容类型（如有）
var meta bson.M
err = file.GetMeta(&meta)       // 解码自定义元数据

// 定位读取（正向偏移）
_, err = file.Seek(1024, 0)     // 跳过前 1024 字节（仅支持 io.SeekStart）

// ——— 删除文件 ———
err = gfs.Remove("avatar.png")  // 按文件名删除所有版本
err = gfs.RemoveId(fileId)      // 按 ID 删除
err = gfs.Drop()                // 删除整个 GridFS 存储桶

// ——— 查询文件列表 ———
var files []mongogo.GridFileInfo
err = gfs.Find(bson.D{{Key: "metadata.userId", Value: "user123"}}).
    Sort("-uploadDate").
    Limit(20).
    All(&files)

// 计数
n, err := gfs.Find(nil).Count()

// 按 ID 查询
info := mongogo.GridFileInfo{}
err = gfs.FindId(fileId).One(&info)

// 游标迭代
iter := gfs.Find(nil).Iter()
var info mongogo.GridFileInfo
for iter.Next(&info) {
    fmt.Printf("文件: %s, 大小: %d 字节\n", info.Filename, info.Length)
}
iter.Close()

// 迭代并打开每个文件
var f *mongogo.GridFile
it := gfs.Find(nil).Sort("filename").Iter()
for gfs.OpenNext(it, &f) {
    fmt.Println(f.Name())
    // 处理文件内容...
    f.Close()
}
```

---

### Change Stream（变更流）

需要 MongoDB 3.6+ 和副本集（或分片集群）。

```go
// 三个层级均支持 Watch：
// 1. 集合级别
cs, err := col.Watch(nil)
// 2. 数据库级别（监听库内所有集合）
cs, err  = db.Watch(nil)
// 3. 全局级别（监听所有数据库）
cs, err  = session.Watch(nil)

defer cs.Close()

// ——— 迭代事件（通用方式）———
var raw bson.M
for cs.Next(&raw) {
    fmt.Println(raw["operationType"])
}

// ——— 使用结构化事件类型 ———
for {
    ev, ok := cs.NextEvent()
    if !ok {
        break
    }
    fmt.Printf("操作: %-10s 集合: %s\n",
        ev.OperationType,
        ev.Namespace.Collection,
    )
    if ev.OperationType == "update" && ev.UpdateDescription != nil {
        fmt.Println("  更新字段:", ev.UpdateDescription.UpdatedFields)
    }
}

// ——— 断点续传 ———
token := cs.ResumeToken()
// 保存 token，程序重启后恢复
cs, err = col.Watch(nil, options.ChangeStream().SetResumeAfter(token))

// ——— 错误处理 ———
if err := cs.Err(); err != nil {
    log.Println("Change stream 错误:", err)
}
```

---

### BSON 类型与工具函数

```go
// mongogo 重新导出了常用 bson 类型，无需单独 import bson 包
mongogo.D{{Key: "name", Value: "Alice"}}  // bson.D — 有序文档
mongogo.M{"name": "Alice"}               // bson.M — 无序文档（map）
mongogo.A{"a", "b", "c"}                 // bson.A — 数组
mongogo.E{Key: "name", Value: "Alice"}   // bson.E — 键值对
var raw mongogo.Raw                       // bson.Raw — 延迟解码的原始 BSON

// ——— ObjectID 工具 ———
id  := mongogo.NewObjectId()                          // 生成新 ID
id   = mongogo.ObjectIdHex("507f1f77bcf86cd799439011") // 从 Hex 字符串解析
ok  := mongogo.IsObjectIdHex("507f1f77bcf86cd799439011") // 校验格式
id   = mongogo.NewObjectIdWithTime(time.Now())        // 用指定时间生成 ID
t   := mongogo.ObjectIdTime(id)                       // 提取 ID 中的时间戳

// ——— 错误判断 ———
if mongogo.IsDup(err) { ... }               // 是否为重复键错误（11000）
if err == mongogo.ErrNotFound { ... }       // 查询无结果
if err == mongogo.ErrSessionClosed { ... }  // 会话已关闭
if mongogo.IsErrNoDocuments(err) { ... }    // 同 ErrNotFound

// ——— 写关注快捷方式 ———
session.SetSafe(mongogo.WriteConcernMajority()) // {WMode: "majority", J: true}
```

---

### 调试日志

```go
// 开启调试日志（会输出内部操作信息）
mongogo.SetDebug(true)

// 自定义日志输出（实现 mongogo.Logger 接口）
type myLogger struct{}
func (l *myLogger) Output(calldepth int, s string) error {
    log.Println("[mongogo]", s)
    return nil
}
mongogo.SetLogger(&myLogger{})

// 关闭日志
mongogo.SetDebug(false)
```

---

## 从 mgo 迁移

API 设计与 mgo 保持高度兼容，大多数代码可直接替换包名。主要差异如下：

| mgo 用法 | mongogo 用法 | 说明 |
|---------|-------------|------|
| `import "gopkg.in/mgo.v2"` | `import "github.com/callmejoker9527/mongogo/mongogo"` | 包路径变更 |
| `mgo.Dial(url)` | `mongogo.Dial(url)` | 签名相同 |
| `session.DB("x").C("y")` | `session.DB("x").C("y")` | 完全一致 |
| `query.One(&r)` | `query.One(&r)` | 完全一致 |
| `query.SetMaxTime(d)` | `query.SetMaxTime(d)` | 保留，但 driver v2 内部不再转发 |
| `query.Snapshot()` | `query.Snapshot()` | no-op，MongoDB 3.6+ 已移除 |
| `db.Eval(...)` | `db.Eval(...)` | 返回错误；$eval 在 MongoDB 4.2 移除 |
| `MapReduce` | `MapReduce` | MongoDB 5.0 已废弃，建议改用 `Pipe` |
| `Safe.WTimeout` | 不转发 | 改用 `context.WithTimeout` 控制超时 |
| Background 索引构建 | 不应用 | MongoDB 4.2 移除后台索引构建 |
| `bson.ObjectId` (string) | `bson.ObjectID` ([12]byte) | 官方驱动类型变更，注意序列化差异 |
| `mgo.IsDup(err)` | `mongogo.IsDup(err)` | 函数签名相同 |

---

## MongoDB 版本特性支持矩阵

| 特性 | 最低 MongoDB 版本 | 状态 |
|------|-----------------|------|
| 基本 CRUD | 3.6+ | ✅ |
| 聚合管道 | 3.6+ | ✅ |
| Change Streams | 3.6+（需副本集） | ✅ |
| 视图（View） | 3.4+ | ✅ |
| Collation 排序规则 | 3.4+ | ✅ |
| 部分索引（Partial Index） | 3.2+ | ✅ |
| 通配符索引（Wildcard Index） | 4.2+ | ✅ |
| Find 的 AllowDiskUse | 4.4+ | ✅ |
| 时序集合（Time Series） | 5.0+ | ✅ |
| 可查询加密（FLE2） | 6.0+ | ✅（配置项） |
| Retryable Writes/Reads | 4.0+ | ✅（驱动自动处理） |

---

## 项目结构

```
mongogo/
├── mongogo/           # 核心包
│   ├── session.go     # 连接管理（Dial / Session）
│   ├── database.go    # 数据库操作（Database）
│   ├── collection.go  # 集合操作（Collection / Pipe）
│   ├── query.go       # 查询接口（Query）
│   ├── iter.go        # 游标迭代器（Iter）
│   ├── bulk.go        # 批量写入（Bulk）
│   ├── gridfs.go      # GridFS 文件存储
│   ├── types.go       # 类型定义 / ChangeStream / 工具函数
│   └── writeconcern.go# 写关注辅助
├── main.go            # 使用示例
├── go.mod
└── README.md
```

---

## 许可证

MIT License — 详见 [LICENSE](LICENSE) 文件。

