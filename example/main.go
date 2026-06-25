package main

import (
	"fmt"
	"log"
	"time"

	"github.com/callmejoker9527/mongogo/mongogo"
)

func main() {
	// 示例: 连接 MongoDB
	// session, err := mongogo.Dial("localhost:27017")
	// 或使用完整的 URL:
	// session, err := mongogo.Dial("mongodb://user:pass@localhost:27017/mydb")

	// 展示包的基本使用方式
	fmt.Println("mongogo - MongoDB driver for Go")
	fmt.Println("Based on mgo API, powered by mongo-driver v2")
	fmt.Println()

	// 展示 DialInfo 的用法
	info := &mongogo.DialInfo{
		Addrs:    []string{"localhost:27017"},
		Database: "mydb",
		Timeout:  10 * time.Second,
	}
	fmt.Printf("DialInfo: Addrs=%v, Database=%s\n", info.Addrs, info.Database)

	// 展示 Safe 模式
	safe := &mongogo.Safe{W: 1, J: true}
	fmt.Printf("Safe: W=%d, J=%v\n", safe.W, safe.J)

	// 展示 ObjectID 操作
	oid := mongogo.NewObjectId()
	fmt.Printf("ObjectId: %s\n", oid.Hex())
	fmt.Printf("IsObjectIdHex: %v\n", mongogo.IsObjectIdHex(oid.Hex()))

	// 展示 bson.D 的使用 (与 mgo 兼容)
	query := mongogo.D{
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: 30},
	}
	fmt.Printf("Query: %v\n", query)

	// 展示 Index 定义
	index := mongogo.Index{
		Key:    []string{"email"},
		Unique: true,
		Name:   "email_unique",
	}
	fmt.Printf("Index: Key=%v, Unique=%v\n", index.Key, index.Unique)

	// 展示连接（实际连接需要 MongoDB 运行）
	fmt.Println()
	fmt.Println("To connect to MongoDB:")
	fmt.Println("  session, err := mongogo.Dial(\"localhost:27017\")")
	fmt.Println("  if err != nil { log.Fatal(err) }")
	fmt.Println("  defer session.Close()")
	fmt.Println()
	fmt.Println("  db := session.DB(\"mydb\")")
	fmt.Println("  col := db.C(\"users\")")
	fmt.Println("  err = col.Insert(bson.D{{Key: \"name\", Value: \"Alice\"}})")

	_ = log.Default()
}
