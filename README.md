# xredis

#### **xredis**是一个用Golang实现 仿Redis的 key-value内存数据库服务

> 目前已支持 string, list, set, sorted set 数据结构
>
> 支持多数据库，支持自动过期功能 (TTL) 
>
> 支持AOF持久化及AOF重写
>
> 内置集群模式，集群对客户端是透明的，可以像使用单机版redis一样使用xredis集群



#### 支持的命令：

- Server
  - select
  - rewriteaof
  - bgrewriteaof
  - flushdb
  - flushall
- String
  - Set
  - Get
- List
  - LIndex
  - LLen
  - LPush
  - LPop
  - LPushX
  - LRange
  - LRem
  - LSet
  - RPop
  - RPopLPush
  - RPush
  - RPushX
- Set
  - SAdd
  - SIsMember
  - SRem
  - SPop
  - SCard
  - SMembers
  - SRandMember
  - SInter
  - SInterStore
  - SUnion
  - SUnionStore
  - SDiff
  - SDiffStore



#### 如何使用xredis：

xredis服务端：

<img src="C:\Users\wangf16\AppData\Roaming\Typora\typora-user-images\1667373540567.png" alt="1667373540567"  />

xredis客户端：

![1667373624392](C:\Users\wangf16\AppData\Roaming\Typora\typora-user-images\1667373624392.png)