# Sardine

SeqSvr: 分布式单调递增ID生成器

## 基本要求

> - 递增64位整型，允许空洞，不允许回退
> - 每个用户拥有独立的64位sequence空间

## 实现策略

### AllocSvr 负责ID生成分发

> - 内存中储存最近一个分配出去的序号 `cur_seq`，以及分配上限 `max_seq`
> - 分配序号时，将 `cur_seq++`，同时与分配上限 `max_seq` 比较；如果 `cur_seq >= max_seq`
    ，将分配上限提升一个步长 `max_seq += step`，并持久化 `max_seq`
> - 重启时，读出持久化的 `max_seq`，赋值给 `cur_seq`，重新预分配一段可分配序列号
> - 多个用户共享同一个 `max_seq`，用户按照 `id` 哈希，划分为若干组

重启时，不可避免会产生序号空洞；使用哈希分组是为了避免负载不均，也带来另一个问题：不能随意增删用户。

### AssignSvr 负责指派Alloc负责的用户段

> - 单例部署
> - 注册/注销 `section`
> - 监听有哪些 `Alloc` 实例可以用于分配，有哪些待分配的 `section`
> - 按照一致性哈希规则将 `section` 分配给 `Alloc`
> - 监听下线的 `Alloc`, 防止 `Alloc` 宕机未能及时删除路由规则

### ProxySvr 负责处理请求，将请求路由到不同的 AllocSvr

> - 监听路由规则，将请求分发到正确的实例
> - 批量请求需要同时请求多台实例，性能不一定能保证

### 压力分析

```txt
table: for_dev/for_test/for_prod 区分环境

// 数量 = alloc 实例数量
/segment/{table}/alloc/{addr}                     | timestamp

// 数量 = 业务场景数量 X 哈希组数量
/segment/{table}/section/{tag}/{hashId}           | pending/running

// 数量 = 业务场景数量 X 哈希组数量
/segment/{table}/routing/{addr}/{tag}/{hashId}    | pending/running

```

假设 20 个 `alloc` 实例，20 个 `proxy` 实例， 3 个 `assign` 实例，10 种业务场景，id 空间为 int32，哈希组 512 个。需要

>- 内存 > 16G x 10，主要是 alloc 加载到内存中的占用
>- etcd key 10 x 512 x 2 + 20 > 10240 个，section/routing 占大多数

## TODO

> - Enable auto re-balance in assign server
> - Reduce memory reduce of alloc server
> - Remove the limitations of singleton deployment to assign server
