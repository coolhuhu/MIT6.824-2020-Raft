
# 1.Raft 用来解决什么问题？
Raft 是一种共识算法，用来解决分布式环境下服务器节点的数据一致性的问题。

>Consensus algorithms allow a collection of machines to work as a coherent group that can survive the failures of some of its members.

共识算法保证了一群节点对外提供的服务像一台单独的节点对外提供服务一样，当集群中的少数节点出现故障时，仍然能正常的像一个节点一样对外提供服务。

使用 Raft 的应用有分布式键值存储数据库，[etcd](https://github.com/etcd-io/etcd) 和 [TiKV](https://github.com/tikv/tikv)。

# 2.理解Raft设计中几个重要的概念
## 2.1 理解Raft中的复制状态机
>Consensus algorithms typically arise in the context of replicated state machines. In this approach, state machines on a collection of servers compute identical copies of the same state and can continue operating even if some of the servers are down. Replicated state machines are used to solve a variety of fault tolerance problems in distributed systems.

状态机的一个特征为：相同的输入得到的相同的结果（也即“状态”）。

在 Raft 中，一致性针对的是复制日志的一致性。当保证了集群中节点的日志一致性后，各节点按照相同的顺序执行日志中的指令时，产生的结果（也即“状态”）也是一致的。而复制状态机通常都是基于复制日志来实现的。

一个需要弄清楚的点是，Raft 是一种算法，它提供一种逻辑框架，并证明了运行在该逻辑框架下的服务器节点，能够保证节点之间数据状态的一致性。但若要将该算法应用到实际的业务场景下，需要考虑很多场景细节，做相关的优化等。**因此，要明白在Raft的设计中，“日志被安全的复制到各台服务器节点上” 和 “日志被状态机 apply” 是两个独立的行为，即日志被安全地复制到各台服务器节点上后，并不代表日志就会被状态机 apply。** 对于日志被安全地复制后，何时被状态机apply，Raft中并没有规定，这也是在实际场景中需要去考虑优化的一个重要问题。一种简单的做法是，一旦被安全的复制，就要求同步服务器中的所有节点，将这些已安全地复制的日志apply到状态机中。这种做法虽然可行，能够保证状态机的安全性和一致性，但是一旦日志被安全复制就要apply，性能开销很大。

## 2.2 理解 committed 状态的日志
>The leader decides when it is safe to apply a log entry to the state machines; such an entry is called committed. Raft guarantees that committed entries are durable and will eventually be executed by all of the available state machines. A log entry is committed once the leader that created the entry has replicated it on a majority of the servers.

上述摘录自论文关于 committed 日志的描述。结合上下文语境，committed 翻译为”已提交“，表示日志的一种状态，由论文翻译过来为 ”committed 状态的日志可以安全地应用到状态机“。

显然，”committed 状态的日志可以安全地应用到状态机“，表示得依然含糊不清（哭笑不得）。如何解释呢？

回顾一下 Raft 算法是用来解决什么问题？Raft 保证了当集群中的少数节点出现故障时，整个系统任然能正常的像一个节点一样对外提供服务。注意哈，是少部分结点出现故障，如果大部分结点出现故障，那就叫天天不灵了。

为什么小部分结点出现了故障，整个系统任然能正常的对外服务呢？因为 Raft 保证当一个请求（这里应该特指写请求，写请求可能会改变数据内容，需要日志落盘；而读请求不改变数据内容，不需要日志落盘）从客户端发送给该分布式系统时，该写请求的日志被复制到集群中大多数节点上时（说人话就是，一半以上的节点拥有相同的日志内容），才会成功响应该写请求，不然就响应写失败。需要注意，大多数节点是根据集群中的所有节点来计算的，不要错误地理解为集群中正常运行节点数量中的大多数节点。

因此，”当小部分结点出现了故障，整个系统任然能正常的对外服务“ 就很好理解了。即少数服从多数，多数节点拥有相同的日志内容，那将这些多数节点上的日志应用到状态机中，当然就是正确和安全的。

那 ”committed 状态的日志可以安全地应用到状态机“ 就解释清楚了，”committed 状态的日志表明这些日志已经被复制到集群中的大多数节点中。

**一条日志什么时候可以被设置为 committed 状态？**
换个问法为，如何知道一条日志被复制到集群中的大多数节点中？在 Raft 中，Leader 节点会并发的向集群中的所有 follower 节点发送复制日志的 RPC，收到RPC后，follower 节点会RPC中携带的日志内容复制到自己的本地日志中，然后回复一个 “日志复制成功“ 的响应，当 Leader 节点收到了大多数 follower 节点的成功响应后，就把该条日志设置为 committed 状态。



# 3.Raft中选主的设计
## 3.1理解Raft中的随机超时选举
按照 ”Raft 中 Leader 节点是如何维护 Leader 身份？“ -> ”Leader 节点出现故障不可用，新的 Leader 节点是如何选举出来？“ -> ”集群初始化时，Leader 节点是如何被选举出来？“ 的顺序，来理解 Raft 中选主的设计，顺其自然就理解了 Raft 中的随机超时选举。

### 1.Raft 中 Leader 节点是如何维护 Leader 身份？
在正常运行的状态下，Leader 节点通过定期向 Follower 节点发送心跳包来维护自身的Leader身份。具体地，每个节点在自身内部都会维护一个”定时器“，Follower 节点收到 Leader 节点发来的心跳包（也包括复制日志的RPC请求）后，会重置该”定时器“；若 Follower 节点内部的定时器到期，则 Follower 节点认为集群中的 Leader 节点出现了不可用的情况，于是转变身份为 Candidate，然后发起一轮新的选举，尝试选举自己为新的 Leader。
此外，Leader 节点在心跳包中还会携带最新的 (term, index) 信息，用以日志一致性检查。

### 2.Leader 节点出现故障不可用，新的 Leader 节点是如何选举出来？
如”Raft 中 Leader 节点是如何维护 Leader 身份？“中解释的那样，Leader 节点出现故障，就不能正常的定时发送心跳包给 Follower 节点，维护自身的 Leader 身份。而 Follower 在定时器到期后，仍没有收到 Leader 发送来的任何消息，就转变身份，尝试发起一轮新的选举。


### 3.集群初始化时，Leader 节点是如何被选举出来？
对于集群的初始状态，是没有 Leader 节点的，那 Raft 是如何设计，使得一个 Leader 节点被顺利的选举出来的呢？

由上面两小节 ”Raft 中 Leader 节点是如何维护 Leader 身份？“ 和 ”Leader 节点出现故障不可用，新的 Leader 节点是如何选举出来？“ 所述，每个节点内部都维护着一个定时器，当定时器超时后，Follower节点若仍未收到 Leader 发来的消息，则转变身份，开启一轮新的选举。对于集群初始化时，也是通过定时器超时，来发起 Leader 的选举，称为”超时选举（election timeout）“。

集群初始化后，每个节点内部维护的定时器开始启动，等待 Leader 节点发送的消息，因为初始状态没有 Leader 节点，所有定时器超时，发起新的选举。（Leader 节点是怎样选举成功的？论文讲述的很清楚，这里不在赘述）但是若没有一些额外的限制（其实就是指随机超时选举），很容易出现 “集群中的节点同时开启定时器，定时器同时超时，尝试选举的节点瓜分了投票” 的情况，就会导致在一轮选举中，没有Leader被选举出来。为了减少这种的情况发生的概率，Raft 引入了“随机超时选举（randomized election timeouts）”。

具体地，每个节点的“超时时间”在一个时间范围内随机生成，这样就能大大降低节点在同一时刻发生超时的情况，减少在一轮选举中没有Leader被选举出来的情况。若在一轮选举中，没有Leader被选举出来，则继续发起一轮新的选举，论文9.3节证明了发生这种情况的概率很低。


# 4.如何处理日志不一致的情况
该篇[博客](https://juejin.cn/post/6907151199141625870#heading-21)解释得非常清楚。



# 5.安全性是如何保证的
**Raft算法保证每个节点的状态机会严格按照相同的顺序 apply 日志**，这个 “保证” 是通过Leader选举和日志提交的一些额外限制来实现的。

## 5.1 Leader选举的限制
Raft 的设计核心之一是强领导人（Strong Leader）机制，即 Raft 强制要求 follower 必须复制 leader 的日志集合来解决不一致问题。那当集群中当前 leader 节点出现故障不可用时，就需要选举出新的 leader 节点，从而继续保证集群中的节点日志的一致性。

由于 Raft 采用的是强领导人机制，当集群中当前 leader 节点出现故障需要选举新的 leader 节点时，选举出来的 leader 节点需要满足 “拥有所有 committed 日志”，这样才能保证所有 follower 节点最终能够复制 所有 committed 日志，从而保证 **Raft算法保证每个节点的状态机会严格按照相同的顺序 apply 日志**。

若选举出来的 leader 节点不满足 “拥有所有 committed 日志” 这个条件，会有什么问题？举一个反例来进一步理解。
当前集群中 leader 节点 committed 日志为 [1, 2, 3, 4, 5]，然后该 leader 节点出现故障不可用了，此时一个拥有 committed 日志为 [1, 2, 3] 的 follower 节点A 被选举为 leader 节点，由于强领导人机制，新的 leader 节点需要将自己的日志覆盖 follower 节点的日志，因此对于拥有 committed 日志为 [1, 2, 3, 4, 5] 的 follower 节点B，它的日志内容会变为 [1, 2, 3]，这就导致了对于上一任期中 committed 的日志条目4和日志条目5 “失效了”，从而出现 “上一任期中，服务器已经正确响应了客户端关于日志条目4和日志条目5的请求，但由于还没应用到状态机，而 leader 出现故障不可用，新的 leader 在新的一轮任期中，覆盖掉了上一轮任期中 committed 的部分日志条目” 的情况，这样就导致了状态机是不可靠的。
出现这种情况的一种典型场景是，客户端发起了写操作请求，服务端响应写成功，但还没真正将数据写入磁盘，此时集群中 leader 节点出现故障不可用，新的 leader 选举出来，进行日志同步时，把之前的写请求日志覆盖掉了，并把写操作的数据写入磁盘了；然后客户端查了一下之前的写入的数据，发现查不到，这不就懵逼了吗。


**这个选举限制是什么？**
为了避免上述情况发生，在 leader 的选举上加上一个限制。首先回顾 Raft 设计的一个核心思想，“当日志成功写入服务器集群中的大多数节点时，即使发生网络分区故障，或者少部分节点发生异常，整个集群依然能够像单机一样提供服务”。简单化来讲就是 ”少数服从多数“，同样 leader 的选举始终遵循这一准则。

选举限制为，在每个 candidate 向集群中的其他节点发送投票请求，尝试选举自己为 leader 时，在投票请求中会携带自己本地日志最新的 (term, index) 。收到投票请求的 follower 节点，通过比较 自己本地日志最新的 (term, index) 和 投票请求中的 (term, index) ，来判断是否投票给发起投票请求的节点。比较两个 (term, index) 的逻辑非常简单：如果 term 不同， term 更大的日志更新；否则 index 大的日志更新。


**小结：**
- Leader 选举限制的核心为”少数服从多数“，即，当前节点中拥有的日志内容在集群中大多数节点上都存在时，该节点才具备选举成为 leader 的权利。
- 整个分布式集群在运行过程中始终保持这一限制。
- leader 满足该选举限制后，还不能完全保证 ”**Raft算法保证每个节点的状态机会严格按照相同的顺序 apply 日志**“，还需要对日志提交进行一些限制。


## 5.2 日志提交的限制
>在 Raft 的设计中，对于写日志请求，会先转发给 leader 节点，然后 leader 节点会先写入自己的本地日志，然后并发的向集群中的其他节点发送 ”复制该日志“ 的请求。只有当收到集群中大多数节点的”复制成功“的响应后，leader 节点才会将该条日志设置为 committed，然后 Raft 会保证 committed 的日志最后会被状态机 apply。


在运行环境正常的情况下，leader 节点和集群中的其他 follower 节点稳定运行，上述日志提交过程不会出什么问题。但现实的网络环境中，什么情况都有可能发生，节点宕机、网络延迟、网络不可用等，当由各种情况导致集群中的 leader 节点变换时，对于新上任的 leader 节点，在同步 leader 节点和 follower 节点之间的 committed 日志时，若不添加一些额外的限制，就可能导致集群中节点的状态机不一致的情况。在 Raft 的论文中，给出了一个经典的例子来表示没有额外限制的条件下会出现的情况，[该篇博客](https://juejin.cn/post/6907151199141625870#heading-24)对此场景进行非常好的解释。本文就不再赘述。

额外的日志提交限制为，**leader 节点只允许 commit 当前任期的日志**。回顾上文所述的 “对于 committed 日志（term, index），在该（term, index）日志之前的日志都是已经 committed”。


# 6.集群成员变更
在 Raft 中，集群变更使用的同样写日志的方式。令当前的集群配置为 `Cold`，新的集群配置为 `Cnew`。当 leader 收到将集群配置从 `Cold` 更新为 `Cnew` 的请求后，先创建一个 `Cold,new` 的配置，然后将其作为 log entry，向集群中的其他节点发送 AppendRPC，要求集群中的其他节点复制该 log entry（在Raft中，集群配置使用特殊的log entry进行存储和通信）。**一旦服务器节点将包含配置信息的 log entry 复制到自己的日志当中，它就使用该配置来进行未来所有的决策，而不管包含配置信息的log entry是否已提交。即，只要一个服务器节点接受到了最新的服务器配置信息，就使用这个新的配置。**

因此，当leader收到将集群配置从 `Cold` 更新为 `Cnew` 的请求后，leader会使用`Cold,new`的配置来判断`Cold,new`的log entry何时被提交。那么当该leader不可用时，就只能是配置为`Cold`或`Cold,new`的服务器节点来开启一轮新的选举。因为在当前，尝试开启一轮新的选举的服务器节点要么收到了包含`Cold,new`配置的日志，要么还没收到，仍然保持`Cold`的配置。

因此，一旦包含 `Cold,new` 的log entry被提交，根据领导者完整性属性（Leader Completeness Property），只有包含`Cold,new` log entry的服务器节点能够被选举成为leader。

`Cold,new` 的 log entry 被提交后，在从 `Cold,new` 切换到 `Cnew`。同样基于上述相同的分析，最终集群配置会被安全地切换到`Cnew`。



<details>
<summary>举例说明 Cold --> Cold,new --> Cnew 配置的过度</summary>
由于论文中没有为该部分给出具体的示例说明，我根据网上的一些文章以及自己的理解，给出如下解释。

我们假设更新前的配置为`Cold`，其集群中有 A、B、C 三台服务器；更新后的配置为`Cnew`，其集群中有 A、B、C、D、E 五台服务器，即增加了两台服务器。

对于配置为`Cold`的集群，要想选举成为leader，需要得到除自身外任意一台服务器的投票才能赢得选举。对于配置为`Cnew`的集群，想要选举成为leader，需要得到除自身外的任意两台服务器的投票才能赢得选举。因此，对于配置`Cold`，意味着配置`Cold`下的A、B、C 三台服务器节点是互相可见；对于配置`Cnew`，意味着配置`Cnew`下的A、B、C、D、E 五台服务器节点是互相可见。

那现在来简单分析一下，在集群节点变更时，直接从`Cold`切换为`Cnew`为什么不行。

仍然以上面的假设为例，假设当前集群（配置为`Cold`）中的leader节点为C，当集群配置直接从`Cold`切换到`Cnew`，leader节点先将`Cnew`配置包装为特殊的log entry，之后leader节点就以`Cnew`配置进行未来的决策，因此leader可以看到A、B、D、E节点，然后leader并行的发起AppendRPC，将该log entry复制给其他节点。然而在发起AppendRPC之前，leader节点（C节点）挂了，开始一轮新的选举。此时节点A尝试选举，节点A没有收到包含`Cnew`的log entry，因此节点A目前能看到的节点为B、C，然后向它们发起RequestVoteRPC，此时，节点A只要收到了B节点的投票就可以被选举为leader。而对于节点E，它也可以尝试选举，因为它的配置为`Cnew`，因此它可以看到的集群节点为：A、B、C、D，它只需要收到其中的两个节点的投票，就可以被选举为leader；此时，C节点恢复，C、D节点给E节点投票，E节点被选举为leader。集群被选举出了两个leader，这是危险的。

既然直接进行配置切换会使得系统不安全，那来看看通过一个中间配置是如何保证配置切换的安全，以及中间配置`Cold,new`对集群节点有什么限制。

对于中间状态的配置（论文中称为 joint consensus），该配置下集群节点的可见性为更新前节点可见性和更新后节点可见性的并集。例如：上述示例中，`Cold,new`配置下的节点A、B、C、D、E相互可见。但该配置下，log entry的提交增加了限制，要求更新前集群中节点的大多数和更新后新增的节点中的大多数回复确认了该log entry，才能将该log entry提交。例如：上述示例中，`Cold,new`配置下，leader若要将log entry确认为已提交的，需要A、B节点中至少一个节点和D、E节点中至少一个节点确认。

有了以上限制，就可以保证配置更新的安全性。读者可以根据上述的示例自行推导验证其合理性。

</details>


# 7.Raft中容易混淆的点

对于Raft中的日志复制机制，我们知道，当一条日志已经确保被安全地复制了（被安全地复制是指，N/2 + 1 数量及该数量以上的节点已经复制了该日志），Leader 会将该日志 apply 到它的本地的状态机中，然后把操作的结果返回给客户端。**这里需要注意的是，Raft 协议中并没有规定 Leader 需要把日志应用到状态机后，才把操作成功的结果返回给客户端。** 即，可以是先把操作成功的结果返回给客服端，然后日志暂时还未应用本地的状态机。

总结下来，即 Raft 保证的只是集群中日志的一致性，而对于集群对外的状态机的一致性需要一些额外的限制。




# 参考链接
1. 论文链接：https://raft.github.io/raft.pdf
2. 目前看到的对Raft剖析最好的中文博客：[https://juejin.cn/post/6907151199141625870]()
3. 动画演示：https://raft.github.io/
