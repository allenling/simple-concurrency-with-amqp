simple-concurrency-with-amqp
============================

* 启动一个线程去建立amqp连接, 一旦建立了连接，创建好queue，创建好channel并且接收到rabbimq发送的consume ok，通过pipe通知主线程可以开始
  工作. 并且之后若接收到rabbitmq发送过来的消息，则还是通过pipe发送给猪线程.
* 主进程一旦接收到amqp连接线程的通知, prefork的方式孵化worker，与worker通信也是使用pipe.
* 主进程使用epoll监听amqp线程的消息，worker发送过来的消息.
* 消息格式: {"method": "method_name", "args": ["arg1"], "kwargs": {"key": "value"}
* master_start.png是一个简单图示.

主线程如何工作
-------------
参考gunicorn，master孵化worker并且创建和worker交互的pipe，epoll监听amqp线程，worker交互的pipe.

1. 主线程开始配置, 启动amqp线程去连接rabbitmq，然后通过epoll来监听amqp线程的通知. 若amqp线程通知表示已经准备就绪(创建了queue，channel，绑定了
   queue和channel，开始消费消息), .

2. prefork方式去孵化worker，创建好相互交互的pipe，将pipe加入到epoll监听列表中. 然后等待epoll通知.

3. 若有amqp到来，发送消息给worker.

4. 若有worker发送消息过来，说明需要ack，将消息ack掉.

5. 若没有消息，则不断地去查看worker数量和配置的是否一致，不一致，若worker数少，则孵化新的worker，加入到self.workers中。若多，则根据worker的age来
   杀掉老的worker.

6. worker监听pipe，收到消息，执行函数，执行完之后发送task_done给master

超时
-----
master分配task给worker的时候，记录下worker的当前时间，每隔一段时间去检查worker是否超时.

**TODO:**
1. 根据worker输了更新rabbitmq的qos

2. 支持建立多个queue
