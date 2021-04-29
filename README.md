**内容在multiwayMS_v3** v2是试验品
运行：<br>
在multiwayMS_v3下：<br>
go run !(*_test).go <br>

生成覆盖率文件：<br>
内联会使测试产生问题 因此使用：-v -gcflags=-l<br>
go test -v -gcflags=-l -coverprofile=coverage.out<br>

为了练习使用不同的测试方法，消费者测试使用的是gomonkey，
生产者测试使用的是原始的mock，下一步会把这块的测试改成用gomock的<br>

下一步：<br>
(1)改生产者测试为gomock<br>
(2)加入更多test case，使测试包括一些应该fail的情况<br>
