运行：<br>
在multiwayMS_v3下：<br>
go run !(*_test).go <br>

生成覆盖率文件：<br>
内联会使测试产生问题 因此使用：-v -gcflags=-l<br>
go test -v -gcflags=-l -coverprofile=coverage.out<br>
