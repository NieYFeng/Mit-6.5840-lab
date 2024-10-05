#!/usr/bin/env bash

# crash test

# 检查 timeout 命令
TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # 没有 timeout 命令
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# 清理和创建临时目录
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# 编译 crash.so 插件以及相关的 MapReduce 工具
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build mrcoordinator.go) || exit 1
(cd .. && go build mrworker.go) || exit 1
(cd .. && go build mrsequential.go) || exit 1

echo '***' Starting crash test.

# 生成正确输出
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
(($TIMEOUT2 ../mrcoordinator ../pg*txt); touch mr-done ) &
sleep 1

# 启动多个 workers
$TIMEOUT2 ../mrworker ../../mrapps/crash.so &

# 模仿 rpc.go 的 coordinatorSock()，检查 socket 文件
SOCKNAME=/var/tmp/5840-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  $TIMEOUT2 ../mrworker ../../mrapps/crash.so
  sleep 1
done

wait

# 验证输出
rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  exit 1
fi
