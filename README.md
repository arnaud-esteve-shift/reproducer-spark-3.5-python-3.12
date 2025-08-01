# Spark 3.5.2 + Python 3.12 bug reproducer

Tested with the following setup.

### Versions

* Windows 11
* Python: `3.12.10` (installed under: `$HOME\AppData\Local\Programs\Python\Python312\python.exe`)
* `spark-3.5.2-bin-hadoop3-scala2.13`
* (poetry 2.1 necessary)


### Necessary environment variables

```
SPARK_LOCAL_IP=localhost
SPARK_HOME=<path_to_spark_local>/spark-3.5.2-bin-hadoop3-scala2.13
PYSPARK_PYTHON=$HOME\AppData\Local\Programs\Python\Python312\python.exe
```

### Reproduce

```
poetry lock # will generate a .venv with all dependencies + check for python 3.12
poetry install
 .\.venv\Scripts\python.exe .\reproducer\main.py
```

This will output the following error:


```
asses where applicable
25/08/01 09:36:42 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)/ 1]
org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:612)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:594)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:35)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:789)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:766)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:525)
        at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:594)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        at java.base/java.lang.Thread.run(Thread.java:842)
Caused by: java.io.EOFException
        at java.base/java.io.DataInputStream.readInt(DataInputStream.java:398)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:774)
        ... 26 more
25/08/01 09:36:42 WARN TaskSetManager: Lost task 0.0 in stage 0.0 (TID 0) (127.0.0.1 executor driver): org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:612)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:594)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:35)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:789)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:766)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:525)
        at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:594)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        at java.base/java.lang.Thread.run(Thread.java:842)
Caused by: java.io.EOFException
        at java.base/java.io.DataInputStream.readInt(DataInputStream.java:398)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:774)
        ... 26 more

25/08/01 09:36:42 ERROR TaskSetManager: Task 0 in stage 0.0 failed 1 times; aborting job
Traceback (most recent call last):                                  (0 + 0) / 1]
  File "C:\dev\experiments\python_3.12_reproducer\reproducer\main.py", line 43, in <module>
    print(df.collect())
          ^^^^^^^^^^^^
  File "C:\dev\experiments\python_3.12_reproducer\.venv\Lib\site-packages\pyspark\sql\dataframe.py", line 1263, in collect
    sock_info = self._jdf.collectToPython()
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\dev\experiments\python_3.12_reproducer\.venv\Lib\site-packages\py4j\java_gateway.py", line 1322, in __call__
    return_value = get_return_value(
                   ^^^^^^^^^^^^^^^^^
  File "C:\dev\experiments\python_3.12_reproducer\.venv\Lib\site-packages\pyspark\errors\exceptions\captured.py", line 179, in deco
    return f(*a, **kw)
           ^^^^^^^^^^^
  File "C:\dev\experiments\python_3.12_reproducer\.venv\Lib\site-packages\py4j\protocol.py", line 326, in get_return_value
    raise Py4JJavaError(
py4j.protocol.Py4JJavaError: An error occurred while calling o47.collectToPython.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (127.0.0.1 executor driver): org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:612)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:594)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:35)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:789)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:766)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:525)
        at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:594)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        at java.base/java.lang.Thread.run(Thread.java:842)
Caused by: java.io.EOFException
        at java.base/java.io.DataInputStream.readInt(DataInputStream.java:398)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:774)
        ... 26 more

Driver stacktrace:
        at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2792)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2791)
        at scala.collection.immutable.List.foreach(List.scala:333)
        at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2791)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1247)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1247)
        at scala.Option.foreach(Option.scala:437)
        at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1247)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3060)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2994)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2983)
        at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:989)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2393)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2414)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2433)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2458)
        at org.apache.spark.rdd.RDD.$anonfun$collect$1(RDD.scala:1049)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
        at org.apache.spark.rdd.RDD.withScope(RDD.scala:410)
        at org.apache.spark.rdd.RDD.collect(RDD.scala:1048)
        at org.apache.spark.sql.execution.SparkPlan.executeCollect(SparkPlan.scala:448)
        at org.apache.spark.sql.Dataset.$anonfun$collectToPython$1(Dataset.scala:4150)
        at org.apache.spark.sql.Dataset.$anonfun$withAction$2(Dataset.scala:4324)
        at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:546)
        at org.apache.spark.sql.Dataset.$anonfun$withAction$1(Dataset.scala:4322)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
        at org.apache.spark.sql.Dataset.withAction(Dataset.scala:4322)
        at org.apache.spark.sql.Dataset.collectToPython(Dataset.scala:4147)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:568)
        at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
        at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
        at py4j.Gateway.invoke(Gateway.java:282)
        at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at py4j.commands.CallCommand.execute(CallCommand.java:79)
        at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
        at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
        at java.base/java.lang.Thread.run(Thread.java:842)
Caused by: org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:612)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator$$anonfun$1.applyOrElse(PythonRunner.scala:594)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:35)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:789)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:766)
        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:525)
        at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:594)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at scala.collection.Iterator$$anon$9.hasNext(Iterator.scala:576)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        ... 1 more
Caused by: java.io.EOFException
        at java.base/java.io.DataInputStream.readInt(DataInputStream.java:398)
        at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:774)
        ... 26 more
```