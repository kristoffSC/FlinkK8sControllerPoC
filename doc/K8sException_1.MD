# K8S Exception after cluster restart

## Prerequisites
1. docker 
2. k8s
3. Installed [Flink k8s operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/)
4. java 11

## Steps to reproduce:
1. Run Docker and k8s on your local machine
2. Run [K8sTest:createSessionCluster](../src/test/java/org/example/K8sTest.java)
This will create a Flink Session cluster with two Task Managers.
3. Verify that Flink cluster is up and running. You can set up port forward rule
`kubectl port-forward svc/basic-session-deployment-only-example-rest 8081` to access Flink UI under
`http://localhost:8081/`
4. Submit Flink Session job by running Run [K8sTest:submitSessionJob](../src/test/java/org/example/K8sTest.java)
This will submit a streaming job to session cluster created in previous step.
You can verify that job is running on Flink UI.
5. Modify [K8sTest:createSessionCluster](../src/test/java/org/example/K8sTest.java) in such way that
   1. `taskManagerSpec.setReplicas(2);` -> `taskManagerSpec.setReplicas(3);`
   2. Comment out line `kubernetesClient.resource(flinkDeployment).serverSideApply();`
   3. uncomment line `kubernetesClient.resource(flinkDeployment).patch()`
Execute test. This will patch Flink Deployment by adding one extra Task manager to it.

As a result, after executing step 5, Flink Cluster will be restarted. After restart, it will 
contain 3 task manager. You can verify it on FLink UI. However, no Job will be running on created 
Flink cluster and additionally in flink-operator logs we can see error:
```
 2023-08-25 21:15:11,873 o.a.f.k.o.c.FlinkSessionJobController [INFO ][default/basic-session-job-only-example-3] Starting reconciliation                                                             │
│ 2023-08-25 21:15:11,889 o.a.f.k.o.o.s.FlinkSessionJobObserver [ERROR][default/basic-session-job-only-example-3] Missing Session Job                                                                 │
│ 2023-08-25 21:15:11,918 o.a.f.k.o.l.AuditUtils         [INFO ][default/basic-session-job-only-example-3] >>> Event  | Warning | MISSING         | Missing Session Job                               │
│ 2023-08-25 21:15:11,921 o.a.f.k.o.r.d.AbstractFlinkResourceReconciler [INFO ][default/basic-session-job-only-example-3] Resource fully reconciled, nothing to do...       
```

The `kubectl get FlinkSessionJobs` command returns:
```
NAME                               JOB STATUS    LIFECYCLE STATE
basic-session-job-only-example-3   RECONCILING   STABLE
```

Executing `kubectl delete FlinkSessionJob basic-session-job-only-example-3` hangs on console 
and gives exception in flink-operator logs:
```
2023-08-25 21:19:40,713 o.a.f.k.o.c.FlinkSessionJobController [INFO ][default/basic-session-job-only-example-3] Cleaning up FlinkSessionJob                                                            
2023-08-25 21:19:40,745 o.a.f.k.o.l.AuditUtils         [INFO ][default/basic-session-job-only-example-3] >>> Event  | Info    | CLEANUP         | Cleaning up FlinkSessionJob                          
2023-08-25 21:19:40,748 o.a.f.k.o.s.AbstractFlinkService [INFO ][default/basic-session-job-only-example-3] Cancelling job.                                                                             
    at org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler.cancelJob(SessionJobReconciler.java:88)                                                                         
    at org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler.cleanupInternal(SessionJobReconciler.java:109)                                                                  
    at org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractFlinkResourceReconciler.cleanup(AbstractFlinkResourceReconciler.java:269)                                                    
    at org.apache.flink.kubernetes.operator.controller.FlinkSessionJobController.cleanup(FlinkSessionJobController.java:141)                                                                           
    at org.apache.flink.kubernetes.operator.controller.FlinkSessionJobController.cleanup(FlinkSessionJobController.java:52)                                                                            
    at io.javaoperatorsdk.operator.processing.Controller$2.execute(Controller.java:184)                                                                                                                
    at io.javaoperatorsdk.operator.processing.Controller$2.execute(Controller.java:147)                                                                                                                
    at org.apache.flink.kubernetes.operator.metrics.OperatorJosdkMetrics.timeControllerExecution(OperatorJosdkMetrics.java:80)                                                                         
    at io.javaoperatorsdk.operator.processing.Controller.cleanup(Controller.java:146)                                                                                                                  
    at io.javaoperatorsdk.operator.processing.event.ReconciliationDispatcher.handleCleanup(ReconciliationDispatcher.java:290)                                                                          
    at io.javaoperatorsdk.operator.processing.event.ReconciliationDispatcher.handleDispatch(ReconciliationDispatcher.java:87)                                                                          
    at io.javaoperatorsdk.operator.processing.event.ReconciliationDispatcher.handleExecution(ReconciliationDispatcher.java:62)                                                                         
    at io.javaoperatorsdk.operator.processing.event.EventProcessor$ReconcilerExecutor.run(EventProcessor.java:414)                                                                                     
    at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)                                                                                                                     
    at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)                                                                                                                    
    at java.base/java.lang.Thread.run(Unknown Source)       
Caused by: org.apache.flink.runtime.rest.util.RestClientException: [org.apache.flink.runtime.rest.handler.RestHandlerException: Job could not be found.                                                
    at org.apache.flink.runtime.rest.handler.job.JobCancellationHandler.lambda$handleRequest$0(JobCancellationHandler.java:128)                                                                        
    at java.base/java.util.concurrent.CompletableFuture.uniHandle(Unknown Source)                                                                                                                      
    at java.base/java.util.concurrent.CompletableFuture$UniHandle.tryFire(Unknown Source)                                                                                                              
    at java.base/java.util.concurrent.CompletableFuture.postComplete(Unknown Source)                                                                                                                   
    at java.base/java.util.concurrent.CompletableFuture.completeExceptionally(Unknown Source)                                                                                                          
    at org.apache.flink.runtime.rpc.akka.AkkaInvocationHandler.lambda$invokeRpc$1(AkkaInvocationHandler.java:260)                                                                                      
    at java.base/java.util.concurrent.CompletableFuture.uniWhenComplete(Unknown Source)                                                                                                                
    at java.base/java.util.concurrent.CompletableFuture$UniWhenComplete.tryFire(Unknown Source)                                                                                                        
    at java.base/java.util.concurrent.CompletableFuture.postComplete(Unknown Source)                                                                                                                   
    at java.base/java.util.concurrent.CompletableFuture.completeExceptionally(Unknown Source)                                                                                                          
    at org.apache.flink.util.concurrent.FutureUtils.doForward(FutureUtils.java:1298)                                                                                                                   
    at org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.lambda$null$1(ClassLoadingUtils.java:93)                                                                                             
    at org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java:68)                                                                                 
    at org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.lambda$guardCompletionWithContextClassLoader$2(ClassLoadingUtils.java:92)                                                            
    at java.base/java.util.concurrent.CompletableFuture.uniWhenComplete(Unknown Source)                                                                                                                
    at java.base/java.util.concurrent.CompletableFuture$UniWhenComplete.tryFire(Unknown Source)                                                                                                        
    at java.base/java.util.concurrent.CompletableFuture.postComplete(Unknown Source)                                                                                                                   
    at java.base/java.util.concurrent.CompletableFuture.completeExceptionally(Unknown Source)                                                                                                          
    at org.apache.flink.runtime.concurrent.akka.AkkaFutureUtils$1.onComplete(AkkaFutureUtils.java:45)                                                                                                  
    at akka.dispatch.OnComplete.internal(Future.scala:299)                                                                                                                                             
    at akka.dispatch.OnComplete.internal(Future.scala:297)                                                                                                                                             
    at akka.dispatch.japi$CallbackBridge.apply(Future.scala:224)                                                                                                                                       
    at akka.dispatch.japi$CallbackBridge.apply(Future.scala:221)                                                                                                                                       
    at scala.concurrent.impl.CallbackRunnable.run(Promise.scala:64)                                                                                                                                    
    at org.apache.flink.runtime.concurrent.akka.AkkaFutureUtils$DirectExecutionContext.execute(AkkaFutureUtils.java:65)                                                                                
    at scala.concurrent.impl.CallbackRunnable.executeWithValue(Promise.scala:72)                                                                                                                       
    at scala.concurrent.impl.Promise$DefaultPromise.$anonfun$tryComplete$1(Promise.scala:288)                                                                                                          
    at scala.concurrent.impl.Promise$DefaultPromise.$anonfun$tryComplete$1$adapted(Promise.scala:288)                                                                                                  
    at scala.concurrent.impl.Promise$DefaultPromise.tryComplete(Promise.scala:288)                                                                                                                     
    at akka.pattern.PromiseActorRef.$bang(AskSupport.scala:622)                                                                                                                                        
    at akka.pattern.PipeToSupport$PipeableFuture$$anonfun$pipeTo$1.applyOrElse(PipeToSupport.scala:25)                                                                                                 
    at akka.pattern.PipeToSupport$PipeableFuture$$anonfun$pipeTo$1.applyOrElse(PipeToSupport.scala:23)                                                                                                 
    at scala.concurrent.Future.$anonfun$andThen$1(Future.scala:536)                                                                                                                                    
    at scala.concurrent.impl.Promise.liftedTree1$1(Promise.scala:33)                                                                                                                                   
    at scala.concurrent.impl.Promise.$anonfun$transform$1(Promise.scala:33)                                                                                                                            
    at scala.concurrent.impl.CallbackRunnable.run(Promise.scala:64)                                                                                                                                    
    at akka.dispatch.BatchingExecutor$AbstractBatch.processBatch(BatchingExecutor.scala:63)                                                                                                            
    at akka.dispatch.BatchingExecutor$BlockableBatch.$anonfun$run$1(BatchingExecutor.scala:100)                                                                                                        
    at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)                                                                                                                          
    at scala.concurrent.BlockContext$.withBlockContext(BlockContext.scala:85)                                                                                                                          
    at akka.dispatch.BatchingExecutor$BlockableBatch.run(BatchingExecutor.scala:100)                                                                                                                   
    at akka.dispatch.TaskInvocation.run(AbstractDispatcher.scala:49)                                                                                                                                   
    at akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(ForkJoinExecutorConfigurator.scala:48)                                                                                         
    at java.base/java.util.concurrent.ForkJoinTask.doExec(Unknown Source)
        at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(Unknown Source)                                                                                                              
    at java.base/java.util.concurrent.ForkJoinPool.scan(Unknown Source)                                                                                                                                
    at java.base/java.util.concurrent.ForkJoinPool.runWorker(Unknown Source)                                                                                                                           
    at java.base/java.util.concurrent.ForkJoinWorkerThread.run(Unknown Source)                                                                                                                         
Caused by: org.apache.flink.runtime.messages.FlinkJobNotFoundException: Could not find Flink job (5cca3044f87e4b690000000000000002)                                                                    
    at org.apache.flink.runtime.dispatcher.Dispatcher.cancelJob(Dispatcher.java:814)                                                                                                                   
    at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)                                                                                                                  
    at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)                                                                                                                  
    at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)                                                                                                              
    at java.base/java.lang.reflect.Method.invoke(Unknown Source)                                                                                                                                       
    at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.lambda$handleRpcInvocation$1(AkkaRpcActor.java:309)                                                                                              
    at org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.runWithContextClassLoader(ClassLoadingUtils.java:83)                                                                                 
    at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleRpcInvocation(AkkaRpcActor.java:307)                                                                                                       
    at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleRpcMessage(AkkaRpcActor.java:222)                                                                                                          
    at org.apache.flink.runtime.rpc.akka.FencedAkkaRpcActor.handleRpcMessage(FencedAkkaRpcActor.java:84)                                                                                               
    at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleMessage(AkkaRpcActor.java:168)                                                                                                             
    at akka.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:24)                                                                                                                                   
    at akka.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:20)                                                                                                                                   
    at scala.PartialFunction.applyOrElse(PartialFunction.scala:127)                                                                                                                                    
    at scala.PartialFunction.applyOrElse$(PartialFunction.scala:126)                                                                                                                                   
    at akka.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:20)                                                                                                                             
    at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:175)                                                                                                                             
    at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)                                                                                                                             
    at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:176)                                                                                                                             
    at akka.actor.Actor.aroundReceive(Actor.scala:537)                                                                                                                                                 
    at akka.actor.Actor.aroundReceive$(Actor.scala:535)                                                                                                                                                
    at akka.actor.AbstractActor.aroundReceive(AbstractActor.scala:220)                                                                                                                                 
    at akka.actor.ActorCell.receiveMessage(ActorCell.scala:579)                                                                                                                                        
    at akka.actor.ActorCell.invoke(ActorCell.scala:547)                                                                                                                                                
    at akka.dispatch.Mailbox.processMailbox(Mailbox.scala:270)                                                                                                                                         
    at akka.dispatch.Mailbox.run(Mailbox.scala:231)                                                                                                                                                    
    at akka.dispatch.Mailbox.exec(Mailbox.scala:243)                                                                                                                                                   
    ... 5 more                                                                                                                                                                                         
]                                                                                                                                                                                                      
    at org.apache.flink.runtime.rest.RestClient.parseResponse(RestClient.java:536)                                                                                                                     
    at org.apache.flink.runtime.rest.RestClient.lambda$submitRequest$3(RestClient.java:516)                                                                                                            
    at java.base/java.util.concurrent.CompletableFuture$UniCompose.tryFire(Unknown Source)                                                                                                             
    at java.base/java.util.concurrent.CompletableFuture$Completion.run(Unknown Source)                                                                                                                 
    ... 3 more                                                                                                           
```

In order to sort this out, K8s Deployments for TM and JM have to be deleted by calling:
`kubectl delete Deployment basic-session-deployment-only-example` and `kubectl delete Deployment basic-session-deployment-only-example-taskmanager`
After that flink k8s operator has to be restarted `kubectl rollout restart deployment flink-kubernetes-operator`
and only after that `FlinkSessionJob` can be deleted by calling `kubectl delete FlinkSessionJob basic-session-job-only-example-3`




