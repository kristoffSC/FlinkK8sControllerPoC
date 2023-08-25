package org.example;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LimitRange;
import io.fabric8.kubernetes.api.model.LimitRangeList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.junit.jupiter.api.Test;

public class AppTest {

    @Test
    public void listServices() {
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            ServiceList list = kubernetesClient.services().list();
            for (Service item : list.getItems()) {
                System.out.println("Service: " + item);
            }
        }
    }

    @Test
    public void listFlinkDeployments() {
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            MixedOperation<FlinkDeployment, KubernetesResourceList<FlinkDeployment>, io.fabric8.kubernetes.client.dsl.Resource<FlinkDeployment>>
                resources = kubernetesClient.resources(FlinkDeployment.class);

            List<FlinkDeployment> items = resources.inNamespace("default").list().getItems();
            for (FlinkDeployment item : items) {
                System.out.println("Flink Deployments: " + item);
                System.out.println("Number of TM replicas: " + item.getSpec().getTaskManager().getReplicas());
            }
        }
    }

    @Test
    public void listFlinkSessionJobs() {
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            MixedOperation<FlinkSessionJob, KubernetesResourceList<FlinkSessionJob>,
                io.fabric8.kubernetes.client.dsl.Resource<FlinkSessionJob>>
                resources = kubernetesClient.resources(FlinkSessionJob.class);

            List<FlinkSessionJob> items = resources.inNamespace("default").list().getItems();
            for (FlinkSessionJob item : items) {
                System.out.println("Flink Session Job: " + item);
                System.out.println("FlinkSessionJob cluster: " + item.getSpec().getDeploymentName());
                System.out.println("FlinkSessionJob name: " + item.getMetadata().getName());
                System.out.println("----");
            }
        }
    }

    @Test
    public void deleteFlinkSessionJob() {

        FlinkSessionJob flinkSessionJob = new FlinkSessionJob();
        flinkSessionJob.setKind("FlinkSessionJob");
        flinkSessionJob.setApiVersion("flink.apache.org/v1beta1");

        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace("default");
        meta.setName("basic-session-job-only-example-3");
        flinkSessionJob.setMetadata(meta);

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            kubernetesClient.resource(flinkSessionJob).delete();
        }
    }

    /*
    Gives NPE, possibly a bug in operator. Also not sure what kind of Status should be used here.
    Caused by: java.lang.NullPointerException
	at org.apache.flink.kubernetes.operator.api.status.CommonStatus.getLifecycleState(CommonStatus.java:64)
     */
    @Test
    public void updateStatusFlinkSessionJob() {

        FlinkSessionJob flinkSessionJob = new FlinkSessionJob();
        flinkSessionJob.setKind("FlinkSessionJob");
        flinkSessionJob.setApiVersion("flink.apache.org/v1beta1");

        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace("default");
        meta.setName("basic-session-job-only-example-2");
        flinkSessionJob.setMetadata(meta);
        flinkSessionJob.setStatus(
            FlinkSessionJobStatus.builder().jobStatus(
                JobStatus.builder()
                    .state("CANCELLED")
                    .build()
                ).build()
        );

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            kubernetesClient.resource(flinkSessionJob).updateStatus();
        }
    }

    @Test
    public void createSessionCluster() {

        FlinkDeployment flinkDeployment = new FlinkDeployment();
        flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
        flinkDeployment.setKind("FlinkDeployment");

        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName("basic-session-deployment-only-example");
        objectMeta.setNamespace("default");
        flinkDeployment.setMetadata(objectMeta);

        FlinkDeploymentSpec flinkDeploymentSpec = new FlinkDeploymentSpec();
        flinkDeploymentSpec.setFlinkVersion(FlinkVersion.v1_17);
        flinkDeploymentSpec.setMode(KubernetesDeploymentMode.STANDALONE);
        flinkDeploymentSpec.setImage("flink:1.17");
        flinkDeploymentSpec.setServiceAccount("flink");

        Map<String, String> flinkConfiguration =
            Map.ofEntries(entry("taskmanager.numberOfTaskSlots", "2"));
        flinkDeploymentSpec.setFlinkConfiguration(flinkConfiguration);

        // JM
        JobManagerSpec jobManagerSpec = new JobManagerSpec();
        jobManagerSpec.setResource(new Resource(0.2, "1024m", "2G"));
        flinkDeploymentSpec.setJobManager(jobManagerSpec);

        // TM
        TaskManagerSpec taskManagerSpec = new TaskManagerSpec();
        taskManagerSpec.setResource(new Resource(0.05, "1024m", "2G"));
        taskManagerSpec.setReplicas(3);
        flinkDeploymentSpec.setTaskManager(taskManagerSpec);

        flinkDeployment.setSpec(flinkDeploymentSpec);
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {

            // Create Cluster
            //kubernetesClient.resource(flinkDeployment).serverSideApply();

            // Update cluster
            kubernetesClient.resource(flinkDeployment).patch();
        }
    }

    @Test
    public void deleteSessionCluster() {

        FlinkDeployment flinkDeployment = new FlinkDeployment();
        flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
        flinkDeployment.setKind("FlinkDeployment");
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName("basic-session-deployment-only-example");
        objectMeta.setNamespace("default");

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            kubernetesClient.resource(flinkDeployment).delete();
        }
    }

    @Test
    public void submitSessionJob() {

        JobSpec jobSpec = JobSpec.builder()
            .jarURI(
                "https://repo1.maven.org/maven2/org/apache/flink/flink-examples-streaming_2.12/1.16.1/flink-examples-streaming_2.12-1.16.1-TopSpeedWindowing.jar")
            .parallelism(1)
            .upgradeMode(UpgradeMode.STATELESS)
            .build();

        FlinkSessionJobSpec sessionJobSpec = FlinkSessionJobSpec.builder()
            .job(jobSpec)
            .deploymentName("basic-session-deployment-only-example")
            .build();

        FlinkSessionJob flinkSessionJob = new FlinkSessionJob();
        flinkSessionJob.setKind("FlinkSessionJob");
        flinkSessionJob.setApiVersion("flink.apache.org/v1beta1");

        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace("default");
        meta.setName("basic-session-job-only-example-3");
        flinkSessionJob.setMetadata(meta);
        flinkSessionJob.setSpec(sessionJobSpec);

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            kubernetesClient.resource(flinkSessionJob).create();
        }
    }
}
