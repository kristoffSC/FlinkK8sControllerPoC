package org.example;

import static java.util.Map.entry;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodTemplate;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.junit.jupiter.api.Test;

public class K8sTest {

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
    public void stopFlinkSessionJobWithSavepoint() {

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
        flinkDeploymentSpec.setMode(KubernetesDeploymentMode.NATIVE);
        //flinkDeploymentSpec.setMode(KubernetesDeploymentMode.STANDALONE);
        flinkDeploymentSpec.setImage("flink:1.17");
        flinkDeploymentSpec.setServiceAccount("flink");
        flinkDeploymentSpec.setPodTemplate(createPodWithVolume());

        Map<String, String> flinkConfiguration =
            Map.of("taskmanager.numberOfTaskSlots", "2",
                "state.savepoints.dir", "file:/opt/flink/jobs/",
                "state.checkpoints.dir", "file:/opt/flink/jobs/",
                "high-availability.type", "kubernetes",
                "high-availability.storageDir", "file:/opt/flink/jobs/");
        flinkDeploymentSpec.setFlinkConfiguration(flinkConfiguration);

        // JM
        JobManagerSpec jobManagerSpec = new JobManagerSpec();
        jobManagerSpec.setResource(new Resource(0.2, "1024m", "2G"));
        flinkDeploymentSpec.setJobManager(jobManagerSpec);

        // TM
        TaskManagerSpec taskManagerSpec = new TaskManagerSpec();
        taskManagerSpec.setResource(new Resource(0.1, "1024m", "2G"));
        //taskManagerSpec.setReplicas(2);
        flinkDeploymentSpec.setTaskManager(taskManagerSpec);

        flinkDeployment.setSpec(flinkDeploymentSpec);
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {

            // Uncomment to Create Cluster
            kubernetesClient.resource(flinkDeployment).serverSideApply();

            // Uncomment to update cluster
            //kubernetesClient.resource(flinkDeployment).update();
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
            List<StatusDetails> delete = kubernetesClient.resource(flinkDeployment).delete();
            for (StatusDetails statusDetails : delete) {
                System.out.println(statusDetails);
            }
            System.out.println("----------------------");
        }
    }

    /* ------------------------------- */

    // Failed Session Job has NULL job status???
    @Test
    public void listFlinkSessionJobs() {
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            MixedOperation<FlinkSessionJob, KubernetesResourceList<FlinkSessionJob>,
                io.fabric8.kubernetes.client.dsl.Resource<FlinkSessionJob>>
                resources = kubernetesClient.resources(FlinkSessionJob.class);

            List<FlinkSessionJob> items = resources.inNamespace("default").list().getItems();
            for (FlinkSessionJob item : items) {
                printFlinSessionJob(item);
            }
        }
    }

    @Test
    public void listFlinkSessionJobsWithLabel() {

        ListOptions listOptions = new ListOptions();
        listOptions.setLabelSelector("CUSTOM_LABEL");
        //listOptions.setLabelSelector("CUSTOM_LABEL=OLD");

        // this label selector will print no Session Jobs.
        //listOptions.setLabelSelector("CUSTOM_LABEL=BOGUS");

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            List<FlinkSessionJob> resources =
                kubernetesClient.resources(FlinkSessionJob.class).inNamespace("default")
                    .list(listOptions).getItems();

            for (FlinkSessionJob resource : resources) {
                printFlinSessionJob(resource);
            }
        }
    }

    @Test
    public void submitSessionJob() {

        JobSpec jobSpec = JobSpec.builder()
            //.jarURI("https://repo1.maven.org/maven2/org/apache/flink/flink-examples-streaming_2.12/1.16.1/flink-examples-streaming_2.12-1.16.1-TopSpeedWindowing.jar")
            .jarURI("https://github.com/kristoffSC/FlinkSimpleStreamingJob/raw/jarTests/FlinkSimpleStreamingJob-1.0-SNAPSHOT.jar")
            //.jarURI("file:///opt/flink/jobs/FlinkSimpleStreamingJob-1.0-SNAPSHOT.jar")
            .parallelism(1)
            .upgradeMode(UpgradeMode.SAVEPOINT)
            .args(new String[]{"--restart", "yes"})
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
            FlinkSessionJob result = kubernetesClient.resource(flinkSessionJob).create();
            System.out.println(result);
            System.out.println("Status Error: " + result.getStatus().getError());
            System.out.println(result.getStatus().getJobStatus());
        }
    }

    // Adding a label does not restarts Flink job.
    @Test
    public void updateJobLabel() {

        List<FlinkSessionJob> updatedJobs = new ArrayList<>();

        // GEt Jobs and update labels
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            MixedOperation<FlinkSessionJob, KubernetesResourceList<FlinkSessionJob>,
                io.fabric8.kubernetes.client.dsl.Resource<FlinkSessionJob>>
                resources = kubernetesClient.resources(FlinkSessionJob.class);

            List<FlinkSessionJob> originalJobs = resources.inNamespace("default").list().getItems();
            for (FlinkSessionJob sessionJob : originalJobs) {
                Map<String, String> oldLabels = sessionJob.getMetadata().getLabels();
                Map<String, String> newLabels;
                if (oldLabels == null) {
                    newLabels = new HashMap<>();
                } else {
                    newLabels = new HashMap<>(oldLabels);
                }

                newLabels.put("CUSTOM_LABEL", "OLD");

                // Maybe we should have a copy of the Job?
                sessionJob.getMetadata().setLabels(newLabels);
                updatedJobs.add(sessionJob);
            }
        }

        if (!updatedJobs.isEmpty()) {
            try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
                for (FlinkSessionJob updatedJob : updatedJobs) {
                    kubernetesClient.resource(updatedJob).update();
                }
            }

        }
    }

    @Test
    public void updateJobArgsAndLabels() {

        List<FlinkSessionJob> updatedJobs = new ArrayList<>();

        // GEt Jobs and update labels
        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            MixedOperation<FlinkSessionJob, KubernetesResourceList<FlinkSessionJob>,
                io.fabric8.kubernetes.client.dsl.Resource<FlinkSessionJob>>
                resources = kubernetesClient.resources(FlinkSessionJob.class);

            List<FlinkSessionJob> originalJobs = resources.inNamespace("default").list().getItems();
            for (FlinkSessionJob sessionJob : originalJobs) {
                Map<String, String> oldLabels = sessionJob.getMetadata().getLabels();
                Map<String, String> newLabels;
                if (oldLabels == null) {
                    newLabels = new HashMap<>();
                } else {
                    newLabels = new HashMap<>(oldLabels);
                }

                newLabels.put("CUSTOM_LABEL", "OLD");

                // this will throw from KubernetesClient.
                //newLabels.put("$@%@#$%@#$", "Invalid_label");

                // Maybe we should have a copy of the Job?
                // TODO Explore error handling here - if job args are not properly formatted,
                //  this test will pass but job will not fail. How we should validate if job is
                //  properly submitted and started?
                sessionJob.getMetadata().setLabels(newLabels);
                sessionJob.getSpec().getJob().setArgs(new String[] {"--hello3=world"});
                updatedJobs.add(sessionJob);
            }
        }

        if (!updatedJobs.isEmpty()) {
            try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
                for (FlinkSessionJob updatedJob : updatedJobs) {

                    // Update Jobs only in SAVEPOINT upgrade mode.
                    if (updatedJob.getSpec().getJob().getUpgradeMode().equals(UpgradeMode.SAVEPOINT)) {
                        FlinkSessionJob update = kubernetesClient.resource(updatedJob).update();
                        System.out.println(update);
                        System.out.println("Status Error: " + update.getStatus().getError());
                        System.out.println(update.getStatus().getJobStatus());
                    }
                }
            }

        }
    }

    @Test
    public void changeSessionJobStatus() {

        JobSpec jobSpec = JobSpec.builder()
            .jarURI(
                "https://repo1.maven.org/maven2/org/apache/flink/flink-examples-streaming_2.12/1.16.1/flink-examples-streaming_2.12-1.16.1-TopSpeedWindowing.jar")
            .parallelism(1)
            .upgradeMode(UpgradeMode.STATELESS)
            .args(new String[0])
            .savepointTriggerNonce(4L)
            //.state(JobState.SUSPENDED)
            //.state(JobState.RUNNING)
            //.initialSavepointPath("file://opt/flink/savepoint-2d71e6-bf9c0639cbb3")
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
            FlinkSessionJob update = kubernetesClient.resource(flinkSessionJob).update();
            System.out.println(update.getStatus().getError());
        }
    }

    @Test
    public void changeAllSessionJobStatus() {

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            List<FlinkSessionJob> resources =
                kubernetesClient.resources(FlinkSessionJob.class).list().getItems();

            // Suspended FlinkSession job is marked as Canceled in FlinkUI but there is a k8s
            // resource still present, in this case Job Status is FINISHED and LifeCycle State is SUSPENDED
            for (FlinkSessionJob resource : resources) {
                resource.getSpec().getJob().setState(JobState.RUNNING);
                kubernetesClient.resource(resource).update();
            }
        }
    }

    // Deleted FLinkSessionJob is marked as Canceled in Flink UI but K8s resource is deleted in
    // this case.
    // Suspended FlinkSession job is also marked as Canceled in FlinkUI but there is a k8s
    // resource still present, in this case Job Status is FINISHED and LifeCycle State is
    // SUSPENDED
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

    @Test
    public void deleteAllSessionJobs() {

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            List<FlinkSessionJob> resources =
                kubernetesClient.resources(FlinkSessionJob.class).list().getItems();

            for (FlinkSessionJob resource : resources) {
                List<StatusDetails> delete = kubernetesClient.resource(resource).delete();
                for (StatusDetails statusDetails : delete) {
                    System.out.println(statusDetails);
                }
                System.out.println("------------------");
            }
        }
    }

    // Some jobs cannot be deleted by kubectl deleted because they are "broken". Flink operator
    // does not see them anymore. Maybe it's a bug. In this case we need to edit a
    // FlinkSessionDeployment and delete the finalizers section.
    @Test
    public void deleteAllBrokenSessionJobs() {

        try (KubernetesClient kubernetesClient = new KubernetesClientBuilder().build()) {
            List<FlinkSessionJob> resources =
                kubernetesClient.resources(FlinkSessionJob.class).list().getItems();

            for (FlinkSessionJob resource : resources) {

                if (resource.getStatus().getJobStatus().getState().equals("RECONCILING")) {
                    resource.getMetadata().setFinalizers(null);
                    FlinkSessionJob update = kubernetesClient.resource(resource).update();
                        System.out.println(update);
                    }
                    System.out.println("------------------");
            }
        }
    }


    // --------------------------- UTILS ----------------------
    private void printFlinSessionJob(final FlinkSessionJob item) {
        System.out.println("Flink Session Job: " + item);
        System.out.println("Cluster: " + item.getSpec().getDeploymentName());
        System.out.println("Job name: " + item.getMetadata().getName());
        System.out.println("Job state: " + item.getStatus().getJobStatus().getState());
        System.out.println("Job upgrade mode: " + item.getSpec().getJob().getUpgradeMode());
        System.out.println("Job Labels: " + item.getMetadata().getLabels());
        System.out.println("SavePoint Info: " + item.getStatus().getJobStatus().getSavepointInfo());
        System.out.println("----");
    }

    private Pod createPodWithVolume() {

        ObjectMeta podTemplateMetadata = new ObjectMeta();
        podTemplateMetadata.setName("pod-template");

        VolumeMount jobVolumeMount = new VolumeMount();
        jobVolumeMount.setMountPath("/opt/flink/jobs/");
        jobVolumeMount.setName("flink-jobs-pv-volume");

        VolumeMount pluginsVolumeMount = new VolumeMount();
        pluginsVolumeMount.setMountPath("/opt/flink/plugins/");
        pluginsVolumeMount.setName("flink-plugin-pv-volume");

        Container flinkMainContainer = new Container();
        flinkMainContainer.setName("flink-main-container");
        // This seems to do not work???
        flinkMainContainer.setCommand(List.of(
            "sh -c (mkdir -p /opt/flink/jobs)",
            "sh -c (mkdir -p /opt/flink/plugins)",
            "sh -c (mkdir -p /opt/flink/dupa)",
            "sh -c (chown flink:flink /opt/flink/jobs/)",
            "sh -c (chown flink:flink /opt/flink/plugins/)"
        ));
        flinkMainContainer.setVolumeMounts(List.of(jobVolumeMount, pluginsVolumeMount));

        PersistentVolumeClaimVolumeSource jobPvc = new PersistentVolumeClaimVolumeSource();
        jobPvc.setClaimName("flink-job-pv-claim");

        PersistentVolumeClaimVolumeSource pluginsPvc = new PersistentVolumeClaimVolumeSource();
        pluginsPvc.setClaimName("flink-plugins-pv-claim");

        Volume jobsVolume = new Volume();
        jobsVolume.setName("flink-jobs-pv-volume");
        jobsVolume.setPersistentVolumeClaim(jobPvc);

        Volume pluginsVolume = new Volume();
        pluginsVolume.setName("flink-plugin-pv-volume");
        pluginsVolume.setPersistentVolumeClaim(pluginsPvc);

        PodSecurityContext secContext = new PodSecurityContext();
        secContext.setFsGroup(9999L);

        PodSpec podSpec = new PodSpec();
        podSpec.setSecurityContext(secContext);
        podSpec.setVolumes(List.of(jobsVolume, pluginsVolume));
        podSpec.setContainers(List.of(flinkMainContainer));

        PodTemplateSpec podtemplateSpec = new PodTemplateSpec();
        podtemplateSpec.setSpec(podSpec);

        Pod pod = new Pod();
        pod.setApiVersion("v1");
        pod.setKind("Pod");
        pod.setMetadata(podTemplateMetadata);
        pod.setSpec(podSpec);

        return pod;
    }

}
