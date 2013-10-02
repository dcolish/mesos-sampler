package com.urbanairship.mesos;

import com.google.common.collect.Lists;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

import static org.apache.mesos.Protos.*;

public class TrivialSampler implements Scheduler {
    private final Logger log = LogManager.getLogger(TrivialSampler.class);

    // args[0] must be the master address
    public static void main(String[] args) {
        BasicConfigurator.configure();
        FrameworkInfo info = FrameworkInfo.newBuilder()
                .setName("Sample")
                .setUser("")
                .build();
        SchedulerDriver schedulerDriver = new MesosSchedulerDriver(new TrivialSampler(), info, args[0]);
        System.exit(schedulerDriver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkID, MasterInfo masterInfo) {
        log.info("Registered scheduler for framework=" + frameworkID.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
        log.info("Reregistered scheduler for framework");
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
        /**
         * For the fun of this example, whenever we get an offer lets launch a task.
         */
        for (Offer offer : offers) {
            CommandInfo commandInfo = CommandInfo.newBuilder()
                    .setValue("echo hello")
                    .build();

            TaskID taskID = TaskID.newBuilder().setValue("1").build();

            TaskInfo info = TaskInfo.newBuilder()
                    .setName("Task")
                    .setTaskId(taskID)
                    .setSlaveId(offer.getSlaveId())
                    .addResources(Resource.newBuilder()
                            .setName("cpus")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder().setValue(1)))
                    .addResources(Resource.newBuilder()
                            .setName("mem")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder().setValue(128)))
                    .setCommand(commandInfo).build();
            schedulerDriver.launchTasks(offer.getId(), Lists.newArrayList(info));
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
        log.info("Offer rescinded, offer=" + offerID.getValue());
    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus taskStatus) {
        switch (taskStatus.getState()) {
            case TASK_FAILED:
            case TASK_KILLED:
            case TASK_LOST:
                // this would be a good place to reschedule tasks
                log.warn("task " + taskStatus.getTaskId() + " failed");
            case TASK_FINISHED:
                log.info("task " + taskStatus.getTaskId() + " completed");
                break;
            case TASK_STARTING:
            case TASK_STAGING:
            case TASK_RUNNING:
                break;
            default:
                log.warn("Illegal state");
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, byte[] bytes) {
        log.info("received an awesome message");
    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        log.info("Disconnected from master");
    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
        log.info("Slave lost");
    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, int i) {
        log.info("Executor lost");
    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {
        log.info("Error, " + s);
    }
}
