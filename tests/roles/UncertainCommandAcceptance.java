package io.github.rivomesh.platform;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Native test fault at the original local revision boundary, using a disposable Agent directory. */
public final class UncertainCommandAcceptance {
    private UncertainCommandAcceptance() {}

    private static void require(boolean condition, String code) {
        if (!condition) throw new AssertionError(code);
    }

    public static void main(String[] args) throws Exception {
        PlatformConfig config = PlatformConfig.load(Path.of(args[0]));
        String requestId = args[1], originalRevision = args[2];
        long previousEpoch = Long.parseLong(args[3]);
        require(config.cluster != null && config.cluster.separateExecution && config.cluster.executes(),
                "TEST_AGENT_REQUIRED");
        try (TaskService tasks = new TaskService(config); NodeDirectory nodes = new NodeDirectory(config, tasks)) {
            SharedAccessStore access = tasks.sharedAccess();
            GlobalTaskStore store = new GlobalTaskStore(config.cluster, access, config.cluster.taskKey,
                    tasks.globalTaskIdentity(), config.cluster.maximumTasks);
            String directory = tasks.ownerDirectoryIdentity();
            AgentDirectory agents = new AgentDirectory(config.cluster, access, store);
            DurableCommands queue = new DurableCommands(config.cluster, access, store, config.cluster.maximumCommands);
            nodes.start();
            ControlStore.Lease lease = nodes.currentLease();
            require(lease.epoch > previousEpoch, "TEST_AGENT_EPOCH_NOT_ADVANCED");
            agents.register(lease, directory);
            DurableCommands.Dispatch dispatch = queue.claim(requestId, agents, lease, directory);
            require(dispatch != null && dispatch.actor != null, "TEST_COMMAND_DISPATCH_REQUIRED");
            DurableCommands.Stored saved = dispatch.command;
            require(requestId.equals(saved.id) && requestId.equals(saved.request.requestId)
                    && originalRevision.equals(saved.request.revision) && saved.dispatchEpoch == lease.epoch
                    && Objects.equals(saved.dispatchIncarnation, lease.incarnation),
                    "TEST_ORIGINAL_COMMAND_NOT_BOUND");
            TaskRequest original = queue.bindLocal(dispatch, tasks.prepareOwnerRequest(saved.request),
                    agents, lease, directory);
            require(requestId.equals(original.requestId) && original.revision != null
                    && tasks.ownerReceipt(original, saved.actor, saved.actorRole) == null,
                    "TEST_EXPECTED_NO_ORIGINAL_LOCAL_RECEIPT");

            TaskRequest intervening = TaskRequest.of(TaskRequest.Kind.ACTION, original.taskId);
            intervening.action = "CHECK";
            intervening.requestId = "native_intervening_" + UUID.randomUUID();
            intervening = tasks.prepareOwnerRequest(intervening);
            new LocalTaskPlane(tasks).invoke(intervening, dispatch.actor);
            require(tasks.ownerReceipt(intervening, saved.actor, saved.actorRole) != null,
                    "TEST_INTERVENING_CHECK_NOT_ADMITTED");
            require(!original.revision.equals(tasks.view(original.taskId).get("revision")),
                    "TEST_LOCAL_REVISION_NOT_ADVANCED");

            new OwnerExecution(queue, agents, nodes::currentLease, directory, tasks).step(requestId);
            Map<String,Object> outcome = queue.view(requestId, dispatch.actor);
            require("UNKNOWN".equals(outcome.get("status")) && "REVISION_CHANGED".equals(outcome.get("code"))
                    && tasks.ownerReceipt(original, saved.actor, saved.actorRole) == null,
                    "TEST_UNCERTAIN_COMMAND_REPLAYED_OR_REPLACED");
            System.out.println("COMMAND_UNKNOWN_OWNER_EPOCH=" + lease.epoch);
        }
    }
}
