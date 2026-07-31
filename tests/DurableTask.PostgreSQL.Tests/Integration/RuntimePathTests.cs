using DurableTask.Core;
using DurableTask.PostgreSQL.Tests.Utils;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;
using static DurableTask.PostgreSQL.Tests.Utils.JsonPayload;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// End-to-end integration tests for the core runtime paths that were previously untested:
/// activity execution, sub-orchestration, durable timers, ContinueAsNew, and external events.
/// Each test runs a real orchestration through a <see cref="TaskHubWorker"/> against PostgreSQL
/// and asserts the provider's lock/checkpoint/complete plumbing holds for these flows.
/// </summary>
[Collection("integration")]
public sealed class RuntimePathTests : IAsyncLifetime
{
    private const string HubName = "RuntimeTestsHub";

    private bool _isDatabaseAvailable;

    public Task InitializeAsync()
    {
        _isDatabaseAvailable = IsDatabaseAvailableAsync(ConnectionString).GetAwaiter().GetResult();
        return Task.CompletedTask;
    }

    public Task DisposeAsync() => Task.CompletedTask;

    /// <summary>Brings up a service + worker with the given orchestrators/activities and runs a test body.</summary>
    private static async Task RunAsync(
        TaskOrchestration[] orchestrations,
        TaskActivity[]? activities,
        Func<TaskHubClient, Task> body)
    {
        var testService = new TestService(ConnectionString, taskHubName: HubName);
        await testService.InitializeAsync();
        TaskHubWorker? worker = null;
        try
        {
            worker = new TaskHubWorker(testService.OrchestrationService, testService.LoggerFactory);
            worker.AddTaskOrchestrations(orchestrations
                .Select(o => new NamedOrchestrationCreator(o.GetType().Name, string.Empty, o)).ToArray());
            if (activities != null)
            {
                worker.AddTaskActivities(activities
                    .Select(a => new NamedActivityCreator(a.GetType().Name, string.Empty, a)).ToArray());
            }
            await worker.StartAsync();

            await body(new TaskHubClient(testService.ClientService));
        }
        finally
        {
            if (worker != null)
            {
                await worker.StopAsync(isForced: true);
            }
            await testService.DisposeAsync();
        }
    }

    // ===========================================================================
    // Activity execution — exercises lock_next_task + complete_tasks + the
    // LockNextTaskActivityWorkItem / CompleteTaskActivityWorkItemAsync C# paths.
    // ===========================================================================
    [Fact]
    public async Task Activity_ExecutesAndReturnsResult()
    {
        if (!_isDatabaseAvailable) return;

        await RunAsync(
            orchestrations: new TaskOrchestration[] { new ScheduleActivityOrchestration() },
            activities: new TaskActivity[] { new EchoActivity() },
            body: async client =>
            {
                string instanceId = Guid.NewGuid().ToString();
                // CreateOrchestrationInstanceAsync serializes the input; pass the raw value.
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    nameof(ScheduleActivityOrchestration), string.Empty, instanceId, "World");

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                // state.Output is the JSON-serialized return value; deserialize to get the string.
                Assert.Equal("Hello, World!", FromJson<string>(state.Output));
            });
    }

    private sealed class ScheduleActivityOrchestration : TaskOrchestration
    {
        public override async Task<string?> Execute(OrchestrationContext context, string? input)
        {
            // input arrives as the raw JSON payload (e.g. "\"World\""); deserialize to the value.
            string name = FromJson<string>(input);
            // ScheduleTask serializes the input object; pass the raw value, not pre-serialized JSON.
            string result = await context.ScheduleTask<string>(nameof(EchoActivity), string.Empty, name);
            // Return the raw value; the provider serializes it. Do not double-serialize.
            return result;
        }
        public override string GetStatus() => string.Empty;
        public override void RaiseEvent(OrchestrationContext context, string name, string input) { }
    }

    private sealed class EchoActivity : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input) => $"Hello, {input}!";
    }

    // ===========================================================================
    // Sub-orchestration — exercises p_new_orchestration_events SQL path +
    // SubOrchestrationInstanceCreatedEvent handling.
    // ===========================================================================
    [Fact]
    public async Task SubOrchestration_RunsAndReturnsResult()
    {
        if (!_isDatabaseAvailable) return;

        // Self-recursive orchestrator: input n -> returns n + sub(n+1) until n >= 3.
        await RunAsync(
            orchestrations: new TaskOrchestration[] { new CountingSubOrchestration() },
            activities: null,
            body: async client =>
            {
                string instanceId = Guid.NewGuid().ToString();
                // CreateOrchestrationInstanceAsync serializes the input object; pass the raw int.
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    nameof(CountingSubOrchestration), string.Empty, instanceId, 1);

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                // 1 + 2 + 3 = 6
                Assert.Equal(6, FromJson<int>(state.Output));
            });
    }

    private sealed class CountingSubOrchestration : TaskOrchestration
    {
        public override async Task<string?> Execute(OrchestrationContext context, string? input)
        {
            // input arrives as raw JSON (e.g. "1"); deserialize to the value.
            int n = FromJson<int>(input);
            int sum = n;
            if (n < 3)
            {
                // CreateSubOrchestrationInstance serializes the input object; pass the raw int.
                int sub = await context.CreateSubOrchestrationInstance<int>(
                    nameof(CountingSubOrchestration), string.Empty, $"sub-{Guid.NewGuid():N}", n + 1);
                sum += sub;
            }
            // Return the raw value; the provider serializes it.
            return ToJson(sum);
        }
        public override string GetStatus() => string.Empty;
        public override void RaiseEvent(OrchestrationContext context, string name, string input) { }
    }

    // ===========================================================================
    // Durable timer — exercises TimerCreatedEvent/TimerFiredEvent checkpoint +
    // visible_time delivery.
    // ===========================================================================
    [Fact]
    public async Task Timer_FiresAfterDelay()
    {
        if (!_isDatabaseAvailable) return;

        var delay = TimeSpan.FromSeconds(2);
        await RunAsync(
            orchestrations: new TaskOrchestration[] { new TimerOrchestration(delay) },
            activities: null,
            body: async client =>
            {
                string instanceId = Guid.NewGuid().ToString();
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    nameof(TimerOrchestration), string.Empty, instanceId, ToJson("start"));

                DateTime createdUtc = DateTime.UtcNow;
                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                // The orchestration only completes after the timer fires.
                Assert.True(state.CompletedTime - createdUtc >= TimeSpan.FromSeconds(1.5));
            });
    }

    private sealed class TimerOrchestration(TimeSpan delay) : TaskOrchestration
    {
        public override async Task<string?> Execute(OrchestrationContext context, string? input)
        {
            await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(delay), null!);
            return ToJson("fired");
        }
        public override string GetStatus() => string.Empty;
        public override void RaiseEvent(OrchestrationContext context, string name, string input) { }
    }

    // ===========================================================================
    // ContinueAsNew — exercises the SQL execution-id swap (logic.postgresql.sql
    // detects continue-as-new when execution_id changes, deletes prior history,
    // and rewrites the input).
    // ===========================================================================
    [Fact]
    public async Task ContinueAsNew_LoopsAndCompletes()
    {
        if (!_isDatabaseAvailable) return;

        // Loops incrementing input until it reaches 3, then completes.
        await RunAsync(
            orchestrations: new TaskOrchestration[] { new ContinueAsNewOrchestration(max: 3) },
            activities: null,
            body: async client =>
            {
                string instanceId = Guid.NewGuid().ToString();
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    nameof(ContinueAsNewOrchestration), string.Empty, instanceId, 0);

                // ContinueAsNew changes the execution id, so wait by instance id only (null execution id).
                OrchestrationState state = await client.ServiceClient.WaitForOrchestrationAsync(instanceId, null, TimeSpan.FromSeconds(30), default);
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.Equal(3, FromJson<int>(state.Output));
            });
    }

    private sealed class ContinueAsNewOrchestration(int max) : TaskOrchestration
    {
        public override async Task<string?> Execute(OrchestrationContext context, string? input)
        {
            int n = FromJson<int>(input);
            if (n < max)
            {
                // Yield point (the runtime processes the continuation after a checkpoint),
                // then continue as new with the incremented value. Mirrors the MSSQL reference.
                await context.CreateTimer<object>(context.CurrentUtcDateTime.AddMilliseconds(50), null!);
                context.ContinueAsNew(n + 1);
                return null;
            }
            return ToJson(n);
        }
        public override string GetStatus() => string.Empty;
        public override void RaiseEvent(OrchestrationContext context, string name, string input) { }
    }

    // ===========================================================================
    // External events — exercises SendTaskOrchestrationMessageAsync +
    // add_orchestration_event + EventRaisedEvent replay. DTFx uses the OnEvent +
    // TaskCompletionSource pattern (there is no built-in WaitForExternalEvent<T>()).
    // ===========================================================================
    [Fact]
    public async Task ExternalEvent_UnblocksOrchestration()
    {
        if (!_isDatabaseAvailable) return;

        await RunAsync(
            orchestrations: new TaskOrchestration[] { new EventWaitOrchestration() },
            activities: null,
            body: async client =>
            {
                string instanceId = Guid.NewGuid().ToString();
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    nameof(EventWaitOrchestration), string.Empty, instanceId, input: null);

                // Let the orchestration start and block on the pending event.
                await Task.Delay(1500);
                // RaiseEventAsync -> SendTaskOrchestrationMessageAsync -> add_orchestration_event.
                await client.RaiseEventAsync(instance, "MyEvent", "hello");

                OrchestrationState state = await client.ServiceClient.WaitForOrchestrationAsync(instanceId, null, TimeSpan.FromSeconds(30), default);
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.Equal("hello", FromJson<string>(state.Output));
            });
    }

    /// <summary>
    /// Waits for an external "MyEvent" using the canonical DTFx OnEvent + TaskCompletionSource
    /// pattern, then completes with the event payload.
    /// </summary>
    private sealed class EventWaitOrchestration : TaskOrchestration<string?, string?>
    {
        private TaskCompletionSource<string?>? _handle;

        public override async Task<string?> RunTask(OrchestrationContext context, string? input)
        {
            this._handle = new TaskCompletionSource<string?>();
            string? value = await this._handle.Task;
            this._handle = null;
            return value;
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (name == "MyEvent")
            {
                this._handle?.TrySetResult(input);
            }
        }
    }
}
