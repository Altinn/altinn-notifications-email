using System.Collections.Concurrent;

using Altinn.Notifications.Email.Integrations.Configuration;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Integrations.Kafka.Consumers;

/// <summary>
/// Abstract base class for Kafka consumers, providing a framework for consuming messages from a specified topic.
/// Supports batch processing with bulk commits, cross-partition concurrency, graceful shutdown, and error handling.
/// </summary>
public abstract class KafkaConsumerBase : BackgroundService
{
    private volatile bool _stopping;
    private readonly string _topicName;
    private const int _pollTimeoutMs = 100;
    private const int _defaultBatchSize = 50;
    private const int _defaultMaxParallelism = 75;
    private readonly SemaphoreSlim _globalConcurrency;
    private readonly ILogger<KafkaConsumerBase> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly ConcurrentDictionary<Guid, Task> _inFlightTasks = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaConsumerBase"/> class.
    /// </summary>
    protected KafkaConsumerBase(
        KafkaSettings settings,
        ILogger<KafkaConsumerBase> logger,
        string topicName)
    {
        _logger = logger;
        _topicName = topicName;

        var config = new SharedClientConfig(settings);

        var consumerConfig = new ConsumerConfig(config.ConsumerConfig)
        {
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = $"{settings.Consumer.GroupId}-{GetType().Name.ToLower()}"
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("// {Class} // Error: {Reason}", GetType().Name, e.Reason))
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation("// {Class} // Partitions assigned: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogInformation("// {Class} // Partitions revoked: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));
            })
            .Build();

        _globalConcurrency = new SemaphoreSlim(_defaultMaxParallelism, _defaultMaxParallelism);
    }

    /// <summary>
    /// Polls for a batch of messages from Kafka with the specified batch size and timeout.
    /// </summary>
    /// <param name="maxBatchSize">Maximum number of messages to poll in a single batch.</param>
    /// <param name="timeoutMs">Timeout in milliseconds for polling operation.</param>
    /// <returns>Array of consumed message results.</returns>
    private ConsumeResult<string, string>[] PollBatch(int maxBatchSize, int timeoutMs)
    {
        var batch = new List<ConsumeResult<string, string>>();
        var timeout = TimeSpan.FromMilliseconds(timeoutMs);
        var startTime = DateTime.UtcNow;

        while (batch.Count < maxBatchSize && (DateTime.UtcNow - startTime) < timeout)
        {
            try
            {
                var remainingTimeout = timeout - (DateTime.UtcNow - startTime);
                if (remainingTimeout <= TimeSpan.Zero)
                {
                    break;
                }

                var result = _consumer.Consume(remainingTimeout);
                if (result != null)
                {
                    batch.Add(result);
                }
                else
                {
                    break;
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning(ex, "// {Class} // Consume exception during batch polling", GetType().Name);
                break;
            }
        }

        return [.. batch];
    }

    /// <summary>
    /// Safely commits the offsets for a batch of processed messages.
    /// </summary>
    private void SafeBulkCommit(IEnumerable<TopicPartitionOffset> offsets)
    {
        if (_stopping || !offsets.Any())
        {
            return;
        }

        try
        {
            _consumer.Commit(offsets);
        }
        catch (KafkaException ex)
        {
            if (ex.Error.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration)
            {
                _logger.LogWarning("// {Class} // Bulk commit skipped due to transient state: {Reason}", GetType().Name, ex.Error.Reason);
            }
            else
            {
                _logger.LogError(ex, "// {Class} // Bulk commit failed unexpectedly", GetType().Name);
            }
        }
    }

    /// <inheritdoc/>
    protected override abstract Task ExecuteAsync(CancellationToken stoppingToken);

    /// <inheritdoc/>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topicName);
        return base.StartAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping = true;

        _consumer.Unsubscribe();

        Task[] tasks = [.. _inFlightTasks.Values];
        if (tasks.Length > 0)
        {
            _logger.LogInformation("// {Class} // Waiting for {Count} in-flight tasks to complete", GetType().Name, tasks.Length);

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// {Class} // Error while awaiting in-flight tasks during shutdown", GetType().Name);
            }
        }

        await base.StopAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Consumes messages from the Kafka topic in batches, ensuring efficient processing
    /// with bulk commits. Messages are polled in batches, processed in parallel, and committed
    /// together for optimal performance. The method handles graceful shutdown and respects
    /// global concurrency limits.
    /// </summary>
    /// <param name="processMessageFunc">A function that takes the message string and processes it asynchronously.</param>
    /// <param name="retryMessageFunc">A function that takes the message string and handles retry logic asynchronously on processing failure.</param>
    /// <param name="stoppingToken">A cancellation token to signal when to stop consuming messages.</param>
    protected async Task ConsumeMessage(
        Func<string, Task> processMessageFunc,
        Func<string, Task> retryMessageFunc,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested && !_stopping)
        {
            try
            {
                // Poll for a batch of messages
                var batch = PollBatch(_defaultBatchSize, _pollTimeoutMs);

                if (batch.Length == 0)
                {
                    await Task.Delay(50, stoppingToken); // Brief pause when no messages
                    continue;
                }

                var batchStartTime = DateTime.UtcNow;
                var successfulOffsets = new List<TopicPartitionOffset>();
                var semaphoreTasks = new List<Task>();

                // Process batch messages with controlled concurrency
                foreach (var consumeResult in batch)
                {
                    await _globalConcurrency.WaitAsync(stoppingToken).ConfigureAwait(false);

                    var processingTaskId = Guid.NewGuid();
                    var processingTask = Task.Run(
                        async () =>
                        {
                            try
                            {
                                await processMessageFunc(consumeResult.Message.Value).ConfigureAwait(false);

                                // Track successful processing for commit
                                lock (successfulOffsets)
                                {
                                    successfulOffsets.Add(new TopicPartitionOffset(
                                        consumeResult.TopicPartition,
                                        consumeResult.Offset + 1)); // Commit next offset
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // Shutdown scenario - don't retry
                            }
                            catch (Exception ex)
                            {
                                try
                                {
                                    await retryMessageFunc(consumeResult.Message.Value).ConfigureAwait(false);

                                    // If retry succeeds, still mark for commit
                                    lock (successfulOffsets)
                                    {
                                        successfulOffsets.Add(new TopicPartitionOffset(
                                            consumeResult.TopicPartition,
                                            consumeResult.Offset + 1));
                                    }
                                }
                                catch (Exception retryEx)
                                {
                                    _logger.LogError(retryEx, "// {Class} // Retry failed for message at offset {Offset}", GetType().Name, consumeResult.Offset);
                                }

                                _logger.LogError(ex, "// {Class} // Error processing message at offset {Offset}", GetType().Name, consumeResult.Offset);
                            }
                            finally
                            {
                                _globalConcurrency.Release();
                                _inFlightTasks.TryRemove(processingTaskId, out _);
                            }
                        },
                        stoppingToken);

                    _inFlightTasks[processingTaskId] = processingTask;
                    semaphoreTasks.Add(processingTask);
                }

                // Wait for all messages in the batch to complete processing
                await Task.WhenAll(semaphoreTasks);

                // Bulk commit all successful offsets
                if (successfulOffsets.Count > 0)
                {
                    SafeBulkCommit(successfulOffsets);

                    var batchDuration = (DateTime.UtcNow - batchStartTime).TotalMilliseconds;
                    _logger.LogDebug(
                        "// {Class} // Processed batch of {BatchSize} messages in {Duration:F0}ms, committed {CommittedCount} offsets",
                        GetType().Name,
                        batch.Length,
                        batchDuration,
                        successfulOffsets.Count);
                }
                else
                {
                    _logger.LogWarning("// {Class} // No messages successfully processed in batch of {BatchSize}", GetType().Name, batch.Length);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// {Class} // Unexpected error in batch processing loop", GetType().Name);
                await Task.Delay(1000, stoppingToken); // Brief delay before retrying
            }
        }
    }
}
