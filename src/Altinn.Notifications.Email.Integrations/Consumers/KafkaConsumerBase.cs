using System.Collections.Concurrent;

using Altinn.Notifications.Email.Integrations.Configuration;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Integrations.Kafka.Consumers;

/// <summary>
/// Represents a framework for consuming messages from a specified Kafka topic.
/// </summary>
public abstract class KafkaConsumerBase : BackgroundService
{
    private readonly string _topicName;
    private volatile bool _isShutdownInitiated;
    private readonly ILogger<KafkaConsumerBase> _logger;
    private readonly IConsumer<string, string> _consumer;

    private const int _maxMessagesCountInBatch = 250;
    private const int _messagesBatchPollTimeoutInMs = 100;
    private const int _maxConcurrentProcessingTasks = 250;

    private readonly SemaphoreSlim _processingConcurrencySemaphore;
    private readonly ConcurrentDictionary<Guid, Task> _inFlightTasks = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _lastCommittedOffsets = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaConsumerBase"/> class.
    /// </summary>
    protected KafkaConsumerBase(string topicName, KafkaSettings settings, ILogger<KafkaConsumerBase> logger)
    {
        _logger = logger;
        _topicName = topicName;

        var configuration = BuildConfiguration(settings);
        _consumer = BuildConsumer(configuration);

        _processingConcurrencySemaphore = new SemaphoreSlim(_maxConcurrentProcessingTasks, _maxConcurrentProcessingTasks);
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        try
        {
            _consumer.Close();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "// KafkaConsumer Dispose // Close failed");
        }
        finally
        {
            _consumer.Dispose();

            base.Dispose();
        }
    }

    /// <inheritdoc/>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topicName);

        _logger.LogInformation("// {Class} // Subscribed to topic {Topic}", GetType().Name, _topicName);

        return base.StartAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _isShutdownInitiated = true;

        _consumer.Unsubscribe();

        var processingTasks = _inFlightTasks.Values.ToArray();

        _logger.LogInformation("// {Class} // Shutdown initiated. In-flight tasks: {Count}", GetType().Name, processingTasks.Length);

        if (processingTasks.Length > 0)
        {
            try
            {
                await Task.WhenAll(processingTasks).WaitAsync(TimeSpan.FromSeconds(15), cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// {Class} // Error awaiting tasks during shutdown", GetType().Name);
            }
        }

        await base.StopAsync(cancellationToken);
    }

    /// <inheritdoc/>
    protected override abstract Task ExecuteAsync(CancellationToken stoppingToken);

    /// <summary>
    /// Consumes messages from the Kafka topic in batches.
    /// </summary>
    /// <param name="processMessageFunc">Function to process a single message.</param>
    /// <param name="retryMessageFunc">Function to retry a message if processing fails.</param>
    /// <param name="cancellationToken">Cancellation token to signal cancellation.</param>
    protected async Task ConsumeMessage(Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && !_isShutdownInitiated)
        {
            try
            {
                var messageBatch = FetchMessageBatch(_maxMessagesCountInBatch, _messagesBatchPollTimeoutInMs, cancellationToken);
                if (messageBatch.Length == 0)
                {
                    await Task.Delay(10, cancellationToken);
                    continue;
                }

                _logger.LogInformation(
                    "// {Class} // Polled {BatchSize} messages. Offsets {StartOffset}..{EndOffset}. In-flight partition tasks: {InFlight}",
                    GetType().Name,
                    messageBatch.Length,
                    messageBatch[0].Offset,
                    messageBatch[^1].Offset,
                    _inFlightTasks.Count);

                var processingStartTime = DateTime.UtcNow;

                var partitionGroups = messageBatch
                    .GroupBy(e => e.TopicPartition)
                    .Select(e => e.OrderBy(e => e.Offset).ToArray())
                    .ToArray();

                var partitionTasks = new List<Task>();
                var commitOffsets = new ConcurrentBag<TopicPartitionOffset>();

                foreach (var partitionMessages in partitionGroups)
                {
                    await _processingConcurrencySemaphore.WaitAsync(cancellationToken);

                    var taskId = Guid.NewGuid();
                    var partition = partitionMessages[0].TopicPartition;

                    var task = Task.Run(
                        async () =>
                        {
                            long lastSuccessfulOffset = -1;
                            try
                            {
                                _logger.LogInformation(
                                    "// {Class} // Start processing partition {Partition} batch size {Count} (offsets {Start}..{End})",
                                    GetType().Name,
                                    partition.Partition.Value,
                                    partitionMessages.Length,
                                    partitionMessages[0].Offset,
                                    partitionMessages[^1].Offset);

                                foreach (var message in partitionMessages)
                                {
                                    if (_isShutdownInitiated || cancellationToken.IsCancellationRequested)
                                    {
                                        break;
                                    }

                                    try
                                    {
                                        await processMessageFunc(message.Message.Value);
                                        lastSuccessfulOffset = message.Offset;
                                        _logger.LogInformation(
                                            "// {Class} // Processed partition {Partition} offset {Offset}",
                                            GetType().Name,
                                            partition.Partition.Value,
                                            message.Offset);
                                    }
                                    catch (OperationCanceledException)
                                    {
                                        break;
                                    }
                                    catch (Exception ex)
                                    {
                                        _logger.LogError(
                                            ex,
                                            "// {Class} // Error processing partition {Partition} offset {Offset}, attempting retry",
                                            GetType().Name,
                                            partition.Partition.Value,
                                            message.Offset);

                                        try
                                        {
                                            await retryMessageFunc(message.Message.Value);
                                            lastSuccessfulOffset = message.Offset;

                                            _logger.LogInformation(
                                                "// {Class} // Retry succeeded partition {Partition} offset {Offset}",
                                                GetType().Name,
                                                partition.Partition.Value,
                                                message.Offset);
                                        }
                                        catch (Exception retryEx)
                                        {
                                            _logger.LogError(
                                                retryEx,
                                                "// {Class} // Retry failed partition {Partition} offset {Offset}. Halting further processing in this partition for this batch",
                                                GetType().Name,
                                                partition.Partition.Value,
                                                message.Offset);
                                            break;
                                        }
                                    }
                                }

                                if (lastSuccessfulOffset >= 0)
                                {
                                    var previousCommitted = _lastCommittedOffsets.GetOrAdd(partition, -1);

                                    if (lastSuccessfulOffset > previousCommitted)
                                    {
                                        // Commit the next offset to consume.
                                        var nextOffset = lastSuccessfulOffset + 1;
                                        commitOffsets.Add(new TopicPartitionOffset(partition, new Offset(nextOffset)));
                                        _lastCommittedOffsets[partition] = lastSuccessfulOffset;

                                        _logger.LogInformation(
                                            "// {Class} // Partition {Partition} prepared commit next offset {NextOffset} (last successful {LastOffset})",
                                            GetType().Name,
                                            partition.Partition.Value,
                                            nextOffset,
                                            lastSuccessfulOffset);
                                    }
                                    else
                                    {
                                        _logger.LogDebug(
                                            "// {Class} // Partition {Partition} no new progress (last successful {LastOffset}, previously committed {PrevCommitted})",
                                            GetType().Name,
                                            partition.Partition.Value,
                                            lastSuccessfulOffset,
                                            previousCommitted);
                                    }
                                }
                                else
                                {
                                    _logger.LogWarning(
                                        "// {Class} // Partition {Partition} no messages processed successfully in this batch",
                                        GetType().Name,
                                        partition.Partition.Value);
                                }
                            }
                            finally
                            {
                                _processingConcurrencySemaphore.Release();
                                _inFlightTasks.TryRemove(taskId, out _);
                            }
                        },
                        cancellationToken);

                    _inFlightTasks[taskId] = task;
                    partitionTasks.Add(task);
                }

                await Task.WhenAll(partitionTasks);

                if (!commitOffsets.IsEmpty)
                {
                    _logger.LogInformation("// {Class} // Committing {Count} partition offsets", GetType().Name, commitOffsets.Count);
                    SafeCommit(commitOffsets);
                }
                else
                {
                    _logger.LogWarning("// {Class} // No offsets eligible for commit in this batch (size {BatchSize})", GetType().Name, messageBatch.Length);
                }

                _logger.LogInformation(
                    "// KafkaConsumerBase // ConsumeMessage // Batch completed topic {Topic}. Messages {BatchSize} in {Duration:F0}ms, committed {CommittedCount} partition offsets",
                    _topicName,
                    messageBatch.Length,
                    (DateTime.UtcNow - processingStartTime).TotalMilliseconds,
                    commitOffsets.Count);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// {Class} // Unexpected error in batch processing loop", GetType().Name);
            }
        }
    }

    /// <summary>
    /// Consumes messages from the Kafka topic using partition-level batch processing.
    /// Applies batch processing function to all messages within a partition, with safe commit handling.
    /// </summary>
    /// <param name="processBatchFunc">Function to process a batch of messages from a single partition.</param>
    /// <param name="retryMessageFunc">Function to retry a single message if batch processing fails.</param>
    /// <param name="cancellationToken">Cancellation token to signal cancellation.</param>
    protected async Task ConsumeMessageBatch(
        Func<IEnumerable<string>, Task> processBatchFunc,
        Func<string, Task> retryMessageFunc,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && !_isShutdownInitiated)
        {
            try
            {
                var messageBatch = FetchMessageBatch(_maxMessagesCountInBatch, _messagesBatchPollTimeoutInMs, cancellationToken);
                if (messageBatch.Length == 0)
                {
                    await Task.Delay(10, cancellationToken);
                    continue;
                }

                _logger.LogInformation(
                    "// {Class} // Polled {BatchSize} messages for batch processing. Offsets {StartOffset}..{EndOffset}. In-flight partition tasks: {InFlight}",
                    GetType().Name,
                    messageBatch.Length,
                    messageBatch[0].Offset,
                    messageBatch[^1].Offset,
                    _inFlightTasks.Count);

                var processingStartTime = DateTime.UtcNow;

                // Group by partition; maintain ordering inside each group.
                var partitionGroups = messageBatch
                    .GroupBy(m => m.TopicPartition)
                    .Select(g => g.OrderBy(m => m.Offset).ToArray())
                    .ToArray();

                var partitionTasks = new List<Task>();
                var commitOffsets = new ConcurrentBag<TopicPartitionOffset>();

                foreach (var partitionMessages in partitionGroups)
                {
                    await _processingConcurrencySemaphore.WaitAsync(cancellationToken);

                    var taskId = Guid.NewGuid();
                    var partition = partitionMessages[0].TopicPartition;

                    var task = Task.Run(
                        async () =>
                        {
                            long lastSuccessfulOffset = -1;
                            try
                            {
                                _logger.LogInformation(
                                    "// {Class} // Start batch processing partition {Partition} batch size {Count} (offsets {Start}..{End})",
                                    GetType().Name,
                                    partition.Partition.Value,
                                    partitionMessages.Length,
                                    partitionMessages[0].Offset,
                                    partitionMessages[^1].Offset);

                                var messageValues = partitionMessages.Select(m => m.Message.Value).ToArray();

                                try
                                {
                                    // Try to process the entire batch at once
                                    await processBatchFunc(messageValues);

                                    // If batch processing succeeds, mark all messages as successfully processed
                                    lastSuccessfulOffset = partitionMessages[^1].Offset;

                                    _logger.LogInformation(
                                        "// {Class} // Successfully batch processed partition {Partition} {Count} messages (offsets {Start}..{End})",
                                        GetType().Name,
                                        partition.Partition.Value,
                                        partitionMessages.Length,
                                        partitionMessages[0].Offset,
                                        partitionMessages[^1].Offset);
                                }
                                catch (OperationCanceledException)
                                {
                                    // Don't process individual messages if cancelled
                                    return;
                                }
                                catch (Exception batchEx)
                                {
                                    _logger.LogError(
                                        batchEx,
                                        "// {Class} // Batch processing failed for partition {Partition}, falling back to individual message processing",
                                        GetType().Name,
                                        partition.Partition.Value);

                                    // Fallback: Process messages individually with retry logic
                                    foreach (var message in partitionMessages)
                                    {
                                        if (_isShutdownInitiated || cancellationToken.IsCancellationRequested)
                                        {
                                            break;
                                        }

                                        try
                                        {
                                            await retryMessageFunc(message.Message.Value);
                                            lastSuccessfulOffset = message.Offset;

                                            _logger.LogInformation(
                                                "// {Class} // Individual retry succeeded partition {Partition} offset {Offset}",
                                                GetType().Name,
                                                partition.Partition.Value,
                                                message.Offset);
                                        }
                                        catch (OperationCanceledException)
                                        {
                                            break;
                                        }
                                        catch (Exception retryEx)
                                        {
                                            _logger.LogError(
                                                retryEx,
                                                "// {Class} // Individual retry failed partition {Partition} offset {Offset}. Halting further processing in this partition for this batch",
                                                GetType().Name,
                                                partition.Partition.Value,
                                                message.Offset);
                                            break;
                                        }
                                    }
                                }

                                // Commit logic - same as single message processing
                                if (lastSuccessfulOffset >= 0)
                                {
                                    var previousCommitted = _lastCommittedOffsets.GetOrAdd(partition, -1);

                                    if (lastSuccessfulOffset > previousCommitted)
                                    {
                                        // Commit the next offset to consume.
                                        var nextOffset = lastSuccessfulOffset + 1;
                                        commitOffsets.Add(new TopicPartitionOffset(partition, new Offset(nextOffset)));
                                        _lastCommittedOffsets[partition] = lastSuccessfulOffset;

                                        _logger.LogInformation(
                                            "// {Class} // Partition {Partition} prepared commit next offset {NextOffset} (last successful {LastOffset})",
                                            GetType().Name,
                                            partition.Partition.Value,
                                            nextOffset,
                                            lastSuccessfulOffset);
                                    }
                                    else
                                    {
                                        _logger.LogDebug(
                                            "// {Class} // Partition {Partition} no new progress (last successful {LastOffset}, previously committed {PrevCommitted})",
                                            GetType().Name,
                                            partition.Partition.Value,
                                            lastSuccessfulOffset,
                                            previousCommitted);
                                    }
                                }
                                else
                                {
                                    _logger.LogWarning(
                                        "// {Class} // Partition {Partition} no messages processed successfully in this batch",
                                        GetType().Name,
                                        partition.Partition.Value);
                                }
                            }
                            finally
                            {
                                _processingConcurrencySemaphore.Release();
                                _inFlightTasks.TryRemove(taskId, out _);
                            }
                        },
                        cancellationToken);

                    _inFlightTasks[taskId] = task;
                    partitionTasks.Add(task);
                }

                await Task.WhenAll(partitionTasks);

                if (!commitOffsets.IsEmpty)
                {
                    _logger.LogInformation("// {Class} // Committing {Count} partition offsets", GetType().Name, commitOffsets.Count);
                    SafeCommit(commitOffsets);
                }
                else
                {
                    _logger.LogWarning("// {Class} // No offsets eligible for commit in this batch (size {BatchSize})", GetType().Name, messageBatch.Length);
                }

                _logger.LogInformation(
                    "// KafkaConsumerBase // ConsumeMessageBatch // Batch completed topic {Topic}. Messages {BatchSize} in {Duration:F0}ms, committed {CommittedCount} partition offsets",
                    _topicName,
                    messageBatch.Length,
                    (DateTime.UtcNow - processingStartTime).TotalMilliseconds,
                    commitOffsets.Count);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// {Class} // Unexpected error in batch processing loop", GetType().Name);
            }
        }
    }

    /// <summary>
    /// Commits a batch of processed offsets to Kafka, normalizing duplicates per partition (keeping the highest offset).
    /// </summary>
    /// <param name="offsets">The offsets to commit.</param>
    private void SafeCommit(IEnumerable<TopicPartitionOffset> offsets)
    {
        if (_isShutdownInitiated || offsets is null)
        {
            return;
        }

        var normalized = offsets
            .GroupBy(o => o.TopicPartition)
            .Select(g =>
            {
                var maxNext = g.Select(x => x.Offset.Value).Max();
                return new TopicPartitionOffset(g.Key, new Offset(maxNext));
            })
            .ToList();

        if (normalized.Count == 0)
        {
            return;
        }

        _logger.LogInformation(
            "// {Class} // Committing offsets: {Offsets}",
            GetType().Name,
            string.Join(',', normalized.Select(o => $"{o.Topic}:{o.Partition}-{o.Offset.Value}")));

        try
        {
            _consumer.Commit(normalized);

            _logger.LogInformation("// {Class} // Successfully committed {Count} offsets", GetType().Name, normalized.Count);
        }
        catch (KafkaException ex) when (ex.Error.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration)
        {
            _logger.LogWarning(ex, "// {Class} // Commit skipped transient state: {Reason}", GetType().Name, ex.Error.Reason);
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "// {Class} // Commit failed", GetType().Name);
        }
    }

    /// <summary>
    /// Builds the Kafka <see cref="ConsumerConfig"/> using the shared client configuration and
    /// applies consumer-specific tuning for batching, throughput and cooperative offset management.
    /// </summary>
    /// <param name="settings">The configuration object used to hold integration settings for a Kafka.</param>
    /// <returns>
    /// A fully initialized <see cref="ConsumerConfig"/> ready to be used by a <see cref="ConsumerBuilder{TKey, TValue}"/>.
    /// </returns>
    private ConsumerConfig BuildConfiguration(KafkaSettings settings)
    {
        var config = new SharedClientConfig(settings);

        var consumerConfig = new ConsumerConfig(config.ConsumerConfig)
        {
            FetchWaitMaxMs = 50,
            QueuedMinMessages = 1000,
            SessionTimeoutMs = 30000,
            EnableAutoCommit = false,
            FetchMinBytes = 64 * 1024,
            HeartbeatIntervalMs = 5000,
            MaxPollIntervalMs = 300000,
            EnableAutoOffsetStore = false,
            QueuedMaxMessagesKbytes = 32768,
            MaxPartitionFetchBytes = 4 * 1024 * 1024,
            SocketReceiveBufferBytes = 2 * 1024 * 1024,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = $"{settings.Consumer.GroupId}-{GetType().Name.ToLower()}"
        };

        return consumerConfig;
    }

    /// <summary>
    /// Creates and configures a Kafka consumer instance with error, statistics, and partition assignment handlers.
    /// </summary>
    /// <param name="consumerConfig">The <see cref="ConsumerConfig"/> used to build the consumer.</param>
    /// <returns>
    /// A configured <see cref="IConsumer{TKey, TValue}"/> for consuming messages with string keys and values.
    /// </returns>
    /// <remarks>
    /// The consumer includes:
    /// - Error handling that logs fatal, error, and warning conditions.
    /// - Statistics logging for operational insights.
    /// - Partition assignment and revocation handlers to trace rebalances.
    /// </remarks>
    private IConsumer<string, string> BuildConsumer(ConsumerConfig consumerConfig)
    {
        return new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) =>
            {
                if (e.IsFatal)
                {
                    _logger.LogCritical("FATAL Kafka error. Code={ErrorCode}. Reason={Reason}", e.Code, e.Reason);
                }
                else if (e.IsError)
                {
                    _logger.LogError("Kafka error. Code={ErrorCode}. Reason={Reason}", e.Code, e.Reason);
                }
                else
                {
                    _logger.LogWarning("Kafka warning. Code={ErrorCode}. Reason={Reason}", e.Code, e.Reason);
                }
            })
            .SetStatisticsHandler((_, json) =>
            {
                _logger.LogDebug("// KafkaConsumer // Stats: {StatsJson}", json);
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogInformation("// {Class} // Partitions revoked: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation("// {Class} // Partitions assigned: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));
            })
            .Build();
    }

    /// <summary>
    /// Polls messages from the underlying Kafka consumer until one of the stopping conditions is met.
    /// </summary>
    /// <param name="maxBatchSize">The maximum number of messages to return.</param>
    /// <param name="timeoutMs">The total maximum time (in milliseconds) spent polling for this batch.</param>
    /// <param name="cancellationToken">Token observed for cooperative cancellation.</param>
    /// <returns>An array (possibly empty) of consecutively polled <see cref="ConsumeResult{TKey,TValue}"/> instances.</returns>
    private ConsumeResult<string, string>[] FetchMessageBatch(int maxBatchSize, int timeoutMs, CancellationToken cancellationToken)
    {
        if (maxBatchSize <= 0 || timeoutMs <= 0 || cancellationToken.IsCancellationRequested)
        {
            return [];
        }

        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        var polledMessages = new List<ConsumeResult<string, string>>(maxBatchSize);

        while (polledMessages.Count < maxBatchSize && !cancellationToken.IsCancellationRequested)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            try
            {
                var consumeResult = _consumer.Consume(remaining);
                if (consumeResult is null)
                {
                    break;
                }

                polledMessages.Add(consumeResult);
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning(ex, "// {Class} // Consume exception during batch polling", GetType().Name);

                break;
            }
        }

        _logger.LogInformation("// {Class} // Fetched {Count} messages from Kafka in batch", GetType().Name, polledMessages.Count);

        return [.. polledMessages];
    }
}
