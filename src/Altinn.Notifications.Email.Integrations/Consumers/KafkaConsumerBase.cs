using System.Collections.Concurrent;

using Altinn.Notifications.Email.Integrations.Configuration;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Integrations.Kafka.Consumers;

/// <summary>
/// Abstract base class for Kafka consumers, providing a framework for consuming messages from a specified topic.
/// Maximizes throughput with bounded parallelism while enforcing: do not start more messages after the first failure
/// (including failed retry). Already-started messages finish; contiguous successes are committed.
/// </summary>
public abstract class KafkaConsumerBase : BackgroundService
{
    private readonly string _topicName;
    private volatile bool _isShutdownInitiated;
    private readonly ILogger<KafkaConsumerBase> _logger;
    private readonly IConsumer<string, string> _consumer;

    private const int _maxMessagesCountInBatch = 50;
    private const int _maxConcurrentProcessingTasks = 50;
    private const int _messagesBatchPollTimeoutInMs = 100;

    private readonly SemaphoreSlim _processingConcurrencySemaphore;
    private readonly ConcurrentDictionary<Guid, Task> _inFlightTasks = new();

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
                    "// {Class} // Polled {BatchSize} messages. Offsets {StartOffset}..{EndOffset}. In-flight tasks: {InFlight}",
                    GetType().Name,
                    messageBatch.Length,
                    messageBatch[0].Offset,
                    messageBatch[^1].Offset,
                    _inFlightTasks.Count);

                var processingStartTime = DateTime.UtcNow;

                int failureDetectedFlag = 0;

                var successfulOffsets = new ConcurrentBag<TopicPartitionOffset>();
                var launchedTasks = new List<Task>(messageBatch.Length);
                var launchedMessages = new List<ConsumeResult<string, string>>(messageBatch.Length);

                foreach (var message in messageBatch)
                {
                    if (Volatile.Read(ref failureDetectedFlag) == 1 || cancellationToken.IsCancellationRequested || _isShutdownInitiated)
                    {
                        break;
                    }

                    await _processingConcurrencySemaphore.WaitAsync(cancellationToken);

                    var processingTaskId = Guid.NewGuid();
                    launchedMessages.Add(message);

                    _logger.LogInformation("// {Class} // Start processing message at offset {Offset}", GetType().Name, message.Offset);

                    var processingTask = Task.Run(
                        async () =>
                        {
                            try
                            {
                                if (Volatile.Read(ref failureDetectedFlag) == 1)
                                {
                                    return;
                                }

                                await processMessageFunc(message.Message.Value);

                                successfulOffsets.Add(new TopicPartitionOffset(message.TopicPartition, message.Offset + 1));

                                _logger.LogInformation("// {Class} // Successfully processed message at offset {Offset}", GetType().Name, message.Offset);
                            }
                            catch (OperationCanceledException)
                            {
                                Interlocked.CompareExchange(ref failureDetectedFlag, 1, 0);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "// {Class} // Error processing message at offset {Offset}, attempting retry", GetType().Name, message.Offset);

                                try
                                {
                                    await retryMessageFunc(message.Message.Value);

                                    successfulOffsets.Add(new TopicPartitionOffset(message.TopicPartition, message.Offset + 1));

                                    _logger.LogInformation("// {Class} // Retry succeeded for message at offset {Offset}", GetType().Name, message.Offset);
                                }
                                catch (Exception retryEx)
                                {
                                    _logger.LogError(retryEx, "// {Class} // Retry failed for message at offset {Offset}. Halting further launches.", GetType().Name, message.Offset);
                                    Interlocked.CompareExchange(ref failureDetectedFlag, 1, 0);
                                }
                            }
                            finally
                            {
                                _processingConcurrencySemaphore.Release();
                                _inFlightTasks.TryRemove(processingTaskId, out _);
                                _logger.LogInformation("// {Class} // Released semaphore for message at offset {Offset}. In-flight tasks: {InFlight}", GetType().Name, message.Offset, _inFlightTasks.Count);
                            }
                        },
                        cancellationToken);

                    _inFlightTasks[processingTaskId] = processingTask;
                    launchedTasks.Add(processingTask);
                }

                try
                {
                    await Task.WhenAll(launchedTasks);
                }
                catch (Exception aggregateEx)
                {
                    _logger.LogDebug(aggregateEx, "// {Class} // Aggregate completion contained failures", GetType().Name);
                }

                if (!successfulOffsets.IsEmpty)
                {
                    _logger.LogInformation(
                        "// {Class} // Computing contiguous commit offsets from {Count} successes. FailureDetected={Failure}",
                        GetType().Name,
                        successfulOffsets.Count,
                        Volatile.Read(ref failureDetectedFlag) == 1);

                    var commitCandidates = ComputeContiguousCommitOffsets(launchedMessages.ToArray(), successfulOffsets);

                    if (commitCandidates.Count != 0)
                    {
                        _logger.LogInformation("// {Class} // Committing {Count} offsets to Kafka", GetType().Name, commitCandidates.Count);
                        SafeCommit(commitCandidates);
                    }
                    else
                    {
                        _logger.LogWarning("// {Class} // No contiguous offsets eligible for commit in launched set of {Launched}", GetType().Name, launchedMessages.Count);
                    }
                }
                else
                {
                    _logger.LogWarning("// {Class} // No messages successfully processed in launched set of {Launched}", GetType().Name, launchedMessages.Count);
                }

                _logger.LogInformation(
                    "// KafkaConsumerBase // ConsumeMessage // Batch completed for topic {Topic}. Launched={Launched} Polled={Polled} FailureDetected={Failure} Duration={Duration:F0}ms",
                    _topicName,
                    launchedMessages.Count,
                    messageBatch.Length,
                    Volatile.Read(ref failureDetectedFlag) == 1,
                    (DateTime.UtcNow - processingStartTime).TotalMilliseconds);
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
                var max = g.Select(x => x.Offset.Value).Max();
                return new TopicPartitionOffset(g.Key, new Offset(max));
            })
            .ToList();

        if (normalized.Count == 0)
        {
            return;
        }

        _logger.LogInformation("// {Class} // Committing normalized offsets per partition: {Offsets}", GetType().Name, string.Join(',', normalized.Select(o => $"{o.Topic}:{o.Partition}-{o.Offset}")));

        try
        {
            _consumer.Commit(normalized);

            _logger.LogInformation("// {Class} // Successfully committed {Count} offsets", GetType().Name, normalized.Count);
        }
        catch (KafkaException ex) when (ex.Error.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration)
        {
            _logger.LogWarning(ex, "// {Class} // Bulk commit skipped due to transient state: {Reason}", GetType().Name, ex.Error.Reason);
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "// {Class} // Bulk commit failed unexpectedly", GetType().Name);
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
            FetchWaitMaxMs = 100,
            QueuedMinMessages = 50,
            SessionTimeoutMs = 30000,
            EnableAutoCommit = false,
            FetchMinBytes = 512 * 1024,
            MaxPollIntervalMs = 300000,
            HeartbeatIntervalMs = 5000,
            EnableAutoOffsetStore = false,
            QueuedMaxMessagesKbytes = 16384,
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
                _logger.LogDebug("// KafkaProducer // Stats: {StatsJson}", json);
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
                var result = _consumer.Consume(remaining);
                if (result is null)
                {
                    break;
                }

                polledMessages.Add(result);
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

    /// <summary>
    /// Computes per-partition commit offsets as the largest contiguous prefix of successful messages from the earliest offset in the launched subset.
    /// Prevents committing past gaps caused by failures.
    /// </summary>
    private static List<TopicPartitionOffset> ComputeContiguousCommitOffsets(
        ConsumeResult<string, string>[] launchedBatch,
        IEnumerable<TopicPartitionOffset> successfulOffsets)
    {
        var successesByTp = successfulOffsets
            .GroupBy(s => s.TopicPartition)
            .ToDictionary(
                g => g.Key,
                g => new HashSet<long>(g.Select(s => s.Offset.Value)));

        var batchByTp = launchedBatch
            .GroupBy(m => m.TopicPartition)
            .ToDictionary(
                g => g.Key,
                g => g.Select(m => m.Offset.Value).OrderBy(x => x).ToList());

        var commitOffsets = new List<TopicPartitionOffset>();

        foreach (var kvp in batchByTp)
        {
            var topicPartition = kvp.Key;
            var orderedOffsets = kvp.Value;

            if (!successesByTp.TryGetValue(topicPartition, out var successSet) || successSet.Count == 0)
            {
                continue;
            }

            long? lastContiguousNext = null;

            foreach (var offset in orderedOffsets)
            {
                var nextPosition = offset + 1;

                if (successSet.Contains(nextPosition))
                {
                    lastContiguousNext = nextPosition;
                }
                else
                {
                    break;
                }
            }

            if (lastContiguousNext.HasValue)
            {
                commitOffsets.Add(new TopicPartitionOffset(topicPartition, new Offset(lastContiguousNext.Value)));
            }
        }

        return commitOffsets;
    }
}
