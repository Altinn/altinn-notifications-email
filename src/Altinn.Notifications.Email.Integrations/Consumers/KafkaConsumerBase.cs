using System.Collections.Concurrent;

using Altinn.Notifications.Email.Integrations.Configuration;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Integrations.Kafka.Consumers;

/// <summary>
/// Abstract base class for Kafka consumers, providing a framework for consuming messages from a specified topic.
/// </summary>
public abstract class KafkaConsumerBase : BackgroundService
{
    private readonly string _topicName;
    private volatile bool _isShutdownInitiated;
    private readonly ILogger<KafkaConsumerBase> _logger;
    private readonly IConsumer<string, string> _consumer;

    private const int _maxMessagesCountInBatch = 200;
    private const int _messagesBatchPollTimeoutInMs = 100;
    private const int _maxConcurrentProcessingTasks = 100;

    private readonly SemaphoreSlim _processingConcurrencySemaphore;
    private readonly ConcurrentDictionary<Guid, Task> _inFlightTasks = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaConsumerBase"/> class.
    /// </summary>
    protected KafkaConsumerBase(string topicName, KafkaSettings settings, ILogger<KafkaConsumerBase> logger)
    {
        _logger = logger;
        _topicName = topicName;

        var config = new SharedClientConfig(settings);

        var consumerConfig = new ConsumerConfig(config.ConsumerConfig)
        {
            FetchWaitMaxMs = 20,
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

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
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
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation("// {Class} // Partitions assigned: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogInformation("// {Class} // Partitions revoked: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));
            })
            .Build();

        _processingConcurrencySemaphore = new SemaphoreSlim(_maxConcurrentProcessingTasks, _maxConcurrentProcessingTasks);
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();

        GC.SuppressFinalize(this);
    }

    /// <inheritdoc/>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topicName);

        return base.StartAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _isShutdownInitiated = true;

        _consumer.Unsubscribe();

        var processingTasks = _inFlightTasks.Values.ToArray();

        if (processingTasks.Length > 0)
        {
            _logger.LogInformation("// {Class} // Awaiting {Count} in-flight tasks", GetType().Name, processingTasks.Length);

            try
            {
                await Task.WhenAll(processingTasks);
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
    /// Consumes messages from the Kafka topic in batches, ensuring efficient processing
    /// with bulk commits. Messages are polled in batches, processed in parallel, and committed
    /// together for optimal performance. The method handles graceful shutdown and respects
    /// global concurrency limits.
    /// </summary>
    /// <param name="processMessageFunc">A function that takes the message string and processes it asynchronously.</param>
    /// <param name="retryMessageFunc">A function that takes the message string and handles retry logic asynchronously on processing failure.</param>
    /// <param name="cancellationToken">A cancellation token to signal when to stop consuming messages.</param>
    protected async Task ConsumeMessage(Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && !_isShutdownInitiated)
        {
            try
            {
                var messageBatch = FetchMessageBatch(_maxMessagesCountInBatch, _messagesBatchPollTimeoutInMs, cancellationToken);
                if (messageBatch.Length == 0)
                {
                    await Task.Delay(50, cancellationToken);
                    continue;
                }

                var processingTasks = new List<Task>();
                var processingStartTime = DateTime.UtcNow;
                var successfulOffsets = new ConcurrentBag<TopicPartitionOffset>();

                foreach (var message in messageBatch)
                {
                    await _processingConcurrencySemaphore.WaitAsync(cancellationToken);

                    var processingTaskId = Guid.NewGuid();
                    var processingTask = Task.Run(
                        async () =>
                        {
                            try
                            {
                                await processMessageFunc(message.Message.Value);

                                successfulOffsets.Add(new TopicPartitionOffset(message.TopicPartition, message.Offset + 1));
                            }
                            catch (OperationCanceledException)
                            {
                                // Shutdown scenario - don't retry
                            }
                            catch (Exception ex)
                            {
                                try
                                {
                                    await retryMessageFunc(message.Message.Value);
                                    successfulOffsets.Add(new TopicPartitionOffset(message.TopicPartition, message.Offset + 1));
                                }
                                catch (Exception retryEx)
                                {
                                    _logger.LogError(retryEx, "// {Class} // Retry failed for message at offset {Offset}", GetType().Name, message.Offset);
                                }

                                _logger.LogError(ex, "// {Class} // Error processing message at offset {Offset}", GetType().Name, message.Offset);
                            }
                            finally
                            {
                                _processingConcurrencySemaphore.Release();

                                _inFlightTasks.TryRemove(processingTaskId, out _);
                            }
                        },
                        cancellationToken);

                    _inFlightTasks[processingTaskId] = processingTask;
                    processingTasks.Add(processingTask);
                }

                await Task.WhenAll(processingTasks);

                if (!successfulOffsets.IsEmpty)
                {
                    SafeCommit(successfulOffsets);
                }
                else
                {
                    _logger.LogWarning("// {Class} // No messages successfully processed in batch of {BatchSize}", GetType().Name, messageBatch.Length);
                }

                _logger.LogInformation(
                    "// KafkaConsumerBase // ConsumeMessage // Batch consuming completed for topic {Topic}. Processed batch of {BatchSize} messages in {Duration:F0}ms, committed {CommittedCount} offsets",
                    _topicName,
                    messageBatch.Length,
                    (DateTime.UtcNow - processingStartTime).TotalMilliseconds,
                    successfulOffsets.Count);
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
                var maxOffset = g.Max(o => o.Offset);
                return new TopicPartitionOffset(g.Key, maxOffset);
            })
            .ToList();

        if (normalized.Count == 0)
        {
            return;
        }

        try
        {
            _consumer.Commit(normalized);
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
    /// Polls messages from the underlying Kafka consumer until one of the stopping conditions is met.
    /// </summary>
    /// <param name="maxBatchSize">The maximum number of messages to return.</param>
    /// <param name="timeoutMs">The total maximum time (in milliseconds) spent polling for this batch.</param>
    /// <param name="cancellationToken">Token observed for cooperative cancellation.
    /// <returns>
    /// An array (possibly empty) of consecutively polled <see cref="ConsumeResult{TKey,TValue}"/> instances.
    /// </returns>
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

        return [.. polledMessages];
    }
}
