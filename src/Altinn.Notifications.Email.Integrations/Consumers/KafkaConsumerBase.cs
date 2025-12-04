using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;

using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Integrations.Kafka.Consumers
{
    /// <summary>
    /// Abstract base class for Kafka consumers, offering a robust framework for consuming messages from a specified topic.
    /// </summary>
    public abstract class KafkaConsumerBase : BackgroundService
    {
        private int _batchFailureFlag;
        private int _consumerClosedFlag;
        private int _shutdownInitiatedFlag;

        private readonly int _maxBatchSize = 100;
        private readonly int _batchPollTimeoutMs = 100;

        private readonly string _subscribedTopicName;
        private readonly ILogger<KafkaConsumerBase> _logger;
        private volatile KafkaBatchState? _lastProcessedBatch;
        private readonly IConsumer<string, string> _kafkaConsumer;
        private CancellationTokenSource? _internalCancellationSource;

        private static readonly Meter _meter = new("Altinn.Notifications.KafkaConsumer", "1.0.0");
        private static readonly Counter<int> _messagesPolledCounter = _meter.CreateCounter<int>("kafka.consumer.polled");
        private static readonly Counter<int> _messagesProcessedCounter = _meter.CreateCounter<int>("kafka.consumer.processed");
        private static readonly Counter<int> _retryFailureCounter = _meter.CreateCounter<int>("kafka.consumer.retried.failed");
        private static readonly Counter<int> _retrySuccessCounter = _meter.CreateCounter<int>("kafka.consumer.retried.succeeded");
        private static readonly Histogram<double> _batchProcessingLatency = _meter.CreateHistogram<double>("kafka.consumer.batch.latency.ms");

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerBase"/> class.
        /// </summary>
        protected KafkaConsumerBase(string topicName, KafkaSettings settings, ILogger<KafkaConsumerBase> logger)
        {
            _logger = logger;
            _subscribedTopicName = topicName;

            var configuration = BuildConfiguration(settings);
            _kafkaConsumer = BuildConsumer(configuration);
        }

        /// <inheritdoc/>
        public override void Dispose()
        {
            try
            {
                if (!IsConsumerClosed)
                {
                    SignalConsumerClosed();

                    _kafkaConsumer.Close();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// KafkaConsumer Dispose // Close failed");
            }
            finally
            {
                _kafkaConsumer.Dispose();

                _internalCancellationSource?.Dispose();

                base.Dispose();
            }
        }

        /// <inheritdoc/>
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _internalCancellationSource = new CancellationTokenSource();

            _kafkaConsumer.Subscribe(_subscribedTopicName);

            _logger.LogInformation("// {Class} // subscribed to topic {Topic}", GetType().Name, ComputeTopicFingerprint(_subscribedTopicName));

            return base.StartAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            SignalShutdownIsInitiated();

            if (_internalCancellationSource != null)
            {
                await _internalCancellationSource.CancelAsync();
            }

            _kafkaConsumer.Unsubscribe();

            _logger.LogInformation("// {Class} // unsubscribed from topic {Topic} because shutdown is initiated ", GetType().Name, ComputeTopicFingerprint(_subscribedTopicName));

            await base.StopAsync(cancellationToken);

            var contiguousOffsets = _lastProcessedBatch is not null ? GetSafeCommitOffsets(_lastProcessedBatch) : [];
            if (contiguousOffsets.Count > 0 && !IsConsumerClosed)
            {
                try
                {
                    _kafkaConsumer.Commit(contiguousOffsets);

                    _logger.LogInformation("// {Class} // Committed contiguous offsets for processed messages during shutdown", GetType().Name);
                }
                catch (KafkaException ex)
                {
                    _logger.LogError(ex, "// {Class} // Failed to commit contiguous offsets during shutdown", GetType().Name);
                }
            }

            if (!IsConsumerClosed)
            {
                SignalConsumerClosed();

                _kafkaConsumer.Close();
            }
        }

        /// <inheritdoc/>
        protected override abstract Task ExecuteAsync(CancellationToken stoppingToken);

        /// <summary>
        /// Consumes messages from the configured Kafka topic using batch polling and bounded parallel processing.
        /// </summary>
        /// <param name="processMessageFunc">
        /// Delegate that processes a single message value. Exceptions trigger a retry via <paramref name="retryMessageFunc"/>.
        /// </param>
        /// <param name="retryMessageFunc">
        /// Delegate invoked when <paramref name="processMessageFunc"/> fails. If it also fails, the batch stops launching new processing tasks.
        /// </param>
        /// <param name="cancellationToken">
        /// Token observed for cooperative cancellation. When signaled, polling and new task launches stop and in-flight tasks are awaited.
        /// </param>
        protected async Task ConsumeMessageAsync(Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            using var cancellationTokenSource = _internalCancellationSource is null
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCancellationSource.Token);

            var linkedCancellationToken = cancellationTokenSource.Token;

            while (!linkedCancellationToken.IsCancellationRequested && !IsShutdownInitiated)
            {
                var batchStopwatch = Stopwatch.StartNew();

                ResetMessageProcessingFailureSignal();

                var KafkaBatchState = PollConsumeResults(linkedCancellationToken);
                if (KafkaBatchState.PolledConsumeResults.Count == 0)
                {
                    batchStopwatch.Stop();
                    await Task.Delay(10, linkedCancellationToken);
                    continue;
                }

                _messagesPolledCounter.Add(KafkaBatchState.PolledConsumeResults.Count, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_subscribedTopicName)));

                KafkaBatchState = await ProcessConsumeResultsAsync(KafkaBatchState, processMessageFunc, retryMessageFunc, linkedCancellationToken);

                _lastProcessedBatch = KafkaBatchState;

                var contiguousCommitOffsets = GetSafeCommitOffsets(KafkaBatchState);
                if (contiguousCommitOffsets.Count > 0)
                {
                    CommitOffsets(contiguousCommitOffsets);
                }

                batchStopwatch.Stop();

                _batchProcessingLatency.Record(batchStopwatch.Elapsed.TotalMilliseconds, KeyValuePair.Create<string, object?>("topic", _subscribedTopicName));
            }
        }

        /// <summary>
        /// Indicates whether the consumer has been closed.
        /// </summary>
        private bool IsConsumerClosed => Volatile.Read(ref _consumerClosedFlag) != 0;

        /// <summary>
        /// Indicates whether the consumer shutdown has been initiated.
        /// </summary>
        private bool IsShutdownInitiated => Volatile.Read(ref _shutdownInitiatedFlag) != 0;

        /// <summary>
        /// Indicates whether a message processing failure has occurred in the current batch.
        /// </summary>
        private bool IsMessageProcessingFailureSignaled => Volatile.Read(ref _batchFailureFlag) != 0;

        /// <summary>
        /// Computes a deterministic truncated SHA-256 hexadecimal fingerprint for a Kafka topic name.
        /// The fingerprint is intended for log correlation and diagnostics without exposing the raw topic identifier.
        /// </summary>
        /// <param name="topicName">
        /// The original Kafka topic name to fingerprint. If <c>null</c>, empty, or whitespace,
        /// the literal string <c>"EMPTY"</c> is returned.
        /// </param>
        /// <returns>
        /// A 16 character lowercase hexadecimal string representing the first 8 bytes of the SHA-256 hash
        /// of <paramref name="topicName"/>, or <c>"EMPTY"</c> if the input is blank.
        /// </returns>
        private static string ComputeTopicFingerprint(string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName))
            {
                return "EMPTY";
            }

            var digest = SHA256.HashData(Encoding.UTF8.GetBytes(topicName));
            string hex = Convert.ToHexString(digest.AsSpan(0, 8));

            return hex.ToLowerInvariant();
        }

        /// <summary>
        /// Builds the Kafka <see cref="ConsumerConfig"/> using the shared client configuration.
        /// </summary>
        /// <param name="settings">The configuration object used to hold integration settings for Kafka.</param>
        /// <returns>A fully initialized <see cref="ConsumerConfig"/> ready to be used by a <see cref="ConsumerBuilder{TKey, TValue}"/>.</returns>
        private ConsumerConfig BuildConfiguration(KafkaSettings settings)
        {
            var configuration = new SharedClientConfig(settings);

            var consumerConfig = new ConsumerConfig(configuration.ConsumerConfig)
            {
                FetchWaitMaxMs = 100,
                QueuedMinMessages = 100,
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
        /// Commits offsets to Kafka safely by normalizing per-partition offsets and handling transient
        /// consumer group states. Normalization ensures only the highest next-offset per partition is committed.
        /// </summary>
        /// <param name="offsets">
        /// The per-message next-offsets (original offset + 1) to commit. May contain multiple entries per partition.
        /// </param>
        private void CommitOffsets(IEnumerable<TopicPartitionOffset> offsets)
        {
            if (offsets is null || IsShutdownInitiated || IsConsumerClosed)
            {
                return;
            }

            var normalizedOffsets = offsets
                .GroupBy(e => e.TopicPartition)
                .Select(e =>
                {
                    var maxOffset = e.Select(x => x.Offset.Value).Max();
                    return new TopicPartitionOffset(e.Key, new Offset(maxOffset));
                })
                .ToList();

            if (normalizedOffsets.Count == 0)
            {
                return;
            }

            try
            {
                _kafkaConsumer.Commit(normalizedOffsets);
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
        /// Creates and configures a Kafka consumer instance.
        /// </summary>
        /// <param name="consumerConfig">The <see cref="ConsumerConfig"/> used to build the consumer.</param>
        /// <returns>A configured <see cref="IConsumer{TKey, TValue}"/> for consuming messages with string keys and values.</returns>
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
                    _logger.LogDebug("// KafkaConsumerBase // Stats: {StatsJson}", json);
                })
                .SetPartitionsRevokedHandler((_, partitions) =>
                {
                    if (IsConsumerClosed)
                    {
                        return;
                    }

                    var contiguousOffsets = _lastProcessedBatch is not null ? GetSafeCommitOffsets(_lastProcessedBatch) : [];
                    var toCommit = contiguousOffsets
                        .Where(o => partitions.Any(p => p.TopicPartition.Equals(o.TopicPartition)))
                        .ToList();

                    if (toCommit.Count > 0 && !IsShutdownInitiated)
                    {
                        try
                        {
                            _kafkaConsumer.Commit(toCommit);
                        }
                        catch (KafkaException ex) when (ex.Error.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration)
                        {
                            _logger.LogWarning(ex, "// {Class} // Commit on revocation skipped due to transient state: {Reason}", GetType().Name, ex.Error.Reason);
                        }
                        catch (KafkaException ex)
                        {
                            _logger.LogError(ex, "// {Class} // Commit on revocation failed unexpectedly", GetType().Name);
                        }
                    }

                    _logger.LogInformation("// {Class} // Partitions revoked: {Partitions}", GetType().Name, string.Join(',', partitions.Select(e => e.Partition.Value)));
                })
                .SetPartitionsAssignedHandler((_, partitions) =>
                {
                    _logger.LogInformation("// {Class} // Partitions assigned: {Partitions}", GetType().Name, string.Join(',', partitions.Select(e => e.Partition.Value)));
                })
                .Build();
        }

        /// <summary>
        /// Polls the Kafka consumer for new messages until either the time budget or the per-batch item cap is reached, or shutdown/cancellation is observed.
        /// </summary>
        /// <param name="cancellationToken">Token observed for cooperative cancellation and shutdown.</param>
        /// <returns>
        /// A <see cref="KafkaBatchState"/> with <see cref="KafkaBatchState.PolledConsumeResults"/>
        /// containing the consecutively polled <see cref="ConsumeResult{TKey, TValue}"/> items. The list may be empty.
        /// </returns>
        private KafkaBatchState PollConsumeResults(CancellationToken cancellationToken)
        {
            var deadlineTickMs = Environment.TickCount64 + _batchPollTimeoutMs;
            var polledConsumeResults = new List<ConsumeResult<string, string>>(_maxBatchSize);

            while (!cancellationToken.IsCancellationRequested && !IsShutdownInitiated)
            {
                var remainingMs = (int)Math.Max(0, deadlineTickMs - Environment.TickCount64);
                if (remainingMs <= 0)
                {
                    break;
                }

                if (polledConsumeResults.Count >= _maxBatchSize)
                {
                    break;
                }

                try
                {
                    var consumeResult = _kafkaConsumer.Consume(TimeSpan.FromMilliseconds(remainingMs));
                    if (consumeResult is null)
                    {
                        break;
                    }

                    polledConsumeResults.Add(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "// {Class} // Exception during polling", GetType().Name);
                    break;
                }
            }

            return new KafkaBatchState
            {
                PolledConsumeResults = [.. polledConsumeResults]
            };
        }

        /// <summary>
        /// Atomically signals that the consumer has been closed.
        /// </summary>
        private void SignalConsumerClosed() => Interlocked.Exchange(ref _consumerClosedFlag, 1);

        /// <summary>
        /// Atomically signals that consumer shutdown has been initiated.
        /// </summary>
        private void SignalShutdownIsInitiated() => Interlocked.Exchange(ref _shutdownInitiatedFlag, 1);

        /// <summary>
        /// Determines the highest safe commit offset for each partition by finding
        /// the largest contiguous sequence of successfully processed messages from the start of the batch.
        /// </summary>
        /// <param name="batchState">
        /// The batch state containing polled messages and their processing results.
        /// </param>
        /// <returns>
        /// Safe-to-commit offsets for each partition. Only includes partitions where a contiguous 
        /// sequence of processed messages exists from the earliest polled offset. Returns an empty 
        /// list if no safe commit points can be established.
        /// </returns>
        private static List<TopicPartitionOffset> GetSafeCommitOffsets(KafkaBatchState batchState)
        {
            var safeOffsetsToCommit = new List<TopicPartitionOffset>();

            var polledOffsetsByPartition = batchState.PolledConsumeResults
                .GroupBy(cr => cr.TopicPartition)
                .ToDictionary(grp => grp.Key, grp => grp.Select(cr => cr.Offset.Value).OrderBy(offset => offset).ToList());

            var processedOffsetsByPartition = batchState.CommitReadyOffsets
                .GroupBy(tpo => tpo.TopicPartition)
                .ToDictionary(grp => grp.Key, grp => new HashSet<long>(grp.Select(tpo => tpo.Offset.Value)));

            foreach (var partition in polledOffsetsByPartition)
            {
                var topicPartition = partition.Key;
                var orderedOffsets = partition.Value;

                if (!processedOffsetsByPartition.TryGetValue(topicPartition, out var successSet) || successSet.Count == 0)
                {
                    continue;
                }

                long? safeCommitOffset = null;

                foreach (var offset in orderedOffsets)
                {
                    var nextPosition = offset + 1;

                    if (successSet.Contains(nextPosition))
                    {
                        safeCommitOffset = nextPosition;
                    }
                    else
                    {
                        break;
                    }
                }

                if (safeCommitOffset.HasValue)
                {
                    safeOffsetsToCommit.Add(new TopicPartitionOffset(topicPartition, new Offset(safeCommitOffset.Value)));
                }
            }

            return safeOffsetsToCommit;
        }

        /// <summary>
        /// Atomically signals that a message processing failure has occurred in the current batch.
        /// </summary>
        private void SignalMessageProcessingFailure() => Interlocked.Exchange(ref _batchFailureFlag, 1);

        /// <summary>
        /// Atomically clears the message processing failure signal before handling a new batch.
        /// </summary>
        private void ResetMessageProcessingFailureSignal() => Interlocked.Exchange(ref _batchFailureFlag, 0);

        /// <summary>
        /// Launches processing tasks using Parallel.ForEachAsync for efficient concurrent processing with built-in resource management.
        /// Replaces manual task orchestration with Task.WhenAll() and provides better concurrency control.
        /// </summary>
        /// <param name="processingContext">
        /// The current batch context containing the polled consume results to be processed.
        /// </param>
        /// <param name="processMessageFunc">
        /// Delegate that processes a single message value. Exceptions trigger a retry via <paramref name="retryMessageFunc"/>.
        /// </param>
        /// <param name="retryMessageFunc">
        /// Delegate invoked when <paramref name="processMessageFunc"/> fails. If it also fails, the batch stops launching new processing tasks.
        /// </param>
        /// <param name="cancellationToken">
        /// Token observed for cooperative cancellation. When signaled, polling and new task launches stop and in-flight tasks are awaited.
        /// </param>
        /// <returns>
        /// An updated <see cref="KafkaBatchState"/> with <see cref="KafkaBatchState.CommitReadyOffsets"/> populated from completed tasks.
        /// </returns>
        private async Task<KafkaBatchState> ProcessConsumeResultsAsync(KafkaBatchState processingContext, Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested && IsShutdownInitiated)
            {
                return processingContext;
            }

            var parallelOptions = new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = _maxBatchSize
            };

            var successfulOffsets = new ConcurrentBag<TopicPartitionOffset>();

            try
            {
                await Parallel.ForEachAsync(
                    processingContext.PolledConsumeResults,
                    parallelOptions,
                    async (consumeResult, cancellationToken) =>
                    {
                        var result = await ProcessSingleMessageAsync(consumeResult, processMessageFunc, retryMessageFunc, cancellationToken);

                        if (result is not null)
                        {
                            successfulOffsets.Add(result);
                        }
                    });
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }

            return processingContext with
            {
                CommitReadyOffsets = [.. successfulOffsets]
            };
        }

        /// <summary>
        /// Processes a single Kafka message with semaphore-controlled concurrency, retry logic, and comprehensive error handling.
        /// </summary>
        /// <param name="consumeResult">
        /// The consume result which cotains the Kafka-message to process.
        /// </param>
        /// <param name="processMessageFunc">
        /// Delegate that processes a single message value. Exceptions trigger a retry via <paramref name="retryMessageFunc"/>.
        /// </param>
        /// <param name="retryMessageFunc">
        /// Delegate invoked when <paramref name="processMessageFunc"/> fails. If it also fails, the batch stops launching new processing tasks.
        /// </param>
        /// <param name="cancellationToken">
        /// Token observed for cooperative cancellation. When signaled, polling and new task launches stop and in-flight tasks are awaited.
        /// </param>
        /// <returns>
        /// An updated <see cref="KafkaBatchState"/> with <see cref="KafkaBatchState.CommitReadyOffsets"/> populated from completed tasks.
        /// </returns>
        /// <returns>
        /// A <see cref="TopicPartitionOffset"/> representing the next offset to commit (original offset + 1)
        /// if message processing succeeds (either via primary or retry processing);
        /// otherwise <c>null</c> if processing fails or is short-circuited due to cancellation/shutdown.
        /// </returns>
        private async Task<TopicPartitionOffset?> ProcessSingleMessageAsync(ConsumeResult<string, string> consumeResult, Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            try
            {
                if (cancellationToken.IsCancellationRequested || IsMessageProcessingFailureSignaled || IsShutdownInitiated || IsConsumerClosed)
                {
                    return null;
                }

                await processMessageFunc(consumeResult.Message.Value);

                _messagesProcessedCounter.Add(1, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_subscribedTopicName)));

                return new TopicPartitionOffset(consumeResult.TopicPartition, consumeResult.Offset + 1);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// {Class} // Error processing message at offset {Offset}, attempting retry", GetType().Name, consumeResult.Offset);

                try
                {
                    if (cancellationToken.IsCancellationRequested || IsMessageProcessingFailureSignaled || IsShutdownInitiated || IsConsumerClosed)
                    {
                        return null;
                    }

                    await retryMessageFunc(consumeResult.Message.Value);

                    _retrySuccessCounter.Add(1, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_subscribedTopicName)));

                    return new TopicPartitionOffset(consumeResult.TopicPartition, consumeResult.Offset + 1);
                }
                catch (Exception retryEx)
                {
                    _logger.LogError(retryEx, "// {Class} // Retry failed for message at offset {Offset}. Halting further launches.", GetType().Name, consumeResult.Offset);

                    _retryFailureCounter.Add(1, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_subscribedTopicName)));

                    SignalMessageProcessingFailure();

                    return null;
                }
            }
        }
    }
}
