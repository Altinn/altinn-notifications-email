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
    /// Abstract base class for Kafka consumers, providing a framework for consuming messages from a specified topic.
    /// Maximizes throughput with bounded parallelism while enforcing: do not start more messages after the first failure
    /// (including failed retry). Already-started messages finish; contiguous successes are committed.
    /// </summary>
    public abstract class KafkaConsumerBase : BackgroundService
    {
        private int _failureFlag = 0;
        private readonly string _topicName;
        private volatile bool _isShutdownInitiated;
        private readonly ILogger<KafkaConsumerBase> _logger;
        private readonly IConsumer<string, string> _consumer;

        private readonly int _maxMessagesPerBatch = 50;
        private readonly int _messagesBatchPollTimeoutMs = 100;
        private readonly int _maxConcurrentProcessingTasks = 50;

        private readonly int _progressiveCommitIntervalMs = 250;
        private readonly int _progressiveCommitMinNewSuccesses = 10;

        private volatile IReadOnlyList<Task> _currentBatchTasks = [];
        private readonly SemaphoreSlim _processingConcurrencySemaphore;

        private static readonly Meter _meter = new("Altinn.Notifications.KafkaConsumer", "1.0.0");
        private static readonly Counter<int> _failedCounter = _meter.CreateCounter<int>("kafka.consumer.failed");
        private static readonly Counter<int> _consumedCounter = _meter.CreateCounter<int>("kafka.consumer.consumed");
        private static readonly Counter<int> _committedCounter = _meter.CreateCounter<int>("kafka.consumer.committed");
        private static readonly Counter<int> _processedCounter = _meter.CreateCounter<int>("kafka.consumer.processed");
        private static readonly Counter<int> _retriedFailedCounter = _meter.CreateCounter<int>("kafka.consumer.retried.failed");
        private static readonly Counter<int> _retriedSucceededCounter = _meter.CreateCounter<int>("kafka.consumer.retried.succeeded");
        private static readonly Histogram<double> _batchLatencyMs = _meter.CreateHistogram<double>("kafka.consumer.batch.latency.ms");

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
                _processingConcurrencySemaphore.Dispose();
                base.Dispose();
            }
        }

        /// <inheritdoc/>
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topicName);

            _logger.LogInformation("// {Class} // Subscribed to topic {Topic}", GetType().Name, ComputeTopicFingerprint(_topicName));

            return base.StartAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _isShutdownInitiated = true;

            _consumer.Unsubscribe();

            while (!cancellationToken.IsCancellationRequested)
            {
                var pending = _currentBatchTasks.Where(e => !e.IsCompleted).ToArray() ?? [];
                if (pending.Length == 0)
                {
                    break;
                }

                try
                {
                    var waitAll = Task.WhenAll(pending);
                    var completed = await Task.WhenAny(waitAll, Task.Delay(TimeSpan.FromSeconds(10), cancellationToken));

                    if (completed == waitAll)
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            await base.StopAsync(cancellationToken);
        }

        /// <inheritdoc/>
        protected override abstract Task ExecuteAsync(CancellationToken stoppingToken);

        /// <summary>
        /// Consumes messages from the Kafka topic in batches, launching up to a bounded number of concurrent processors.
        /// </summary>
        /// <param name="processMessageFunc">A function that processes a single message value.</param>
        /// <param name="retryMessageFunc">A function that retries processing a single message value when the initial processing fails.</param>
        /// <param name="cancellationToken">A cancellation token used to observe shutdown requests and coordinate graceful termination of processing tasks.</param>
        protected async Task ConsumeMessage(Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !_isShutdownInitiated)
            {
                var batchStopwatch = Stopwatch.StartNew();

                var batchProcessingContext = PollConsumeResults(_maxMessagesPerBatch, _messagesBatchPollTimeoutMs, cancellationToken);
                if (batchProcessingContext.PolledConsumeResults.Count == 0)
                {
                    await Task.Delay(10, cancellationToken);
                    continue;
                }

                _consumedCounter.Add(batchProcessingContext.PolledConsumeResults.Count, KeyValuePair.Create<string, object?>("topic", _topicName));

                try
                {
                    batchProcessingContext = await ProcessPolledConsumeResults(batchProcessingContext, processMessageFunc, retryMessageFunc, cancellationToken);

                    await ContiguousCommitCompletedConsumingTasks(batchProcessingContext, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "// {Class} // Unexpected error in batch processing loop", GetType().Name);
                }
                finally
                {
                    batchStopwatch.Stop();

                    _batchLatencyMs.Record(batchStopwatch.Elapsed.TotalMilliseconds, KeyValuePair.Create<string, object?>("topic", _topicName));
                }
            }
        }

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

            ReadOnlySpan<byte> topicNameBytes = Encoding.UTF8.GetBytes(topicName);

            Span<byte> digest = stackalloc byte[32];
            SHA256.HashData(topicNameBytes, digest);

            // First 8 bytes -> 16 hex chars (truncated fingerprint)
            Span<char> fingerprintBuffer = stackalloc char[16];
            const string hexAlphabet = "0123456789abcdef";

            for (int i = 0; i < 8; i++)
            {
                byte byteValue = digest[i];
                fingerprintBuffer[i * 2] = hexAlphabet[byteValue >> 4];
                fingerprintBuffer[(i * 2) + 1] = hexAlphabet[byteValue & 0x0F];
            }

            return new string(fingerprintBuffer);
        }

        /// <summary>
        /// Builds the Kafka <see cref="ConsumerConfig"/> using the shared client configuration.
        /// </summary>
        /// <param name="settings">The configuration object used to hold integration settings for Kafka.</param>
        /// <returns>A fully initialized <see cref="ConsumerConfig"/> ready to be used by a <see cref="ConsumerBuilder{TKey, TValue}"/>.</returns>
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
        /// Indicates whether the current batch has encountered a failure.
        /// </summary>
        private bool HasFailed => Volatile.Read(ref _failureFlag) == 1;

        /// <summary>
        /// Atomically signals a failure for the current batch, preventing further task launches.
        /// </summary>
        private void SignalFailure() => Interlocked.Exchange(ref _failureFlag, 1);

        /// <summary>
        /// Creates and configures a Kafka consumer instance with error, statistics, and partition assignment handlers.
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
                    _logger.LogInformation("// {Class} // Partitions revoked: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));

                    SignalFailure();
                })
                .SetPartitionsAssignedHandler((_, partitions) =>
                {
                    _logger.LogInformation("// {Class} // Partitions assigned: {Partitions}", GetType().Name, string.Join(',', partitions.Select(p => p.Partition.Value)));
                })
                .Build();
        }

        /// <summary>
        /// Commits offsets to Kafka safely by normalizing per-partition offsets and handling transient
        /// consumer group states. Normalization ensures only the highest next-offset per partition is committed.
        /// </summary>
        /// <param name="offsets">
        /// The per-message next-offsets (original offset + 1) to commit. May contain multiple entries per partition.
        /// </param>
        private void CommitNormalizedOffsets(IEnumerable<TopicPartitionOffset> offsets)
        {
            if (offsets is null || _isShutdownInitiated)
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
                _consumer.Commit(normalizedOffsets);

                _committedCounter.Add(normalizedOffsets.Count, KeyValuePair.Create<string, object?>("topic", _topicName));
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
        /// Computes per-partition commit offsets by determining the largest contiguous
        /// sequence of successfully processed messages from the earliest offset in each partition
        /// within the launched batch.
        /// </summary>
        /// <param name="batchContext">
        /// The batch context providing launched consume results and the successful next-offsets (offset + 1).
        /// </param>
        /// <returns>
        /// A list of <see cref="TopicPartitionOffset"/> values that are safe to commit to Kafka,
        /// representing the highest contiguous offset that can be committed for each partition without gaps.
        /// Returns an empty list if no contiguous sequences can be established.
        /// </returns>
        private static List<TopicPartitionOffset> ComputeContiguousCommitOffsets(BatchProcessingContext batchContext)
        {
            var launchedBatch = batchContext.ConsumeResultsForLaunchedTasks;
            var successfulOffsets = batchContext.SuccessfulNextOffsets;

            var commitOffsets = new List<TopicPartitionOffset>();

            var batchByTopicPartition = launchedBatch
                .GroupBy(e => e.TopicPartition)
                .ToDictionary(e => e.Key, e => e.Select(x => x.Offset.Value).OrderBy(x => x).ToList());

            var successesByTopicPartition = successfulOffsets
                .GroupBy(e => e.TopicPartition)
                .ToDictionary(e => e.Key, e => new HashSet<long>(e.Select(s => s.Offset.Value)));

            foreach (var kvp in batchByTopicPartition)
            {
                var topicPartition = kvp.Key;
                var orderedOffsets = kvp.Value;

                if (!successesByTopicPartition.TryGetValue(topicPartition, out var successSet) || successSet.Count == 0)
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

        /// <summary>
        /// Polls consume results from the underlying Kafka consumer until one of the stopping conditions is met.
        /// </summary>
        /// <param name="maxConsumeResults">The maximum number of consume results to return.</param>
        /// <param name="pollTimeoutMs">The total maximum time (in milliseconds) spent polling.</param>
        /// <param name="cancellationToken">Token observed for cooperative cancellation.</param>
        /// <returns>
        /// A <see cref="BatchProcessingContext"/> containing:
        /// - <see cref="BatchProcessingContext.PolledConsumeResults"/>: The consecutively polled <see cref="ConsumeResult{TKey,TValue}"/> instances.
        /// </returns>
        private BatchProcessingContext PollConsumeResults(int maxConsumeResults, int pollTimeoutMs, CancellationToken cancellationToken)
        {
            if (maxConsumeResults <= 0 || pollTimeoutMs <= 0 || cancellationToken.IsCancellationRequested)
            {
                return new BatchProcessingContext();
            }

            var pollDeadline = DateTime.UtcNow.AddMilliseconds(pollTimeoutMs);
            var polledConsumeResults = new List<ConsumeResult<string, string>>(maxConsumeResults);

            while (polledConsumeResults.Count < maxConsumeResults && !cancellationToken.IsCancellationRequested)
            {
                var remainingTimeSpan = pollDeadline - DateTime.UtcNow;
                if (remainingTimeSpan <= TimeSpan.Zero)
                {
                    break;
                }

                try
                {
                    var consumeResult = _consumer.Consume(remainingTimeSpan);
                    if (consumeResult is null)
                    {
                        break;
                    }

                    polledConsumeResults.Add(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "// {Class} // Consume exception during polling", GetType().Name);
                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            return new BatchProcessingContext
            {
                PolledConsumeResults = polledConsumeResults
            };
        }

        /// <summary>
        /// Awaits completion of in-flight processing tasks and periodically commits advancing,
        /// contiguous successes from the current batch. Commits only offsets that advance per partition,
        /// preserving the "contiguous only" rule to avoid gaps.
        /// </summary>
        /// <param name="batchProcessingContext">
        /// The batch context providing launched consume results and the successful next-offsets (offset + 1).
        /// </param>
        /// <param name="cancellationToken">
        /// Token used to observe shutdown requests and cancel waiting operations.
        /// </param>
        private async Task ContiguousCommitCompletedConsumingTasks(BatchProcessingContext batchProcessingContext, CancellationToken cancellationToken)
        {
            if (batchProcessingContext.ConsumeResultsForLaunchedTasks.Count == 0)
            {
                return;
            }

            int lastObservedSuccessCount = 0;
            var commitStopwatch = Stopwatch.StartNew();
            var tasksToAwait = batchProcessingContext.LaunchedProcessingTasks;
            var lastCommittedByPartition = new Dictionary<TopicPartition, long>();

            while (!cancellationToken.IsCancellationRequested && !_isShutdownInitiated)
            {
                if (tasksToAwait.Count > 0 && tasksToAwait.All(e => e.IsCompleted))
                {
                    CommitContiguousAdvancingOffsets(batchProcessingContext, lastCommittedByPartition);

                    break;
                }

                int successCount = batchProcessingContext.SuccessfulNextOffsets.Count;
                bool intervalReached = commitStopwatch.ElapsedMilliseconds >= _progressiveCommitIntervalMs;
                bool thresholdReached = successCount - lastObservedSuccessCount >= _progressiveCommitMinNewSuccesses;

                if (thresholdReached || intervalReached)
                {
                    CommitContiguousAdvancingOffsets(batchProcessingContext, lastCommittedByPartition);

                    lastObservedSuccessCount = successCount;

                    commitStopwatch.Restart();
                }

                try
                {
                    if (tasksToAwait.Count > 0)
                    {
                        await Task.WhenAny(tasksToAwait.Append(Task.Delay(_progressiveCommitIntervalMs, cancellationToken)));
                    }
                    else
                    {
                        await Task.Delay(_progressiveCommitIntervalMs, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            CommitContiguousAdvancingOffsets(batchProcessingContext, lastCommittedByPartition);

            await Task.Yield();
        }

        /// <summary>
        /// Commits advancing offsets derived from contiguous successes in the current batch.
        /// </summary>
        /// <param name="batchContext">
        /// The batch context providing launched consume results and the successful next-offsets (offset + 1).
        /// </param>
        /// <param name="lastCommittedByPartition">
        /// A map tracking the last committed next-offset per partition to avoid redundant commits.
        /// </param>
        private void CommitContiguousAdvancingOffsets(BatchProcessingContext batchContext, Dictionary<TopicPartition, long> lastCommittedByPartition)
        {
            if (batchContext is null || batchContext.SuccessfulNextOffsets.IsEmpty)
            {
                return;
            }

            var commitCandidates = ComputeContiguousCommitOffsets(batchContext);
            if (commitCandidates.Count == 0)
            {
                return;
            }

            var advancing = new List<TopicPartitionOffset>(commitCandidates.Count);
            foreach (var commitCandidate in commitCandidates)
            {
                var nextOffset = commitCandidate.Offset.Value;
                var topicPartition = commitCandidate.TopicPartition;

                if (lastCommittedByPartition.TryGetValue(topicPartition, out var lastOffset) && nextOffset <= lastOffset)
                {
                    continue;
                }

                advancing.Add(commitCandidate);
                lastCommittedByPartition[topicPartition] = nextOffset;
            }

            if (advancing.Count > 0)
            {
                CommitNormalizedOffsets(advancing);
            }
        }

        /// <summary>
        /// Launches processing tasks for the polled consume results in the provided batch context.
        /// </summary>
        /// <param name="batchContext">The batch context containing consume results polled for this iteration.</param>
        /// <param name="processMessageFunc">Function that processes a single message value extracted from a consume result </param>
        /// <param name="retryMessageFunc">Function that retries processing a single message value when the initial processing fails.</param>
        /// <param name="cancellationToken">Token used to observe shutdown requests and to cancel waiting on concurrency slots and task launches.</param>
        /// <returns>
        /// A <see cref="BatchProcessingContext"/> with:
        /// - <see cref="BatchProcessingContext.ConsumeResultsForLaunchedTasks"/> containing the consume results that had processing tasks started.
        /// - <see cref="BatchProcessingContext.SuccessfulNextOffsets"/> containing per-message next offsets (original offset + 1) for successfully processed consume results.
        /// </returns>
        private async Task<BatchProcessingContext> ProcessPolledConsumeResults(BatchProcessingContext batchContext, Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            Interlocked.Exchange(ref _failureFlag, 0);

            var successfulNextOffsets = new ConcurrentBag<TopicPartitionOffset>();
            var processingTasks = new List<Task>(batchContext.PolledConsumeResults.Count);
            var consumeResultsForLaunchedTasks = new List<ConsumeResult<string, string>>(batchContext.PolledConsumeResults.Count);

            foreach (var polledConsumeResult in batchContext.PolledConsumeResults)
            {
                await _processingConcurrencySemaphore.WaitAsync(cancellationToken);

                if (cancellationToken.IsCancellationRequested || HasFailed || _isShutdownInitiated)
                {
                    _processingConcurrencySemaphore.Release();

                    break;
                }

                async Task ProcessingWrapper()
                {
                    try
                    {
                        if (HasFailed)
                        {
                            return;
                        }

                        await processMessageFunc(polledConsumeResult.Message.Value);

                        _processedCounter.Add(1, KeyValuePair.Create<string, object?>("topic", _topicName));

                        successfulNextOffsets.Add(new TopicPartitionOffset(polledConsumeResult.TopicPartition, polledConsumeResult.Offset + 1));
                    }
                    catch (OperationCanceledException)
                    {
                        SignalFailure();
                    }
                    catch (Exception ex)
                    {
                        _failedCounter.Add(1, KeyValuePair.Create<string, object?>("topic", _topicName));

                        _logger.LogError(ex, "// {Class} // Error processing message at offset {Offset}, attempting retry", GetType().Name, polledConsumeResult.Offset);

                        try
                        {
                            await retryMessageFunc(polledConsumeResult.Message.Value);

                            _retriedSucceededCounter.Add(1, KeyValuePair.Create<string, object?>("topic", _topicName));

                            successfulNextOffsets.Add(new TopicPartitionOffset(polledConsumeResult.TopicPartition, polledConsumeResult.Offset + 1));
                        }
                        catch (Exception retryEx)
                        {
                            SignalFailure();

                            _retriedFailedCounter.Add(1, KeyValuePair.Create<string, object?>("topic", _topicName));

                            _logger.LogError(retryEx, "// {Class} // Retry failed for message at offset {Offset}. Halting further launches.", GetType().Name, polledConsumeResult.Offset);
                        }
                    }
                    finally
                    {
                        try
                        {
                            _processingConcurrencySemaphore.Release();
                        }
                        catch (SemaphoreFullException ex)
                        {
                            _logger.LogWarning(ex, "// {Class} // Semaphore already full when releasing for offset {Offset}", GetType().Name, polledConsumeResult.Offset);
                        }
                    }
                }

                var processingTask = ProcessingWrapper();

                processingTasks.Add(processingTask);
                consumeResultsForLaunchedTasks.Add(polledConsumeResult);
            }

            _currentBatchTasks = processingTasks;

            await Task.WhenAll(processingTasks);

            return batchContext with
            {
                LaunchedProcessingTasks = processingTasks,
                SuccessfulNextOffsets = successfulNextOffsets,
                ConsumeResultsForLaunchedTasks = consumeResultsForLaunchedTasks
            };
        }
    }
}
