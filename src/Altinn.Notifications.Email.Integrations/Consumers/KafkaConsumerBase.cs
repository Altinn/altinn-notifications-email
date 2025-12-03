using System.Collections.Concurrent;

using Altinn.Notifications.Email.Integrations.Configuration;

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
        private readonly string _topicName;
        private volatile bool _isShutdownInitiated;
        private readonly ILogger<KafkaConsumerBase> _logger;
        private readonly IConsumer<string, string> _consumer;

        private readonly int _maxMessagesPerBatch = 50;
        private readonly int _messagesBatchPollTimeoutMs = 100;
        private readonly int _maxConcurrentProcessingTasks = 50;

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
                _processingConcurrencySemaphore.Dispose();
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
                    await Task.WhenAll(processingTasks).WaitAsync(TimeSpan.FromSeconds(15));
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
        /// Consumes messages from the Kafka topic in batches, launching up to a bounded number of concurrent processors.
        /// </summary>
        /// <param name="processMessageFunc">Function to process a single message.</param>
        /// <param name="retryMessageFunc">Function to retry a message if processing fails.</param>
        /// <param name="cancellationToken">Cancellation token to observe for shutdown.</param>
        protected async Task ConsumeMessage(Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !_isShutdownInitiated)
            {
                try
                {
                    var messageBatch = FetchMessageBatch(_maxMessagesPerBatch, _messagesBatchPollTimeoutMs, cancellationToken);
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

                    var (launchedMessages, successfulNextOffsets, failureDetected) = await LaunchBatchProcessing(messageBatch, processMessageFunc, retryMessageFunc, cancellationToken);

                    await AwaitLaunchedTasksAndCommit(launchedMessages, successfulNextOffsets);

                    _logger.LogInformation(
                        "// KafkaConsumerBase // ConsumeMessage // Batch completed for topic {Topic}. Launched={Launched} Polled={Polled} FailureDetected={Failure} Duration={Duration:F0}ms",
                        _topicName,
                        launchedMessages.Count,
                        messageBatch.Length,
                        Volatile.Read(ref failureDetected) == 1,
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
        /// Safely commits normalized offsets to Kafka with built-in resilience against transient consumer group states.
        /// This method performs offset normalization by selecting the highest offset per partition, handles rebalancing
        /// scenarios gracefully, and provides comprehensive logging for commit operations. The method is designed to
        /// be fault-tolerant and will skip commits during unsafe states rather than failing the entire consumer operation.
        /// </summary>
        /// <param name="offsets">
        /// A collection of TopicPartitionOffset values representing the offsets to commit to Kafka. These offsets
        /// typically represent the "next offset" position (message offset + 1) for successfully processed messages.
        /// The method will normalize multiple offsets for the same partition by keeping only the highest offset value.
        /// Can be null or empty, in which case the method returns immediately without attempting any commit operations.
        /// </param>
        protected virtual void SafeCommit(IEnumerable<TopicPartitionOffset> offsets)
        {
            if (_isShutdownInitiated || offsets is null)
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
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            _logger.LogInformation("// {Class} // Fetched {Count} messages from Kafka in batch", GetType().Name, polledMessages.Count);

            return [.. polledMessages];
        }

        /// <summary>
        /// Launches processing tasks for a batch of messages while honoring bounded concurrency and fail-fast semantics.
        /// This method processes messages concurrently up to the configured limit, but stops launching new tasks
        /// immediately after the first failure (including retry failures). Already-started tasks are allowed to complete.
        /// Returns the launched messages, the set of successful "next offsets" (offset + 1), and an interlocked failure flag (0/1).
        /// </summary>
        /// <param name="messageBatch">The batch of Kafka messages to process, in the order they were consumed from the topic.</param>
        /// <param name="processMessageFunc">A function that processes a single message value. Should be idempotent and handle business logic exceptions gracefully.</param>
        /// <param name="retryMessageFunc">A function that retries processing a single message value when the initial processing fails. Called once per failed message.</param>
        /// <param name="cancellationToken">A cancellation token used to observe shutdown requests and coordinate graceful termination of processing tasks.</param>
        /// <returns>
        /// A tuple containing:
        /// - LaunchedMessages: The subset of messages from the batch that were actually launched for processing (may be less than the full batch if fail-fast was triggered).
        /// - SuccessfulNextOffsets: A thread-safe collection of TopicPartitionOffset values representing the next offset to commit for successfully processed messages (original offset + 1).
        /// - FailureDetectedFlag: An interlocked integer flag (0 = no failures, 1 = failure detected) used to coordinate fail-fast behavior across concurrent tasks.
        /// </returns>
        private async Task<(List<ConsumeResult<string, string>> LaunchedMessages, ConcurrentBag<TopicPartitionOffset> SuccessfulNextOffsets, int FailureDetectedFlag)> LaunchBatchProcessing(
            ConsumeResult<string, string>[] messageBatch,
            Func<string, Task> processMessageFunc,
            Func<string, Task> retryMessageFunc,
            CancellationToken cancellationToken)
        {
            var failureDetectedFlag = 0;
            var launchedTasks = new List<Task>(messageBatch.Length);
            var successfulNextOffsets = new ConcurrentBag<TopicPartitionOffset>();
            var launchedMessages = new List<ConsumeResult<string, string>>(messageBatch.Length);

            foreach (var message in messageBatch)
            {
                if (Volatile.Read(ref failureDetectedFlag) == 1 || cancellationToken.IsCancellationRequested || _isShutdownInitiated)
                {
                    break;
                }

                // Wait for concurrency slot - this will throw on cancellation
                await _processingConcurrencySemaphore.WaitAsync(cancellationToken);

                launchedMessages.Add(message);
                var processingTaskId = Guid.NewGuid();

                _logger.LogInformation("// {Class} // Start processing message at offset {Offset}", GetType().Name, message.Offset);

                // Capture local references for the task closure
                var capturedMessage = message;
                var capturedProcessingTaskIdId = processingTaskId;

                var processingTask = Task.Run(
                    async () =>
                    {
                        try
                        {
                            if (Volatile.Read(ref failureDetectedFlag) == 1)
                            {
                                return;
                            }

                            await processMessageFunc(capturedMessage.Message.Value).ConfigureAwait(false);

                            successfulNextOffsets.Add(new TopicPartitionOffset(capturedMessage.TopicPartition, capturedMessage.Offset + 1));
                        }
                        catch (OperationCanceledException)
                        {
                            Interlocked.Exchange(ref failureDetectedFlag, 1);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "// {Class} // Error processing message at offset {Offset}, attempting retry", GetType().Name, capturedMessage.Offset);

                            try
                            {
                                await retryMessageFunc(capturedMessage.Message.Value).ConfigureAwait(false);

                                successfulNextOffsets.Add(new TopicPartitionOffset(capturedMessage.TopicPartition, capturedMessage.Offset + 1));
                            }
                            catch (Exception retryEx)
                            {
                                _logger.LogError(retryEx, "// {Class} // Retry failed for message at offset {Offset}. Halting further launches.", GetType().Name, capturedMessage.Offset);
                                Interlocked.Exchange(ref failureDetectedFlag, 1);
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
                                _logger.LogError(ex, "// {Class} // Semaphore already full when releasing for offset {Offset}", GetType().Name, capturedMessage.Offset);
                            }

                            _inFlightTasks.TryRemove(capturedProcessingTaskIdId, out _);
                        }
                    },
                    cancellationToken);

                _inFlightTasks[processingTaskId] = processingTask;
                launchedTasks.Add(processingTask);
            }

            try
            {
                await Task.WhenAll(launchedTasks).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "// {Class} // Aggregate completion contained failures", GetType().Name);
            }

            return (launchedMessages, successfulNextOffsets, failureDetectedFlag);
        }

        /// <summary>
        /// Finalizes batch processing by computing contiguous commit candidates from successful message processing results
        /// and safely committing them to Kafka. This method ensures that only contiguous sequences of successfully processed
        /// messages are committed, preventing gaps in the offset progression that could lead to message reprocessing.
        /// Also provides comprehensive logging of processing results, commit decisions, and potential issues.
        /// </summary>
        /// <param name="launchedMessages">
        /// The complete list of messages that were launched for processing in this batch, in the order they were launched.
        /// Used to determine the contiguous sequence boundaries for safe offset commits.
        /// </param>
        /// <param name="successfulNextOffsets">
        /// A thread-safe collection containing the "next offset" (original offset + 1) for each message that was
        /// successfully processed (including successful retries). These represent the offsets that are safe to commit to Kafka.
        /// </param>
        /// <returns>
        /// A task that completes when all commit operations and logging have finished. The task includes a yield
        /// operation to allow tracing systems to flush their buffers.
        /// </returns>
        private async Task AwaitLaunchedTasksAndCommit(List<ConsumeResult<string, string>> launchedMessages, ConcurrentBag<TopicPartitionOffset> successfulNextOffsets)
        {
            if (!successfulNextOffsets.IsEmpty)
            {
                var commitCandidates = ComputeContiguousCommitOffsets([.. launchedMessages], successfulNextOffsets);

                if (commitCandidates.Count != 0)
                {
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

            await Task.Yield();
        }

        /// <summary>
        /// Computes per-partition commit offsets by determining the largest contiguous sequence of successfully processed 
        /// messages from the earliest offset in each partition within the launched batch. This algorithm ensures that
        /// Kafka offset commits maintain strict ordering guarantees and prevent message gaps that could lead to 
        /// reprocessing issues or message loss scenarios in distributed consumer groups.
        /// </summary>
        /// <param name="launchedBatch">
        /// An array of Kafka consume results representing the subset of messages that were actually launched for processing 
        /// in this batch. Messages may be in arbitrary order but represent consecutive offsets from the original poll operation.
        /// This parameter is used to establish the ordering baseline and determine partition-specific commit boundaries.
        /// </param>
        /// <param name="successfulOffsets">
        /// A collection of TopicPartitionOffset values representing the "next offset" (original message offset + 1) for 
        /// each message that completed processing successfully, including those that succeeded after retry attempts.
        /// These offsets use Kafka's commit semantics where committing offset N means "I have processed up to and including offset N-1".
        /// </param>
        /// <returns>
        /// A list of TopicPartitionOffset values that are safe to commit to Kafka, representing the highest contiguous
        /// offset that can be committed for each partition without creating gaps. Returns an empty list if no contiguous
        /// sequences can be established (e.g., if the first message in any partition failed).
        /// </returns>
        private static List<TopicPartitionOffset> ComputeContiguousCommitOffsets(ConsumeResult<string, string>[] launchedBatch, IEnumerable<TopicPartitionOffset> successfulOffsets)
        {
            var commitOffsets = new List<TopicPartitionOffset>();

            // For each partition, get the ordered list of launched offsets.
            var batchByTopicPartition = launchedBatch.GroupBy(e => e.TopicPartition).ToDictionary(e => e.Key, e => e.Select(e => e.Offset.Value).OrderBy(x => x).ToList());

            // Map successes by TopicPartition to a set of next-offset values (since commits use next-offset)
            var successesByTopicPartition = successfulOffsets.GroupBy(e => e.TopicPartition).ToDictionary(e => e.Key, e => new HashSet<long>(e.Select(s => s.Offset.Value)));

            foreach (var kvp in batchByTopicPartition)
            {
                var topicPartition = kvp.Key;
                var orderedOffsets = kvp.Value;

                if (!successesByTopicPartition.TryGetValue(topicPartition, out var successSet) || successSet.Count == 0)
                {
                    continue;
                }

                long? lastContiguousNext = null;

                // Find the largest prefix of the ordered launched offsets where each offset's (offset + 1) is in successSet.
                foreach (var offset in orderedOffsets)
                {
                    var nextPosition = offset + 1;

                    if (successSet.Contains(nextPosition))
                    {
                        lastContiguousNext = nextPosition;
                    }
                    else
                    {
                        // Gap encountered — we cannot commit past this point.
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
}
