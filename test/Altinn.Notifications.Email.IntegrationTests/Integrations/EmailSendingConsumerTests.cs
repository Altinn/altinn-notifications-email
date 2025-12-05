using System.Diagnostics;
using System.Text.Json;

using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Sending;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.Integrations.Producers;
using Altinn.Notifications.Email.IntegrationTests.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

namespace Altinn.Notifications.Email.IntegrationTests.Integrations;

public class EmailSendingConsumerTests : IAsyncLifetime
{
    private readonly KafkaSettings _kafkaSettings;

    private readonly string _emailSendingConsumerTopic = Guid.NewGuid().ToString();
    private readonly string _emailSendingAcceptedProducerTopic = Guid.NewGuid().ToString();

    public EmailSendingConsumerTests()
    {
        _kafkaSettings = new KafkaSettings
        {
            BrokerAddress = "localhost:9092",
            Consumer = new()
            {
                GroupId = "email-sending-consumer"
            },
            SendEmailQueueTopicName = _emailSendingConsumerTopic,
            EmailSendingAcceptedTopicName = _emailSendingAcceptedProducerTopic,
            Admin = new()
            {
                TopicList = [_emailSendingConsumerTopic, _emailSendingAcceptedProducerTopic]
            }
        };
    }

    public async Task DisposeAsync()
    {
        await KafkaUtil.DeleteTopicAsync(_emailSendingConsumerTopic);
        await KafkaUtil.DeleteTopicAsync(_emailSendingAcceptedProducerTopic);
    }

    public async Task InitializeAsync()
    {
        await Task.CompletedTask;
    }

    [Fact]
    public async Task GivenValidEmailMessage_WhenConsumed_ThenSendingServiceIsCalledOnce()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);
        await using var testFixture = CreateTestFixture(sendingServiceMock.Object);

        var email = new Core.Sending.Email(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

        // Act
        await testFixture.Consumer.StartAsync(CancellationToken.None);
        await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));

        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        await testFixture.Consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(processed, "Email was not processed within the expected time window.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Once);
    }

    [Fact]
    public async Task GivenInvalidEmailMessage_WhenConsumed_ThenSendingServiceIsNotCalled()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);
        await using var testFixture = CreateTestFixture(sendingServiceMock.Object);

        // Act
        await testFixture.Consumer.StartAsync(CancellationToken.None);
        await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, "Not an email");

        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(50));
        await testFixture.Consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.False(processed, "Service should not be called when deserialization fails.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Never);
    }

    [Fact]
    public async Task GivenMultipleMessages_ThenProcessedConcurrently_WithinExpectedTimeframe()
    {
        var concurrentExecutions = 0;
        var processedMessagesCount = 0;
        var maxConcurrentExecutions = 0;
        var allMessagesProcessedSignal = new ManualResetEventSlim(false);

        var sendingServiceMock = new Mock<ISendingService>();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.IsAny<Core.Sending.Email>()))
            .Returns(async () =>
            {
                var currentConcurrent = Interlocked.Increment(ref concurrentExecutions);

                var currentMax = Volatile.Read(ref maxConcurrentExecutions);
                while (currentConcurrent > currentMax)
                {
                    var originalMax = Interlocked.CompareExchange(ref maxConcurrentExecutions, currentConcurrent, currentMax);
                    if (originalMax == currentMax)
                    {
                        break;
                    }

                    currentMax = Volatile.Read(ref maxConcurrentExecutions);
                }

                await Task.Delay(250); // Simulated email processing.

                Interlocked.Decrement(ref concurrentExecutions);

                if (Interlocked.Increment(ref processedMessagesCount) >= 50)
                {
                    allMessagesProcessedSignal.Set();
                }
            });

        await using var testFixture = CreateTestFixture(sendingServiceMock.Object);

        var emails = Enumerable.Range(0, 50)
            .Select(i => new Core.Sending.Email(Guid.NewGuid(), $"subject-{i}", $"body-{i}", "from", "to", EmailContentType.Plain))
            .ToList();

        // Act
        await testFixture.Consumer.StartAsync(CancellationToken.None);

        foreach (var email in emails)
        {
            await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));
        }

        var isProcessed = await WaitForConditionAsync(() => allMessagesProcessedSignal.IsSet, TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(25));

        await testFixture.Consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(isProcessed, "All messages were not processed within expected time.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Exactly(50));
        Assert.True(maxConcurrentExecutions > 1, $"Expected concurrent execution, but max concurrent was only {maxConcurrentExecutions}");
    }

    [Fact]
    public async Task GivenStartedConsumer_WhenMessageProduced_ThenConfiguredTopicIsSubscribed()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);
        await using var testFixture = CreateTestFixture(sendingServiceMock.Object);

        var email = new Core.Sending.Email(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

        // Act
        await testFixture.Consumer.StartAsync(CancellationToken.None);
        await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));

        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        await testFixture.Consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(processed, "Message produced to the configured topic was not consumed, implying missing subscription.");
    }

    [Fact]
    public async Task GivenActiveConsumerProcessingMessages_WhenStopAsyncCalled_ThenStopCompletesPromptly()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);
        await using var testFixture = CreateTestFixture(sendingServiceMock.Object);

        var email = new Core.Sending.Email(Guid.NewGuid(), "subject-1", "body-1", "from-1", "to-1", EmailContentType.Plain);

        // Act
        await testFixture.Consumer.StartAsync(CancellationToken.None);
        await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));
        var isProcessed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));

        var stopwatch = Stopwatch.StartNew();
        using var stopTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await testFixture.Consumer.StopAsync(stopTimeout.Token);
        stopwatch.Stop();

        // Assert
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Once);
        Assert.True(isProcessed, "First email was not processed within the expected time window");
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(2), "StopAsync took too long, suggesting internal cancellation was not signaled.");
    }

    [Fact]
    public async Task GivenShutdown_ThenLastBatchSafeOffsetsCommittedOnce_AndPendingMessageProcessedAfterRestart()
    {
        // Arrange
        var firstEmailNotificationIdentifer = Guid.NewGuid();
        var firstProcessedSignal = new ManualResetEventSlim(false);

        var secondEmailNotificationIdentifer = Guid.NewGuid();
        var secondProcessedSignal = new ManualResetEventSlim(false);

        var allowSecondProcessing = new SemaphoreSlim(0, 1);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();

        var sendingServiceMock = new Mock<ISendingService>();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.Is<Core.Sending.Email>(e => e.NotificationId == firstEmailNotificationIdentifer)))
            .Callback(firstProcessedSignal.Set)
            .Returns(Task.CompletedTask);

        sendingServiceMock
            .Setup(e => e.SendAsync(It.Is<Core.Sending.Email>(e => e.NotificationId == secondEmailNotificationIdentifer)))
            .Callback(async () =>
            {
                await allowSecondProcessing.WaitAsync(TimeSpan.FromSeconds(10));

                secondProcessedSignal.Set();
            })
            .Returns(Task.CompletedTask);

        await using var firstTestFixture = CreateTestFixture(sendingServiceMock.Object, loggerMock.Object);

        var firstEmail = new Core.Sending.Email(firstEmailNotificationIdentifer, "first", "body-1", "from", "to", EmailContentType.Plain);
        var secondEmail = new Core.Sending.Email(secondEmailNotificationIdentifer, "second", "body-2", "from", "to", EmailContentType.Plain);

        // Act
        await firstTestFixture.Consumer.StartAsync(CancellationToken.None);

        await firstTestFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(firstEmail));
        await firstTestFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(secondEmail));

        var firstProcessed = await WaitForConditionAsync(() => firstProcessedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));

        await firstTestFixture.Consumer.StopAsync(CancellationToken.None);

        loggerMock.Verify(
           e => e.Log(
                It.Is<LogLevel>(e => e == LogLevel.Information),
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
           Times.AtLeastOnce);

        allowSecondProcessing.Release();

        await using var secondTestFixture = CreateTestFixture(sendingServiceMock.Object);

        await secondTestFixture.Consumer.StartAsync(CancellationToken.None);

        var secondProcessed = await WaitForConditionAsync(() => secondProcessedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));

        await secondTestFixture.Consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(firstProcessed, "First email was not processed within the expected time window.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Exactly(2));
        sendingServiceMock.Verify(e => e.SendAsync(It.Is<Core.Sending.Email>(m => m.Subject == "first")), Times.Once);
        sendingServiceMock.Verify(e => e.SendAsync(It.Is<Core.Sending.Email>(m => m.Subject == "second")), Times.Once);
        Assert.True(secondProcessed, "Second email was not processed after restart, indicating offsets may have been committed beyond the contiguous boundary.");

    }

    [Fact]
    public async Task GivenShutdownInitiated_ThenNoFurtherMessagesAreProcessed_IncludingMessagesProducedDuringStop()
    {
        // Arrange
        var firstProcessedSignal = new ManualResetEventSlim(false);
        var sendingServiceMock = CreateSendingServiceMock(firstProcessedSignal);
        await using var testFixture = CreateTestFixture(sendingServiceMock.Object);

        var firstEmail = new Core.Sending.Email(Guid.NewGuid(), "first", "body-1", "from-1", "to-1", EmailContentType.Plain);
        var afterStopEmail = new Core.Sending.Email(Guid.NewGuid(), "after", "body-3", "from-3", "to-3", EmailContentType.Plain);
        var duringShutdownEmail = new Core.Sending.Email(Guid.NewGuid(), "during", "body-2", "from-2", "to-2", EmailContentType.Plain);

        // Act
        await testFixture.Consumer.StartAsync(CancellationToken.None);
        await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(firstEmail));
        var isFirstProcessed = await WaitForConditionAsync(() => firstProcessedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));

        var stopTask = testFixture.Consumer.StopAsync(CancellationToken.None);
        await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(duringShutdownEmail));
        await stopTask;

        await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(afterStopEmail));
        await Task.Delay(TimeSpan.FromSeconds(1));

        // Assert
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Once);
        Assert.True(isFirstProcessed, "First email was not processed within the expected time window.");
    }

    [Fact]
    public async Task GivenMoreThanMaxBatchSizeMessages_ThenAtLeastMaxBatchAreProcessedInFirstBatch_RemainderInNextBatch()
    {
        // Arrange
        var processedCount = 0;
        var reached100Signal = new ManualResetEventSlim(false);
        var allProcessedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();

        var sendingServiceMock = new Mock<ISendingService>();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.IsAny<Core.Sending.Email>()))
            .Returns(async () =>
            {
                var current = Interlocked.Increment(ref processedCount);
                await Task.Delay(5);

                if (current == 100)
                {
                    reached100Signal.Set();
                }

                if (current == 150)
                {
                    allProcessedSignal.Set();
                }
            });

        await using var testFixture = CreateTestFixture(sendingServiceMock.Object);

        // Produce 150 messages; base max batch size is 100
        var emails = Enumerable.Range(0, 150)
            .Select(i => new Core.Sending.Email(Guid.NewGuid(), $"s-{i}", $"b-{i}", "from", "to", EmailContentType.Plain))
            .ToList();

        // Act
        await testFixture.Consumer.StartAsync(CancellationToken.None);

        foreach (var email in emails)
        {
            await testFixture.Producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));
        }

        var processedFirst100 = await WaitForConditionAsync(() => reached100Signal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        var processedAll = await WaitForConditionAsync(() => allProcessedSignal.IsSet, TimeSpan.FromSeconds(8), TimeSpan.FromMilliseconds(50));

        await testFixture.Consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(processedFirst100, "Did not process at least the first batch of 100 messages promptly.");
        Assert.True(processedAll, "Remaining messages from the next batch were not processed within the expected window.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Exactly(150));
    }

    /// <summary>
    /// Creates and configures a <see cref="SendEmailQueueConsumer"/> instance from the provided <see cref="ServiceProvider"/>.
    /// </summary>
    /// <param name="serviceProvider">
    /// The <see cref="ServiceProvider"/> containing the required services for the consumer.
    /// </param>
    /// <returns>
    /// A fully constructed <see cref="SendEmailQueueConsumer"/> registered as an <see cref="IHostedService"/>.
    /// </returns>
    /// <exception cref="Xunit.Sdk.XunitException">
    /// Thrown when the consumer instance cannot be resolved from the service provider.
    /// </exception>
    private static SendEmailQueueConsumer GetEmailSendingConsumer(ServiceProvider serviceProvider)
    {
        var emailSendingConsumer = serviceProvider.GetService(typeof(IHostedService)) as SendEmailQueueConsumer;

        if (emailSendingConsumer == null)
        {
            Assert.Fail("Unable to create an instance of EmailSendingConsumer.");
        }

        return emailSendingConsumer;
    }

    /// <summary>
    /// Creates a mocked <see cref="ISendingService"/> that signals a provided <see cref="ManualResetEventSlim"/>
    /// when <see cref="ISendingService.SendAsync(Core.Sending.Email)"/> is invoked.
    /// </summary>
    /// <param name="processedSignal">
    /// The synchronization primitive to set when the mock's <c>SendAsync</c> method is called,
    /// allowing tests to await message processing completion without fixed delays.
    /// </param>
    /// <returns>
    /// A configured <see cref="Mock{T}"/> of <see cref="ISendingService"/> whose <c>SendAsync</c> completes immediately
    /// and triggers <paramref name="processedSignal"/> via its callback.
    /// </returns>
    private static Mock<ISendingService> CreateSendingServiceMock(ManualResetEventSlim processedSignal)
    {
        var sendingServiceMock = new Mock<ISendingService>();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.IsAny<Core.Sending.Email>()))
            .Callback(() => processedSignal.Set())
            .Returns(Task.CompletedTask);
        return sendingServiceMock;
    }

    /// <summary>
    /// Polls a boolean condition until it becomes <c>true</c> or a timeout elapses.
    /// </summary>
    /// <param name="condition">A function returning the current state to evaluate.</param>
    /// <param name="timeout">The maximum time to wait for the condition to become <c>true</c>.</param>
    /// <param name="pollInterval">The interval between successive evaluations of <paramref name="condition"/>.</param>
    /// <returns>
    /// A task that completes with <c>true</c> if the condition became <c>true</c> before the timeout; otherwise <c>false</c>.
    /// </returns>
    /// <remarks>
    /// This helper avoids fixed delays in tests by polling frequently and returning as soon as the condition is met.
    /// </remarks>
    private static async Task<bool> WaitForConditionAsync(Func<bool> condition, TimeSpan timeout, TimeSpan pollInterval)
    {
        var stopwatch = Stopwatch.StartNew();

        while (stopwatch.Elapsed < timeout)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(pollInterval);
        }

        return false;
    }

    /// <summary>
    /// Creates a fully configured <see cref="EmailConsumerTestFixture"/> for integration testing of the email consumer.
    /// </summary>
    /// <param name="sendingService">
    /// The <see cref="ISendingService"/> implementation to be used for email processing.
    /// </param>
    /// <param name="logger">
    /// Optional <see cref="ILogger{T}"/> for <see cref="SendEmailQueueConsumer"/> to capture or control log output during testing.
    /// </param>
    /// <returns>
    /// A configured <see cref="EmailConsumerTestFixture"/> containing:
    /// <list type="bullet">
    /// <item><description>A <see cref="SendEmailQueueConsumer"/> ready for testing</description></item>
    /// <item><description>A <see cref="CommonProducer"/> for sending test messages to Kafka topics</description></item>
    /// <item><description>A <see cref="ServiceProvider"/> with all required dependencies</description></item>
    /// </list>
    /// The fixture implements <see cref="IAsyncDisposable"/> and must be disposed properly to clean up resources.
    /// </returns>
    private EmailConsumerTestFixture CreateTestFixture(ISendingService sendingService, ILogger<SendEmailQueueConsumer>? logger = null)
    {
        var serviceProvider = CreateServiceProvider(sendingService, logger);
        var consumer = GetEmailSendingConsumer(serviceProvider);
        var producer = KafkaUtil.GetKafkaProducer(serviceProvider);

        return new EmailConsumerTestFixture(producer, consumer, serviceProvider);
    }

    /// <summary>
    /// Creates a configured <see cref="ServiceProvider"/> for integration tests.
    /// </summary>
    /// <param name="sendingService">
    /// The <see cref="ISendingService"/> to be injected into the consumer, typically a mocked implementation.
    /// </param>
    /// <param name="sendEmailQueueConsumerLogger">
    /// Optional typed <see cref="ILogger{T}"/> for <see cref="SendEmailQueueConsumer"/> to capture or control logs emitted by the consumer.
    /// </param>
    /// <returns>
    /// A configured <see cref="ServiceProvider"/> with all necessary services registered for testing.
    /// </returns>
    private ServiceProvider CreateServiceProvider(ISendingService sendingService, ILogger<SendEmailQueueConsumer>? sendEmailQueueConsumerLogger = null)
    {
        IServiceCollection services = new ServiceCollection()
           .AddLogging()
           .AddSingleton(_kafkaSettings)
           .AddSingleton(sendingService)
           .AddHostedService<SendEmailQueueConsumer>()
           .AddSingleton<ICommonProducer, CommonProducer>();

        if (sendEmailQueueConsumerLogger != null)
        {
            services.AddSingleton(sendEmailQueueConsumerLogger);
        }

        return services.BuildServiceProvider();
    }
}
