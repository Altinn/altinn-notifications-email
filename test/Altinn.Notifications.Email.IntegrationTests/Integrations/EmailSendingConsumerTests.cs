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
    private ServiceProvider? _serviceProvider;
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
    public async Task GivenConsumerIsStopping_ThenStopCompletesPromptly()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);

        using SendEmailQueueConsumer consumer = GetEmailSendingConsumer(sendingServiceMock.Object, loggerMock.Object);
        using CommonProducer producer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        var firstEmail = new Core.Sending.Email(Guid.NewGuid(), "subject-1", "body-1", "from-1", "to-1", EmailContentType.Plain);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(firstEmail));
        Assert.True(
            await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50)),
            "First email was not processed within the expected time window.");

        var stopwatch = Stopwatch.StartNew();
        using var stopTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await consumer.StopAsync(stopTimeout.Token);
        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(2), "StopAsync took too long, suggesting internal cancellation was not signaled.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Once);
    }

    [Fact]
    public async Task GivenValidEmailMessage_WhenConsumed_ThenSendingServiceIsCalledOnce()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);

        using SendEmailQueueConsumer consumer = GetEmailSendingConsumer(sendingServiceMock.Object, loggerMock.Object);
        using CommonProducer producer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        var email = new Core.Sending.Email(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));

        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(processed, "Email was not processed within the expected time window.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Once);
    }

    [Fact]
    public async Task GivenInvalidEmailMessage_WhenConsumed_ThenSendingServiceIsNeverCalled()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);

        using SendEmailQueueConsumer consumer = GetEmailSendingConsumer(sendingServiceMock.Object, loggerMock.Object);
        using CommonProducer producer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await producer.ProduceAsync(_emailSendingConsumerTopic, "Not an email");

        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(50));
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.False(processed, "Service should not be called when deserialization fails.");
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Never);
    }

    [Fact]
    public async Task GivenStartedConsumer_WhenMessageProduced_ThenConfiguredTopicIsSubscribed()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();
        var sendingServiceMock = CreateSendingServiceMock(processedSignal);

        using SendEmailQueueConsumer consumer = GetEmailSendingConsumer(sendingServiceMock.Object, loggerMock.Object);
        using CommonProducer producer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        var email = new Core.Sending.Email(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));

        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(processed, "Message produced to the configured topic was not consumed, implying missing subscription.");
    }

    [Fact]
    public async Task GivenShutdown_ThenLastBatchSafeOffsetsCommittedOnce_AndPendingMessageProcessedAfterRestart()
    {
        // Arrange
        var firstProcessedSignal = new ManualResetEventSlim(false);
        var allowSecondProcessing = new ManualResetEventSlim(false);
        var secondProcessedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();

        var sendingServiceMock = new Mock<ISendingService>();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.Is<Core.Sending.Email>(m => m.Subject == "first")))
            .Callback(firstProcessedSignal.Set)
            .Returns(Task.CompletedTask);

        sendingServiceMock
            .Setup(e => e.SendAsync(It.Is<Core.Sending.Email>(m => m.Subject == "second")))
            .Callback(() =>
            {
                if (!allowSecondProcessing.IsSet)
                {
                    allowSecondProcessing.Wait(TimeSpan.FromSeconds(10));
                }

                secondProcessedSignal.Set();
            })
            .Returns(Task.CompletedTask);

        using SendEmailQueueConsumer consumer1 = GetEmailSendingConsumer(sendingServiceMock.Object, loggerMock.Object);
        using CommonProducer producer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        var firstEmail = new Core.Sending.Email(Guid.NewGuid(), "first", "body-1", "from", "to", EmailContentType.Plain);
        var secondEmail = new Core.Sending.Email(Guid.NewGuid(), "second", "body-2", "from", "to", EmailContentType.Plain);

        // Act: start and produce two messages
        await consumer1.StartAsync(CancellationToken.None);
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(firstEmail));
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(secondEmail));

        Assert.True(
            await WaitForConditionAsync(() => firstProcessedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50)),
            "First email was not processed within the expected time window.");

        await consumer1.StopAsync(CancellationToken.None);

        // Verify the shutdown commit log appeared exactly once
        loggerMock.Verify(
            l => l.Log(
                It.Is<LogLevel>(e => e == LogLevel.Information),
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>(state => state.ToString()!.Contains("Committed last batch safe offsets for processed messages during shutdown")),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);

        // Restart and process pending second message
        allowSecondProcessing.Set();

        using SendEmailQueueConsumer consumer2 = GetEmailSendingConsumer(sendingServiceMock.Object, loggerMock.Object);
        await consumer2.StartAsync(CancellationToken.None);

        Assert.True(
            await WaitForConditionAsync(() => secondProcessedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50)),
            "Second email was not processed after restart, indicating offsets may have been committed beyond the contiguous boundary.");

        await consumer2.StopAsync(CancellationToken.None);

        // Assert
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Exactly(2));
        sendingServiceMock.Verify(e => e.SendAsync(It.Is<Core.Sending.Email>(m => m.Subject == "first")), Times.Once);
        sendingServiceMock.Verify(e => e.SendAsync(It.Is<Core.Sending.Email>(m => m.Subject == "second")), Times.Once);
    }

    [Fact]
    public async Task GivenShutdownInitiated_ThenNoFurtherMessagesAreProcessed_IncludingMessagesProducedDuringStop()
    {
        // Arrange
        var firstProcessedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();
        var sendingServiceMock = CreateSendingServiceMock(firstProcessedSignal);

        using SendEmailQueueConsumer consumer = GetEmailSendingConsumer(sendingServiceMock.Object, loggerMock.Object);
        using CommonProducer producer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        var firstEmail = new Core.Sending.Email(Guid.NewGuid(), "first", "body-1", "from-1", "to-1", EmailContentType.Plain);
        var afterStopEmail = new Core.Sending.Email(Guid.NewGuid(), "after", "body-3", "from-3", "to-3", EmailContentType.Plain);
        var duringShutdownEmail = new Core.Sending.Email(Guid.NewGuid(), "during", "body-2", "from-2", "to-2", EmailContentType.Plain);

        // Act: process first
        await consumer.StartAsync(CancellationToken.None);
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(firstEmail));
        Assert.True(
            await WaitForConditionAsync(() => firstProcessedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50)),
            "First email was not processed within the expected time window.");

        // Initiate shutdown and produce during shutdown
        var stopTask = consumer.StopAsync(CancellationToken.None);
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(duringShutdownEmail));
        await stopTask;

        // Produce after stop
        await producer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(afterStopEmail));
        await Task.Delay(TimeSpan.FromSeconds(1));

        // Assert: only first was processed
        sendingServiceMock.Verify(e => e.SendAsync(It.IsAny<Core.Sending.Email>()), Times.Once);
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
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
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
    /// Creates and configures a <see cref="SendEmailQueueConsumer"/> instance for integration tests.
    /// </summary>
    /// <param name="sendingService">
    /// The <see cref="ISendingService"/> to be injected into the consumer, typically a mocked implementation.
    /// </param>
    /// <param name="sendEmailQueueConsumerLogger">
    /// Optional typed <see cref="ILogger{T}"/> for <see cref="SendEmailQueueConsumer"/> to capture or control logs emitted by the derived consumer.
    /// </param>
    /// <returns>
    /// A fully constructed <see cref="SendEmailQueueConsumer"/> registered as an <see cref="IHostedService"/>.
    /// </returns>
    /// <exception cref="Xunit.Sdk.XunitException">
    /// Thrown when the consumer instance cannot be resolved from the service provider.
    /// </exception>
    private SendEmailQueueConsumer GetEmailSendingConsumer(ISendingService sendingService, ILogger<SendEmailQueueConsumer> sendEmailQueueConsumerLogger)
    {
        IServiceCollection services = new ServiceCollection()
           .AddLogging()
           .AddSingleton(_kafkaSettings)
           .AddSingleton(sendingService)
           .AddHostedService<SendEmailQueueConsumer>()
           .AddSingleton(sendEmailQueueConsumerLogger)
           .AddSingleton<ICommonProducer, CommonProducer>();

        _serviceProvider = services.BuildServiceProvider();

        var emailSendingConsumer = _serviceProvider.GetService(typeof(IHostedService)) as SendEmailQueueConsumer;

        if (emailSendingConsumer == null)
        {
            Assert.Fail("Unable to create an instance of EmailSendingConsumer.");
        }

        return emailSendingConsumer;
    }
}
