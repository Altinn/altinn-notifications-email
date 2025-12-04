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
    public async Task GivenValidEmailMessage_WhenConsumed_ThenSendingServiceIsCalledOnce()
    {
        // Arrange
        var processedSignal = new ManualResetEventSlim(false);
        var loggerMock = new Mock<ILogger<SendEmailQueueConsumer>>();

        Mock<ISendingService> sendingServiceMock = new();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.IsAny<Core.Sending.Email>()))
            .Callback(processedSignal.Set)
            .Returns(Task.CompletedTask);

        Core.Sending.Email email =
            new(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

        using CommonProducer kafkaProducer = KafkaUtil.GetKafkaProducer(_serviceProvider!);
        using SendEmailQueueConsumer sendEmailQueueConsumer = GetEmailSendingConsumer(sendingServiceMock.Object, sendEmailQueueConsumerLogger: loggerMock.Object);

        // Act
        await sendEmailQueueConsumer.StartAsync(CancellationToken.None);
        await kafkaProducer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));

        // Wait until the service is called (or timeout)
        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        await sendEmailQueueConsumer.StopAsync(CancellationToken.None);

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

        var sendingServiceMock = new Mock<ISendingService>();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.IsAny<Core.Sending.Email>()))
            .Callback(processedSignal.Set)
            .Returns(Task.CompletedTask);

        using SendEmailQueueConsumer sendEmailQueueConsumer = GetEmailSendingConsumer(sendingServiceMock.Object, sendEmailQueueConsumerLogger: loggerMock.Object);
        using CommonProducer kafkaProducer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        // Act
        await sendEmailQueueConsumer.StartAsync(CancellationToken.None);
        await kafkaProducer.ProduceAsync(_emailSendingConsumerTopic, "Not an email");

        // Wait briefly ensuring the signal is never set
        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(50));
        await sendEmailQueueConsumer.StopAsync(CancellationToken.None);

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

        var sendingServiceMock = new Mock<ISendingService>();
        sendingServiceMock
            .Setup(e => e.SendAsync(It.IsAny<Core.Sending.Email>()))
            .Callback(processedSignal.Set)
            .Returns(Task.CompletedTask);

        using SendEmailQueueConsumer sendEmailQueueConsumer = GetEmailSendingConsumer(sendingServiceMock.Object, sendEmailQueueConsumerLogger: loggerMock.Object);
        using CommonProducer kafkaProducer = KafkaUtil.GetKafkaProducer(_serviceProvider!);

        var email = new Core.Sending.Email(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

        // Act
        await sendEmailQueueConsumer.StartAsync(CancellationToken.None);
        await kafkaProducer.ProduceAsync(_emailSendingConsumerTopic, JsonSerializer.Serialize(email));

        bool processed = await WaitForConditionAsync(() => processedSignal.IsSet, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
        await sendEmailQueueConsumer.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(processed, "Message produced to the configured topic was not consumed, implying missing subscription.");
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
