using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Integrations.Interfaces;
using Altinn.Notifications.Email.Core.Models;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.Integrations.Producers;
using Altinn.Notifications.Email.IntegrationTests.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Moq;

using Xunit;

namespace Altinn.Notifications.Email.IntegrationTests.Integrations
{
    public class EmailOperationConsumerTests : IAsyncLifetime
    {
        private readonly string EmailSendingAcceptedTopicName = Guid.NewGuid().ToString();
        private readonly string EmailSendingAcceptedRetryTopicName = Guid.NewGuid().ToString();
        private ServiceProvider _serviceProvider;
        private readonly string _validTopicMessage = $"{{ \"NotificationId\": \"{Guid.NewGuid()}\", \"OperationId\" : \"operationId\" }}";

        public async Task InitializeAsync()
        {
            await Task.CompletedTask;
        }

        async Task IAsyncLifetime.DisposeAsync()
        {
            await KafkaUtil.DeleteTopicAsync(EmailSendingAcceptedTopicName);
            await KafkaUtil.DeleteTopicAsync(EmailSendingAcceptedRetryTopicName);
        }

        [Fact]
        public async Task ConsumeOperation_ValidOperation_ServiceCalledOnce()
        {
            // Arrange
            Mock<IEmailService> emailServiceMock = new();
            emailServiceMock.Setup(m => m.UpdateSendStatus(It.IsAny<SendNotificationOperationIdentifier>()));

            using EmailOperationConsumer sut = GetConsumer(emailServiceMock.Object);

            // Act
            await PopulateKafkaTopic(_validTopicMessage);

            await sut.StartAsync(CancellationToken.None);
            await Task.Delay(10000);
            await sut.StopAsync(CancellationToken.None);

            // Assert
            emailServiceMock.Verify(m => m.UpdateSendStatus(It.IsAny<SendNotificationOperationIdentifier>()), Times.Once);
        }


        [Fact]
        public async Task ConsumeOperation_DeserialisationFails_ServiceIsNotCalled()
        {
            // Arrange
            Mock<IEmailService> emailServiceMock = new();
            emailServiceMock.Setup(m => m.UpdateSendStatus(It.IsAny<SendNotificationOperationIdentifier>()));

            using EmailOperationConsumer sut = GetConsumer(emailServiceMock.Object);

            // Act
            await PopulateKafkaTopic("{\"key\":\"value\"}");

            await sut.StartAsync(CancellationToken.None);
            await Task.Delay(10000);
            await sut.StopAsync(CancellationToken.None);

            // Assert
            emailServiceMock.Verify(m => m.UpdateSendStatus(It.IsAny<SendNotificationOperationIdentifier>()), Times.Never);
        }

        private async Task PopulateKafkaTopic(string message)
        {
            using CommonProducer kafkaProducer = KafkaUtil.GetKafkaProducer(_serviceProvider);
            await kafkaProducer.ProduceAsync(EmailSendingAcceptedTopicName, message);
        }

        private EmailOperationConsumer GetConsumer(IEmailService? emailService = null)
        {
            if (emailService == null)
            {
                Mock<IEmailService> mock = new();
                mock.Setup(m => m.UpdateSendStatus(It.IsAny<SendNotificationOperationIdentifier>()));
                emailService = mock.Object;
            }

            var kafkaSettings = new KafkaSettings
            {
                BrokerAddress = "localhost:9092",
                Consumer = new()
                {
                    GroupId = "email-sending-consumer"
                },
                EmailSendingAcceptedTopicName = EmailSendingAcceptedTopicName,
                EmailSendingAcceptedRetryTopicName = EmailSendingAcceptedRetryTopicName,
                Admin = new()
                {
                    TopicList = new List<string> { EmailSendingAcceptedTopicName, EmailSendingAcceptedRetryTopicName }
                }
            };

            IServiceCollection services = new ServiceCollection()
                .AddLogging()
                .AddSingleton(kafkaSettings)
                .AddSingleton<ICommonProducer, CommonProducer>()
                .AddSingleton(emailService)
                .AddHostedService<EmailOperationConsumer>();

            _serviceProvider = services.BuildServiceProvider();

            var emailOperationConsumer = _serviceProvider.GetService(typeof(IHostedService)) as EmailOperationConsumer;

            if (emailOperationConsumer == null)
            {
                Assert.Fail("Unable to create an instance of EmailOperationConsumer.");
            }

            return emailOperationConsumer;
        }
    }
}
