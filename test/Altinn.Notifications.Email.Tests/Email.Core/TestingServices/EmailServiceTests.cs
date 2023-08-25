using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Configuration;
using Altinn.Notifications.Email.Core.Enums;
using Altinn.Notifications.Email.Core.Integrations.Interfaces;
using Altinn.Notifications.Email.Core.Models;

using Moq;

using Xunit;

namespace Altinn.Notifications.Email.Tests.Email.Core.TestingServices
{
    public class EmailServiceTests
    {
        private readonly TopicSettings _topicSettings;

        public EmailServiceTests()
        {
            _topicSettings = new()
            {
                EmailOperationResultTopicName = "EmailOperationResultTopic",
                EmailSendingAcceptedTopicName = "EmailSendingAcceptedTopic",
                EmailSendingAcceptedRetryTopicName = "EmailSendingAcceptedRetryTopic"
            };
        }

        [Fact]
        public async Task SendAsync_OperationIdentifierGenerated_PublishedToExpectedKafkaTopic()
        {
            // Arrange
            Guid id = Guid.NewGuid();
            Notifications.Email.Core.Models.Email email =
            new(id, "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

            Mock<IEmailServiceClient> clientMock = new();
            clientMock.Setup(c => c.SendEmail(It.IsAny<Notifications.Email.Core.Models.Email>()))
                .ReturnsAsync("operation-id");

            Mock<ICommonProducer> producerMock = new();
            producerMock.Setup(p => p.ProduceAsync(
                It.Is<string>(s => s.Equals("EmailSendingAcceptedTopic")),
                It.Is<string>(s =>
                s.Contains("\"operationId\":\"operation-id\"") &&
                s.Contains($"\"notificationId\":\"{id}\""))));

            var sut = new EmailService(clientMock.Object, producerMock.Object, _topicSettings);

            // Act
            await sut.SendAsync(email);

            // Assert
            producerMock.VerifyAll();
        }

        [Fact]
        public async Task UpdateSendStatus_OperationResultGenerated_PublishedToExpectedKafkaTopic()
        {
            // Arrange
            Guid id = Guid.NewGuid();
            SendNotificationOperationIdentifier identifier = new()
            {
                OperationId = "operation-id",
                NotificationId = id
            };

            Mock<IEmailServiceClient> clientMock = new();
            clientMock.Setup(c => c.GetOperationUpdate(It.IsAny<string>()))
                .ReturnsAsync(EmailSendResult.Delivered);

            Mock<ICommonProducer> producerMock = new();
            producerMock.Setup(p => p.ProduceAsync(
                It.Is<string>(s => s.Equals("EmailOperationResultTopic")),
                It.Is<string>(s =>
                s.Contains("\"operationId\":\"operation-id\"") &&
                  s.Contains("\"sendResult\":\"Delivered\"") &&
                s.Contains($"\"notificationId\":\"{id}\""))));

            var sut = new EmailService(clientMock.Object, producerMock.Object, _topicSettings);

            // Act
            await sut.UpdateSendStatus(identifier);

            // Assert
            producerMock.VerifyAll();
        }

    }
}
