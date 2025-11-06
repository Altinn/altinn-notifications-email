using Altinn.Notifications.Email.Integrations.Clients;
using Altinn.Notifications.Email.Integrations.Configuration;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

namespace Altinn.Notifications.Email.Tests.Email.Integrations
{
    public class EmailServiceClientTests
    {
        [Theory]
        [InlineData("PerSubscriptionPerHourLimitExceeded - Please try again after 3636 seconds.", 3636)]
        [InlineData("PerSubscriptionPerMinuteLimitExceeded - Please try again after 60 seconds.", 60)]
        [InlineData("Random error message not mentioning specific seconds", 60)]
        [InlineData("PerSubscriptionPerHourLimitExceeded - Please try again after 4000 seconds. Status: 429 (Too Many Requests) ErrorCode: TooManyRequests", 4000)]
        public void GetDelayFromString(string input, int expentedDelay)
        {
            // Arrange
            var communicationServicesSettings = new CommunicationServicesSettings
            {
                ConnectionString = "endpoint=https://test.communication.azure.com/;accesskey=testkey"
            };
            var emailServiceAdminSettings = new EmailServiceAdminSettings
            {
                IntermittentErrorDelay = 60
            };
            var loggerMock = new Mock<ILogger<EmailServiceClient>>();
            var client = new EmailServiceClient(communicationServicesSettings, emailServiceAdminSettings, loggerMock.Object);

            // Act
            var result = client.GetDelayFromString(input);

            // Assert
            Assert.Equal(expentedDelay, result);
        }
    }
}
