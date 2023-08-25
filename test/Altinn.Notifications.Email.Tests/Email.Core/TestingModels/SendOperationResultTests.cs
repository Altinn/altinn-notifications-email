using Altinn.Notifications.Email.Core.Enums;
using Altinn.Notifications.Email.Core.Models;

using System.Text.Json.Nodes;

using Xunit;

namespace Altinn.Notifications.Email.Tests.Email.Core.TestingModels
{
    public class SendOperationResultTests
    {
        private readonly SendOperationResult _operationResult;
        private readonly string _serializedOperationResult;

        public SendOperationResultTests()
        {
            Guid id = Guid.NewGuid();
            _operationResult = new SendOperationResult()
            {
                NotificationId = id,
                OperationId = "operation-id",
                SendResult = EmailSendResult.Sending
            };

            _serializedOperationResult = new JsonObject()
                {
                    { "notificationId", id },
                     { "operationId",  "operation-id" },
                    {"sendResult", "Sending" }
                }.ToJsonString();
        }


        [Fact]
        public void SerializeToJson()
        {
            // Arrange
            string expected = _serializedOperationResult;

            // Act
            var actual = _operationResult.Serialize();

            // Assert
            Assert.Equal(expected, actual);
        }
    }
}
