using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SqsRepro
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await DoMagic(1);

            await DoMagic(2);

            await DoMagic(3);

            await DoMagic(4);

            Console.ReadLine();
        }

        private static async Task DoMagic(int attempt)
        {
            var client = new AmazonSQSClient(new AmazonSQSConfig {/*RegionEndpoint = RegionEndpoint.EUCentral1,*/ ServiceURL = "http://sqs.eu-central-1.amazonaws.com"});
            var queueUrl = await CreateQueue(client);

            var concurrencyLevel = 2;
            var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(10));
            var consumerTasks = new List<Task>();

            for (var i = 0; i < concurrencyLevel; i++)
            {
                consumerTasks.Add(ConsumeMessage(client, queueUrl, i, cancellationTokenSource.Token));
            }

            await Task.WhenAll(consumerTasks.Union(new[] {ProduceMessages(client, queueUrl, attempt, cancellationTokenSource.Token)}));

            client.Dispose();
        }

        static async Task ProduceMessages(IAmazonSQS sqsClient, string queueUrl, int attempt, CancellationToken token)
        {
            await Task.Delay(2000);

            var dateTime = DateTime.UtcNow;

            Console.WriteLine($"{DateTime.UtcNow} (Main) - Sending attempt {attempt} {1}");
            await sqsClient.SendMessageAsync(new SendMessageRequest(queueUrl, $"{dateTime} attempt {attempt} / {1}"));
            await Task.Delay(1000);
            Console.WriteLine($"{DateTime.UtcNow} (Main) - Sent attempt {attempt} {1}");
        }

        static async Task ConsumeMessage(IAmazonSQS sqsClient, string queueUrl, int pumpNumber, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - Receiving");
                    var receiveResult = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                        {
                            MaxNumberOfMessages = 10,
                            QueueUrl = queueUrl,
                            WaitTimeSeconds = 20,
                        },
                        token).ConfigureAwait(false);

                    Console.WriteLine(
                        $"{DateTime.UtcNow} ({pumpNumber}) - Received {receiveResult.Messages.Count} / {receiveResult.HttpStatusCode} / {receiveResult.ContentLength}");

                    var concurrentReceives = new List<Task>(receiveResult.Messages.Count);
                    foreach (var message in receiveResult.Messages)
                    {
                        concurrentReceives.Add(Consume(sqsClient, queueUrl, pumpNumber, message, token));
                    }

                    await Task.WhenAll(concurrentReceives)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - cancelled");
                }
                catch (OverLimitException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - throttled");
                }
                catch (AmazonSQSException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - error");
                }
            }
        }

        private static async Task Consume(IAmazonSQS sqsClient, string queueUrl, int pumpNumber, Message message, CancellationToken token)
        {
            Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - deleting {message.Body}");
            await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None).ConfigureAwait(false);
            Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - deleted {message.Body}");
        }

        private static async Task<string> CreateQueue(AmazonSQSClient client)
        {
            var sqsRequest = new CreateQueueRequest
            {
                QueueName = "repro-queue"
            };
            var createQueueResponse = await client.CreateQueueAsync(sqsRequest).ConfigureAwait(false);
            var queueUrl = createQueueResponse.QueueUrl;
            var sqsAttributesRequest = new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl
            };
            sqsAttributesRequest.Attributes.Add(QueueAttributeName.MessageRetentionPeriod,
                TimeSpan.FromDays(4).TotalSeconds.ToString());

            await client.SetQueueAttributesAsync(sqsAttributesRequest).ConfigureAwait(false);
            return queueUrl;
        }
    }
}
