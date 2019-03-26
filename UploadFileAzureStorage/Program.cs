using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace UploadFileAzureStorage
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine("Usage UploadFileAzureStorage.exe file_path container_name storage_account_name storage_account_access_key" +
                    "\nExample:" +
                    "\nUploadFileAzureStorage.exe C:\\Users\\test-user\\Downloads\\test-file.zip test-container test-storage-account XXXXXXXXXXXXXXXXXXXX");
                return;
            }

            string filePath = args[0];
            string containerName = args[1];
            string storageAccountName = args[2];
            string storageAccountAccessKey = args[3];

            string uploadUrl = GetUploadUrl(Path.GetFileName(filePath), containerName, storageAccountName, storageAccountAccessKey);

            var headers = new Dictionary<string, string>();

            Action<long, long, int> uploadCallback = (totalBytes, uploadedBytes, percentCompleted) =>
            {
                string progressText = String.Format("\r{0} ({1} bytes)...{2}%{3}",
                    filePath, totalBytes.ToString("N0"), percentCompleted, percentCompleted == 100 ? Environment.NewLine : "");

                Console.Write(progressText);
            };

            headers["x-ms-blob-content-disposition"] = String.Format("attachment; filename=\"{0}\"",
                Path.GetFileName(filePath).Replace("\"", "\\\""));

            // upload to Azure
            await UploadWithProgressAzure(
                filePath, uploadUrl,
                600 * 1000, // 10 minutes
                headers, uploadCallback);
        }

        private static string GetUploadUrl(string fileName, string containerName, string storageAccountName, string storageAccountAccessKey)
        {
            CloudStorageAccount storageAccount = new CloudStorageAccount(new StorageCredentials(storageAccountName, storageAccountAccessKey), true);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExists();

            var blob = container.GetBlockBlobReference(fileName);

            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-1);
            sasConstraints.SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(60);
            sasConstraints.Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write;
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);
            return blob.Uri + sasContainerToken;
        }

        public static async Task UploadWithProgressAzure(string filePath, string uploadUrl, int timeout,
            Dictionary<string, string> customHeaders = null, Action<long, long, int> progressCallback = null,
            long grandTotalBytes = 0, long initialUploadedBytes = 0)
        {
            var headers = new Dictionary<string, string>();
            if (customHeaders != null)
            {
                foreach (var key in customHeaders.Keys)
                {
                    headers[key] = customHeaders[key];
                }
            }

            long totalBytes = GetFileSize(filePath);
            if (totalBytes > 64 * 1024 * 1024 /*64 MB - https://msdn.microsoft.com/en-ca/library/azure/dd179451.aspx#Anchor_3*/)
            {
                // upload by blocks
                await UploadWithProgressAzureChunked(filePath, uploadUrl, timeout, headers, progressCallback, grandTotalBytes: grandTotalBytes, initialUploadedBytes: initialUploadedBytes);
            }
            else
            {
                // regular upload
                headers["x-ms-blob-type"] = "BlockBlob";
                await UploadWithProgress(filePath, uploadUrl, timeout, headers, progressCallback, grandTotalBytes: grandTotalBytes, initialUploadedBytes: initialUploadedBytes);
            }
        }

        public static long GetFileSize(string filePath)
        {
            return new FileInfo(filePath).Length;
        }

        private static async Task UploadWithProgressAzureChunked(string filePath, string uploadUrl, int timeout,
            Dictionary<string, string> headers = null, Action<long, long, int> progressCallback = null,
            long grandTotalBytes = 0, long initialUploadedBytes = 0)
        {
            int bufferSize = 64 * 1024; // 64 KB
            int AzureBlobBlockSize = 64 * bufferSize; // 4 MB
            const string AzureApiVersion = "2013-08-15";
            var blocks = new List<string>();
            var blockNumber = 0;

            long totalBytes = grandTotalBytes;
            if (totalBytes == 0)
            {
                totalBytes = GetFileSize(filePath);
            }

            long uploadedTotalBytes = initialUploadedBytes;
            long uploadedBlockBytes = 0;
            int percentCompleted = -1;

            try
            {
                using (var fileStream = File.OpenRead(filePath))
                {
                    HttpWebRequest request = null;
                    Stream blockStream = null;

                    byte[] buffer = new byte[bufferSize];
                    int bytesRead;
                    while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                    {
                        if (blockStream == null)
                        {
                            // init new block
                            var blockId = blockNumber++.ToString().PadLeft(10, '0');
                            blocks.Add(blockId);

                            // create block request
                            string blockUrl = String.Format("{0}&comp=block&blockid={1}", uploadUrl, Convert.ToBase64String(Encoding.UTF8.GetBytes(blockId)));
                            request = (HttpWebRequest)HttpWebRequest.Create(blockUrl);
                            request.Method = "PUT";
                            request.Timeout = timeout;
                            request.Headers["x-ms-date"] = DateTime.Now.ToUniversalTime().ToString("r");
                            request.Headers["x-ms-version"] = AzureApiVersion;

                            long blockSize = totalBytes - uploadedTotalBytes;
                            if (blockSize > AzureBlobBlockSize)
                            {
                                blockSize = AzureBlobBlockSize;
                            }
                            request.ContentLength = blockSize;
                            request.AllowWriteStreamBuffering = false;

                            // get request stream
                            blockStream = await request.GetRequestStreamAsync();

                            //Console.WriteLine("Send Azure blob block (id={0}, size={1}): {2}", blockId, blockSize.ToString("N0"), blockUrl);
                        }

                        // write bytes to block
                        await blockStream.WriteAsync(buffer, 0, bytesRead);

                        uploadedTotalBytes += bytesRead;
                        uploadedBlockBytes += bytesRead;

                        // update progress
                        int percent = Convert.ToInt32(Math.Ceiling((float)uploadedTotalBytes / (float)totalBytes * (float)100));
                        if ((percent % 10 == 0 || percentCompleted == -1) && percent != percentCompleted)
                        {
                            percentCompleted = percent;
                            progressCallback?.Invoke(totalBytes, uploadedTotalBytes, percentCompleted);
                        }

                        if (uploadedBlockBytes >= AzureBlobBlockSize)
                        {
                            // send request
                            using (var response = await request.GetResponseAsync())
                            {
                                response.Close();
                            }

                            uploadedBlockBytes = 0;
                            request = null;
                            blockStream = null;
                        }
                    }

                    if (request != null)
                    {
                        // send last request
                        using (var response = await request.GetResponseAsync())
                        {
                            response.Close();
                        }
                    }
                }

                // combine blocks into final blob
                var sb = new StringBuilder();
                sb.AppendLine(@"<?xml version=""1.0"" encoding=""utf-8""?>");
                sb.AppendLine("<BlockList>");
                foreach (var blockId in blocks)
                {
                    sb.AppendFormat("<Uncommitted>{0}</Uncommitted>", Convert.ToBase64String(Encoding.UTF8.GetBytes(blockId)));
                }
                sb.AppendLine("</BlockList>");
                var contentString = sb.ToString();

                // create block request
                string blockListUrl = String.Format("{0}&comp=blocklist", uploadUrl);
                var listRequest = (HttpWebRequest)HttpWebRequest.Create(blockListUrl);
                listRequest.Method = "PUT";
                listRequest.Timeout = timeout;
                listRequest.Headers["x-ms-date"] = DateTime.Now.ToUniversalTime().ToString("r");
                listRequest.Headers["x-ms-version"] = AzureApiVersion;
                listRequest.ContentType = "text/plain";
                listRequest.ContentLength = contentString.Length;

                //Console.WriteLine("Combining {0} blocks to Azure block blob: {1}", blocks.Count, blockListUrl);

                if (headers != null)
                {
                    foreach (var key in headers.Keys)
                    {
                        listRequest.Headers[key] = headers[key];
                    }
                }

                using (var contentStream = new MemoryStream(Encoding.UTF8.GetBytes(contentString)))
                {
                    using (Stream reqStream = await listRequest.GetRequestStreamAsync())
                    {
                        byte[] buffer = new byte[bufferSize * 1024];
                        int bytesRead;
                        while ((bytesRead = await contentStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                        {
                            await reqStream.WriteAsync(buffer, 0, bytesRead);
                        }
                    }
                }

                using (var response = await listRequest.GetResponseAsync())
                {
                    response.Close();
                }
            }
            catch (WebException wex)
            {
                if (wex.Response != null)
                {
                    var httpResponse = wex.Response as HttpWebResponse;
                    if (httpResponse != null)
                    {
                        var statusCode = httpResponse.StatusCode;
                        var statusDescr = httpResponse.StatusDescription;
                        string responseBody = null;

                        using (var stream = wex.Response.GetResponseStream())
                        {
                            using (var reader = new StreamReader(stream))
                            {
                                responseBody = await reader.ReadToEndAsync();
                            }
                        }

                        Console.WriteLine("Error uploading file (URL: {0}, status code: {1}, reason: {2}): {3}",
                            uploadUrl, (int)statusCode, statusDescr, responseBody);

                        throw new Exception(String.Format("Remote server returned {0}: {1}",
                            (int)statusCode, statusDescr));
                    }
                }
                else
                {
                    throw;
                }
            }
        }

        public static async Task UploadWithProgress(string filePath, string uploadUrl, int timeout,
            Dictionary<string, string> headers = null, Action<long, long, int> progressCallback = null, bool ignoreInvalidSsl = false,
            long grandTotalBytes = 0, long initialUploadedBytes = 0)
        {
            long fileSize = GetFileSize(filePath);

            long totalBytes = grandTotalBytes;
            if (totalBytes == 0)
            {
                totalBytes = fileSize;
            }

            long uploadedBytes = initialUploadedBytes;
            int percentCompleted = -1;

            try
            {
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uploadUrl);
                request.Method = "PUT";
                request.Timeout = timeout;

                request.AllowWriteStreamBuffering = false;
                request.ContentLength = fileSize;

                if (headers != null)
                {
                    foreach (var key in headers.Keys)
                    {
                        request.Headers[key] = headers[key];
                    }
                }

                if (ignoreInvalidSsl)
                {
                    request.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => { return true; };
                }

                int bufferSize = 64;

                using (var fileStream = File.OpenRead(filePath))
                {
                    using (Stream reqStream = await request.GetRequestStreamAsync())
                    {
                        byte[] buffer = new byte[bufferSize * 1024];
                        int bytesRead;
                        while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                        {
                            uploadedBytes += bytesRead;
                            await reqStream.WriteAsync(buffer, 0, bytesRead);

                            // update progress
                            int percent = Convert.ToInt32(Math.Ceiling((float)uploadedBytes / (float)totalBytes * (float)100));
                            if ((percent % 10 == 0 || percentCompleted == -1) && percent != percentCompleted)
                            {
                                percentCompleted = percent;

                                progressCallback?.Invoke(totalBytes, uploadedBytes, percentCompleted);
                            }
                        }
                    }
                }

                using (var response = await request.GetResponseAsync())
                {
                    response.Close();
                }
            }
            catch (WebException wex)
            {
                if (wex.Response != null)
                {
                    var httpResponse = wex.Response as HttpWebResponse;
                    if (httpResponse != null)
                    {
                        var statusCode = httpResponse.StatusCode;
                        var statusDescr = httpResponse.StatusDescription;
                        string responseBody = null;

                        using (var stream = wex.Response.GetResponseStream())
                        {
                            using (var reader = new StreamReader(stream))
                            {
                                responseBody = await reader.ReadToEndAsync();
                            }
                        }

                        Console.WriteLine("Error uploading file (URL: {0}, status code: {1}, reason: {2}): {3}",
                            uploadUrl, (int)statusCode, statusDescr, responseBody);

                        throw new Exception(String.Format("Remote server returned {0}: {1}",
                            (int)statusCode, statusDescr));
                    }
                }
                else
                {
                    throw;
                }
            }
        }
    }
}
