﻿//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    internal static class LargePayloadBlobManager
    {
        internal static async Task AddBlobsData(AzureStorageOrchestrationServiceSettings settings, List<InstanceBlob> instanceBlobs)
        {
            if (instanceBlobs.Count > 0)
            {
                CloudTable instancesTable = await CreateInstancesTable(settings, $"{settings.TaskHubName}LargeMessages");
                await InsertBlobData(instancesTable, instanceBlobs);
            }
        }

        static async Task InsertBlobData(CloudTable instancesTable, List<InstanceBlob> instanceBlobs)
        {
            var batchOperation = new TableBatchOperation();
            foreach (InstanceBlob instanceBlob in instanceBlobs)
            {
                batchOperation.InsertOrReplace(new DynamicTableEntity(instanceBlob.InstanceId, instanceBlob.BlobName));
            }

            await instancesTable.ExecuteBatchAsync(batchOperation);
        }

        static async Task<CloudTable> CreateInstancesTable(AzureStorageOrchestrationServiceSettings settings, string tableName)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(settings.StorageConnectionString);

            CloudTableClient tableClient = account.CreateCloudTableClient();
            tableClient.BufferManager = SimpleBufferManager.Shared;

            string instancesTableName = tableName;
            NameValidator.ValidateTableName(instancesTableName);

            CloudTable instancesTable = tableClient.GetTableReference(instancesTableName);
            await instancesTable.CreateIfNotExistsAsync();

            return instancesTable;
        }
    }

    internal class InstanceBlob
    {
        internal string InstanceId { get; set; }
        internal string BlobName { get; set; }
    }
}
