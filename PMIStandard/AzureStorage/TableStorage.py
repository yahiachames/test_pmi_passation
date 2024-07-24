import logging
from azure.data.tables import TableSasPermissions, TableServiceClient, generate_table_sas, TableClient
from azure.core.credentials import AzureNamedKeyCredential
from datetime import datetime, timezone, timedelta


class AzureDataTablesClient:
    """
    This class allows interaction with Azure Data Tables.

    :param table_name: The name of the table in Azure Data Tables.
    :type table_name: str
    :param connection_string: The connection string to use to connect to the service. Defaults to None.
    :type connection_string: str, optional
    :param account_name: The name of the Azure storage account. Defaults to None.
    :type account_name: str, optional
    :param account_key: The key for the Azure storage account. Defaults to None.
    :type account_key: str, optional

    :ivar table_name: The name of the table in Azure Data Tables.
    :vartype table_name: str
    :ivar connection_string: The connection string to use to connect to the service.
    :vartype connection_string: str
    :ivar account_name: The name of the Azure storage account.
    :vartype account_name: str
    :ivar account_key: The key for the Azure storage account.
    :vartype account_key: str
    """

    def __init__(self, table_name, connection_string=None, account_name=None, account_key=None):
        """
        Initializes an instance of the AzureDataTablesClient class.
        """
        self.connection_string = connection_string
        self.account_name = account_name
        self.account_key = account_key
        self.table_name = table_name
        self.table_service_client = None
        self.logger = logging.getLogger('AzureDataTablesClient')

    def connect_table_service(self, table_permissions=TableSasPermissions(read=True), table_expiry=None):
        """
        Establishes a connection with the Azure Table service.
        """
        try:
            if self.connection_string:
                self.table_service_client = TableServiceClient.from_connection_string(self.connection_string)
                self.logger.info("Connected to Azure Table Service using connection string")
            elif self.account_name and self.account_key:
                sas_key = self.generate_table_sas_key(self.table_name, permissions=table_permissions, expiry=table_expiry)
                self.table_service_client = TableServiceClient(account_url=f"https://{self.account_name}.table.core.windows.net", credential=sas_key)
                self.logger.info("Connected to Azure Table Service using SAS token")
            else:
                raise ValueError("Either connection string or both account name and account key must be provided")
        except Exception as e:
            self.logger.error(f"Error connecting to Azure Table Service: {e}")

    def generate_table_sas_key(self, table_name, permissions=TableSasPermissions(read=True), expiry=None):
        """
        Generates a SAS token for Azure Table storage.
        """
        expiry = expiry or datetime.now(timezone.utc) + timedelta(hours=1)
        sas_token = generate_table_sas(
            account_name=self.account_name,
            account_key=self.account_key,
            table_name=table_name,
            permission=permissions,
            expiry=expiry
        )
        return sas_token

    def create_entity(self, entity):
        """
        Creates a single entity in the Azure Data Table.

        :param entity: The entity to be created.
        :type entity: dict
        """
        try:
            table_client = self.table_service_client.get_table_client(self.table_name)
            table_client.create_entity(entity=entity)
            self.logger.info(f"Entity created successfully in {self.table_name}")
        except Exception as e:
            self.logger.error(f"Error creating entity in {self.table_name}: {e}")

    def get_entity(self, partition_key, row_key):
        """
        Retrieves a single entity from the Azure Data Table.

        :param partition_key: The partition key of the entity.
        :type partition_key: str
        :param row_key: The row key of the entity.
        :type row_key: str
        :return: The retrieved entity.
        :rtype: dict
        """
        try:
            table_client = self.table_service_client.get_table_client(self.table_name)
            entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)
            self.logger.info(f"Entity retrieved successfully from {self.table_name}")
            return entity
        except Exception as e:
            self.logger.error(f"Error retrieving entity from {self.table_name}: {e}")
            return None

    def insert_batch_entities(self, entities, batch_size=1):
        """
        Inserts entities into an Azure Data Table in batches.
        """
        try:
            self.logger.info(f'Preparing data to insert into {self.table_name} table')
            table_client = self.table_service_client.get_table_client(self.table_name)
            entities_to_insert = []
            for row in entities:
                entities_to_insert.append(("upsert", row))
                if len(entities_to_insert) == batch_size:
                    self.logger.info(f'Batch to insert: {entities_to_insert}')
                    table_client.submit_transaction(entities_to_insert)
                    entities_to_insert = []
            if entities_to_insert:
                self.logger.info(f'Batch to insert: {entities_to_insert}')
                table_client.submit_transaction(entities_to_insert)
            self.logger.info(f'All lines are successfully inserted into {self.table_name} table.')
        except Exception as e:
            self.logger.error(f'Error inserting data into {self.table_name} table: {e}')

    def query_entities(self, filter_condition, batch_size=1):
        """
        Executes a query to retrieve entities from an Azure Data Table.
        """
        try:
            table_client = self.table_service_client.get_table_client(self.table_name)
            entities = []
            self.logger.info(f'Executing query: {filter_condition}')
            for entity_page in table_client.query_entities(query_filter=filter_condition, results_per_page=batch_size).by_page():
                entities.extend(list(entity_page))
                break
            self.logger.info(f'Query Results: {entities}')
            return entities
        except Exception as e:
            self.logger.error(f'Error querying data from {self.table_name} table: {e}')
            return []
