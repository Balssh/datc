import asyncio
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.data.tables.aio import TableClient
import pandas as pd
from datetime import datetime

connection_string = "DefaultEndpointsProtocol=https;AccountName=datcapi;AccountKey=r2mGYgXkTDo+0ibMCC4lwkUEHOlkY0bb+fS+cn9ar6uDiS6/EbCgG9zO0ow+WhMl9nsmFX9j0MoR+AStE4fkPg==;EndpointSuffix=core.windows.net"
# table_name = "players"


async def get_table_entries(table_name):
    table_client = TableClient.from_connection_string(connection_string, table_name)
    async with table_client:
        try:
            entities = []
            async for entity in table_client.list_entities():
                entities.append(dict(entity))
        except HttpResponseError:
            print("Table is empty")
    return entities


def get_metrics(data: pd.DataFrame):
    metrics = {}
    today = datetime.today()
    partition_key = f"{today.date()}"
    row_key = f"{today.time()}"
    metrics["PartitionKey"] = partition_key
    metrics["RowKey"] = row_key
    teams_count = data["team"].value_counts()
    print(teams_count)
    metrics["NrOfPlayers"] = data.shape[0]
    metrics["NrOfRetiredPlayers"] = teams_count["Retired"].item()
    metrics["AverageAge"] = data["age"].mean().item()
    metrics["NrOfTeams"] = teams_count.loc[teams_count.index != "Retired"].size

    return metrics


async def publish_metrics(metrics):
    table_client = TableClient.from_connection_string(connection_string, "metricsdatc")
    async with table_client:
        try:
            await table_client.create_table()
        except HttpResponseError:
            print("Table already exists")
        try:
            resp = await table_client.create_entity(entity=metrics)
            print(resp)
        except ResourceExistsError:
            print("Entity already exists")


async def main():
    # Get all the entries
    entities = await get_table_entries("players")

    # Create a dataframe for easier data manipulation and format the data
    df = pd.DataFrame(entities)
    df.rename(columns={"PartitionKey": "team", "RowKey": "ign"}, inplace=True)

    metrics = get_metrics(df)
    print(metrics)
    await publish_metrics(metrics)


if __name__ == "__main__":
    asyncio.run(main())
