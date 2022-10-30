# General modules
from select import select
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.data.tables.aio import TableClient
from fastapi import FastAPI

# Personal modules
from player import Player

# Global variables
# connection_string - ideally would be hashed but for convenience it's in plain text here
connection_string = "DefaultEndpointsProtocol=https;AccountName=datcapi;AccountKey=r2mGYgXkTDo+0ibMCC4lwkUEHOlkY0bb+fS+cn9ar6uDiS6/EbCgG9zO0ow+WhMl9nsmFX9j0MoR+AStE4fkPg==;EndpointSuffix=core.windows.net"
table_name = "players"

# The principal thing in this application
app = FastAPI()


# Home endpoint
@app.get("/")
async def root():
    return {"message": "Hello World"}


# List all entities in table
@app.get("/players")
async def display_players():
    """
    The table needs to be reaccesed everytime a function is called,
    else it would close the transport layer, more info on this at
    https://github.com/Azure/azure-sdk-for-python/issues/15773#issuecomment-744579283
    """
    table_client = TableClient.from_connection_string(connection_string, table_name)

    async with table_client:
        try:
            entities = []
            i = 0
            async for entity in table_client.list_entities():
                entities.append(entity)
                print("Entity #{}: {}".format(i, entity))
                i += 1
        except HttpResponseError:
            print("Table is empty")
    return entities


# List player(s) with a certain in game name
@app.get("/players/{player_ign}")
async def player_details(player_ign: str):
    table_client = TableClient.from_connection_string(connection_string, table_name)

    async with table_client:
        try:
            parameters = {"RowKey": player_ign}
            name_filter = "RowKey eq @RowKey"
            response = table_client.query_entities(
                query_filter=name_filter,
                select=["PartitionKey", "RowKey", "name", "position", "age"],
                parameters=parameters,
            )

            entities = []
            async for entity_chosen in response:
                entities.append(entity_chosen)
                print(entity_chosen)
        except HttpResponseError:
            pass

    if entities:
        return entities
    else:
        return {"message": "Player not found!"}


# Display a given team
@app.get("/teams/{team_name}")
async def display_team(team_name: str):
    table_client = TableClient.from_connection_string(connection_string, table_name)

    async with table_client:
        try:
            parameters = {"PartitionKey": team_name}
            team_filter = "PartitionKey eq @PartitionKey"
            response = table_client.query_entities(
                query_filter=team_filter,
                select=["PartitionKey", "RowKey", "name", "position", "age"],
                parameters=parameters,
            )

            entities = []
            async for entity_chosen in response:
                entities.append(entity_chosen)
                print(entity_chosen)
        except HttpResponseError:
            pass

    if entities:
        return entities
    else:
        return {"message": "Team not found!"}


# Add player to table
@app.post("/players")
async def add_player(player: Player):
    table_client = TableClient.from_connection_string(connection_string, table_name)

    entity = {
        "PartitionKey": player.PartitionKey,
        "RowKey": player.RowKey,
        "name": player.name,
        "position": player.position,
        "age": player.age,
    }

    async with table_client:
        try:
            await table_client.create_table()
        except HttpResponseError:
            print("Table already exists")

        try:
            resp = await table_client.create_entity(entity=entity)
            print(resp)
        except ResourceExistsError:
            print("Entity already exists")


# Add team
@app.post("/teams")
async def add_team(players_list: list[Player]):

    for entity in players_list:
        await add_player(entity)

    return players_list


# Delete a player
@app.delete("/players/{player_ign}")
async def delete_player(player_ign: str, team_name: str):
    table_client = TableClient.from_connection_string(connection_string, table_name)

    async with table_client:
        try:
            await table_client.delete_entity(
                row_key=player_ign, partition_key=team_name
            )
            print("Player deleted succsefully")
        except HttpResponseError:
            pass

    return await display_players()
