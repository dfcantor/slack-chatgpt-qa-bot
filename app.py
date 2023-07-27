# Libary Modules needed for this script: slack_bolt, os, json, llama_index, openai
import json
import os

import config
import openai
import psycopg2
from llama_index import (
    LLMPredictor,
    ServiceContext,
    SimpleDirectoryReader,
    StorageContext,
    VectorStoreIndex,
    load_index_from_storage,
)
from llama_index.llms import OpenAI
from llama_index.prompts import Prompt
from llama_index.query_engine import CitationQueryEngine
from llama_index.retrievers import VectorIndexRetriever
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Define OpenAI API key
openai.api_key = config.OPENAI_KEY

# Initialize Slack App with the provided bot token
app = App(token=config.BOT_LEVEL_TOKEN_SLACK)


# Function to create a connection to the PostgreSQL database
def create_connection():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="user",
            host="localhost",
            port="5432",
            database="ABS_Bot_Log",
        )
        return connection
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None


# Function to create a connection to the PostgreSQL database
def create_connection():
    try:
        connection = psycopg2.connect(
            dbname="ABS_Bot_Log",
            user="postgres",
            password="user",
            host="localhost",
            port="5432",
        )
        return connection
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None


# Function to insert data into the Users table
def insert_user(connection, username):
    try:
        with connection.cursor() as cursor:
            sql = f"INSERT INTO users (username) VALUES ('{str(username)}')"
            cursor.execute(sql)
        conn.commit()

        with connection.cursor() as cursor:
            sql = f"SELECT slack_user_id FROM users WHERE username = {str(username)}"
            cursor.execute(sql)
            user_id = cursor.fetchone()[0]
        conn.commit()
        return user_id
    except psycopg2.Error as e:
        print("Error inserting data into Users table:", e)


# Function to insert data into the Questions table
def insert_question(connection, username, timestamp, message):
    try:
        # Convert the timestamp to a proper timestamp format
        timestamp = psycopg2.TimestampFromTicks(float(timestamp))

        with connection.cursor() as cursor:
            # Check if the user already exists in the Users table
            check_user_sql = "SELECT user_slack_id FROM users WHERE username = %s"
            cursor.execute(check_user_sql, (username,))
            user_exists = cursor.fetchone()

            # If the user doesn't exist, insert the user data into the Users table
            if not user_exists:
                user_id = insert_user(connection, username)

            elif user_exists:
                user_id = user_exists[0]

            # Insert the question data into the Questions table
            sql = f"INSERT INTO questions (user_slack_id, timestamp, message) VALUES ({user_id}, CAST({timestamp} AS timestamp), '{message}') RETURNING question_id"
            cursor.execute(sql)
            question_id = cursor.fetchone()[0]

        conn.commit()
        return question_id
    except psycopg2.Error as e:
        print("Error inserting data into Questions table:", e)
        return None


# Function to insert data into the Responses table
def insert_response(connection, question_id, response):
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO responses (question_id, response) VALUES (%s, %s)"
            cursor.execute(sql, (question_id, response))
        conn.commit()
    except psycopg2.Error as e:
        print("Error inserting data into Responses table:", e)


# Load the GPT index from disk

from llama_index import StorageContext, load_index_from_storage

# rebuild storage context
storage_context = StorageContext.from_defaults(
    persist_dir=r"C:\Users\DanielCantorBaez\Documents\SyncierGPT\slack-chatgpt-qa-bot\abs-prof-index"
)


# Create a service context for the OpenAI model
service_context = ServiceContext.from_defaults(
    llm=OpenAI(model="gpt-3.5-turbo", temperature=0)
)

# load index
index = load_index_from_storage(
    StorageContext.from_defaults(
        persist_dir=r"C:\Users\DanielCantorBaez\Documents\SyncierGPT\slack-chatgpt-qa-bot\abs-prof-index"
    ),
    service_context=service_context,
)

# Create the database connection

conn = create_connection()


# Listens to any incoming messages
@app.message("")
def message_all(message, say):
    # Print the incoming message text
    print(message["text"])

    # Query the index with the message text and get a response
    text = message["text"]

    # Get the user_id of the user who sent the message
    user_id = message["user"]

    # Get the timestamp of the message
    ts = message["ts"]

    query_engine = CitationQueryEngine.from_args(
        index,
        similarity_top_k=3,
        citation_chunk_size=1024,
    )

    response = query_engine.query(text)

    # Extract the desired message and sources from the response object
    response = str(response)  # Convert the 'Response' object to a string
    # sources = json.dumps(response.get_formatted_sources(length=100))

    # Print the message and sources and send them as a message back to the user
    print(response)
    # print(response.source_nodes[0].node.get_text())
    # print(sources)
    say(response)

    # Insert question data and handle user insertion if necessary
    question_id = insert_question(
        connection=conn, username=user_id, timestamp=ts, message=text
    )
    # Insert response data
    insert_response(connection=conn, question_id=question_id, response=response)


# Responds to mentions


@app.event("app_mention")
def event_test(body):
    event = body.get(
        "event", {}
    )  ## Gets the dictionary with the event variables via a GET API request

    print(event)

    # Get and Print the incoming message text

    text = get_text(event)
    print(text)

    user_id = event.get("user")  ## Gets user id from current event
    channel_id = event.get("channel")  ## Gets current event channel
    ts = event.get("ts")  ## Gets the timestamp from the event

    # Query the index with the message text and get a response
    query_engine = CitationQueryEngine.from_args(
        index,
        similarity_top_k=3,
        citation_chunk_size=1024,
    )
    response = query_engine.query(text)

    # Extract the desired message and sources from the response object
    message = (
        f"<@{user_id}>: \n {str(response)}"  # Convert the 'Response' object to a string
    )

    app.client.chat_postMessage(channel=channel_id, thread_ts=ts, text=message)

    # Insert question data and handle user insertion if necessary
    question_id = insert_question(
        connection=conn, username=user_id, timestamp=ts, message=message
    )
    # Insert response data
    insert_response(connection=conn, question_id=question_id, response=response)


def get_text(data: dict) -> str:
    for block in data["blocks"]:
        if block["type"] == "rich_text":
            for element in block["elements"]:
                if element["type"] == "rich_text_section":
                    for sub_element in element["elements"]:
                        if sub_element["type"] == "text":
                            return sub_element["text"]

    text = get_text(data)
    return text


# Start the Socket Mode handler
if __name__ == "__main__":
    SocketModeHandler(app, config.APP_LEVEL_TOKEN_SLACK).start()
