import sys
import argparse
import time
from typing import List
from xml.dom.minidom import parse
import meilisearch
from meilisearch import Client
from meilisearch.index import Index
from slugify import slugify


def create_connection(url: str) -> Client:
    """
    Create a connection to MeiliSearch
    and returns the instance.

    Params:
        url: The url to the MeiliSearch API.

    Returns:
        Instance of MeiliSearch.
    """
    # Init instance of MeiliSearch
    client = meilisearch.Client(url)

    # Try to connect to MeiliSearch
    try:
        client.health()
    except:
        sys.exit(f"\033[31mNo instance of MeiliSearch is running on {url} ...")

    return client


def get_or_create_index(client: Client, uid: str, name: str) -> Index:
    """
    Create or get the MeiliSearch index.

    Params:
        client: Instance of MeiliSearch.
        uid: uid of the index to get or create.
        name: Name of the index to get or create.

    Returns:
        MeiliSearch index.
    """
    # Search in all indexes if the index exists or not
    exist = next((index for index in client.get_indexes() if index["uid"] == uid), None)

    if exist is not None:
        meilisearch_index = client.get_index(uid)
    else:
        meilisearch_index = client.create_index(uid, name=name)

    # Remove all documents
    meilisearch_index.delete_all_documents()

    return meilisearch_index


def add_documents(meilisearch_index: Index, documents: List[dict]):
    """
    Add documents to the MeiliSearch Index.

    Params:
        meilisearch_index: MeiliSearch's Index.
        documents: Documents to index to MeiliSearch.
    """
    try:
        meilisearch_index.add_documents(documents)
    except:
        sys.exit("\033[31mAn error occurs while indexing to MeiliSearch...")


def parse_and_index(meilisearch_index: Index):
    """
    Parse the anidb dump file and index all animes
    to MeiliSearch.

    Params:
        meilisearch_index: MeiliSearch Index where to index animes
    """
    # Parse the xml file
    try:
        dom = parse("anime-titles.xml")
    except FileNotFoundError:
        sys.exit("\033[31mThe file \"anime-titles.xml\" hasn't been found...")

    # Get anime entries
    animes = dom.firstChild.getElementsByTagName("anime")

    # Get the total of animes
    total_animes = len(animes)

    # Stop here if no anime found
    if total_animes == 0:
        sys.exit("No anime found during parsing...")

    # Data list to index
    data_queue = []

    # Loop over all entries
    for index, anime in enumerate(animes, start=1):
        # Get the id
        anime_id = int(anime.getAttribute("aid"))

        # Get all title tags
        titles = anime.getElementsByTagName("title")

        # Get the main title
        main = next(tag for tag in titles if tag.getAttribute("type") == "main")

        # Get all official names
        officials = [tag for tag in titles if tag.getAttribute("type") == "official"]

        # Create the list to send to MeiliSearch
        data = {
            "anime_id": anime_id,
            "main": main.firstChild.nodeValue
        }

        for official in officials:
            # Get the language
            language = official.getAttribute("xml:lang")

            # Create the language official key name
            official_key = slugify(f"official_{language}", separator="_")
            data[official_key] = official.firstChild.nodeValue

            # Create an empty short names list
            # and search if have some
            short_names_list = []

            # Find the short name and add it if found
            for title in titles:
                if title.getAttribute("type") == "short" and title.getAttribute("xml:lang") == language:
                    short_names_list.append(title.firstChild.nodeValue)

            # Add short names have at least one
            for short_name_index, short_name in enumerate(short_names_list, start=1):
                # Create the language short key name
                short_key = slugify(f"short_{language}_{short_name_index}", separator="_")

                # Add short names to the list
                data[short_key] = short_name

        # Add the data to the queue
        data_queue.append(data)

        # Send to MeiliSearch if the index modulo
        # is equal to 500
        if index % 500 == 0:
            # Index to MeiliSearch
            add_documents(meilisearch_index, data_queue)

            # Reset the queue
            data_queue = []

            # Print the advancement
            print(f"Indexing {index} of {total_animes}...", end="\r")

            # Wait a little
            time.sleep(1)

    # Index to MeiliSearch the latest animes parsed
    # in the queue
    add_documents(meilisearch_index, data_queue)

    # Print the finish message
    print(f"Indexed all {total_animes} animes! Have fun!")


def main():
    # Parse the cli arguments
    parser = argparse.ArgumentParser(description="AniDB Database MeiliSearch Indexer.")

    parser.add_argument("--url", default="http://127.0.0.1:7700", help="the url to the MeiliSearch API")
    parser.add_argument("--index-uid", default="animes", help="id of the index (default animes)")
    parser.add_argument("--index-name", default="Animes", help="name of the index (default Animes)")

    args = parser.parse_args()

    # Get MeiliSearch Instance
    client = create_connection(args.url)

    # Get the index
    index = get_or_create_index(client, args.index_uid, args.index_name)

    # Parse and index animes
    parse_and_index(index)


if __name__ == "__main__":
    main()
