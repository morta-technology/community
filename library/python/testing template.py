"""
This is a template testing file.
Copy-paste it and rename to 'testing.py' in your own local copy of the repo
it will be ignored by github, so do not worry about changing it
"""

# import packages
import morta.api as ma

# global variables
# paste your API Morta Key here between the double quotations
API_KEY = ""


def main():
    """
    This function runs automatically when running this Python file
    You can add your test code here
    """
    # get a document from Morta
    # -------------------------
    # 1. paste the document ID.
    #   the document is the portion of the URL after '/process/'
    #
    #   example URL:
    #       https://app.morta.io/project/c635af4b-5e32-4d42-98a1-994108710141/
    #       process/7a9603b6-a5a0-48e4-a1c2-021329f7fe6c
    #
    #   in this case, document_id = "7a9603b6-a5a0-48e4-a1c2-021329f7fe6c"
    document_id = "input document id here"

    # 2. perform an api call to get the document from Morta
    #   note you may need to download the Python Library: 'requests'.
    #   In vs code, type in: pip install requests in the terminal
    document = ma.get_document(document_id=document_id, api_key=API_KEY)

    # 3. print the document to the terminal
    #   you can also add breakpoints and inspect the variables when running python in debug mode
    print(document)
    # -------------------------

    """
    The next piece of code gets all hubs, and within each hub, gets the documents and tables within the hub
    """
    hubs = ma.get_projects(api_key=API_KEY)

    # loop over hubs
    for hub in hubs:
        # get documents in hub
        documents = ma.get_documents(project_id=hub["publicId"], api_key=API_KEY)

        # get tables in hub
        tables = ma.get_tables(project_id=hub["publicId"], api_key=API_KEY)

        # loop over tables and get rows
        for table in tables:
            # get the columns in a table
            columns = table["columns"]
            # get table rows
            rows = ma.get_table(table_id=table["publicId"], api_key=API_KEY)
    # The end !
    print("----\ndone\n----")


if __name__ == "__main__":
    main()
