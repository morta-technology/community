# packages
import json
import requests
import urllib
import traceback
from enum import Enum
from time import sleep

# global variables
URL = "https://api.morta.io"
DEFAULT_MORTA_USER_TOKEN = ""
MAX_ROW_COUNT_LIMIT_ON_INSERT = 2500
MAX_API_CALL_TRIES = 3


class Role(Enum):
    VIEWER = 0
    CONTRIBUTOR = 2
    OWNER = 4


class ResourceKind(Enum):
    document = "process"
    table = "table"
    view = "table_view"


class AttributeKind(Enum):
    user = "user"
    tag = "tag"
    all_table_tags = "all_table_tags"
    project = "project"


def api_call(
    method: str,
    endpoint: str,
    params: dict = None,
    tries: int = 0,
    api_key: str = None,
    data: dict = None,
    files: list = None,
) -> requests.Response:
    # checking if the method is one of the accepted values
    assert method in ["GET", "POST", "PUT", "DELETE"], "method should be one of GET, POST, PUT, DELETE"

    if api_key is not None:
        user_token = api_key
    else:
        user_token = DEFAULT_MORTA_USER_TOKEN

    # constructing headers and url
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {user_token}",
    }
    dest_url = f"{URL}{endpoint}"
    # print(f"{method}: {dest_url}")

    # try executing the api request. if failed, wait for 1 second and retry if tries < Max number of tries
    # otherwise, raise and exception
    try:
        if method == "GET":
            response = requests.get(url=dest_url, headers=headers, params=params)
        elif method == "POST":
            if data and files:
                response = requests.post(url=dest_url, files=files, data=data, headers=headers)
            elif files:
                response = requests.post(url=dest_url, files=files, headers=headers)
            else:
                response = requests.post(url=dest_url, headers=headers, json=params)
        elif method == "PUT":
            response = requests.put(url=dest_url, headers=headers, json=params)
        elif method == "DELETE":
            response = requests.delete(url=dest_url, headers=headers, json=params)
    except Exception:
        sleep(1)
        tries = tries + 1
        if tries < MAX_API_CALL_TRIES:
            return api_call(method=method, endpoint=endpoint, params=params, data=data, files=files, tries=tries)
        else:
            raise Exception(f"Exception:\n{traceback.format_exc()}")

    # if the response code is not 200 or 201:
    if response.status_code != 200 and response.status_code != 201:
        # increase the tries and log the response
        tries = tries + 1
        log_responses(response)
        sleep_time = 1

        # if the response status code is one of 502, 500, 503 and max tries have not been reached yet
        # wait for 1 second, log the response
        # response 429 happens when more than 10 api calls are made on a resource in a second
        if response.status_code in [502, 500, 429, 503] and tries < MAX_API_CALL_TRIES:
            # only for the 429 error, we need to incrementally increase the wait time.
            if response.status_code in [429]:
                sleep_time = tries * 2

            sleep(sleep_time)
            print(f"retrying api call. total tries: {str(tries)}")
            return api_call(
                method=method, endpoint=endpoint, params=params, data=data, files=files, tries=tries, api_key=api_key
            )
        elif response.status_code in [502, 500, 429, 503] and tries >= MAX_API_CALL_TRIES:
            sleep(1)

            exception_message = (
                "maximum tries reached"
                f"url:\n{dest_url}\n\n"
                f"response content:\n{str(response.content)}\n\n"
                f"response status code:\n{str(response.status_code)}"
            )
        else:
            exception_message = (
                f"url:\n{dest_url}\n\n"
                f"response content:\n{str(response.content)}\n\n"
                f"response status code:\n{str(response.status_code)}"
            )

        # raise the exception
        raise Exception(exception_message)

    return response


def log_responses(response: requests.Response):
    print(f"response content: {str(response.content)}")
    print(f"response status code: {str(response.status_code)}")


def get_document(document_id: str, api_key: str = None) -> dict:
    """
    Purpose
    -------
    Gets a morta document
    Input
    -----

    Output
    ------
    """
    response = api_call("GET", f"/v1/process/{document_id}", api_key=api_key)
    print(
        f"get document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_document_pdf(document_id: str, api_key: str = None) -> str:
    response = api_call("GET", f"/v1/process/{document_id}/export", api_key=api_key)
    print(
        f"get document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.content


def create_document(project_id: str, name: str, document_type: str = "", api_key: str = None) -> dict:
    params = {"name": name, "type": document_type, "projectId": project_id}
    response = api_call("POST", "/v1/process", params=params, api_key=api_key)
    print(
        f"create document in project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes project_id and document_id
# duplicates the said document
# returns document data as a json object {'data': {}, 'metadata': {}}
def duplicate_document(project_id: str, document_id: str, api_key: str = None) -> dict:
    params = {"projectId": project_id, "processId": document_id}
    response = api_call("POST", "/v1/process/duplicate", params, api_key=api_key)
    print(
        f"duplicate document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# duplicates the document and embeded tables and permissions async
def duplicate_document_async(
    target_project_id: str,
    document_id: str,
    duplicate_linked_tables: bool = False,
    duplicate_permissions: bool = False,
    api_key: str = None,
) -> dict:
    params = {
        "targetProjectId": target_project_id,
        "duplicateLinkedTables": duplicate_linked_tables,
        "duplicatePermissions": duplicate_permissions,
    }
    response = api_call("POST", f"/v1/process/{document_id}/duplicate", params, api_key=api_key)
    print(
        f"async duplicate document: {document_id}, to projet: {target_project_id}"
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes document_id, and params
# params = {"name": "xxx", "type": "xxx","description": "xxx", "logo": "xxx"}
# no return
def update_document(document_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/process/{document_id}", params, api_key=api_key)
    print(
        f"update document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes document_id
def delete_document(document_id: str, api_key: str = None) -> str:
    response = api_call("DELETE", f"/v1/process/{document_id}", api_key=api_key)
    print(
        f"delete document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes document_id
def restore_document(document_id: str, api_key: str = None) -> str:
    response = api_call("PUT", f"/v1/process/{document_id}/restore", api_key=api_key)
    print(
        f"restore document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id
def restore_table(table_id: str, api_key: str = None) -> str:
    response = api_call("PUT", f"/v1/table/{table_id}/restore", api_key=api_key)
    print(
        f"restore table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# get a document section
# takes:
# proecss_id is the publicId of the document
# section_id is the publicId of the section
def get_section(document_id: str, section_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/process/{document_id}/section/{section_id}", api_key=api_key)
    print(
        f"get document section: {section_id} from document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# creates a new document section
# takes:
#   document_id
#   section_name
#   parent_section_id, if you want to create a section that doesn't have a parent, no need to provide parent_section_id
def create_section(document_id: str, section_name: str, parent_section_id: str = None, api_key: str = None) -> dict:
    params = {"name": section_name, "parentId": parent_section_id}
    response = api_call("POST", f"/v1/process/{document_id}/section", params=params, api_key=api_key)
    print(
        f"create section: {section_name}, in document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# creates new multiple document sections
# takes:
#   document_id
#   sections = [{"parentId": parent_section_id or None, "name": "Test"}]
def create_sections(document_id: str, sections: list, api_key: str = None) -> dict:
    section_ids = []
    batch_size = 200
    for i in range(0, len(sections), batch_size):
        current_sections = sections[i : i + batch_size]
        params = {"sections": current_sections}
        response = api_call("POST", f"/v1/process/{document_id}/multiple-section", params=params, api_key=api_key)
        print(
            f"create sections in document: {document_id}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
        json_response = response.json()
        section_ids = section_ids + json_response["metadata"]["resourceIds"]

    return section_ids


# takes document_id, section_id,
# params = {"name": name, "description": description} optionally you can add "pdfIncludeDescription": True or False
# no return
def update_section(document_id: str, section_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/process/{document_id}/section/{section_id}", params, api_key=api_key)
    print(
        f"update morta section: {section_id}, in document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# updates multiple document sections
# takes:
#   document_id
#   sections = [{"publicId": publicId_of_section, "name": "Test"}]
def update_sections(document_id: str, sections: list, api_key: str = None) -> dict:
    batch_size = 200
    for i in range(0, len(sections), batch_size):
        current_sections = sections[i : i + batch_size]
        params = {"sections": current_sections}
        response = api_call("PUT", f"/v1/process/{document_id}/update-multiple-section", params=params, api_key=api_key)
        print(
            f"update sections in document: {document_id}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )


# duplicates a document section
# input: document_id, section_id
# returns:
def duplicate_section(document_id: str, section_id: str, api_key: str = None) -> dict:
    response = api_call("POST", f"/v1/process/{document_id}/section/{section_id}/duplicate", api_key=api_key)
    print(
        f"duplicate section: {section_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# delete a document section
# takes the document_id, section_id
def delete_section(document_id: str, section_id: str, api_key: str = None) -> dict:
    response = api_call("DELETE", f"/v1/process/{document_id}/section/{section_id}", api_key=api_key)
    print(
        f"delete section: {section_id}, in document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# sections = [{"parentId": None, "position": 0, "sectionId": section_id}]
def update_section_order(document_id: str, sections: list, api_key: str = None) -> dict:
    params = {"processSections": sections}
    response = api_call("PUT", f"/v1/process/{document_id}/changesectionorder", params=params, api_key=api_key)
    print(
        f"change section order for document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_deleted_sections(document_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/process/{document_id}/deletedsections", api_key=api_key)
    print(
        f"get deleted sections for document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# creates a document response
# takes the document id, section id and a response type
def create_response(document_id: str, section_id: str, response_type: str = "Flexible", api_key: str = None) -> dict:
    params = {"type": response_type}
    response = api_call(
        "POST", f"/v1/process/{document_id}/section/{section_id}/response", params=params, api_key=api_key
    )
    print(
        f"create response in section: {section_id}, in document: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# take document_id, section_id, response_id,
# params =  {
#   "responders": list_of_users
#   "type"?: string
#   "typeOptions"?: any
#   "dueDate"?: any
#   "pdfIncludeResponse"?: boolean
#   }
# no return
def update_response(document_id: str, section_id: str, response_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call(
        "PUT", f"/v1/process/{document_id}/section/{section_id}/response/{response_id}", params, api_key=api_key
    )
    print(
        f"update document response: {response_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# delete a document response
# takes the document_id, section_id and response_id
def delete_response(document_id: str, section_id: str, response_id: str, api_key: str = None) -> str:
    response = api_call(
        "DELETE", f"/v1/process/{document_id}/section/{section_id}/response/{response_id}", api_key=api_key
    )
    print(
        f"delete response: {response_id}, in section: {section_id},in process: {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def submit_response(document_id: str, section_id: str, response_id: str, api_key: str = None) -> dict:
    response = api_call(
        "PUT",
        f"/v1/process/{document_id}/section/{section_id}/response/{response_id}/submit",
        params={"response": None},
        api_key=api_key,
    )
    print(
        f"submit response: {response_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# take document_id, section_id, response_id
def reset_response(document_id: str, section_id: str, response_id: str, api_key: str = None) -> dict:
    response = api_call(
        "PUT", f"/v1/process/{document_id}/section/{section_id}/response/{response_id}/reset", api_key=api_key
    )
    print(
        f"reset document response: {response_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def update_draft_response(
    document_id: str, section_id: str, response_id: str, params: dict, api_key: str = None
) -> dict:
    response = api_call(
        "PUT",
        f"/v1/process/{document_id}/section/{section_id}/response/{response_id}/draft",
        params=params,
        api_key=api_key,
    )
    print(
        f"update draft response: {response_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id
# returns json {'data': {}, 'metadata': {}}
def get_table(table_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/table/{table_id}", api_key=api_key)
    print(
        f"get table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# gets the table events done on a table
def get_table_views(table_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/table/{table_id}/views", api_key=api_key)
    print(
        f"get views for table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# take table_id
# returns response
# to get the file from the response, you need: response.content
def get_table_csv(table_id: str, api_key: str = None) -> str:
    response = api_call("GET", f"/v1/table/{table_id}/csv?", api_key=api_key)
    print(
        f"get morta table csv: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.text


# takes:
#    project_id,
#    table_name = "table name",
#    table_columns = [{"name":"col1", "kind": "text", "width": 100}]
# creates a table with given params
# returns json {'data': {}, 'metadata': {}}
def create_table(project_id: str, name: str, columns: list, api_key: str = None, table_type: str = None) -> dict:
    params = {"projectId": project_id, "name": name, "columns": columns}
    if table_type:
        params["type"] = table_type
    response = api_call("POST", "/v1/table", params, api_key=api_key)
    print(
        f"create morta table: {name}, in project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id, params : {"name": "some name here"}
# no return
def update_table(table_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/table/{table_id}", params, api_key=api_key)
    print(
        f"update table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id and truncates a table
def truncate_table(table_id: str, api_key: str = None) -> str:
    response = api_call("DELETE", f"/v1/table/{table_id}/truncate", api_key=api_key)
    print(
        f"truncate table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id
def delete_table(table_id: str, api_key: str = None) -> str:
    response = api_call("DELETE", f"/v1/table/{table_id}", api_key=api_key)
    print(
        f"delete table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def duplicate_table_async(target_project_id: str, table_id: str, duplicate_permissions: bool, api_key: str):
    params = {
        "targetProjectId": target_project_id,
        "duplicatePermissions": duplicate_permissions,
        "duplicateLinkedTables": True,
    }
    response = api_call("POST", f"/v1/table/{table_id}/duplicate", params, api_key=api_key)
    print(
        f"async duplicate table: {table_id}, to projet: {target_project_id}"
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes:
#    table_id,
#    join_view_id,
#    join_columns: [
#     {
#         "targetColumnId": join_target_col_id,
#         "sourceColumnId": join_source_col_id,
#     }
# ]
#    data_columns = [col1, col2]
# creates a table join
# returns json 'data': {}
def create_join(table_id: str, join_view_id: str, join_columns: list, data_columns: list, api_key: str = None) -> dict:
    params = {
        "joinViewId": join_view_id,
        "joinColumns": join_columns,
        "dataColumns": data_columns,
    }
    response = api_call("POST", f"/v1/table/{table_id}/join", params, api_key=api_key)
    print(
        f"create table join for table: {table_id}, with view: {join_view_id}, and return columns: {data_columns}"
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# deletes the join on a table
# takes the table_id and the join_id
def delete_join(table_id: str, join_id: str, api_key: str = None) -> dict:
    response = api_call("DELETE", f"/v1/table/{table_id}/join/{join_id}", api_key=api_key)
    print(
        f"delete join: {join_id}, in table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# get distinct values of a certain column
def get_disctinct_values_in_column(table_id: str, column_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/table/{table_id}/column/{column_id}/distinct", api_key=api_key)
    print(
        f"get distinct values from column: {column_id}, "
        f"in table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# create a column in a table
# takes:
# table_id which is the publicId of the table
# params which is a dictionary as below:
# {
#   "name": "string",
#   "kind": "string",
#   "width": 0,
#   "locked": true
# }
def create_column_in_table(table_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("POST", f"/v1/table/{table_id}/column", params, api_key=api_key)
    print(
        f"create column in table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# create a column in a view
# takes:
# view_id which is the publicId of the view
# params which is a dictionary as below:
# {
# "name": "",
# "kind": "",
# "width": 120,
# "locked": False,
# "sortOrder": ,
# "required": False,
# }
def create_column_in_view(view_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("POST", f"/v1/table/views/{view_id}/columns", params, api_key=api_key)
    print(
        f"create column in view: {view_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# updates a column in a table
# takes:
# table_id which is the publicId of the table
# column_id which is the publicId of the column
# params which is a dictionary as below:
# {
#   "name": "string",
#   "kind": "string",
#   "width": 0,
#   "locked": true
# }
def update_column(table_id: str, column_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/table/{table_id}/column/{column_id}", params, api_key=api_key)
    print(
        f"update column: {column_id} in table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# deletes a column in a table
# takes:
# table_id which is the publicId of the table
# column_id which is the publicId of the column
def delete_column(table_id: str, column_id: str, api_key: str = None) -> str:
    response = api_call("DELETE", f"/v1/table/{table_id}/column/{column_id}", api_key=api_key)
    print(
        f"delete column: {column_id} in table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id
# returns json {'data': {}, 'metadata': {}}
def get_view(view_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/table/views/{view_id}", api_key=api_key)
    print(
        f"get view: {view_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id, view_params: {"name": "view1",
# "filterSettings"=[{"columnName":"CompanyCodeID","filterType":"in","orGroup":"main","value":"{{process.variables}}"}]
# "groupSettings"=[{"columnName":"DataQualityIssue","direction":"asc"}]
# "columns"=[{"columnName":"CompanyCodeID"}]
# "sortSettings"=[{"columnName":"WorkDate","direction":"asc"}])
# no return
def create_view(table_id: str, view_params: dict, api_key: str = None) -> dict:
    response = api_call("POST", f"/v1/table/{table_id}/views", view_params, api_key=api_key)
    print(
        f"create table view on table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id, view_params: {"name": "view1",
# "filterSettings":[{"columnName":"CompanyCodeID","filterType":"in","orGroup":"main","value":"{{process.variables}}"}],
# "groupSettings":[{"columnName":"DataQualityIssue","direction":"asc"}],
# "columns":[{"columnName":"CompanyCodeID"}],
# "sortSettings":[{"columnName":"WorkDate","direction":"asc"}]}
# no return
def update_view(view_id: str, view_params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/table/views/{view_id}", view_params, api_key=api_key)
    print(
        f"update view: {view_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def duplicate_default_view(table_id: str, api_key: str = None):
    response = api_call("POST", f"/v1/table/{table_id}/views/duplicate-default", api_key=api_key)
    print(
        f"duplicate default view: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# deletes a morta table view
def delete_view(view_id: str, api_key: str = None) -> str:
    response = api_call("DELETE", f"/v1/table/views/{view_id}", api_key=api_key)
    print(
        f"delete view: {view_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# duplicate a morta table view
def duplicate_view(table_id: str, view_id: str, api_key: str = None) -> str:
    response = api_call("POST", f"/v1/table/{table_id}/views/{view_id}/duplicate", api_key=api_key)
    print(
        f"duplicate view: {view_id}, in table {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes project_name
# returns json object with all documents and tables related to that project
def create_project(project_name: str, api_key: str = None) -> dict:
    response = api_call("POST", "/v1/project", params={"name": project_name}, api_key=api_key)
    print(
        f"create project: {project_name}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes project_id
# returns json object with all documents and tables related to that project
def get_project(project_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/project/{project_id}", api_key=api_key)
    print(
        f"get project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_projects(api_key: str = None) -> list:
    """
    Note this gets all projects: archived and active
    """
    response = api_call("GET", "/v1/user/projects", api_key=api_key)
    print(
        f"get projects with access"
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes project_id
def get_documents(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/processes", api_key=api_key)
    print(
        f"get documents from project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_deleted_documents(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/deletedprocesses", api_key=api_key)
    print(
        f"get deleted documents for project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes project_id
# returns json of table data that is table properties not rows
def get_tables(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/tables", api_key=api_key)
    print(
        f"get tables from project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_deleted_tables(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/deletedtables", api_key=api_key)
    print(
        f"get deleted tables for project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes project_id
# returns api call response which contains the project tags in the text property:
# # {
#    "data":[
#       {
#          "cells":[
#             {"column":{"name": column_name,"publicId": column_id}, "id": tag_id_here, "value": tag_name_here},
#             ]}]}
def get_tags(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/tags", api_key=api_key)
    print(
        f"get tags from project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes project_id
# returns api call response which contains the project variables in the text property:
# # {
#    "data":[
#       {
#          "cells":[
#             {"column":{"name": column_name,"publicId": column_id}, "id": tag_id_here, "value": variable_name_here},
#             ]}]}
def get_variables(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/variables", api_key=api_key)
    print(
        f"get variables from project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# get the list of project members
def get_members(project_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/project/{project_id}/members", api_key=api_key)
    print(
        f"get project members in project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# get the list of invited project members
def get_invited_members(project_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/project/{project_id}/invitedmembers", api_key=api_key)
    print(
        f"get invited members in project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# updates a morta project
# takes a project_id and params. Please check params via the developer tools in your browser
def update_project(project_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/project/{project_id}", params=params, api_key=api_key)
    print(
        f"update project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response


def archive_project(project_id, api_key: str = None) -> dict:
    response = api_call("DELETE", f"/v1/project/{project_id}", api_key=api_key)
    print(
        f"archive project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response


def get_sent_notifications(project_id: str, api_key: str = None) -> list:
    page = 1
    get_next_page = True
    result = []
    while get_next_page:
        response = api_call("GET", f"/v1/project/{project_id}/sent-notifications?page={page}", api_key=api_key)

        current_result = response.json()["data"]
        result = result + current_result

        print(
            f"get project sent notifications in project: {project_id}, total: {str(len(result))} "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )

        get_next_page = True if len(current_result) > 0 else False
        page = page + 1

    return result


# takes table_id,
#       page_size=2500 by default,
#       included_column_names is a list of column names *optional,
#       filters *optional :  [{"columnName": "column_name", "value": "value", "filterType": "eq", "orGroup": "main"}]
#       wanted_rows = 1 (or any number) this is in case you only want a certain number of rows
#       sort = [{"columnName": "column_name", "sortDirection": "asc or desc"}]
# returns json object with table data
def get_table_rows(
    table_id: str,
    page_size: int = 2500,
    included_column_names: list = [],
    filters: list = [],
    wanted_rows: int = -1,
    sort: list = [],
    api_key: str = None,
) -> list:
    rows = []
    token = None
    total = 0
    is_first_page = True
    encoded_filter = "&".join(
        [urllib.parse.urlencode({"filter": json.dumps(current_filter)}) for current_filter in filters]
    )

    encoded_sorts = "&".join(
        [
            f"sort={urllib.parse.quote_plus(current_sort['columnName'])}:{current_sort['sortDirection']}"
            for current_sort in sort
        ]
    )

    if encoded_filter != "" and encoded_sorts != "":
        encoded_sorts = f"&{encoded_sorts}"

    if wanted_rows > 0 and page_size > wanted_rows:
        page_size = wanted_rows

    while is_first_page or token:
        is_first_page = False
        params = {"nextPageToken": token, "size": page_size}
        response = api_call("GET", f"/v1/table/{table_id}/row?{encoded_filter}{encoded_sorts}", params, api_key=api_key)
        json_response = response.json()
        rows = rows + json_response["data"]
        total += len(json_response["data"])
        print(
            f"get rows from table: {table_id}, total rows: {str(total)}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
        token = json_response["metadata"]["nextPageToken"]
        if wanted_rows > 0 and total >= wanted_rows:
            break

    if len(included_column_names) > 0:
        for row in rows:
            row["rowData"] = {key: item for key, item in row["rowData"].items() if key in included_column_names}

    return rows


# takes table_id,
#       page_size=2500 by default,
#       filters *optional :  [{"columnName":"Price","value":"100","filterType":"eq", "orGroup": "1"}]
#       wanted_rows = 1 (or any number) this is in case you only want a certain number of rows
#       document_id: publicId of document. useful when applying dynamic view filtering
# returns json object with table data
def get_view_rows(
    view_id: str,
    page_size: int = 2500,
    filters: list = [],
    wanted_rows: int = -1,
    document_id: str = None,
    api_key: str = None,
) -> list:
    rows = []
    token = None
    total = 0
    is_first_page = True
    encoded_filter = "&".join(
        [urllib.parse.urlencode({"filter": json.dumps(current_filter)}) for current_filter in filters]
    )

    if wanted_rows > 0 and page_size > wanted_rows:
        page_size = wanted_rows

    document_url_portion = f"&processId={document_id}" if document_id else ""

    while is_first_page or token:
        is_first_page = False
        params = {"nextPageToken": token, "size": page_size}
        response = api_call(
            "GET", f"/v1/table/views/{view_id}/rows?{encoded_filter}{document_url_portion}", params, api_key=api_key
        )
        json_response = response.json()
        rows = rows + json_response["data"]
        total += len(json_response["data"])
        print(
            f"get rows from table view: {view_id}, total rows: {str(total)}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
        token = json_response["metadata"]["nextPageToken"]
        if wanted_rows > 0 and total >= wanted_rows:
            break

    return rows


# get distinct values of a certain column
def get_disctinct_values_in_column_from_view(view_id: str, column_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/table/views/{view_id}/column/{column_id}/distinct", api_key=api_key)
    print(
        f"get distinct values from column: {column_id}, "
        f"in view: {view_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes table_id, rows : [{"rowData": {"col1": "val1"}}, {"rowData": {"col1": "val2"}}]
# insert the new rows to the table
# no return
def insert_rows(table_id: str, rows: list, insert_row_count: int = 2000, api_key: str = None) -> list:
    if len(rows) == 0:
        return []
    if insert_row_count > MAX_ROW_COUNT_LIMIT_ON_INSERT:
        raise Exception(
            f"insert_row_count: {insert_row_count}, should be less or equal to {str(MAX_ROW_COUNT_LIMIT_ON_INSERT)}"
        )
    results = []
    cum_length = 0
    for i in range(0, len(rows), insert_row_count):
        current_rows = rows[i : i + insert_row_count]
        cum_length = cum_length + len(current_rows)
        params = {"rows": current_rows}
        response = api_call("POST", f"/v1/table/{table_id}/row", params, api_key=api_key)
        print(
            f"insert data into morta table: {table_id}, Total rows: {str(cum_length)}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
        results.append(response.json()["data"])
    return results


# takes table_id, rows : [{"rowData": {"col1": "val1"}}, {"rowData": {"col1": "val2"}}]
# insert the new rows to the table
# no return
def insert_rows_into_view(view_id: str, rows: list, insert_row_count: int = 2000, api_key: str = None) -> list:
    if len(rows) == 0:
        return []
    if insert_row_count > MAX_ROW_COUNT_LIMIT_ON_INSERT:
        raise Exception(
            f"insert_row_count: {insert_row_count}, should be less or equal to {str(MAX_ROW_COUNT_LIMIT_ON_INSERT)}"
        )
    results = []
    cum_length = 0
    for i in range(0, len(rows), insert_row_count):
        current_rows = rows[i : i + insert_row_count]
        cum_length = cum_length + len(current_rows)
        params = {"rows": current_rows}
        response = api_call("POST", f"/v1/table/views/{view_id}/rows", params, api_key=api_key)
        print(
            f"insert data into view: {view_id}, Total rows: {str(cum_length)}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
        results.append(response.json()["data"])
    return results


# takes table_id, params = {"rows": [{"publicId": row['publicId'],"rowData": {'Field 1':'d', 'Field 2':'d'}}]}
# no return
def update_row(table_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/table/{table_id}/row", params, api_key=api_key)
    print(
        f"update rows in  table: {table_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes =
# upsertColumnName
#   rows= [
#     {
#       "rowData": {
#         "First column": "Red",
#         "Scond column": "Green"
#       }
#     }
#   ]
def upsert_rows(table_id: str, upsert_column_name: str, rows: list, api_key: str = None) -> list:
    responses = []
    upsert_limit = 200
    cum_length = 0
    for i in range(0, len(rows), upsert_limit):
        current_rows = rows[i : i + upsert_limit]
        cum_length = cum_length + len(current_rows)
        response = api_call(
            "POST",
            f"/v1/table/{table_id}/row/upsert",
            params={"upsertColumnName": upsert_column_name, "rows": current_rows},
            api_key=api_key,
        )
        responses.append(response)
        print(
            f"upsert rows: {cum_length}, in table: {table_id}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
    return responses


# takes table_id, list of row_id's
def delete_rows(table_id: str, row_ids: list, api_key: str = None) -> list:
    responses = []
    if len(row_ids) > 0:
        for i in range(0, len(row_ids), 2000):
            current_row_ids = row_ids[i : i + 2000]
            response = api_call(
                "DELETE", f"/v1/table/{table_id}/rows", params={"rowIds": current_row_ids}, api_key=api_key
            )
            print(
                f"delete {str(i + len(current_row_ids))} rows from table: {table_id}, "
                f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
            )
            responses.append(response.json()["data"])

    return responses


# takes:
# table_id
# cells = [{"columnName": "Field 2", "rowId": row["publicId"], "value": "TEST"}]
# no return
def update_cells(table_id: str, cells: list, batch_size: int = 1000, api_key: str = None) -> list:
    responses = []
    for i in range(0, len(cells), batch_size):
        current_cells = cells[i : i + batch_size]
        response = api_call("PUT", f"/v1/table/{table_id}/cells", {"cells": current_cells}, api_key=api_key)
        responses.append(response.json()["data"])
        print(
            f"update {str(i + len(current_cells))} cells in table: {table_id}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
    return responses


# takes:
# view_id
# cells = [{"columnName": "Field 2", "rowId": row["publicId"], "value": "TEST"}]
# no return
def update_cells_in_view(view_id: str, cells: list, batch_size: int = 1000, api_key: str = None) -> list:
    responses = []
    for i in range(0, len(cells), batch_size):
        current_cells = cells[i : i + batch_size]
        response = api_call("PUT", f"/v1/table/views/{view_id}/cells", {"cells": current_cells}, api_key=api_key)
        responses.append(response.json()["data"])
        print(
            f"update {str(i + len(current_cells))} cells in view: {view_id}, "
            f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
        )
    return responses


# takes:
# project_id
# description
# webhookURL
# tables , list of table ids
# documents, list of document ids
# and triggers as a list example [{"resource": "table", "verb": "cell_updated"}]
# return response data
def create_notification(
    project_id: str,
    description: str,
    webhook_url: str,
    tables: list,
    documents: list,
    triggers: list,
    api_key: str = None,
) -> dict:
    params = {
        "description": description,
        "projectId": project_id,
        "webhookUrl": webhook_url,
        "processes": documents,
        "tables": tables,
        "triggers": triggers,
    }
    response = api_call("POST", "/v1/notifications", params, api_key=api_key)
    print(
        f"create notification: {description}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_notifications(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/notifications", api_key=api_key)
    print(
        f"get notifications in project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def update_notification(notification_id: str, params: dict, api_key: str = None):
    response = api_call("PUT", f"/v1/notifications/{notification_id}", params=params, api_key=api_key)
    print(
        f"update notifications: {notification_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# takes the file URL
def get_file(file_url: str, api_key: str = None) -> dict:
    response = api_call("POST", "/v1/files/sign", params={"url": file_url}, api_key=api_key)
    print(
        f"get file from url: {file_url}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# upload file to Morta
# takes:
#   file: = tuple which has (file_name : str, uploaded_file : binary, file_type : mimetype of file)
#       (check pyhon library mimetype, method guess_type(file_name))
#   resource (optional) = the type of resource: table, document, etc. (check resources global variable in api.py)
#   resource_id (optional) = publicId of table or document
# returns:
#   response
def upload_file(file: tuple, resource: str = None, resource_id: str = None, api_key: str = None) -> dict:
    files = {"file": file}
    if resource and resource_id:
        data = {"resources": json.dumps([{"resource": resource, "publicId": resource_id}])}
        response = api_call(method="POST", endpoint="/v1/files", data=data, files=files, api_key=api_key)
    else:
        response = api_call(method="POST", endpoint="/v1/files", files=files, api_key=api_key)
    print(f"upload file, " f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}")
    return response.json()["data"]


# take a string user_keyword : "jad eid" , or can be an email domain "@morta.com"
# returns a json with a list of all users with names nearly matching that name
def get_user(
    project_id: str = None, user_keyword: str = None, document_id: str = None, view_id: str = None, api_key: str = None
) -> dict:
    user_filter = ""
    document_filter = ""
    project_filter = ""
    view_filter = ""

    if user_keyword:
        user_filter = f"query={user_keyword}"
    if document_id:
        document_filter = f"processId={document_id}"
    if view_id:
        view_filter = f"tableViewId={view_id}"
    if project_id:
        project_filter = f"projectId={project_id}"

    filters = [user_filter, document_filter, view_filter, project_filter]
    filters = "&".join([current_filter for current_filter in filters if current_filter])
    response = api_call("GET", f"/v1/user/search?{filters}", api_key=api_key)
    print(
        f"search for user: {user_keyword}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# role can be either "admin" or "member"
def update_user_role(project_id: str, user_firebase_id: str, role: str, api_key: str = None):
    if role not in ["admin", "member"]:
        raise Exception("user should be one of 'admin' or 'member'")
    params = {"role": role}
    response = api_call("PUT", f"/v1/project/{project_id}/changeuserrole/{user_firebase_id}", params, api_key=api_key)
    print(
        f"update role of user with firebaseId: {user_firebase_id}, in project: {project_id}, to: {role}"
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# user_id is the publicId of the user
def add_user_tag(user_id: str, tag_reference_id: str, api_key: str = None) -> dict:
    response = api_call(
        "POST", f"/v1/user/{user_id}/tags", params={"tagReferenceId": tag_reference_id}, api_key=api_key
    )
    print(
        f"add tag {tag_reference_id} to user: {user_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# user_id is the publicId of the user
def remove_user_tag(user_id: str, user_tag_id: str, api_key: str = None) -> dict:
    response = api_call("DELETE", f"/v1/user/{user_id}/tags/{user_tag_id}", api_key=api_key)
    print(
        f"remove tag {user_tag_id} from user: {user_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# invite user into Morta
def invite_users(project_id: str, emails: list, tags: list = [], api_key: str = None) -> dict:
    response = api_call(
        "POST", f"/v1/project/{project_id}/invite-multiple", params={"emails": emails, "tags": tags}, api_key=api_key
    )
    print(
        f"invite user {', '.join(emails)}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# set all responders in a document
def set_all_responders(document_id: str, firebase_ids: list, api_key: str = None):
    params = {"responders": firebase_ids}
    response = api_call("PUT", f"/v1/process/{document_id}/setallresponders", params=params, api_key=api_key)
    print(
        f"adding firebaseids: {firebase_ids} to document {document_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# remove a user from a project
def remove_user_from_project(project_id: str, firebase_user_id: str, api_key: str = None):
    response = api_call("DELETE", f"/v1/project/{project_id}/removeuser/{firebase_user_id}", api_key=api_key)
    print(
        f"remove user {firebase_user_id} from project {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# resource kind can be one of "document" or "table"
def get_permissions(resource_kind: str, resource_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/permissions?resource={resource_kind}&resourceId={resource_id}", api_key=api_key)
    print(
        f"get permissions from {resource_kind}: {resource_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


# resource kind can be one of ResourceKind enum values
def create_permission(
    resource_kind: str, resource_id: str, attribute_kind: str, attribute_identifier: str, role: int, api_key: str = None
) -> dict:
    if attribute_kind == "user":
        attribute_key = "attributeId"
    elif attribute_kind == "tag":
        attribute_key = "tagReferenceId"
    elif attribute_kind == "all_table_tags":
        attribute_key = "attributeId"
    elif attribute_kind == "project":
        attribute_key = "attributeId"
    else:
        raise Exception(
            f"wrong attribute kind, {attribute_kind},supplied to function. Please choose one of user or tag"
        )

    params = {
        "resourceKind": resource_kind,
        "resourceId": resource_id,
        "attributeKind": attribute_kind,
        attribute_key: attribute_identifier,
        "role": role,
    }
    response = api_call("POST", "/v1/permissions", params=params, api_key=api_key)
    print(
        f"create permission on {resource_kind}: {resource_id}, "
        f"for {attribute_kind}: {attribute_identifier}, as role: {role}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def update_permission(permission_id: str, role: int, api_key: str = None) -> dict:
    params = {"role": role}
    response = api_call("PUT", f"/v1/permissions/{permission_id}", params=params, api_key=api_key)
    print(
        f"update permission: {permission_id}, to role: {role}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def delete_permission(permission_id, api_key: str = None) -> str:
    response = api_call("DELETE", f"/v1/permissions/{permission_id}", api_key=api_key)
    print(
        f"delete permission: {permission_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["metadata"]["message"]


# audits
# gets the events done on a table
def get_table_audits(
    table_id: str,
    verb: str = None,
    user_public_id: str = None,
    start_date: str = None,
    end_date: str = None,
    search: str = None,
    api_key: str = None,
) -> list:
    audits = []
    current_page_audits = None
    page = 1
    while True:
        current_page_audits = get_resource_audits(
            resource_id=table_id,
            resource_type="table",
            page=page,
            verb=verb,
            user_public_id=user_public_id,
            start_date=start_date,
            end_date=end_date,
            search=search,
            api_key=api_key,
        )
        if len(current_page_audits) == 0:
            break
        audits = audits + current_page_audits
        page = page + 1

    return audits


# gets the events done on a document
def get_document_audits(
    document_id: str,
    verb: str = None,
    user_public_id: str = None,
    start_date: str = None,
    end_date: str = None,
    search: str = None,
    api_key: str = None,
) -> list:
    audits = []
    current_page_audits = None
    page = 1
    while True:
        current_page_audits = get_resource_audits(
            resource_id=document_id,
            resource_type="process",
            page=page,
            verb=verb,
            user_public_id=user_public_id,
            start_date=start_date,
            end_date=end_date,
            search=search,
            api_key=api_key,
        )
        if len(current_page_audits) == 0:
            break
        audits = audits + current_page_audits
        page = page + 1

    return audits


# gets the events done on a project
def get_project_audits(
    project_id: str,
    verb: str = None,
    user_public_id: str = None,
    start_date: str = None,
    end_date: str = None,
    search: str = None,
    api_key: str = None,
) -> list:
    audits = []
    current_page_audits = None
    page = 1
    while True:
        current_page_audits = get_resource_audits(
            resource_id=project_id,
            resource_type="project",
            page=page,
            verb=verb,
            user_public_id=user_public_id,
            start_date=start_date,
            end_date=end_date,
            search=search,
            api_key=api_key,
        )
        if len(current_page_audits) == 0:
            break
        audits = audits + current_page_audits
        page = page + 1

    return audits


# get audits on a resource for a particular page
def get_resource_audits(
    resource_id: str,
    resource_type: str,
    page: int,
    verb: str = None,
    user_public_id: str = None,
    start_date: str = None,
    end_date: str = None,
    search: str = None,
    api_key: str = None,
) -> list:
    # create filters
    verb_filter = f"&verb={verb}" if verb else ""
    user_filter = f"&user={user_public_id}" if user_public_id else ""
    start_date_filter = f"&startDate={start_date}" if start_date else ""
    end_date_filter = f"&endDate={end_date}" if end_date else ""
    search_filters = f"&search={search}" if search else ""

    # concat filters
    filters = f"{verb_filter}{user_filter}{start_date_filter}{end_date_filter}{search_filters}"

    response = api_call(
        "GET", f"/v1/notifications/events/{resource_id}?type={resource_type}&page={page}{filters}", api_key=api_key
    )
    print(
        f"get audits for {resource_type}: {resource_id}, page: {page}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )

    return response.json()["data"]


# updates a column in a view
# takes:
# view_id which is the publicId of the table
# column_id which is the publicId of the column
# params which is a dictionary as below:
# {
#   "validationNoBlanks": true
#   "validationNoDuplicates": true
#   "hardValidation": true
#   "validationMessage":
#   "stringValidation":
# }
# for running scripts use params = {"alterOptions": {"runScriptOnAllCells": True}}
def update_column_in_view(view_id: str, column_id: str, params: dict, api_key: str = None) -> dict:
    response = api_call("PUT", f"/v1/table/views/{view_id}/columns/{column_id}", params, api_key=api_key)
    print(
        f"update column: {column_id} in view: {view_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_comments(resource_type: str, resource_id: str, api_key: str = None) -> dict:
    """
    This function is used to get all the comments on a resource
    it only returns the id of the threads
    To get the comments text, use one of the functions below
    """
    response = api_call(
        "GET", f"/v1/comment_thread/stats?referenceType={resource_type}&mainReferenceId={resource_id}", api_key=api_key
    )
    print(
        f"get comments on {resource_type}: {resource_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_comment(resource_type: str, resource_id: str, reference_id: str, api_key: str = None) -> requests.Response:
    """
    this is a wrapper function which isn't meant to be used directly.
    for your convinience, use the below functions instead:
    - get_comments_in_row
    - get_comments_in_section
    """
    endpoint = (
        f"/v1/comment_thread?referenceType={resource_type}&referenceId={reference_id}&mainReference={resource_id}"
    )
    response = api_call(method="GET", endpoint=endpoint, api_key=api_key)
    return response


def get_comments_in_row(table_id: str, row_id: str, api_key: str = None) -> dict:
    response = get_comment(
        resource_type=ResourceKind.table.value, resource_id=table_id, reference_id=row_id, api_key=api_key
    )
    print(
        f"get comments in table {table_id} on row {row_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_comments_in_section(document_id: str, section_id: str, api_key: str = None) -> dict:
    response = get_comment(
        resource_type="process_section", resource_id=document_id, reference_id=section_id, api_key=api_key
    )
    print(
        f"get comments in document {document_id} on section {section_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def delete_comment_thread(thread_id: str, api_key: str = None) -> dict:
    response = api_call("DELETE", f"/v1/comment_thread/{thread_id}", api_key=api_key)
    print(
        f"delete thread: {thread_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()


# def autodesk_call(project_id: str, params: dict, api_key: str = None) -> dict:
#     response = api_call("POST", f"/v1/project/{project_id}/autodesk/api", params=params, api_key=api_key)
#     print(
#         f"autodesk api call from project: {project_id}, "
#         f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
#     )
#     return response.json()


def get_project_secrets(project_id: str, api_key: str = None) -> list:
    response = api_call("GET", f"/v1/project/{project_id}/secrets", api_key=api_key)
    print(
        f"get secrets for project: {project_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()["data"]


def get_user_achievements(user_firebase_id: str, api_key: str = None) -> dict:
    response = api_call("GET", f"/v1/user/{user_firebase_id}/achievements", api_key=api_key)
    print(
        f"get achievements for user: {user_firebase_id}, "
        f"response: {str(response.status_code)}, duration: {str(response.elapsed.total_seconds())}"
    )
    return response.json()
