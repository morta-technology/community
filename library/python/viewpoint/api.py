# packages
import requests
import traceback
from time import sleep

# custom
import library.python.viewpoint.config as config


def api_call(
    method: str,
    endpoint: str,
    params: dict = None,
    tries: int = 0,
    data: dict = None,
    files: list = None,
) -> requests.Response:
    # checking if the method is one of the accepted values
    assert method in ["GET", "POST", "PUT", "DELETE"], "method should be one of GET, POST, PUT, DELETE"

    dest_url = f"{config.BASE_URL}{endpoint}"
    # print(f"{method}: {dest_url}")

    # try executing the api request. if failed, wait for 1 second and retry if tries < Max number of tries
    # otherwise, raise and exception
    try:
        if method == "GET":
            response = requests.get(url=dest_url, params=params)
        elif method == "POST":
            if data and files:
                response = requests.post(url=dest_url, files=files, data=data)
            elif files:
                response = requests.post(url=dest_url, files=files)
            else:
                response = requests.post(url=dest_url, json=params)
        elif method == "PUT":
            response = requests.put(url=dest_url, json=params)
        elif method == "DELETE":
            response = requests.delete(url=dest_url, json=params)
    except Exception:
        sleep(1)
        tries = tries + 1
        if tries < config.MAX_API_CALL_TRIES:
            return api_call(method=method, endpoint=endpoint, params=params, data=data, files=files, tries=tries)
        else:
            raise Exception(f"Exception:\n{traceback.format_exc()}")

    try:
        response.json()
    except Exception as c:
        tries = tries + 1
        sleep_time = 1
        sleep_time = tries * 2

        print(f"retrying api call. total tries: {str(tries)}")
        if tries <= config.MAX_API_CALL_TRIES:
            return api_call(method=method, endpoint=endpoint, params=params, data=data, files=files, tries=tries)
        else:
            raise Exception(f"{str(c)}\n\n{str(response)}\n\n{endpoint}")

    # if the response code is not 200 or 201:
    if response.status_code != 200 and response.status_code != 201:
        # increase the tries and log the response
        tries = tries + 1
        log_responses(response)
        sleep_time = 1

        # if the response status code is one of 502, 500, 503 and max tries have not been reached yet
        # wait for 1 second, log the response, save the failure and rerun the api call
        # response 429 happens when more than 10 api calls are made on a resource in a second
        if response.status_code in [502, 500, 429, 503] and tries < config.MAX_API_CALL_TRIES:
            # only for the 429 error, we need to incrementally increase the wait time.
            # this might not be enough when concurrent api calls are running
            # but it does de-risk to a certain extent.
            if response.status_code in [429]:
                sleep_time = tries * 2

            sleep(sleep_time)

            print(f"retrying api call. total tries: {str(tries)}")
            return api_call(method=method, endpoint=endpoint, params=params, data=data, files=files, tries=tries)

        # if the response status code is one of 502, 500, 503 and max tries were reached
        # wait for 1 second, save the failure and set the exception message
        # response 429 happens when more than 10 api calls are made on a resource in a second
        elif response.status_code in [502, 500, 429, 503] and tries >= config.MAX_API_CALL_TRIES:
            sleep(1)

            exception_message = (
                "maximum tries reached"
                f"url:\n{dest_url}\n\n"
                f"response content:\n{str(response.content)}\n\n"
                f"response status code:\n{str(response.status_code)}"
            )

        # otherwise, set the exception message
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


# api call to retreive a token
def get_token(user_name: str, password: str, application_id: str) -> str:
    view_point_api_call = f"{config.BASE_URL}/STS"
    body = {"Username": user_name, "Password": password, "ApplicationID": application_id}
    response = requests.post(view_point_api_call, json=body)
    operation = response.json()
    token = operation["IssueResult"]["Token"]
    return token


def upload_file_to_document_revision(
    document_id: str, revision_id: str, file_name: str, is_primary_file: str, token: str, bytes_object: bytes
) -> dict:
    endpoint = (
        f"https://api.4projects.com/API/RevisionFile/{document_id}/{revision_id}?FileName={file_name}"
        f"&IsPrimaryFile={is_primary_file}&Token={token}"
    )
    headers = {"Content-Type": "application/octet-stream"}
    response = requests.post(url=endpoint, headers=headers, data=bytes_object)
    return response.json()


def create_new_revision(token: str, document_id: str, status_id: str, status_name: str):
    endpoint = "https://api.4projects.com/api/revision"
    body = {
        "Request": {
            "Token": token,
            "RevisionInfos": [
                {
                    "ParentID": document_id,
                    "Status": {"ID": status_id, "Name": status_name},
                }
            ],
        }
    }
    response = requests.post(url=endpoint, json=body)
    return response.json()
