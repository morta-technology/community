"""
This class was done to easily access Viewpoint API Endpoints via the Passthrough method

Important point: to set up new functions, add the text 'Token=$token$' as a URL param or a JSON Param
In the backend, Morta will replace this with an actual token from the CDE
"""

# packages
import json
import requests
import pandas as pd

# custom packages
import library.python.morta.api as ma
import library.python.pandas.functions as pf

SOURCE_SYSTEM = "viewpoint"
PREFIX_URL = "https://api.4projects.com/api"


def get_enterprises(select: list = [], api_key: str = None):
    # prepare
    if len(select) == 0:
        select = ["id", "name"]
    select_str = ",".join(select)

    # make api call
    method = "GET"
    endpoint = (
        f"{PREFIX_URL}/QueryList?token=$token$"
        f"&contextId=FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"
        f"&select={select_str}"
        f"&resource=ENTERPRISE"
        f"&recursiveSearch=true"
        f"&latestRevisionOnly=true"
        "&orderBy=nameornumber"
        "&orderDirection=ascending"
    )
    response = ma.passthrough(method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, api_key=api_key)
    return response


def get_projects(enterprise_id: str, select: list = [], as_dataframe: bool = False, api_key: str = None):
    # prepare
    if len(select) == 0:
        select = ["id", "name"]

    # make api call
    response = get_resources(
        context_id=enterprise_id, select=select, resource="PROJECT", as_dataframe=as_dataframe, api_key=api_key
    )
    return response


def get_folders(project_id: str, select: list = [], as_dataframe: bool = False, api_key: str = None):
    # prepare
    if len(select) == 0:
        select = ["id", "name"]

    # make api call
    response = get_resources(
        context_id=project_id, select=select, resource="DOCUMENTFOLDER", as_dataframe=as_dataframe, api_key=api_key
    )
    return response


def get_documents(
    project_id: str,
    select: list = [],
    latest_revision_only: str = "true",
    as_dataframe: bool = False,
    api_key: str = None,
):
    # prepare
    if len(select) == 0:
        select = [
            "ID",
            "RevisionID",
            "Name",
            "Description",
            "CreatedBy",
            "Extension",
            "RevisionRef",
            "Size",
            "State",
            "AuthoredDate",
            "DateCreated",
            "DateModified",
            "DueDate",
            "DateCheckedOut",
            "DateCheckOutExpires",
            "DateFlaggedOut",
            "DateFlagOutExpires",
            "NoDueDate",
            "RevisionDateCreated",
            "Status",
            "OwnerOrganisationName",
            "HeaderIsWorkflowOverdue",
            "IsWorkflowOverdue",
            "WorkflowName",
            "HasMarkups",
            "ActivityStatusName",
            "NodeType",
            "NodeName",
            "DocumentShortCode",
            "RevisionShortCode",
            "FullPath",
            "IsCurrentRevision",
            "ActionTo",
            "uniclass",
            "keywords",
        ]

    # make api call
    response = get_resources(
        context_id=project_id,
        select=select,
        resource="DOCUMENT",
        latest_revision_only=latest_revision_only,
        as_dataframe=as_dataframe,
        api_key=api_key,
    )
    return response


def get_document(document_id: str, api_key: str = None):
    method = "GET"
    endpoint = f"{PREFIX_URL}/Document/{document_id}?Token=$token$"
    response = ma.passthrough(method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, api_key=api_key)
    return response


def get_revision(document_id: str, revision_id: str, api_key: str = None):
    method = "GET"
    endpoint = f"{PREFIX_URL}/Revision/{document_id}/{revision_id}?Token=$token$"
    response = ma.passthrough(method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, api_key=api_key)
    return response


def get_file(document_id: str, revision_id: str, file_id: str, api_key: str = None):
    method = "GET"
    endpoint = f"{PREFIX_URL}/RevisionFile/{document_id}/{revision_id}/{file_id}?Token=$token$&ParentID={revision_id}"
    response = ma.passthrough_download(method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, api_key=api_key)
    return response


def get_resources(
    context_id: str,
    select: list,
    resource: str,
    recursive_search: str = "true",
    latest_revision_only: str = "true",
    search_id: str = None,
    on_or_after_date: str = None,
    on_or_before_date: str = None,
    as_dataframe: bool = False,
    api_key: str = None,
) -> requests.Response:
    """
    context_id can be different things depending on the context:
    - if you are getting projects, context_id is the enterprise_id
    - if you are getting documents, context_id is the folder_id
    - etc

    resource can be one of the below:
    ---------------------------------
    BIDCONTAINER
    BIMCONTAINER
    CALENDERITEM
    CONTRACT
    DISCUSSIONFOLDER
    DISCUSSIONITEM
    DISTRIBUTIONGROUP
    DOCUMENT
    DOCUMENTFOLDER
    ENTERPRISE
    FORM
    FORMCONTAINER
    FORMTASK
    ISSUE
    JOURNAL
    MARKUP
    MILESTONEMANAGER
    NOTIFICATIONTRANSMITTAL
    ORGANISATION
    PROFILE
    PROJECT
    REVISABLESTATICVIRTUALCONTAINER
    SECURITYGROUP
    SITE
    TASKFOLDER
    TASKITEM
    UNREGISTEREDDOCUMENT
    VIRTUALCONTAINER

    select can be one of the below:
    -------------------------------
    ActionTo
    ActivityStatusColor
    ActivityStatusName
    Address
    AssignedToName
    Author
    AuthoredDate
    AuthorID
    AuthorOrganisationName
    AutoName
    AutoNamingCode
    BlockedBy
    CheckedOutToId
    CreatedBy
    CurrentNodeCanMarkup
    CurrentNodeIsDocControler
    CurrentNodeIsEditable
    CurrentNodeIsRevisable
    CurrentProfileHasRead
    CustomFields
    DateCheckedOut
    DateCheckOutExpires
    DateCreated
    DateFlaggedOut
    DateFlagOutExpires
    DateModified
    Description
    DisplayFileTypes
    DocumentNumber
    DocumentShortCode
    DocumentStateNotes
    DueDate
    EffectiveComponentBitMask
    Email
    EnterpriseName
    Extension
    Fax
    FileCount
    FileID
    Filename
    FlaggedOutToId
    FullPath
    GeoTag
    HasAttachments
    HasBeenRead
    HasBimSaveViews
    HasMarkups
    HeaderActivityStatusColor
    HeaderActivityStatusName
    HeaderAttachments
    HeaderCreatedById
    HeaderCurrentNodeCanMarkup
    HeaderCurrentNodeIsDocController
    HeaderCurrentNodeIsEditable
    HeaderCurrentNodeIsRevisable
    HeaderIsInAVisibileWFState
    HeaderIsWorkflowOverdue
    HeaderItemCreatedById
    HeaderItemHasBeenRead
    HeaderItemIsInAVisibleWFState
    HeaderLocationId
    ID
    Industry
    IsAMandatoryRecipient
    IsARecipient
    IsCurrentRevision
    IsLinkedItem
    IsPrivate
    IsPublic
    IsWorkflowOverdue
    ItemAttachments
    ItemStatusDescription
    ItemStatusID
    ItemType
    LastPostedBy
    MarkedAsRead
    MimeType
    Mobile
    ModifiedBy
    Name
    NodeName
    NodeType
    NoDueDate
    NoForwarding
    NoOfPosts
    NoOfThreads
    OwnerId
    OwnerName
    OwnerOrganisationId
    OwnerOrganisationName
    PanelMemberType
    ParentId
    ParentName
    PercentageComplete
    PrimaryKey
    Priority
    RevisionCreatedByOrganisation
    RevisionDateCreated
    RevisionDescription
    RevisionID
    RevisionRef
    RevisionShortCode
    RevisionStateChangedBy
    SequenceNumber
    Size
    State
    Status
    Thumb
    WorkflowID
    WorkflowName
    """
    # 32617 is the maximum number of records fetched via the QueryListPaging endpoint
    # however, we will decrease it to 500 so that the API calls are faster and we
    # do not overload Viewpoint backend and also for the passthrough request in Morta
    # not to time out
    MAX_RECORDS = 500

    rows = []
    index = 1
    method = "GET"
    fetch_next_page = True
    select_str = ",".join(select)

    while fetch_next_page:
        # prepare endpoint
        endpoint = (
            f"{PREFIX_URL}/QueryListPaging?Token=$token$"
            f"&contextId={context_id}"
            f"&select={select_str}"
            f"&resource={resource}"
            f"&recursiveSearch={recursive_search}"
            f"&latestRevisionOnly={latest_revision_only}"
            f"&currentIndex={str(index)}"
            f"&fetch={str(MAX_RECORDS)}"
            "&orderBy=nameornumber"
            "&orderDirection=ascending"
        )

        if search_id:
            endpoint = f"{endpoint}&searchId={search_id}"
        if on_or_after_date:
            endpoint = f"{endpoint}&filter=ModifiedOnOrAfter='{on_or_after_date}'"
        if on_or_before_date:
            endpoint = f"{endpoint}&filter=ModifiedOnOrBefore='{on_or_before_date}'"

        # make api call
        response = ma.passthrough(method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, api_key=api_key)
        response_json = response.json()

        # check if successfull
        message = response_json["data"]["body"]["OperationResults"][0]["Message"]
        if message != "Operation Successful":
            operation_results = response_json["data"]["body"]["OperationResults"]
            operation_results = json.dumps(operation_results, indent=4, sort_keys=True)
            exception_message = (
                "Operation not successful\n\n"
                f"method:\n{method}\n\n"
                f"url:\n{endpoint}\n\n"
                f"Operation Results:\n{operation_results}\n\n"
            )
            raise Exception(exception_message)

        # append rows to result
        total_number_of_rows = response_json["data"]["body"]["TotalRecords"]
        rows = rows + response_json["data"]["body"]["QueryListResponseInfo"]["Rows"]

        # if total number of rows has been reached, end the while loop
        if total_number_of_rows > len(rows):
            index = index + MAX_RECORDS
        else:
            fetch_next_page = False

    # format rows as [{"column1": "value1",...},...]
    rows = [row["Fields"] for row in rows]
    rows = [{field["Name"]: field["Value"] for field in row} for row in rows]

    # convert to dataframe to clean uniclass and keywords columns
    df = pd.DataFrame(data=rows)

    # format uniclass and keywords columns
    if "Uniclass" in df.columns.values:
        df["Uniclass"] = df["Uniclass"].apply(
            lambda x: (
                list(set(x.replace('["', "").replace('"]', "").replace('"', "").split(","))) if x != "[]" else None
            )
        )

    if "Keywords" in df.columns.values:
        df["Keywords"] = df["Keywords"].apply(lambda x: x if x != "[]" else None)

    # return
    if as_dataframe:
        return df
    else:
        rows = pf.dataframe_to_list(input_df=df)
        return rows


def create_document(data: dict, api_key: str = None):
    """
    sample data:
    data = json.dumps({
            "Name": "PASSTHROUGH-neww",
            "Description": "ABC",
            "ParentID": "0641e9ba-836f-42cc-9335-392120518326",
            "RevisionReference": "P01",
            })
    """
    # make api call
    method = "POST"
    endpoint = "https://api-uk.vfp.viewpoint.com/vfp/api/v1/documents"
    headers = {
            "Authorization": "Bearer $token$",
            "Content-Type": "application/json",
            "Accept": "application/json"
            }

    response = ma.passthrough(
        method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, data=data, headers=headers, api_key=api_key
    )
    return response


def create_new_revision(document_id: str, status_id: str, status_name: str, api_key: str = None):
    method = "POST"
    endpoint = f"{PREFIX_URL}/revision"
    body = {
        "Request": {
            "Token": "$token$",
            "RevisionInfos": [
                {
                    "ParentID": document_id,
                    "Status": {"ID": status_id, "Name": status_name},
                }
            ],
        }
    }
    response = ma.passthrough(method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, data=body, api_key=api_key)
    return response


def upload_file_to_document_revision(
    document_id: str, revision_id: str, file_name: str, is_primary_file: str, bytes_object: bytes, api_key: str = None
) -> dict:
    method = "POST"
    endpoint = (
        f"{PREFIX_URL}/RevisionFile/{document_id}/{revision_id}?FileName={file_name}"
        f"&IsPrimaryFile={is_primary_file}&Token=$token$"
    )
    headers = {"Content-Type": "application/octet-stream"}
    response = ma.passthrough(
        method=method, source_system=SOURCE_SYSTEM, endpoint=endpoint, headers=headers, api_key=api_key
    )
    return response
