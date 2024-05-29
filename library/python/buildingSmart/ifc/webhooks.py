# packages
import time
import json
import requests
import traceback
import pandas as pd

# from repo
import library.python.morta.api as ma
import library.python.morta.functions as mf
import library.python.pandas.functions as pf
import library.python.buildingSmart.ifc.functions as ifc_functions


EXTRACT_ENDPOINT = "https://services.morta.io/ifc/tool/viewpoint/extract"
WRITE_ENDPOINT = "https://services.morta.io/ifc/tool/viewpoint/write"


def process_cell_update(webhook_response: dict):
    updates = []
    table_id = webhook_response["contextTableId"]
    table = ma.get_table(table_id=table_id)
    columns = table["columns"]
    column_names = [col["name"] for col in columns]
    changed_cells = webhook_response["context"]["cells"]

    if "Changed?" in column_names:
        for changed_cell in changed_cells:
            changed_column = changed_cell["column"]["name"]
            row = changed_cell["row"]
            row_id = row["publicId"]

            if changed_column != "Changed?":
                updates.append({"columnName": "Changed?", "rowId": row_id, "value": "Yes"})

    ma.update_cells(table_id=table_id, cells=updates)


def extract_request_process_row_add(webhook_response: dict):
    post_extraction_request(webhook_response=webhook_response)


def post_extraction_request(webhook_response: dict):
    payload = json.dumps(webhook_response)
    headers = {"Content-Type": "application/json"}
    response = requests.request(method="POST", url=EXTRACT_ENDPOINT, headers=headers, data=payload)
    if response.status_code != 200:
        raise Exception(f"response code: {str(response.status_code)}")


def extract_next_file(webhook_response: dict, testing: bool):
    try:
        # get tables
        table_id = webhook_response["contextTableId"]
        project_id = webhook_response["contextProjectId"]

        # define table names and ids
        column_mapping_table_name = "IFC Data Columns"
        viewpoint_table_name = "Viewpoint - Documents"
        ifc_entities_table_name = "IFC - Entites to Extract"
        table_names = [column_mapping_table_name, viewpoint_table_name, ifc_entities_table_name]
        table_mapping = ifc_functions.get_tables_mapping(project_id=project_id, table_names=table_names)
        column_mapping_table_id = table_mapping[column_mapping_table_name]
        viewpoint_table_id = table_mapping[viewpoint_table_name]
        ifc_entities_table_id = table_mapping[ifc_entities_table_name]

        # get viewpoint data for extraction
        viewpoint_views = ma.get_table_views(table_id=viewpoint_table_id)
        for_extraction_view_name = "For Extraction"
        for_extraction_view_id = ""
        for view in viewpoint_views:
            if view["name"] == for_extraction_view_name:
                for_extraction_view_id = view["publicId"]

        if for_extraction_view_id == "":
            raise Exception(
                f"Count not find view: '{for_extraction_view_name}' in "
                f"viewpoint table id {viewpoint_table_id} in project: {project_id}"
            )

        for_extraction_rows = ma.get_view_rows(view_id=for_extraction_view_id)

        # get api token
        secrets = ma.get_project_secrets(project_id=project_id)
        secret_name = "API_KEY"
        api_key = None
        for secret in secrets:
            if secret["name"] == secret_name:
                api_key = secret["value"]

        if not api_key:
            raise Exception(f"Could not find secret with name {secret_name} in project {project_id}")

        # get new table type
        variables = ma.get_variables(project_id=project_id)
        variable_name = "​IFC Schedules"
        new_table_type = mf.get_variable_by_name(variable_name=variable_name, variables=variables)["id"]

        # get admin tag
        tags = ma.get_tags(project_id=project_id)
        tag_name = "Global Morta Admin"
        admin_tag_id = mf.get_tag_by_name(tag_name=tag_name, tags=tags)["id"]

        # check if there are rows for extraction
        if len(for_extraction_rows) == 0:
            webhook_rows = webhook_response["context"]["rows"]
            webhook_row = webhook_rows[0]
            webhook_row_id = webhook_row["publicId"]
            updates = [{"columnName": "Processed", "rowId": webhook_row_id, "value": "Yes"}]
            ma.update_cells(table_id=table_id, cells=updates)
            return
        else:
            for_extraction_row = for_extraction_rows[0]
            for_extraction_row_id = for_extraction_row["publicId"]
            row_data = for_extraction_row["rowData"]
            document_id = row_data["Document ID"]
            revision_id = row_data["Revision ID"]
            ifc_file_name = row_data["Name + Revision"]

            # get the file file
            ifc_file = ifc_functions.get_viewpoint_file(
                document_id=document_id, revision_id=revision_id, api_key=api_key
            )

            # get the ifc_entities
            filters = [{"columnName": "Extract", "value": True, "filterType": "eq", "orGroup": "main"}]
            ifc_entity_rows = ma.get_table_rows(table_id=ifc_entities_table_id, filters=filters)
            if len(ifc_entity_rows) == 0:
                raise Exception(f"No ifc types for extraction in project {project_id} in table {ifc_entities_table_id}")
            ifc_types = [row["rowData"]["IFC Entity"] for row in ifc_entity_rows]

            # extract the ifc file
            result_df = ifc_functions.extract_ifc_file(ifc_file=ifc_file, ifc_types=ifc_types)

            result_df["Revision Id"] = revision_id
            if "Attributes - GlobalId" not in result_df.columns.values:
                result_df["Attributes - GlobalId"] = None
            else:
                result_df = result_df.fillna(value={"GlobalId": ""})
                result_df["Attributes - GlobalId"] = result_df["Attributes - GlobalId"].apply(
                    lambda x: None if x == "" else x
                )

            result_df["Key"] = result_df.apply(lambda row: ifc_functions.create_key(row=row), axis=1)

            # remove duplicates
            """
            Duplicates might occur:
            For example, if you get the "IfcAirTerminalType" elements in the Ifc
            and then get the "IfcFlowTerminalType" elements, they will be the same exact list
            because IfcFlowTerminalType is a subtype of IfcAirTerminalType
            """
            result_df = result_df.drop_duplicates(subset=["Key"])

            # get header data
            header_row = ifc_functions.get_header_data(ifc_file=ifc_file, model_name=ifc_file_name)
            header_df = pd.DataFrame(data=[header_row])
            header_df["Revision Id"] = revision_id

            if not result_df.empty:
                ifc_data_table_name = "IFC Data"
                ifc_data_table_id = ""
                tables = ma.get_tables(project_id=project_id)
                for table in tables:
                    if table["name"] == ifc_data_table_name:
                        ifc_data_table_id = table["publicId"]

                if ifc_data_table_id == "":
                    # create a new table
                    ifc_data_table_id = ifc_functions.create_ifc_table(
                        project_id=project_id,
                        columns=list(result_df.columns.values),
                        new_table_type=new_table_type,
                        admin_tag_id=admin_tag_id,
                        column_mapping_table_id=column_mapping_table_id,
                        table_name="IFC Data",
                        add_globalid_column=True,
                    )

                # check if all column names are in table
                ifc_functions.upsert_column_mapping(
                    table_id=ifc_data_table_id, column_mapping_table_id=column_mapping_table_id
                )
                column_mapping_rows = ma.get_table_rows(table_id=column_mapping_table_id)
                column_mapping = {
                    row["rowData"]["Original Column Name"]: row["rowData"]["ColumnId"] for row in column_mapping_rows
                }
                missing_columns = []
                for column_name in result_df.columns.values:
                    if column_name not in column_mapping:
                        missing_columns.append(column_name)

                ifc_functions.create_missing_columns(
                    table_id=ifc_data_table_id,
                    column_names=missing_columns,
                    column_mapping_table_id=column_mapping_table_id,
                )

                column_mapping_rows = ma.get_table_rows(table_id=column_mapping_table_id)
                column_mapping = {
                    row["rowData"]["Original Column Name"]: row["rowData"]["ColumnId"]
                    for row in column_mapping_rows
                    if row["rowData"]["Table ID"] == ifc_data_table_id
                }
                ifc_data_table = ma.get_table(table_id=ifc_data_table_id)
                ifc_data_table_columns = ifc_data_table["columns"]
                ifc_data_table_column_mapping = {
                    column["publicId"]: column["name"] for column in ifc_data_table_columns
                }

                result_df = result_df.rename(columns=column_mapping)
                result_df = result_df.rename(columns=ifc_data_table_column_mapping)

                # insert rows
                result_df = ifc_functions.check_datatypes(df=result_df, ifc_data_table_id=ifc_data_table_id)
                ifc_data_rows = pf.dataframe_to_morta_rows(input_df=result_df)
                filters = [
                    {
                        "columnName": ifc_data_table_column_mapping[column_mapping["Revision Id"]],
                        "value": revision_id,
                        "filterType": "eq",
                        "orGroup": "main",
                    }
                ]
                mf.delete_rows_by_filter(table_id=ifc_data_table_id, filters=filters)
                ma.insert_rows(table_id=ifc_data_table_id, rows=ifc_data_rows)

                # header
                ifc_header_table_name = "IFC Header"
                ifc_header_table_id = ""
                tables = ma.get_tables(project_id=project_id)
                for table in tables:
                    if table["name"] == ifc_header_table_name:
                        ifc_header_table_id = table["publicId"]

                if ifc_header_table_id == "":
                    # create a new table
                    ifc_header_table_id = ifc_functions.create_ifc_table(
                        project_id=project_id,
                        columns=list(header_df.columns.values),
                        new_table_type=new_table_type,
                        admin_tag_id=admin_tag_id,
                        column_mapping_table_id=column_mapping_table_id,
                        table_name="IFC Header",
                        add_globalid_column=False,
                    )

                # check if all column names are in table
                ifc_functions.upsert_column_mapping(
                    table_id=ifc_header_table_id, column_mapping_table_id=column_mapping_table_id
                )
                column_mapping_rows = ma.get_table_rows(table_id=column_mapping_table_id)
                column_mapping = {
                    row["rowData"]["Original Column Name"]: row["rowData"]["ColumnId"]
                    for row in column_mapping_rows
                    if row["rowData"]["Table ID"] == ifc_header_table_id
                }
                missing_columns = []
                for column_name in header_df.columns.values:
                    if column_name not in column_mapping:
                        missing_columns.append(column_name)

                ifc_functions.create_missing_columns(
                    table_id=ifc_header_table_id,
                    column_names=missing_columns,
                    column_mapping_table_id=column_mapping_table_id,
                )

                column_mapping_rows = ma.get_table_rows(table_id=column_mapping_table_id)
                column_mapping = {
                    row["rowData"]["Original Column Name"]: row["rowData"]["ColumnId"]
                    for row in column_mapping_rows
                    if row["rowData"]["Table ID"] == ifc_header_table_id
                }
                ifc_header_table = ma.get_table(table_id=ifc_header_table_id)
                ifc_header_table_columns = ifc_header_table["columns"]
                ifc_header_table_columns_mapping = {
                    column["publicId"]: column["name"] for column in ifc_header_table_columns
                }

                header_df = header_df.rename(columns=column_mapping)
                header_df = header_df.rename(columns=ifc_header_table_columns_mapping)

                # insert rows
                header_df = ifc_functions.check_datatypes(df=header_df, ifc_data_table_id=ifc_header_table_id)
                ifc_header_rows = pf.dataframe_to_morta_rows(input_df=header_df)
                filters = [
                    {
                        "columnName": ifc_header_table_columns_mapping[column_mapping["Revision Id"]],
                        "value": revision_id,
                        "filterType": "eq",
                        "orGroup": "main",
                    }
                ]
                mf.delete_rows_by_filter(table_id=ifc_header_table_id, filters=filters)
                ma.insert_rows(table_id=ifc_header_table_id, rows=ifc_header_rows)

            updates = [{"columnName": "Extract IFC?", "rowId": for_extraction_row_id, "value": None}]
            ma.update_cells(table_id=viewpoint_table_id, cells=updates)

        if not testing:
            send_extraction_success_email(email=webhook_response["user"]["email"], ifc_file_name=ifc_file_name)

        time.sleep(1)
        post_extraction_request(webhook_response=webhook_response)
    except Exception:
        if not testing:
            send_extraction_failure_email(email=webhook_response["user"]["email"])
        raise Exception(traceback.format_exc())


def write_request_process_row_add(webhook_response: dict):
    post_write_request(webhook_response=webhook_response)


def post_write_request(webhook_response: dict):
    payload = json.dumps(webhook_response)
    headers = {"Content-Type": "application/json"}
    response = requests.request(method="POST", url=WRITE_ENDPOINT, headers=headers, data=payload)
    if response.status_code != 200:
        raise Exception(f"response code: {str(response.status_code)}")


def write_next_file(webhook_response: dict, testing: bool):
    try:
        # get tables
        table_id = webhook_response["contextTableId"]
        project_id = webhook_response["contextProjectId"]

        # define table names and ids
        column_mapping_table_name = "IFC Data Columns"
        viewpoint_table_name = "Viewpoint - Documents"
        ifc_data_table_name = "IFC Data"
        table_names = [column_mapping_table_name, viewpoint_table_name, ifc_data_table_name]
        table_mapping = ifc_functions.get_tables_mapping(project_id=project_id, table_names=table_names)
        column_mapping_table_id = table_mapping[column_mapping_table_name]
        viewpoint_table_id = table_mapping[viewpoint_table_name]
        ifc_data_table_id = table_mapping[ifc_data_table_name]

        # get ifc files for writing
        viewpoint_views = ma.get_table_views(table_id=viewpoint_table_id)
        for_write_view_name = "For Writing"
        for_write_view_id = ""
        for view in viewpoint_views:
            if view["name"] == for_write_view_name:
                for_write_view_id = view["publicId"]

        if for_write_view_id == "":
            raise Exception(
                f"Count not find view: '{for_write_view_name}' in "
                f"viewpoint table id {viewpoint_table_id} in project: {project_id}"
            )

        for_writing_rows = ma.get_view_rows(view_id=for_write_view_id)

        # get api token
        secrets = ma.get_project_secrets(project_id=project_id)
        api_key_secret_name = "API_KEY"
        user_name_secret_name = "VIEWPOINT_USER"
        user_password_secret_name = "VIEWPOINT_PASSWORD"
        api_key = None
        user_name = None
        password = None
        for secret in secrets:
            if secret["name"] == api_key_secret_name:
                api_key = secret["value"]
            elif secret["name"] == user_name_secret_name:
                user_name = secret["value"]
            elif secret["name"] == user_password_secret_name:
                password = secret["value"]

        if not api_key:
            raise Exception(f"Could not find secret with name {api_key_secret_name} in project {project_id}")

        # check if there are rows for extraction
        if len(for_writing_rows) == 0:
            webhook_rows = webhook_response["context"]["rows"]
            webhook_row = webhook_rows[0]
            webhook_row_id = webhook_row["publicId"]
            updates = [{"columnName": "Processed", "rowId": webhook_row_id, "value": "Yes"}]
            ma.update_cells(table_id=table_id, cells=updates)
            return
        else:
            for_writing_row = for_writing_rows[0]
            for_writing_row_id = for_writing_row["publicId"]
            row_data = for_writing_row["rowData"]
            document_id = row_data["Document ID"]
            revision_id = row_data["Revision ID"]
            file_name = row_data["Name"]
            document_name = f"{file_name}.ifc"

            # get the file file
            ifc_file = ifc_functions.get_viewpoint_file(
                document_id=document_id, revision_id=revision_id, api_key=api_key
            )

            # modify ifc_file
            updates = []
            filters = [
                {"columnName": "Changed?", "value": "yes", "filterType": "contains", "orGroup": "main"},
                {"columnName": "Revision Id", "value": revision_id, "filterType": "eq", "orGroup": "main"},
            ]
            ifc_data_table_rows = ma.get_table_rows(table_id=ifc_data_table_id, filters=filters)

            # rename columns back to original column names
            column_mapping_rows = ma.get_table_rows(table_id=column_mapping_table_id)
            original_column_name_mapping = {
                row["rowData"]["ColumnId"]: row["rowData"]["Original Column Name"] for row in column_mapping_rows
            }
            ifc_data_table = ma.get_table(table_id=ifc_data_table_id)
            ifc_data_table_columns = ifc_data_table["columns"]
            ifc_data_table_column_mapping = {column["publicId"]: column["name"] for column in ifc_data_table_columns}

            column_mapping = {}
            for column_id1, original_column_name in original_column_name_mapping.items():
                for column_id2, current_column_name in ifc_data_table_column_mapping.items():
                    if column_id1 == column_id2:
                        column_mapping[current_column_name] = original_column_name

            for row in ifc_data_table_rows:
                row_id = row["publicId"]
                old_row_data: dict = row["rowData"]
                row_data: dict = {}

                # replace column names
                for current_column_name in old_row_data.keys():
                    if current_column_name in column_mapping:
                        row_data[column_mapping[current_column_name]] = old_row_data[current_column_name]

                guid = row_data["Attributes - GlobalId"]

                try:
                    element = ifc_file.by_guid(guid=guid)
                except Exception:
                    continue

                for column, value in row_data.items():
                    if " - " in column:
                        pset_name = column.split(" - ")[0]
                        prop_name = column.split(" - ")[1]

                        if pset_name == "Attributes" and prop_name not in ["id", "type", "GlobalId"] and value:
                            if value[0:1] != "#":
                                try:
                                    exec(f"element.{prop_name} = value")
                                except Exception as c:
                                    if "Expected Double" in str(c) or "setArgumentAsDouble" in str(c):
                                        exec(f"element.{prop_name} = float(value)")
                                    elif "Expected Int" in str(c) or "setArgumentAsInt" in str(c):
                                        exec(f"element.{prop_name} = int(value)")
                                    elif "Expected Boolean" in str(c):
                                        exec(f"element.{prop_name} = float(value)")
                                    else:
                                        print("stop")

                        else:
                            definitions = (
                                element.IsDefinedBy if hasattr(element, "IsDefinedBy") and element.IsDefinedBy else ()
                            )
                            property_sets = (
                                element.HasPropertySets
                                if hasattr(element, "HasPropertySets") and element.HasPropertySets
                                else ()
                            )
                            definitions = definitions + property_sets

                            for definition in definitions:
                                # for ifc elements, there is something called RelatingPropertyDefinition
                                # under which, there's another thing called HasProperties
                                # under which we find the properties
                                if hasattr(definition, "RelatingPropertyDefinition"):
                                    relating_prop = definition.RelatingPropertyDefinition
                                # for ifc types, the HasProperties is directly under the definition
                                else:
                                    relating_prop = definition

                                if hasattr(relating_prop, "HasProperties"):
                                    if relating_prop.Name == pset_name:
                                        props = relating_prop.HasProperties
                                        for prop in props:
                                            if len(ifc_file.get_inverse(prop)) > 1:
                                                continue
                                            if prop.Name == prop_name:
                                                if hasattr(prop, "NominalValue"):
                                                    if "IfcReal" in str(prop.NominalValue):
                                                        prop.NominalValue.wrappedValue = float(value) if value else None
                                                    else:
                                                        try:
                                                            prop.NominalValue.wrappedValue = value
                                                        except Exception as c:
                                                            if "Expected Double" in str(
                                                                c
                                                            ) or "setArgumentAsDouble" in str(c):
                                                                prop.NominalValue.wrappedValue = float(value)
                                                            elif "Expected Int" in str(c) or "setArgumentAsInt" in str(
                                                                c
                                                            ):
                                                                prop.NominalValue.wrappedValue = int(value)
                                                            else:
                                                                print("stop")

                updates.append({"columnName": "Changed?", "rowId": row_id, "value": None})

            ifc_text = ifc_file.to_string()
            bytes_object = bytes(ifc_text, encoding="utf-8")

            # upload to viewpoint
            ifc_functions.upload_document_to_viewpoint(
                document_id=document_id,
                document_name=document_name,
                bytes_object=bytes_object,
                user_name=user_name,
                password=password,
                api_key=api_key,
            )

            ma.update_cells(table_id=ifc_data_table_id, cells=updates)

            # update Write back column
            updates = [{"columnName": "Write new revision?", "rowId": for_writing_row_id, "value": None}]
            ma.update_cells(table_id=viewpoint_table_id, cells=updates)

        send_writing_success_email(email=webhook_response["user"]["email"], ifc_file_name=file_name)

        time.sleep(1)
        if not testing:
            post_extraction_request(webhook_response=webhook_response)
    except Exception:
        if not testing:
            send_writing_failure_email(email=webhook_response["user"]["email"])
        raise Exception(traceback.format_exc())


def send_extraction_failure_email(email: str):
    subject = "Issue Extracting IFC"
    html = """
<html>
<head>
</head>
<body>Dears,
<br>
<br>Please note that an issue has occurred while extracting an IFC File.
<br>Apologies for any inconveniences. Morta team will get back to you as soon as possible.
<br>
<br>Regards
</body>
</html>
"""
    # you can add your own send email code here
    print(subject)
    print(html)


def send_extraction_success_email(email: str, ifc_file_name: str):
    subject = "IFC Extraction Completed"
    html = f"""
<html>
<head>
</head>
<body>Dears,
<br>
<br>Please note that the file {ifc_file_name} has been extracted.
<br>
<br>Regards
</body>
</html>
"""
    # you can add your own send email code here
    print(subject)
    print(html)


def send_writing_failure_email(email: str):
    subject = "Issue Writing IFC"
    html = """
<html>
<head>
</head>
<body>Dears,
<br>
<br>Please note that an issue has occurred while writing a new IFC File.
<br>Apologies for any inconveniences. Morta team will get back to you as soon as possible.
<br>
<br>Regards
</body>
</html>
"""
    # you can add your own send email code here
    print(subject)
    print(html)


def send_writing_success_email(email: str, ifc_file_name: str):
    subject = "IFC Writing Completed"
    html = f"""
<html>
<head>
</head>
<body>Dears,
<br>
<br>Please note that a new version of the file {ifc_file_name} has been uploaded to your CDE.
<br>
<br>Regards
</body>
</html>
"""
    # you can add your own send email code here
    print(subject)
    print(html)
