# packages
import json
import time
import numpy
import logging
import regex as re
import ifcopenshell
import pandas as pd
from ifcopenshell.util import classification as ifc_classification

# custom
import library.python.morta.api as ma
import library.python.morta.functions as mf
import library.python.pandas.functions as pf
import library.python.viewpoint.api as vp_api
import library.python.viewpoint.config as config
import library.python.morta.passthrough.viewpoint as mva

logger = logging.getLogger("app")


def extract_ifc_files(ifc_files: list, ifc_types: list, file_name_column: str = "File Name") -> pd.DataFrame:
    """
    Purpose
    -------
    Extracts a list of ifc files and returns a pandas dataframe

    Input
    -----
    - ifc_files: [{"fileName": file_name, "ifcFile": ifc_file}]
        - where ifc_file is the result of ifcopenshell.open function
    - ifc_types: list of str. ifc types examples: IfcDoor, IfcWindow, etc.
    """
    # initialize variables
    result_dfs = []

    # loop over files and append their extraction results
    for ifc_file_data in ifc_files:
        file_name = ifc_file_data["fileName"]
        ifc_file = ifc_file_data["ifcFile"]

        current_df = extract_ifc_file(ifc_file=ifc_file, ifc_types=ifc_types)
        current_df.insert(loc=0, column=file_name_column, value=file_name)
        result_dfs.append(current_df)

    # return combined dataframe
    result_df = pd.concat(objs=result_dfs)
    return result_df


def extract_ifc_file(ifc_file: ifcopenshell.file, ifc_types: list) -> pd.DataFrame:
    """
    Purpose
    -------
    Extracts an file and returns a pandas dataframe:
    - spatial containers
    - attributes
    - property sets and properties
    - quantities
    - relating type
    - classification

    Input
    -----
    - ifc_file is the result of ifcopenshell.open function
    - ifc_types: list of str. ifc types examples: IfcDoor, IfcWindow, etc.
    """
    results = []
    # loop over ifc_types
    for ifc_type in ifc_types:
        elements = ifc_file.by_type(type=ifc_type)
        element: ifcopenshell.entity_instance
        for element in elements:
            element_result = extract_element(element=element)
            results.append(element_result)

    return pd.DataFrame(data=results)


def extract_element(
    element: ifcopenshell.entity_instance,
    spatial_container_properties: list = ["Name", "LongName"],
) -> dict:
    # get spatial containers
    spatial_breakdown: dict = get_spatial_containers(
        element=element, properties=spatial_container_properties
    )
    # get attributes
    attributes: dict = get_attributes(element=element)
    # get quantities and property sets and properties
    properties: dict = get_properties(element=element)
    # get classification
    classifications: dict = get_classficiations(element=element)
    # get relating type
    relating_type: dict = get_relating_type(element=element)

    return {
        **spatial_breakdown,
        **relating_type,
        **attributes,
        **properties,
        **classifications,
    }


def get_spatial_containers(
    element: ifcopenshell.entity_instance, properties: list = ["Name", "LongName"]
) -> dict:
    properties = list(reversed(properties))
    result = {}
    container = ifcopenshell.util.element.get_container(element)
    if container:
        info = container.get_info()
        container_type = info["type"].replace("Ifc", "")
        for prop in properties:
            if prop in info:
                result[f"{container_type} - {prop}"] = info[prop]

        parent = ifcopenshell.util.element.get_aggregate(container)
    else:
        parent = ifcopenshell.util.element.get_aggregate(element)

    while parent:
        info = parent.get_info()
        parent_type = info["type"].replace("Ifc", "")
        for prop in properties:
            if prop in info:
                result[f"{parent_type} - {prop}"] = info[prop]

        parent = ifcopenshell.util.element.get_aggregate(parent)

    # reorder keys
    result = dict(reversed(list(result.items())))

    return result


def get_attributes(element: ifcopenshell.entity_instance) -> dict:
    info = element.get_info()
    result = {
        f"Attributes - {key}": value
        for key, value in info.items()
        if numpy.isscalar(element=value)
    }
    return result


def get_properties(element: ifcopenshell.entity_instance) -> dict:
    psets = ifcopenshell.util.element.get_psets(element=element, should_inherit=True)
    results = {}
    for pset_name, properties in psets.items():
        for property_name, value in properties.items():
            results[f"{pset_name} - {property_name}"] = value
    return results


def get_classficiations(element: ifcopenshell.entity_instance) -> dict:
    results = {}
    references = ifc_classification.get_references(element=element)
    for reference in references:
        info = reference.get_info(recursive=True)
        if info["ReferencedSource"]:
            source_name = info["ReferencedSource"]["Name"]
            classification_name = info["Name"]
            item_reference = info["ItemReference"]
            results[f"{source_name} - Name"] = classification_name
            results[f"{source_name} - ItemReference"] = item_reference

    return results


def get_relating_type(
    element: ifcopenshell.entity_instance,
    properties: list = ["type", "Name", "Description", "PredefinedType", "GlobalId"],
) -> dict:
    result = {}
    relating_type = ifcopenshell.util.element.get_type(element=element)
    if relating_type:
        info = relating_type.get_info()
        for prop in properties:
            if prop in info:
                result[f"RelatingType - {prop}"] = info[prop]

    return result


def extract(
    project_id: str,
    new_table_type: str,
    ifc_file: ifcopenshell.file,
    rules_df: pd.DataFrame,
    headers_table_id: str,
    model_name: str,
    admin_tag_id: str,
    contributor_tag_id: str,
    should_truncate_tables: bool,
):
    """
    Purpose
    -------
    This function extracts ifc  elements based on the properties defined in the rules table.

    Steps
    -----
    1. for each element type:
        1.1 check if table exists, if not, create the table based on defined properties and types
        1.2. get elements from model
        1.3. loop over elements and get properties

    Input
    -----
    a list of IfcElements like IfcSlab, etc.
    """
    # check if input is not empty
    if rules_df.empty:
        return
    rules_df = rules_df.dropna(subset=["Attribute/ Property", "Grouping"], how="all")
    rules_df = rules_df.drop_duplicates(
        subset=["Element Type", "Grouping", "Attribute/ Property"]
    )
    if rules_df.empty:
        return

    # get tables and properties
    rules_df["columnName"] = rules_df.apply(
        lambda row: f"{row['Grouping']} - {row['Attribute/ Property']}", axis=1
    )
    table_and_properties = (
        rules_df.groupby(by=["Element Type"])
        .agg(columnName=("columnName", lambda x: sorted(list(set([z for z in x])))))
        .reset_index()
    )
    table_and_properties = pf.dataframe_to_list(input_df=table_and_properties)

    # check tables
    table_mapping = get_table_mapping(
        project_id=project_id,
        table_and_properties=table_and_properties,
        new_table_type=new_table_type,
        admin_tag_id=admin_tag_id,
        contributor_tag_id=contributor_tag_id,
    )

    for element_type, table_id in table_mapping.items():
        # initialize variables
        new_rows = []
        current_rules_df = rules_df.loc[rules_df["Element Type"] == element_type]
        if current_rules_df.empty:
            continue

        # get the elements
        elements = ifc_file.by_type(type=element_type)

        for element in elements:
            row = {}
            row["Model Name"] = model_name

            if hasattr(element, "ContainedInStructure"):
                contained_in = element.ContainedInStructure
                if len(contained_in) > 0:
                    contained_in = contained_in[0]
                    relating_structure = contained_in.RelatingStructure
                    relating_structure_type = relating_structure.get_info()["type"]
                    relating_structure_type_formatted = relating_structure_type.replace(
                        "Ifc", ""
                    )
                    name_componenets = [
                        relating_structure.Name if relating_structure.Name else ""
                    ]
                    if hasattr(relating_structure, "LongName"):
                        if relating_structure.LongName:
                            name_componenets.append(relating_structure.LongName)
                    row[relating_structure_type_formatted] = " - ".join(
                        name_componenets
                    )

                    while len(relating_structure.Decomposes) > 0:
                        relating_structure = relating_structure.Decomposes[
                            0
                        ].RelatingObject
                        relating_structure_type = relating_structure.get_info()["type"]
                        relating_structure_type_formatted = (
                            relating_structure_type.replace("Ifc", "")
                        )
                        name_componenets = [
                            relating_structure.Name if relating_structure.Name else ""
                        ]
                        if hasattr(relating_structure, "LongName"):
                            if relating_structure.LongName:
                                name_componenets.append(relating_structure.LongName)
                        row[relating_structure_type_formatted] = " - ".join(
                            name_componenets
                        )
            else:
                relating_structure = element
                while (
                    hasattr(relating_structure, "Decomposes")
                    and len(relating_structure.Decomposes) > 0
                ):
                    relating_structure = relating_structure.Decomposes[0].RelatingObject
                    relating_structure_type = relating_structure.get_info()["type"]
                    relating_structure_type_formatted = relating_structure_type.replace(
                        "Ifc", ""
                    )
                    name_componenets = [
                        relating_structure.Name if relating_structure.Name else ""
                    ]
                    if hasattr(relating_structure, "LongName"):
                        if relating_structure.LongName:
                            name_componenets.append(relating_structure.LongName)
                    row[relating_structure_type_formatted] = " - ".join(
                        name_componenets
                    )

            for key, value in element.__dict__.items():
                colm_name = f"Attributes - {key}"
                row[colm_name] = str(value)

            definitions = (
                element.IsDefinedBy
                if hasattr(element, "IsDefinedBy") and element.IsDefinedBy
                else ()
            )
            property_sets = (
                element.HasPropertySets
                if hasattr(element, "HasPropertySets") and element.HasPropertySets
                else ()
            )
            definitions = definitions + property_sets

            for definition in definitions:
                if hasattr(definition, "RelatingPropertyDefinition"):
                    relating_pset = definition.RelatingPropertyDefinition
                    if hasattr(relating_pset, "Quantities"):
                        quantities = (
                            relating_pset.Quantities if relating_pset.Quantities else ()
                        )
                        for quantity in quantities:
                            if quantity:
                                colm_name = f"{relating_pset.Name} - {quantity.Name}"
                                if hasattr(quantity, "LengthValue"):
                                    value = quantity.LengthValue
                                    row[colm_name] = value
                                elif hasattr(quantity, "AreaValue"):
                                    value = quantity.AreaValue
                                    row[colm_name] = value
                                elif hasattr(quantity, "VolumeValue"):
                                    value = quantity.VolumeValue
                                    row[colm_name] = value
                    elif hasattr(relating_pset, "HasProperties"):
                        props = (
                            relating_pset.HasProperties
                            if relating_pset.HasProperties
                            else ()
                        )
                        for prop in props:
                            if prop:
                                colm_name = f"{relating_pset.Name} - {prop.Name}"
                                if hasattr(prop, "NominalValue"):
                                    if prop.NominalValue:
                                        value = prop.NominalValue.wrappedValue
                                        row[colm_name] = value

                elif hasattr(definition, "RelatingType"):
                    relating_type_name = str(definition.RelatingType)
                    relating_type_name = relating_type_name[
                        relating_type_name.find("=") + 1 : relating_type_name.find("(")
                    ]
                    row["RelatingType - Element Type"] = relating_type_name
                    row["RelatingType - Name"] = (
                        definition.RelatingType.Name
                        if hasattr(definition.RelatingType, "Name")
                        else None
                    )
                    row["RelatingType - Description"] = (
                        definition.RelatingType.Description
                        if hasattr(definition.RelatingType, "Description")
                        else None
                    )
                    row["RelatingType - GlobalId"] = (
                        definition.RelatingType.GlobalId
                        if hasattr(definition.RelatingType, "GlobalId")
                        else None
                    )
                    row["RelatingType - PredefinedType"] = (
                        definition.RelatingType.PredefinedType
                        if hasattr(definition.RelatingType, "PredefinedType")
                        else None
                    )
                elif hasattr(definition, "HasProperties"):
                    props = definition.HasProperties if definition.HasProperties else ()
                    for prop in props:
                        if prop:
                            colm_name = f"{definition.Name} - {prop.Name}"
                            if hasattr(prop, "NominalValue"):
                                if prop.NominalValue:
                                    value = prop.NominalValue.wrappedValue
                                    row[colm_name] = value
            associations = (
                element.HasAssociations
                if hasattr(element, "HasAssociations") and element.HasAssociations
                else ()
            )

            for association in associations:
                association_type = str(association)
                association_type = association_type[
                    association_type.find("=") + 1 : association_type.find("(")
                ]
                if association_type == "IfcRelAssociatesClassification":
                    row["Classification - Name"] = association.Name
                    row["Classification - ItemReference"] = (
                        association.RelatingClassification.ItemReference
                    )
                    row["Classification - ItemName"] = (
                        association.RelatingClassification.Name
                    )

            new_rows.append(row)

        if should_truncate_tables:
            ma.truncate_table(table_id=table_id)

        if len(new_rows) > 0:
            df = pd.DataFrame(data=new_rows)
            table = ma.get_table(table_id=table_id)
            columns = table["columns"]
            column_names = [column["name"] for column in columns]
            df = df.loc[:, [col for col in df.columns.values if col in column_names]]
            new_rows = pf.dataframe_to_morta_rows(input_df=df)
            filters = [
                {
                    "columnName": "Model Name",
                    "value": model_name,
                    "filterType": "eq",
                    "orGroup": "1",
                }
            ]
            mf.delete_rows_by_filter(table_id=table_id, filters=filters)
            ma.insert_rows(table_id=table_id, rows=new_rows)

    # extract header
    header_rows = get_header_data(ifc_file=ifc_file, model_name=model_name)
    if len(header_rows) > 0:
        filters = [
            {
                "columnName": "Model Name",
                "value": model_name,
                "filterType": "eq",
                "orGroup": "1",
            }
        ]
        mf.delete_rows_by_filter(table_id=headers_table_id, filters=filters)
        if should_truncate_tables:
            ma.truncate_table(table_id=headers_table_id)
        ma.insert_rows(table_id=headers_table_id, rows=header_rows)


def get_table_mapping(
    project_id: str,
    table_and_properties: list,
    new_table_type: str,
    admin_tag_id: str,
    contributor_tag_id: str,
) -> dict:
    """
    Purpose
    -------
    Check if all tables and properties exist on the project
    Note: this checks the table and column names as indicated in the rules table!

    Input
    -----
    a list containing the element types along with the properties

    Steps
    -----
    - for each table:
        - check if table name exists
            if yes, check if columns exist and create missing columns
            if no, create table with given columns

    Output
    ------
    dict:
    - key: element type
    - value: table_id
    """
    # initialize parameters
    tables = ma.get_tables(project_id=project_id)
    mapping = {}

    # loop over tables
    for table in table_and_properties:
        table_name = table["Element Type"]
        not_found = True

        for project_table in tables:
            if project_table["name"] == table_name:
                # add to mapping
                mapping[table_name] = project_table["publicId"]
                not_found = False
                # check columns
                columns = project_table["columns"]
                columns = [col["name"] for col in columns]
                table["columnName"].append("Model Name")
                for needed_column in table["columnName"]:
                    if needed_column not in columns:
                        params = {
                            "name": needed_column,
                            "kind": "text",
                            "width": 240,
                            "locked": False,
                            "sortOrder": len(columns),
                            "required": False,
                        }
                        table = ma.create_column_in_view(
                            view_id=project_table["defaultViewId"], params=params
                        )
                        time.sleep(0.3)

        if not_found:
            if table_name == "IfcProject":
                columns = ["Model Name"]
                view_settings = {
                    "groupSettings": [{"columnName": "Model Name", "direction": "asc"}]
                }
            elif table_name == "IfcSite":
                columns = ["Model Name", "Project"]
                view_settings = {
                    "groupSettings": [
                        {"columnName": "Model Name", "direction": "asc"},
                        {"columnName": "Project", "direction": "asc"},
                    ]
                }
            elif table_name == "IfcBuilding":
                columns = ["Model Name", "Project", "Site"]
                view_settings = {
                    "groupSettings": [
                        {"columnName": "Model Name", "direction": "asc"},
                        {"columnName": "Project", "direction": "asc"},
                        {"columnName": "Site", "direction": "asc"},
                    ]
                }
            elif table_name == "IfcBuildingStorey":
                columns = ["Model Name", "Project", "Site", "Building"]
                view_settings = {
                    "groupSettings": [
                        {"columnName": "Model Name", "direction": "asc"},
                        {"columnName": "Project", "direction": "asc"},
                        {"columnName": "Site", "direction": "asc"},
                        {"columnName": "Building", "direction": "asc"},
                    ]
                }
            elif table_name == "IfcSpace":
                columns = [
                    "Model Name",
                    "Project",
                    "Site",
                    "Building",
                    "BuildingStorey",
                ]
                view_settings = {
                    "groupSettings": [
                        {"columnName": "Model Name", "direction": "asc"},
                        {"columnName": "Project", "direction": "asc"},
                        {"columnName": "Site", "direction": "asc"},
                        {"columnName": "Building", "direction": "asc"},
                        {"columnName": "BuildingStorey", "direction": "asc"},
                    ]
                }
            else:
                columns = [
                    "Model Name",
                    "Project",
                    "Site",
                    "Building",
                    "BuildingStorey",
                    "Space",
                ]
                view_settings = {
                    "groupSettings": [
                        {"columnName": "Model Name", "direction": "asc"},
                        {"columnName": "Project", "direction": "asc"},
                        {"columnName": "Site", "direction": "asc"},
                        {"columnName": "Building", "direction": "asc"},
                        {"columnName": "BuildingStorey", "direction": "asc"},
                        {"columnName": "Space", "direction": "asc"},
                    ]
                }

            columns = columns + table["columnName"]
            if "Attributes - GlobalId" not in columns:
                columns.append("Attributes - GlobalId")
            columns.append("Changed?")
            columns = [{"name": name, "kind": "text", "width": 240} for name in columns]
            new_table = ma.create_table(
                project_id=project_id,
                name=table_name,
                columns=columns,
                table_type=new_table_type,
            )

            # create owner permission
            ma.create_permission(
                resource_kind=ma.ResourceKind.table.value,
                resource_id=new_table["publicId"],
                attribute_kind=ma.AttributeKind.tag.value,
                attribute_identifier=admin_tag_id,
                role=4,
            )

            mapping[table_name] = new_table["publicId"]
            default_view_id = new_table["defaultViewId"]
            view_settings["disableNewRow"] = True
            ma.update_view(view_id=default_view_id, view_params=view_settings)

            # create contribute permission
            ma.create_permission(
                resource_kind=ma.ResourceKind.view.value,
                resource_id=default_view_id,
                attribute_kind=ma.AttributeKind.tag.value,
                attribute_identifier=contributor_tag_id,
                role=2,
            )

    return mapping


def check_rules(
    project_id: str,
    new_table_type: str,
    rules_df: pd.DataFrame,
    values_list_table_id: str,
    admin_tag_id: str,
    contributor_tag_id: str,
) -> pd.DataFrame:
    """
    Steps
    -----
    - get tables of each element type
    - get rows from each table
    - for each row
    - for each rule
    - check if rule pset name and prop name are equal to one in row
    - if yes, conduct checks
    """
    # initialize variables
    acceptable_not_acceptable_values = ma.get_table_rows(table_id=values_list_table_id)
    type_mapping = {
        "str": "text",
        "int": "integer",
        "float": "decimal",
        "bool": "boolean",
        "list": "list",
    }

    if rules_df.empty:
        return
    rules_df = rules_df.dropna(subset=["Attribute/ Property", "Grouping"], how="all")
    rules_df = rules_df.drop_duplicates(
        subset=["Element Type", "Grouping", "Attribute/ Property"]
    )
    if rules_df.empty:
        return

    rules_df["columnName"] = rules_df.apply(
        lambda row: f"{row['Grouping']} - {row['Attribute/ Property']}", axis=1
    )
    table_and_properties = (
        rules_df.groupby(by=["Element Type"])
        .agg(columnName=("columnName", lambda x: sorted(list(set([z for z in x])))))
        .reset_index()
    )
    table_and_properties = pf.dataframe_to_list(input_df=table_and_properties)
    table_mapping = get_table_mapping(
        project_id=project_id,
        table_and_properties=table_and_properties,
        new_table_type=new_table_type,
        admin_tag_id=admin_tag_id,
        contributor_tag_id=contributor_tag_id,
    )

    results = []

    for element_type, table_id in table_mapping.items():
        # initialize variables
        current_rules_df = rules_df.loc[rules_df["Element Type"] == element_type]
        if current_rules_df.empty:
            continue
        rule_rows = pf.dataframe_to_list(input_df=current_rules_df)
        element_rows = ma.get_table_rows(table_id=table_id)

        for element in element_rows:
            element = element["rowData"]
            model_name = element["Model Name"]
            element_guid = element["Attributes - GlobalId"]
            for prop, value in element.items():
                if prop in [
                    "Model Name",
                    "Project",
                    "Building",
                    "BuildingStorey",
                    "Site",
                    "Space",
                    "Changed?",
                ]:
                    continue
                pset_name = prop.split(" - ")[0]
                prop_name = prop.split(" - ")[1]
                for rule in rule_rows:
                    rule_pset_name = rule["Grouping"]
                    rule_prop_name = rule["Attribute/ Property"]
                    if pset_name == rule_pset_name and rule_prop_name == prop_name:
                        rule1 = rule["Should Exist"]
                        rule2 = rule["Should have value"]
                        rule3 = rule["Must be unique"]
                        rule4 = rule["Value Regex Expression"]
                        rule5 = rule["Should be one of the below"]
                        rule6 = rule["Should not be one of below"]
                        rule7 = rule["Data Type"]

                        if rule1:
                            if value == "Not Found":
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should Exist",
                                        "Result": "Fail",
                                    }
                                )
                            else:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should Exist",
                                        "Result": "Pass",
                                    }
                                )
                        else:
                            results.append(
                                {
                                    "Model Name": model_name,
                                    "Element Type": element_type,
                                    "Element ID": element_guid,
                                    "Attribute/ Property": prop,
                                    "Rule": "Should Exist",
                                    "Result": "Check not required",
                                }
                            )

                        if rule2:
                            if value and value != "Not Found":
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should have value",
                                        "Result": "Pass",
                                    }
                                )
                            else:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should have value",
                                        "Result": "Fail",
                                    }
                                )
                        else:
                            results.append(
                                {
                                    "Model Name": model_name,
                                    "Element Type": element_type,
                                    "Element ID": element_guid,
                                    "Attribute/ Property": prop,
                                    "Rule": "Should have value",
                                    "Result": "Check not required",
                                }
                            )

                        if rule3:
                            all_values = [
                                current_element["rowData"][prop]
                                for current_element in element_rows
                                if prop in current_element["rowData"]
                                and current_element["rowData"]["Model Name"]
                                == model_name
                            ]
                            all_values = [
                                current_value
                                for current_value in all_values
                                if current_value == value
                            ]

                            if len(all_values) == 1:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Must by unique",
                                        "Result": "Pass",
                                    }
                                )
                            else:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Must by unique",
                                        "Result": "Fail",
                                    }
                                )
                        else:
                            results.append(
                                {
                                    "Model Name": model_name,
                                    "Element Type": element_type,
                                    "Element ID": element_guid,
                                    "Attribute/ Property": prop,
                                    "Rule": "Must by unique",
                                    "Result": "Check not required",
                                }
                            )

                        if rule4:
                            expression = rule["Value Regex Expression"]
                            if len(expression) > 0 and value:
                                if re.match(expression, value):
                                    results.append(
                                        {
                                            "Model Name": model_name,
                                            "Element Type": element_type,
                                            "Element ID": element_guid,
                                            "Attribute/ Property": prop,
                                            "Rule": "Value Regex Expression",
                                            "Result": "Pass",
                                        }
                                    )
                                else:
                                    results.append(
                                        {
                                            "Model Name": model_name,
                                            "Element Type": element_type,
                                            "Element ID": element_guid,
                                            "Attribute/ Property": prop,
                                            "Rule": "Value Regex Expression",
                                            "Result": "Fail",
                                        }
                                    )
                            else:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Value Regex Expression",
                                        "Result": "Check not required",
                                    }
                                )
                        else:
                            results.append(
                                {
                                    "Model Name": model_name,
                                    "Element Type": element_type,
                                    "Element ID": element_guid,
                                    "Attribute/ Property": prop,
                                    "Rule": "Value Regex Expression",
                                    "Result": "Check not required",
                                }
                            )

                        if rule5:
                            # get list of possible values
                            values_list = [
                                row
                                for row in acceptable_not_acceptable_values
                                if row["rowData"]["Element Type"] == element_type
                                and row["rowData"]["Name"] == value
                            ]
                            values = [
                                value["rowData"]["Value"] for value in values_list
                            ]

                            if value in values:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should be one of the below",
                                        "Result": "Pass",
                                    }
                                )
                            else:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should be one of the below",
                                        "Result": "Fail",
                                    }
                                )
                        else:
                            results.append(
                                {
                                    "Model Name": model_name,
                                    "Element Type": element_type,
                                    "Element ID": element_guid,
                                    "Attribute/ Property": prop,
                                    "Rule": "Should be one of the below",
                                    "Result": "Check not required",
                                }
                            )

                        if rule6:
                            # get list of possible values
                            values_list = [
                                row
                                for row in acceptable_not_acceptable_values
                                if row["rowData"]["Element Type"] == element_type
                                and row["rowData"]["Name"] == value
                            ]
                            values = [
                                value["rowData"]["Value"] for value in values_list
                            ]

                            if value in values:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should not be one of below",
                                        "Result": "Fail",
                                    }
                                )
                            else:
                                results.append(
                                    {
                                        "Model Name": model_name,
                                        "Element Type": element_type,
                                        "Element ID": element_guid,
                                        "Attribute/ Property": prop,
                                        "Rule": "Should not be one of below",
                                        "Result": "Pass",
                                    }
                                )
                        else:
                            results.append(
                                {
                                    "Model Name": model_name,
                                    "Element Type": element_type,
                                    "Element ID": element_guid,
                                    "Attribute/ Property": prop,
                                    "Rule": "Should not be one of below",
                                    "Result": "Check not required",
                                }
                            )

                        if rule7:
                            value_type = str(type(value))
                            value_type = value_type[
                                value_type.find("'") + 1 : value_type.rfind("'")
                            ]
                            value_type = type_mapping[value_type]

                            if value_type == rule7:
                                result = "Fail"
                            else:
                                result = "Pass"
                        else:
                            result = "Check not required"

                        results.append(
                            {
                                "Model Name": model_name,
                                "Element Type": element_type,
                                "Element ID": element_guid,
                                "Attribute/ Property": prop,
                                "Rule": "Data Type",
                                "Result": result,
                            }
                        )

    result_df = pd.DataFrame(data=results)
    return result_df


def get_header_data(ifc_file: ifcopenshell.file, model_name: str):
    full_name = ifc_file.wrapped_data.header.file_name.name
    file_name = full_name.split("/")[-1]
    new_row = {}
    new_row["Name + Revision"] = model_name
    new_row["ApplicationFullName"] = get_application_full_name(ifc_file=ifc_file)
    new_row["ApplicationDeveloper"] = get_application_developer(ifc_file=ifc_file)
    new_row["Version"] = get_application_version(ifc_file=ifc_file)
    new_row["ApplicationIdentifier"] = get_application_identifier(ifc_file=ifc_file)
    new_row["ContainerName"] = file_name
    new_row["Key"] = file_name
    new_row["Organization"] = get_organization(ifc_file=ifc_file)
    new_row["Description"] = get_description(ifc_file=ifc_file)
    new_row["ImplementationLevel"] = get_implementation_level(ifc_file=ifc_file)
    new_row["Name"] = get_file_name(ifc_file=ifc_file)
    new_row["TimeStamp"] = get_timestamp(ifc_file=ifc_file)
    new_row["Author"] = get_author(ifc_file=ifc_file)
    new_row["PreprocessorVersion"] = get_processor_version(ifc_file=ifc_file)
    new_row["OriginatingSystem"] = get_origination_system(ifc_file=ifc_file)
    new_row["FileSchema"] = get_file_schema(ifc_file=ifc_file)
    new_row["Authorization"] = get_authorization(ifc_file=ifc_file)

    return new_row


def get_file_name(ifc_file: ifcopenshell.file) -> str:
    try:
        full_name: str = ifc_file.wrapped_data.header.file_name.name
        if full_name:
            return full_name[full_name.rfind("/") + 1 :]
        else:
            return None
    except Exception:
        return None


def get_application_full_name(ifc_file: ifcopenshell.file) -> str:
    try:
        applications = ifc_file.by_type("IfcApplication")
        for application in applications:
            return application.ApplicationFullName
    except Exception:
        return None


def get_application_identifier(ifc_file: ifcopenshell.file) -> str:
    try:
        applications = ifc_file.by_type("IfcApplication")
        for application in applications:
            return application.ApplicationIdentifier
    except Exception:
        return None


def get_application_version(ifc_file: ifcopenshell.file) -> str:
    try:
        applications = ifc_file.by_type("IfcApplication")
        for application in applications:
            return application.Version
    except Exception:
        return None


def get_application_developer(ifc_file: ifcopenshell.file) -> str:
    try:
        applications = ifc_file.by_type("IfcApplication")
        for application in applications:
            return json.dumps(application.ApplicationDeveloper.get_info())
    except Exception:
        return None


def get_organization(ifc_file: ifcopenshell.file) -> str:
    try:
        return ifc_file.wrapped_data.header.file_name.organization[0]
    except Exception:
        return None


def get_description(ifc_file: ifcopenshell.file) -> str:
    try:
        return ", ".join(ifc_file.wrapped_data.header.file_description.description)
    except Exception:
        return None


def get_implementation_level(ifc_file: ifcopenshell.file) -> str:
    try:
        return ifc_file.wrapped_data.header.file_description.implementation_level
    except Exception:
        return None


def get_timestamp(ifc_file: ifcopenshell.file) -> str:
    try:
        return ifc_file.wrapped_data.header.file_name.time_stamp
    except Exception:
        return None


def get_author(ifc_file: ifcopenshell.file) -> str:
    try:
        return ", ".join(ifc_file.wrapped_data.header.file_name.author)
    except Exception:
        return None


def get_processor_version(ifc_file: ifcopenshell.file) -> str:
    try:
        return ifc_file.wrapped_data.header.file_name.preprocessor_version
    except Exception:
        return None


def get_origination_system(ifc_file: ifcopenshell.file) -> str:
    try:
        return ifc_file.wrapped_data.header.file_name.originating_system
    except Exception:
        return None


def get_file_schema(ifc_file: ifcopenshell.file) -> str:
    try:
        return ifc_file.wrapped_data.header.file_schema.schema_identifiers[0]
    except Exception:
        return None


def get_authorization(ifc_file: ifcopenshell.file) -> str:
    try:
        return ifc_file.wrapped_data.header.file_name.authorization
    except Exception:
        return None


def get_type(ifc_obj) -> str:
    return ifc_obj.get_info()["type"]


def populate_rules_table(ifc_files: list, project_id: str, should_truncate: bool):
    properties = []
    tables = ma.get_tables(project_id=project_id)

    for table in tables:
        if table["name"] == "Ifc Entities":
            filters = [
                {
                    "columnName": "Extract",
                    "value": True,
                    "filterType": "eq",
                    "orGroup": "main",
                }
            ]
            type_rows = ma.get_table_rows(table_id=table["publicId"], filters=filters)
            types = [row["rowData"]["IFC Entity"] for row in type_rows]
        elif table["name"] == "Ifc Checker Rules":
            table_id = table["publicId"]

    for ifc_file in ifc_files:
        for ifc_type in types:
            try:
                elements = ifc_file.by_type(type=ifc_type)
            except Exception:
                elements = []
            for element in elements:
                pset_name = "Attributes"
                attributes = element.__dict__
                for prop in list(attributes.keys()):
                    properties.append(
                        {
                            "Element Type": ifc_type,
                            "Grouping": pset_name,
                            "Attribute/ Property": prop,
                        }
                    )

                definitions = (
                    element.IsDefinedBy
                    if hasattr(element, "IsDefinedBy") and element.IsDefinedBy
                    else ()
                )
                property_sets = (
                    element.HasPropertySets
                    if hasattr(element, "HasPropertySets") and element.HasPropertySets
                    else ()
                )
                definitions = definitions + property_sets

                for definition in definitions:
                    if hasattr(definition, "RelatingPropertyDefinition"):
                        relating_prop = definition.RelatingPropertyDefinition
                        if hasattr(relating_prop, "Quantities"):
                            quantities = relating_prop.Quantities
                            for quantity in quantities:
                                quantity_name = quantity.Name
                                properties.append(
                                    {
                                        "Element Type": ifc_type,
                                        "Grouping": relating_prop.Name,
                                        "Attribute/ Property": quantity_name,
                                    }
                                )

                        elif hasattr(relating_prop, "HasProperties"):
                            props = relating_prop.HasProperties
                            for prop in props:
                                prop_name = prop.Name
                                properties.append(
                                    {
                                        "Element Type": ifc_type,
                                        "Grouping": relating_prop.Name,
                                        "Attribute/ Property": prop_name,
                                    }
                                )

                    elif hasattr(definition, "RelatingType"):
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "RelatingType",
                                "Attribute/ Property": "Element Type",
                            }
                        )
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "RelatingType",
                                "Attribute/ Property": "Name",
                            }
                        )
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "RelatingType",
                                "Attribute/ Property": "Description",
                            }
                        )
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "RelatingType",
                                "Attribute/ Property": "GlobalId",
                            }
                        )
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "RelatingType",
                                "Attribute/ Property": "PredefinedType",
                            }
                        )
                    elif hasattr(definition, "HasProperties"):
                        props = definition.HasProperties
                        for prop in props:
                            prop_name = prop.Name
                            properties.append(
                                {
                                    "Element Type": ifc_type,
                                    "Grouping": definition.Name,
                                    "Attribute/ Property": prop_name,
                                }
                            )
                associations = (
                    element.HasAssociations
                    if hasattr(element, "HasAssociations") and element.HasAssociations
                    else ()
                )

                for association in associations:
                    association_type = str(association)
                    association_type = association_type[
                        association_type.find("=") + 1 : association_type.find("(")
                    ]
                    if association_type == "IfcRelAssociatesClassification":
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "Classification",
                                "Attribute/ Property": "Name",
                            }
                        )
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "Classification",
                                "Attribute/ Property": "ItemReference",
                            }
                        )
                        properties.append(
                            {
                                "Element Type": ifc_type,
                                "Grouping": "Classification",
                                "Attribute/ Property": "ItemName",
                            }
                        )

    df = pd.DataFrame(data=properties)
    df = df.drop_duplicates()
    df = df.sort_values(by=["Element Type", "Grouping", "Attribute/ Property"])

    df["Should Exist"] = True
    df["Should have value"] = True
    df.loc[
        (df["Attribute/ Property"] == "GlobalId") & (df["Grouping"] == "Attributes"),
        ["Must be unique"],
    ] = True

    # create keys
    df["Key"] = df.apply(
        lambda row: " - ".join(
            [row["Element Type"], row["Grouping"], row["Attribute/ Property"]]
        ),
        axis=1,
    )
    df["Property Key"] = df.apply(
        lambda row: " - ".join([row["Grouping"], row["Attribute/ Property"]]), axis=1
    )

    rows = pf.dataframe_to_morta_rows(input_df=df)

    if should_truncate:
        ma.truncate_table(table_id=table_id)
        ma.insert_rows(table_id=table_id, rows=rows)
    else:
        ma.upsert_rows(table_id=table_id, upsert_column_name="Key", rows=rows)


def load_viewpoint_files(
    project_id: str,
    should_populate_rules: bool,
    should_truncate_rules: bool,
    should_truncate_tables: bool,
    api_key: str = None,
):
    # initialize variables
    updates = []
    ifc_files = []

    tables = ma.get_tables(project_id=project_id)

    for table in tables:
        if table["name"] == "Ifc Checker Rules":
            rules_table_id = table["publicId"]
        elif table["name"] == "Folders":
            table_id = table["publicId"]
            folder_rows = ma.get_table_rows(table_id=table_id)
            needed_column = [col for col in table["columns"] if col["name"] == "Name"][
                0
            ]
            column_id = needed_column["publicId"]
            for row in folder_rows:
                if row["rowData"]["Name"] == "IFC Schedules":
                    row_id = row["publicId"]
                    new_table_type = f"table/{table_id}/{column_id}/{row_id}"
        elif table["name"] == "Value List":
            values_list_table_id = table["publicId"]
        elif table["name"] == "Viewpoint - Files":
            viewpoint_table_id = table["publicId"]
        elif table["name"] == "Header":
            headers_table_id = table["publicId"]
        elif table["name"] == "Roles":
            table_id = table["publicId"]
            role_rows = ma.get_table_rows(table_id=table_id)
            needed_column = [col for col in table["columns"] if col["name"] == "Name"][
                0
            ]
            column_id = needed_column["publicId"]
            for row in role_rows:
                if row["rowData"]["Name"] == "Admin":
                    row_id = row["publicId"]
                    admin_tag_id = f"table/{table_id}/{column_id}/{row_id}"
                elif row["rowData"]["Name"] == "Global Morta Admin":
                    row_id = row["publicId"]
                    admin_tag_id = f"table/{table_id}/{column_id}/{row_id}"
                elif row["rowData"]["Name"] == "Contributor":
                    row_id = row["publicId"]
                    contributor_tag_id = f"table/{table_id}/{column_id}/{row_id}"
                elif row["rowData"]["Name"] == "Project Admin":
                    row_id = row["publicId"]
                    contributor_tag_id = f"table/{table_id}/{column_id}/{row_id}"

    # read rows from a table that need extraction
    filters = [
        {
            "columnName": "Extract & Validate Data?",
            "value": True,
            "filterType": "eq",
            "orGroup": "main",
        }
    ]
    rows = ma.get_table_rows(table_id=viewpoint_table_id, filters=filters)

    # loop over rows
    for row in rows:
        # get the file and extract its data
        row_id = row["publicId"]
        row_data = row["rowData"]
        model_name = row_data["⚡ Name"]
        document_id = row_data["Document ID"]
        revision_id = row_data["Revision ID"]

        # get file id
        response = mva.get_revision(
            document_id=document_id, revision_id=revision_id, api_key=api_key
        )
        file_id = response.json()["data"]["body"]["RevisionInfos"][0]["Files"][0]["ID"]

        # get ifc_file
        response = mva.get_file(
            document_id=document_id,
            revision_id=revision_id,
            file_id=file_id,
            api_key=api_key,
        )
        ifc_text = response.text
        ifc_file = ifcopenshell.file.from_string(ifc_text)
        ifc_files.append(ifc_file)

    if should_populate_rules:
        populate_rules_table(
            ifc_files=ifc_files,
            project_id=project_id,
            should_truncate=should_truncate_rules,
        )

    rule_rows = ma.get_table_rows(table_id=rules_table_id)
    rules_df = pf.morta_rows_to_dataframe(input_morta_rows=rule_rows)

    for ifc_file in ifc_files:
        # extract model data
        extract(
            project_id=project_id,
            new_table_type=new_table_type,
            ifc_file=ifc_file,
            rules_df=rules_df,
            headers_table_id=headers_table_id,
            model_name=model_name,
            admin_tag_id=admin_tag_id,
            contributor_tag_id=contributor_tag_id,
            should_truncate_tables=should_truncate_tables,
        )

        updates.append(
            {"columnName": "Extract & Validate Data?", "rowId": row_id, "value": False}
        )

    # validate
    check_rules(
        project_id=project_id,
        new_table_type=new_table_type,
        rules_df=rules_df,
        values_list_table_id=values_list_table_id,
        admin_tag_id=admin_tag_id,
        contributor_tag_id=contributor_tag_id,
    )

    # update rows: uncheck checkbox
    ma.update_cells(table_id=viewpoint_table_id, cells=updates)


def combine_in_one_table(project_id: str, should_truncate: bool):
    # get IFC Schedules folder id
    created = False
    admin_tag_value = "Global Morta Admin"
    contr_tag_value = "Project Admin"
    table_name = "All Model Ifc Entities"
    ifc_schedule_folder_name = "IFC Schedules"

    variables = ma.get_variables(project_id=project_id)
    tags = ma.get_tags(project_id=project_id)
    ifc_schedule_folder_id = mf.get_variable_by_name(
        variable_name=ifc_schedule_folder_name, variables=variables
    )["id"]

    admin_tag_id = mf.get_tag_by_name(tag_name=admin_tag_value, tags=tags)["id"]
    contr_tag_id = mf.get_tag_by_name(tag_name=contr_tag_value, tags=tags)["id"]

    # get data
    tables = ma.get_tables(project_id=project_id)

    for table in tables:
        if table["name"] == "Ifc Entities":
            ifc_entities_table_id = table["publicId"]
            filters = [
                {
                    "columnName": "Master Table",
                    "value": True,
                    "filterType": "eq",
                    "orGroup": "main",
                }
            ]
            entity_rows = ma.get_table_rows(
                table_id=ifc_entities_table_id, filters=filters
            )
            entities = [row["rowData"]["IFC Entity"] for row in entity_rows]

    dfs = []

    for table in tables:
        if table["type"] == ifc_schedule_folder_id and table["name"] in entities:
            filters = [
                {
                    "columnName": "Attributes - GlobalId",
                    "value": None,
                    "filterType": "is_not_null",
                    "orGroup": "main",
                }
            ]
            rows = ma.get_table_rows(table_id=table["publicId"], filters=filters)
            df: pd.DataFrame = pf.morta_rows_to_dataframe(input_morta_rows=rows)
            df["IfcEntity"] = table["name"]
            dfs.append(df)
        if table["name"] == table_name:
            created = True
            master_table = table

    df = pd.concat(objs=dfs)
    df["Key"] = df.apply(
        lambda row: f"{row['Model Name']} - {row['Attributes - GlobalId']}", axis=1
    )
    df = df.drop_duplicates(subset=["Key"])
    insert_rows = pf.dataframe_to_morta_rows(input_df=df)

    if not created:
        # create table
        start_columns = [
            "Model Name",
            "Project",
            "Site",
            "Building",
            "BuildingStorey",
            "Space",
        ]
        column_names = df.columns.values
        column_names = [
            col for col in column_names if col not in start_columns + ["Changed?"]
        ]
        column_names = sorted(column_names)
        column_names = start_columns + column_names + ["Changed?"]
        columns = [
            {"kind": "text", "name": column_name, "width": 240}
            for column_name in column_names
        ]

        master_table = ma.create_table(
            project_id=project_id,
            name=table_name,
            columns=columns,
            table_type=ifc_schedule_folder_id,
        )

        # create owner permission
        ma.create_permission(
            resource_kind=ma.ResourceKind.table.value,
            resource_id=master_table["publicId"],
            attribute_kind=ma.AttributeKind.tag.value,
            attribute_identifier=admin_tag_id,
            role=4,
        )

        default_view_id = master_table["defaultViewId"]
        view_settings = {
            "groupSettings": [
                {"columnName": "Model Name", "direction": "asc"},
                {"columnName": "Project", "direction": "asc"},
                {"columnName": "Site", "direction": "asc"},
                {"columnName": "Building", "direction": "asc"},
                {"columnName": "BuildingStorey", "direction": "asc"},
                {"columnName": "Space", "direction": "asc"},
            ],
            "disableNewRow": True,
        }
        ma.update_view(view_id=default_view_id, view_params=view_settings)

        # create contribute permission
        ma.create_permission(
            resource_kind=ma.ResourceKind.view.value,
            resource_id=default_view_id,
            attribute_kind=ma.AttributeKind.tag.value,
            attribute_identifier=contr_tag_id,
            role=2,
        )

        # insert rows
        ma.insert_rows(table_id=master_table["publicId"], rows=insert_rows)

    else:
        # check if columns are there
        all_column_names = sorted(df.columns.values)
        existing_columns = master_table["columns"]
        existing_column_names = [col["name"] for col in existing_columns]
        default_view_id = master_table["defaultViewId"]
        i = 0

        for current_column in all_column_names:
            if current_column not in existing_column_names:
                params = {
                    "name": current_column,
                    "kind": "text",
                    "width": 240,
                    "sortOrder": len(existing_column_names) + i,
                    "locked": False,
                    "required": False,
                }
                ma.create_column_in_view(
                    view_id=master_table["defaultViewId"], params=params
                )
                i = i + 1

        if should_truncate:
            ma.truncate_table(table_id=master_table["publicId"])
            ma.insert_rows(table_id=master_table["publicId"], rows=insert_rows)
        else:
            ma.upsert_rows(
                table_id=master_table["publicId"],
                upsert_column_name="Attributes - GlobalId",
                rows=insert_rows,
            )


def check_ifc_file_integrity(ifc_file: ifcopenshell.file):
    results = []
    property_id = ""
    property_sets = ifc_file.by_type(type="IfcPropertySet")
    for property_set in property_sets:
        if hasattr(property_set, "HasProperties"):
            properties = property_set.HasProperties
            for prop in properties:
                if hasattr(property_set, "PropertyDefinitionOf"):
                    property_definition_ofs = property_set.PropertyDefinitionOf
                    for property_definition_of in property_definition_ofs:
                        if hasattr(property_definition_of, "RelatedObjects"):
                            related_objects = property_definition_of.RelatedObjects
                            for related_object in related_objects:
                                object_type = related_object.get_info()["type"]
                                object_id = related_object.get_info()["id"]
                                object_guid = related_object.GlobalId
                                object_name = related_object.Name
                                pset_name = property_set.Name
                                pset_guid = property_set.GlobalId
                                results.append(
                                    {
                                        "Property ID": property_id,
                                        "Object Type": object_type,
                                        "Object ID": object_id,
                                        "Object GlobaId": object_guid,
                                        "Object Name": object_name,
                                        "Property Set Name": pset_name,
                                        "Property Set GlobalId": pset_guid,
                                    }
                                )


def get_tables_mapping(project_id: str, table_names: list):
    # get tables
    tables = ma.get_tables(project_id=project_id)
    mapping = {}

    # get table ids
    for table in tables:
        for table_name in table_names:
            if table["name"] == table_name:
                mapping[table_name] = table["publicId"]

    # check if tables are in project
    missing_tables = []

    # check for missing tables
    for table_name in table_names:
        if table_name not in mapping:
            missing_tables.append(table_name)

    if len(missing_tables) > 0:
        missing_tables = ", ".join(missing_tables)
        raise Exception(f"project {project_id} missing tables: {missing_tables}")

    return mapping


def get_viewpoint_file(document_id: str, revision_id: str, api_key: str = None):
    # get file id
    response = mva.get_revision(
        document_id=document_id, revision_id=revision_id, api_key=api_key
    )
    file_id = response.json()["data"]["body"]["RevisionInfos"][0]["Files"][0]["ID"]

    # get ifc_file
    response = mva.get_file(
        document_id=document_id,
        revision_id=revision_id,
        file_id=file_id,
        api_key=api_key,
    )
    ifc_text = response.text
    ifc_file = ifcopenshell.file.from_string(ifc_text)
    return ifc_file


def create_ifc_table(
    project_id: str,
    columns: list,
    new_table_type: str,
    admin_tag_id: str,
    column_mapping_table_id: str,
    table_name: str,
    add_globalid_column: bool,
):
    view_settings = {"disableNewRow": True}

    if add_globalid_column:
        if "Attributes - GlobalId" not in columns:
            columns.append("Attributes - GlobalId")
        columns.append("Changed?")

    columns = [{"name": name, "kind": "text", "width": 240} for name in columns]
    new_table = ma.create_table(
        project_id=project_id,
        name=table_name,
        columns=columns,
        table_type=new_table_type,
    )

    # create owner permission
    ma.create_permission(
        resource_kind=ma.ResourceKind.table.value,
        resource_id=new_table["publicId"],
        attribute_kind=ma.AttributeKind.tag.value,
        attribute_identifier=admin_tag_id,
        role=4,
    )

    # update view
    default_view_id = new_table["defaultViewId"]
    ma.update_view(view_id=default_view_id, view_params=view_settings)

    # insert columns into column mapping table
    upsert_column_mapping(
        table_id=new_table["publicId"], column_mapping_table_id=column_mapping_table_id
    )

    return new_table["publicId"]


def create_missing_columns(
    table_id: str, column_names: list, column_mapping_table_id: str
):
    new_column_mapping = []

    for column_name in column_names:
        params = {"name": column_name, "kind": "text", "width": 240}
        created_column = ma.create_column_in_table(table_id=table_id, params=params)
        new_column_mapping.append(
            {
                "Table ID": table_id,
                "ColumnId": created_column["publicId"],
                "Original Column Name": created_column["name"],
            }
        )

    new_rows = [{"rowData": row} for row in new_column_mapping]
    ma.insert_rows(table_id=column_mapping_table_id, rows=new_rows)


def create_key(row: pd.Series) -> str:
    revision_d = row["Revision Id"]
    global_id = row["Attributes - GlobalId"]
    element_id = row["Attributes - id"]
    element_type = row["Attributes - type"]

    if global_id:
        return f"{revision_d} - {element_type} - {global_id}"
    else:
        return f"{revision_d} - {element_type} - {element_id}"


def check_datatypes(df: pd.DataFrame, ifc_data_table_id: str) -> pd.DataFrame:
    column_names = list(df.columns.values)
    table = ma.get_table(table_id=ifc_data_table_id)
    table_columns = table["columns"]

    for column_name in column_names:
        old_values = df.loc[~df[column_name].isna()][column_name].to_list()
        values = []
        for value in old_values:
            if type(value) is float:
                value_str = str(value)
                ending = value_str[-2:]
                if ending == ".0":
                    value = int(value)
            values.append(value)

        if len(values) == 0:
            continue
        value_types = []
        for value in values:
            if type(value) not in value_types:
                value_types.append(type(value))

        if len(value_types) > 1:
            if int in value_types and float in value_types:
                kind = "float"
                params = {"name": column_name, "kind": kind}
                for table_column in table_columns:
                    if (
                        table_column["name"] == column_name
                        and table_column["kind"] != kind
                    ):
                        ma.update_column(
                            table_id=ifc_data_table_id,
                            column_id=table_column["publicId"],
                            params=params,
                        )
            else:
                df[column_name] = df[column_name].apply(
                    lambda x: str(x) if type(x) is str else x
                )
        else:
            value_type = value_types[0]
            if value_type is str:
                continue
            elif value_type is list:
                kind = "multiselect"
            elif value_type is float:
                kind = "float"
            elif value_type is int:
                kind = "integer"
            elif value_type is bool:
                kind = "checkbox"
            else:
                raise Exception(f"type {str(value_type)} not catered for")

            params = {"name": column_name, "kind": kind}
            for table_column in table_columns:
                if table_column["name"] == column_name:
                    if table_column["kind"] == "text" or (
                        table_column["kind"] == "integer" and kind == "float"
                    ):
                        ma.update_column(
                            table_id=ifc_data_table_id,
                            column_id=table_column["publicId"],
                            params=params,
                        )

            if kind == "integer":
                df[column_name] = df[column_name].apply(
                    lambda x: convert_to_int(value=x)
                )
                df[column_name] = df[column_name].astype("Int32")

    return df


def convert_to_int(value) -> int:
    try:
        return int(value)
    except Exception:
        return None


def upload_document_to_viewpoint(
    document_id: str,
    document_name: str,
    bytes_object: bytes,
    user_name: str,
    password: str,
    api_key: str = None,
):
    # get token
    # ---------
    token = vp_api.get_token(
        user_name=user_name,
        password=password,
        application_id=config.VIEWPOINT_APPLICATION_ID,
    )

    vp_document_response = mva.get_document(
        document_id=document_id, api_key=api_key
    ).json()["data"]["body"]

    status_name = vp_document_response["DocumentInfos"][0]["Status"]["Name"]
    status_id = vp_document_response["DocumentInfos"][0]["Status"]["ID"]
    vp_revision_response = vp_api.create_new_revision(
        document_id=document_id,
        status_id=status_id,
        status_name=status_name,
        token=token,
    )
    revision_id = vp_revision_response["OperationResults"][0]["ObjectChildID"]

    vp_upload_file_response = vp_api.upload_file_to_document_revision(
        document_id=document_id,
        revision_id=revision_id,
        file_name=document_name,
        is_primary_file="true",
        bytes_object=bytes_object,
        token=token,
    )

    if (
        "success"
        not in vp_upload_file_response["OperationResults"][0]["Message"].lower()
    ):
        messages = []
        messages.append(f"Document Name: {document_name}")
        messages.append(f"ViewPoint Document ID: {document_id}")
        messages.append(f"ViewPoint Revision ID: {revision_id}")
        messages.append(f"response: {vp_upload_file_response}")
        # html = "<br>".join(messages)
        # you can use your own mailing server here to send email notifications
        exception_message = "\n".join(messages)
        raise Exception(exception_message)

    url = f"https://n3g.4projects.com/file.aspx?document={document_id}&revision={revision_id}"
    return url


def upsert_column_mapping(table_id: str, column_mapping_table_id: str):
    table = ma.get_table(table_id=table_id)
    columns = table["columns"]
    column_mapping = []
    for column in columns:
        column_mapping.append(
            {
                "Table ID": table_id,
                "ColumnId": column["publicId"],
                "Original Column Name": column["name"],
            }
        )
    new_rows = [{"rowData": row} for row in column_mapping]
    ma.upsert_rows(
        table_id=column_mapping_table_id, rows=new_rows, upsert_column_name="ColumnId"
    )
