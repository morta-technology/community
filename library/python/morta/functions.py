# from repo
import library.python.morta.api as ma


# get tag or variable from project which are returned from Morta API
# input:
# table = {"publicId": table_id} or {"name":table_name}
# property = {"id": property_id} or {"value":property_value}
# properties is the ["data"] value of the get_tags or get_variables function in morta_api.py
# return:
#   cell as returned by morta API.
#   example:
#   {
#      "column":{"name":"Company Code", "publicId":"50c8b26e-d92d-4fcc-86a2-24088257cad5"},
#      "id":"table/be9c6618-cadb-4d29-aa92-bbd0e6644346/50c8b26e-d92d-4fcc-86a2-24088257cad5/0e70462d-f450-485d-ad7b-87729f70891e",
#      "value":"HARMEC"
#   }
def get_property(property: dict, properties: list, table_id: str = None) -> dict:
    for property_key, property_value in property.items():
        for current_table in properties:
            if table_id:
                if current_table["publicId"] == table_id:
                    for cell in current_table["cells"]:
                        if cell[property_key] == property_value:
                            return cell
            else:
                for cell in current_table["cells"]:
                    if cell[property_key] == property_value:
                        return cell


# gets the tag by inputting the id of that tag
def get_tag_by_id(tag_id: str, tags: list, table_id: str = None) -> dict:
    return get_property(property={"id": tag_id}, properties=tags, table_id=table_id)


# gets the tag by inputting the name of that tag
def get_tag_by_name(tag_name: str, tags: list, table_id: str = None) -> dict:
    return get_property(property={"value": tag_name}, properties=tags, table_id=table_id)


# gets the variable by inputting the id of that variable
def get_variable_by_id(variable_id: str, variables: list, table_id: str = None) -> dict:
    return get_property(property={"id": variable_id}, properties=variables, table_id=table_id)


# gets the variable by inputting the name of that variable
def get_variable_by_name(variable_name: str, variables: list, table_id: str = None) -> dict:
    return get_property(property={"value": variable_name}, properties=variables, table_id=table_id)


# delete rows from a morta table according to a filter
# takes: table_id, filters = [{"columnName": "Price", "value": "100", "filterType": "eq", "orGroup": "main"}]
def delete_rows_by_filter(table_id: str, filters: list, api_key: str = None):
    rows = ma.get_table_rows(table_id, filters=filters, api_key=api_key)
    row_ids = list(row["publicId"] for row in rows)
    if len(row_ids) > 0:
        ma.delete_rows(table_id=table_id, row_ids=row_ids, api_key=api_key)
