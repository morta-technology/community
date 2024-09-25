from pandaserd import ERD
import pandas as pd
import morta.api as ma


# this produces a txt file that can be viewed here: https://dreampuf.github.io/GraphvizOnline
def load_tables_for_ERD(hub_id):

    tables = ma.get_tables(hub_id)

    erd = ERD()

    for table in tables:
        column_names = []
        for column in table["columns"]:
            clean_name = (str)(column["name"]).replace("&", " ")
            clean_name = (str)(clean_name).replace("[", " ")
            clean_name = (str)(clean_name).replace("]", " ")
            clean_name = (str)(clean_name).replace("(", " ")
            clean_name = (str)(clean_name).replace(")", " ")
            clean_name = (str)(clean_name).replace(",", " ")
            clean_name = (str)(clean_name).replace("/", " ")
            clean_name = (str)(clean_name).replace("*", " ")
            clean_name = (str)(clean_name).replace("?", " ")
            clean_name = (str)(clean_name).replace("+", " ")
            clean_name = (str)(clean_name).replace("#", " ")
            clean_name = (str)(clean_name).replace("-", " ")
            clean_name = (str)(clean_name).replace(".", " ")
            clean_name = (str)(clean_name).replace(" ", "_")
            clean_name = (str)(clean_name).replace("%", "Percentage")

            clean_name = (str)(clean_name).replace("___", "_")
            clean_name = (str)(clean_name).replace("__", "_")

            column_names.append(clean_name)

        df = pd.DataFrame(columns=column_names)
        table_number = (str)(table["name"]).split(":")
        try:  # MOJ
            df_name = (str)(table_number[1])
        except Exception:
            df_name = (str)(table_number[0])

        df_name = (str)(df_name).replace(":", " ")
        df_name = (str)(df_name).replace("-", " ")
        df_name = (str)(df_name).replace("(", " ")
        df_name = (str)(df_name).replace(")", " ")
        df_name = (str)(df_name).replace(",", " ")
        df_name = (str)(df_name).replace("&", " ")
        df_name = (str)(df_name).replace("/", " ")

        df_name = df_name.strip()
        df_name = (str)(df_name).replace(" ", "_")
        df_name = (str)(df_name).replace("___", "_")
        df_name = (str)(df_name).replace("__", "_")

        erd.add_table(df, df_name, bg_color="skyblue")

    for table in tables:
        column_names = []

        table_number = (str)(table["name"]).split(":")
        try:
            current_table_name = (str)(table_number[1])
        except Exception:
            current_table_name = (str)(table_number[0])

        current_table_name = (str)(current_table_name).replace(":", " ")
        current_table_name = (str)(current_table_name).replace("-", " ")
        current_table_name = (str)(current_table_name).replace("(", " ")
        current_table_name = (str)(current_table_name).replace(")", " ")
        current_table_name = (str)(current_table_name).replace(",", " ")
        current_table_name = (str)(current_table_name).replace("&", " ")
        current_table_name = (str)(current_table_name).replace("/", " ")

        current_table_name = current_table_name.strip()
        current_table_name = (str)(current_table_name).replace(" ", "_")
        current_table_name = (str)(current_table_name).replace("___", "_")
        current_table_name = (str)(current_table_name).replace("__", "_")

        for column in table["columns"]:

            clean_name = (str)(column["name"]).replace("&", " ")
            clean_name = (str)(clean_name).replace("[", " ")
            clean_name = (str)(clean_name).replace("]", " ")
            clean_name = (str)(clean_name).replace("(", " ")
            clean_name = (str)(clean_name).replace(")", " ")
            clean_name = (str)(clean_name).replace(",", " ")
            clean_name = (str)(clean_name).replace("/", " ")
            clean_name = (str)(clean_name).replace("*", " ")
            clean_name = (str)(clean_name).replace("+", " ")
            clean_name = (str)(clean_name).replace("#", " ")
            clean_name = (str)(clean_name).replace("?", " ")
            clean_name = (str)(clean_name).replace("-", " ")
            clean_name = (str)(clean_name).replace(".", " ")
            clean_name = (str)(clean_name).replace(" ", "_")
            clean_name = (str)(clean_name).replace("%", "Percentage")
            clean_name = (str)(clean_name).replace("___", "_")
            clean_name = (str)(clean_name).replace("__", "_")

            column_names.append(clean_name)
            if column["isJoined"] == True:
                for join in table["joins"]:
                    for col in join["dataColumns"]:
                        if col["targetColumnId"] == column["publicId"]:
                            source_table = ma.get_table(join["joinTableId"])
                            source_table_number = (str)(source_table["name"]).split(":")
                            try: 
                                source_table_name = (str)(source_table_number[1])
                            except Exception:
                                source_table_name = (str)(source_table_number[0])

                            source_table_name = (str)(source_table_name).replace(":", " ")
                            source_table_name = (str)(source_table_name).replace("-", " ")
                            source_table_name = (str)(source_table_name).replace("&", " ")
                            source_table_name = (str)(source_table_name).replace("(", " ")
                            source_table_name = (str)(source_table_name).replace(")", " ")
                            source_table_name = (str)(source_table_name).replace("/", " ")
                            source_table_name = (str)(source_table_name).replace(",", " ")

                            source_table_name = source_table_name.strip()
                            source_table_name = (str)(source_table_name).replace(" ", "_")
                            source_table_name = (str)(source_table_name).replace("___", "_")
                            source_table_name = (str)(source_table_name).replace("__", "_")

                            source_col_name = ""
                            for source_col in source_table["columns"]:
                                if source_col["publicId"] == col["sourceColumnId"]:
                                    source_col_name = (str)(source_col["name"]).replace("&", " ")
                                    source_col_name = (str)(source_col_name).replace("[", " ")
                                    source_col_name = (str)(source_col_name).replace("]", " ")
                                    source_col_name = (str)(source_col_name).replace("-", " ")
                                    source_col_name = (str)(source_col_name).replace("(", " ")
                                    source_col_name = (str)(source_col_name).replace(")", " ")
                                    source_col_name = (str)(source_col_name).replace("/", " ")
                                    source_col_name = (str)(source_col_name).replace(",", " ")
                                    source_col_name = (str)(source_col_name).replace("*", " ")
                                    source_col_name = (str)(source_col_name).replace("+", " ")
                                    source_col_name = (str)(source_col_name).replace("#", " ")
                                    source_col_name = (str)(source_col_name).replace("?", " ")
                                    source_col_name = (str)(source_col_name).replace(".", " ")
                                    source_col_name = (str)(source_col_name).replace(" ", "_")
                                    source_col_name = (str)(source_col_name).replace("%", "Percentage")

                                    source_col_name = (str)(source_col_name).replace("___", "_")
                                    source_col_name = (str)(source_col_name).replace("__", "_")

                            erd.create_rel(
                                current_table_name, source_table_name, left_on=clean_name, right_on=source_col_name
                            )

            if column["kind"] in ("select", "multiselect"):
                try:
                    source_table = ma.get_table(column["kindOptions"]["tableOptions"]["tableId"])
                    source_table_number = (str)(source_table["name"]).split(":")
                    try:  # MOJ
                        source_table_name = (str)(source_table_number[1])
                    except Exception:
                        source_table_name = (str)(source_table_number[0])

                    source_table_name = (str)(source_table_name).replace(":", " ")
                    source_table_name = (str)(source_table_name).replace("-", " ")
                    source_table_name = (str)(source_table_name).replace("(", " ")
                    source_table_name = (str)(source_table_name).replace(")", " ")
                    source_table_name = (str)(source_table_name).replace("&", " ")
                    source_table_name = (str)(source_table_name).replace("/", " ")
                    source_table_name = (str)(source_table_name).replace(",", " ")

                    source_table_name = source_table_name.strip()
                    source_table_name = (str)(source_table_name).replace(" ", "_")
                    source_table_name = (str)(source_table_name).replace("___", "_")
                    source_table_name = (str)(source_table_name).replace("__", "_")

                    source_col_name = ""
                    for source_col in source_table["columns"]:
                        if source_col["publicId"] == column["kindOptions"]["tableOptions"]["columnId"]:
                            source_col_name = (str)(source_col["name"]).replace("&", " ")
                            source_col_name = (str)(source_col_name).replace("[", " ")
                            source_col_name = (str)(source_col_name).replace("]", " ")
                            source_col_name = (str)(source_col_name).replace("-", " ")
                            source_col_name = (str)(source_col_name).replace("(", " ")
                            source_col_name = (str)(source_col_name).replace(")", " ")
                            source_col_name = (str)(source_col_name).replace(",", " ")
                            source_col_name = (str)(source_col_name).replace("/", " ")
                            source_col_name = (str)(source_col_name).replace("*", " ")
                            source_col_name = (str)(source_col_name).replace("?", " ")
                            source_col_name = (str)(source_col_name).replace("+", " ")
                            source_col_name = (str)(source_col_name).replace("#", " ")
                            source_col_name = (str)(source_col_name).replace(".", " ")
                            source_col_name = (str)(source_col_name).replace("%", "Percentage")

                            source_col_name = (str)(source_col_name).replace(" ", "_")
                            source_col_name = (str)(source_col_name).replace("___", "_")
                            source_col_name = (str)(source_col_name).replace("__", "_")

                    erd.create_rel(current_table_name, source_table_name, left_on=clean_name, right_on=source_col_name)

                except Exception:
                    print("Exception")
                    continue

    erd.write_to_file("output.txt")

    return ""
