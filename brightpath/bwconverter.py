"""
This module contains the class BrightwayConverter, which is used to convert
Brightway2 inventories to Simapro CSV files.
"""

from .utils import (
    get_simapro_ecoinvent_blacklist,
    get_simapro_fields_list,
    get_simapro_units,
    get_simapro_headers,
    import_bw_inventories,
    get_simapro_technosphere,
    get_simapro_biosphere,
    get_ef31_simapro_elementary_flows,
    get_simapro_subcompartments,
    load_inventory_metadata,
    is_activity_waste_treatment,
    find_production_exchange,
    get_technosphere_exchanges,
    is_a_waste_treatment,
    format_exchange_name,
    get_simapro_uncertainty_type, get_biosphere_exchanges,
    is_blacklisted,
    convert_sd_to_sd2,
    round_floats_in_string,
    get_subcategory,
    flag_exchanges,
    print_unused_exchanges,
    check_exchanges_for_conversion,
    add_distri_transport
)

import datetime
import csv
from pathlib import Path
import re

class BrightwayConverter:
    """
    Convert Brightway2 inventories to Simapro CSV files.
    """

    def __init__(
            self,
            filepath: str = None,
            data: list = None,
            metadata: str = None,
            ecoinvent_version: str = "3.9",
            export_dir: str = None
    ):
        """
        :param filepath: path to the BW inventory spreadsheet file
        """
        self.filepath = filepath
        self.inventories = import_bw_inventories(filepath) if self.filepath else data
        self.simapro_blacklist = get_simapro_ecoinvent_blacklist()
        self.simapro_fields = get_simapro_fields_list()
        self.simapro_units = get_simapro_units()
        self.simapro_headers = get_simapro_headers()
        self.simapro_technosphere = get_simapro_technosphere()
        self.simapro_biosphere = get_simapro_biosphere()
        self.ef31_simapro_elementary_flows = get_ef31_simapro_elementary_flows()
        self.simapro_subcompartment = get_simapro_subcompartments()
        self.ei_version = ecoinvent_version

        if metadata:
            self.metadata = load_inventory_metadata(metadata)

        # export directory is the current working
        # directory unless specified otherwise
        self.export_dir = Path(export_dir) or Path.cwd()


    def get_simapro_biosphere_flow(
            self, ei_exc_name, ei_compartment, ei_subcompartment, ds_location) -> dict:
        """
        Get the name of a Simapro biosphere flow based on ecoinvent
        exchange name and exchange category

        Regionalized biosphere flows are identified; useful 
        for water as natural resource or emissions to air.
        """
        # Mapping of ecoinvent compartments to SimaPro compartments
        compartment_mapping = {
            "air": "Air",
            "water": "Water",
            "soil": "Soil",
            "natural resource": "Raw"
        }
        
        # Get SimaPro compartments and subcompartments
        simapro_compartment = compartment_mapping[ei_compartment]
        try:
            simapro_subcompartment = self.simapro_subcompartment[ei_subcompartment]
        except KeyError:
            simapro_subcompartment = "(unspecified)"

        # Determine search names

        # Handling some special cases where the name is different.
        # Name in ecoinvent: Name in simapro
        special_name_cases = {
            "Particulate Matter, < 2.5 um": "Particulates, < 2.5 um",
            "Particulate Matter, > 2.5 um and < 10um": "Particulates, > 2.5 um, and < 10um",
            "Particulate Matter, > 10 um": "Particulates, > 2.5 um, and < 10um", # PM > 10 um don't exist in SimaPro nomenclature
            "Nitrogen": "Nitrogen, total",
            "Dioxins, measured as 2,3,7,8-tetrachlorodibenzo-p-dioxin": "Dioxin, 2,3,7,8 Tetrachlorodibenzo-p-",
            "Ammonium": "Ammonium, ion",
            "Water, well, in ground": "Water, well"
        }

        roman_numerals = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']
        roman_numerals_patterns = [re.compile(rf'\b{rn}\b') for rn in roman_numerals]

        # Flows with "ion" are named as "Copper ion" in ecoinvent and as "Copper, ion" in SimaPro
        if re.search(r'\bion\b', ei_exc_name):   
            search_ef_name = ei_exc_name.replace(" ion", ", ion")

        # Flows with oxidation state like Silver I are named as Silver (I) in SimaPro
        elif any(p.search(ei_exc_name) for p in roman_numerals_patterns):
            search_ef_name = re.sub(r'^(.*)\s+(I{1,3}|IV|V|VI{0,3}|IX|X)$', r'\1 (\2)', ei_exc_name)

        # Handling the name for special cases:
        elif ei_exc_name in special_name_cases:
            search_ef_name = special_name_cases[ei_exc_name]

        # SimaPro uses "biogenic" instead of "non-fossil"
        elif "non-fossil" in ei_exc_name:
            search_ef_name = ei_exc_name.replace("non-fossil", "biogenic")

        else:
            search_ef_name = ei_exc_name

        simapro_ef_match = []

        # Let's check if there is a regionalized flow
        for subcomp in [simapro_subcompartment, "(unspecified)"]:
            match = [
                ff for ff in self.ef31_simapro_elementary_flows
                if ff["name"] == f"{search_ef_name}, {ds_location}"
                and ff["compartment"] == simapro_compartment
                and ff["subcompartment"] == subcomp
            ]
            if match:
                simapro_ef_match = match[0]
                break 
        
        # If no regionalized flows exist, try to match exact name
        if not simapro_ef_match:
            for subcomp in [simapro_subcompartment, "(unspecified)"]:
                match = [
                    ff for ff in self.ef31_simapro_elementary_flows
                    if ff["name"] == search_ef_name
                    and ff["compartment"] == simapro_compartment
                    and ff["subcompartment"] == subcomp
                ]
                if match:
                    simapro_ef_match = match[0]
                    break
        
        # If match doesn't exist, use ecoinvent elementary flow
        if not simapro_ef_match:
            print("Cannot find appropiate elementary flow for exchange:", (ei_exc_name, ei_compartment, ei_subcompartment))
            return {'name': ei_exc_name, 'compartment': simapro_compartment, "subcompartment": ""}
        else:
            if simapro_ef_match["subcompartment"] == "(unspecified)":
                return {'name': simapro_ef_match["name"], 'compartment': simapro_ef_match["compartment"], "subcompartment": ""}
            else:
                return {'name': simapro_ef_match["name"], 'compartment': simapro_ef_match["compartment"], "subcompartment": simapro_ef_match["subcompartment"]}


    def format_inventories_for_simapro(self, database: str):
        """
        Format inventories to Simapro format.
        :param database: name of the database to link to.
        :return: list
        """

        rows = [
            [item.replace("today_date", datetime.datetime.today().strftime("%d.%m.%Y"))]
            if item.startswith("{Date") else [item]
            for item in self.simapro_headers

        ]
        rows.append([])

        for activity in self.inventories:

            # first, add transport, if uvek
            if database == "uvek":
                activity = add_distri_transport(activity)
            # and flag exchanges
            activity = flag_exchanges(activity)
            dataset_name = ""
            is_a_waste_treatment_activity = is_activity_waste_treatment(activity, database)

            prod_exchange = find_production_exchange(activity)
            prod_exchange.update({"simapro category": self.metadata["system description"]["category"]})
            
            for field in self.simapro_fields:
                if (
                        is_a_waste_treatment_activity is True
                        and field == "Products"
                ):
                    continue

                if (
                        is_a_waste_treatment_activity is False
                        and field == "Waste treatment"
                ):
                    continue

                rows.append([field])
                if field in ["Process", "End"]:
                    rows.append([])
                    continue

                if field == "Process name":
                    dataset_name = format_exchange_name(
                        activity["name"],
                        activity["reference product"],
                        activity["location"],
                        activity["unit"],
                        database
                    )
                    rows.extend(
                        [
                            [dataset_name],
                            [],
                        ]
                    )

                if field == "Type":
                    rows.extend(
                        [
                            ["Unit process"],
                            [],
                        ]
                    )

                if field == "Comment":
                    string = ""
                    if activity.get("comment"):
                        string = f"{round_floats_in_string(activity['comment'])} "

                    if activity.get("source"):
                        string += f"Source: {activity['source']} "
                    # remove line breaks in string
                    string = string.replace("\n", " ")
                    rows.extend(
                        [
                            [string],
                            [],
                        ]
                    )

                if field == "Category type":
                    prod_exchange = find_production_exchange(activity)
                    if "simapro category" in prod_exchange:
                        rows.extend(
                            [
                                [prod_exchange["simapro category"].split("/")[0]],
                                [],
                            ]
                        )
                    else:
                        rows.extend([["Others"], []])

                if field == "Geography":
                    rows.extend(
                        [
                            [activity["location"]],
                            [],
                        ]
                    )

                if field == "Date":
                    rows.extend(
                        [
                            [f"{datetime.datetime.today():%d.%m.%Y}"],
                            [],
                        ]
                    )

                if field in (
                        "Time period",
                        "Record",
                        "Generator",
                        "Cut off rules",
                        "Capital goods",
                        "Technology",
                        "Representativeness",
                        "Boundary with nature",
                        "Infrastructure",
                        "External documents",
                        "System description",
                        "Allocation rules",
                        "Literature references",
                        "Collection method",
                        "Data treatment",
                        "Verification",

                ):
                    if field.lower() in activity:
                        rows.extend(
                            [
                                [activity[field.lower()]],
                                [],
                            ]
                        )
                    else:
                        if field == "Infrastructure":
                            rows.extend([["No"], []])
                        elif field == "System description" and "system description" in self.metadata:
                            rows.extend([[self.metadata["system description"]["name"]], []])
                        elif field == "Literature references" and "literature reference" in self.metadata:
                            rows.extend([[self.metadata["literature reference"]["name"]], []])
                        else:
                            rows.extend([["Unspecified"], []])

                if field in (
                        "Final waste flows",
                        "Non material emission",
                        "Social issues",
                        "Economic issues",

                ):
                    rows.append([])

                if field == "Waste treatment":
                    prod_exchange = find_production_exchange(activity)
                    category = get_subcategory(prod_exchange["simapro category"])

                    if prod_exchange["amount"] < 0:
                        prod_exchange["amount"] *= -1

                    rows.extend(
                        [
                            [
                                dataset_name,
                                self.simapro_units[prod_exchange["unit"]],
                                "{:.3E}".format(prod_exchange["amount"]),
                                "100",
                                "not defined",
                                category,
                                "not defined",
                            ],
                            []
                        ]
                    )
                    prod_exchange["used"] = True

                if field == "Products":
                    prod_exchange = find_production_exchange(activity)
                    category = get_subcategory(prod_exchange["simapro category"])
                    rows.extend(
                        [
                            [
                                dataset_name,
                                self.simapro_units[prod_exchange["unit"]],
                                "{:.3E}".format(prod_exchange["amount"]),
                                "100",
                                "not defined",
                                category,
                                "not defined",
                            ],
                            []
                        ]
                    )
                    prod_exchange["used"] = True

                if field in ["Materials/fuels", "Electricity/heat"]:

                    if field == "Materials/fuels":
                        techno_excs = list(filter(lambda x: x["unit"] not in ["megajoule", "kilowatt hour"],
                                                  get_technosphere_exchanges(activity)))
                    else:
                        techno_excs = list(filter(lambda x: x["unit"] in ["megajoule", "kilowatt hour"],
                                                  get_technosphere_exchanges(activity)))

                    techno_excs = list(filter(lambda x: is_blacklisted(x, database) is False, list(techno_excs)))
                    techno_excs = check_exchanges_for_conversion(techno_excs, database)

                    for exc in filter(lambda x: is_a_waste_treatment(x["name"], database=database) is not True, techno_excs):
                        exchange_name = format_exchange_name(
                            exc["name"],
                            exc["reference product"],
                            exc.get("location", "GLO"),
                            exc["unit"],
                            database
                        )

                        u_type = get_simapro_uncertainty_type(exc.get("uncertainty type"))

                        if is_a_waste_treatment_activity is True and exc["amount"] < 0:
                            exc["amount"] *= -1

                        rows.append(
                            [
                                f"{exchange_name}",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(convert_sd_to_sd2(exc.get("scale", 1), u_type)),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True



                    rows.append([])

                if field == "Resources":
                    biosphere_excs = get_biosphere_exchanges(activity, "natural resource")
                    for exc in filter(lambda x: is_blacklisted(x, database) is False, biosphere_excs):

                        u_type = get_simapro_uncertainty_type(exc.get("uncertainty type"))

                        if len(exc["categories"]) > 1:
                            ei_compartment = exc["categories"][0]
                            ei_subcompartment = exc["categories"][1]
                          #  sub_compartment = self.simapro_subcompartment[exc["categories"][1]]
                        else:
                            ei_compartment = exc["categories"]
                            ei_subcompartment = ""

                        simapro_biosphere_flow = self.get_simapro_biosphere_flow(
                                exc['name'], ei_compartment, ei_subcompartment, activity["location"])
                        
                        rows.append(
                            [
                                simapro_biosphere_flow["name"], #f"{self.simapro_biosphere.get(exc['name'], exc['name'])}",
                                simapro_biosphere_flow["subcompartment"], #sub_compartment,
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(convert_sd_to_sd2(exc.get("scale", 1), u_type)),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True
                    rows.append([])

                if field.startswith("Emissions to"):
                    biosphere_excs = get_biosphere_exchanges(activity, field.split(" ")[-1].lower())
                    for exc in filter(lambda x: is_blacklisted(x, database) is False, biosphere_excs):

                        if exc["name"].lower() == "water":
                            exc["unit"] = "kilogram"
                            exc["amount"] *= 1000

                        u_type = get_simapro_uncertainty_type(exc.get("uncertainty type"))

                        if len(exc["categories"]) > 1:
                            ei_compartment = exc["categories"][0]
                            ei_subcompartment = exc["categories"][1]
                           # sub_compartment = self.simapro_subcompartment[exc["categories"][1]]
                        else:
                            ei_compartment = exc["categories"][0]
                            ei_subcompartment = ""
                        
                        simapro_biosphere_flow = self.get_simapro_biosphere_flow(
                                exc['name'], ei_compartment, ei_subcompartment, activity["location"])

                        rows.append(
                            [
                                simapro_biosphere_flow['name'], #f"{self.simapro_biosphere.get(exc['name'], exc['name'])}",
                                simapro_biosphere_flow['subcompartment'], #sub_compartment,
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(convert_sd_to_sd2(exc.get("scale", 1), u_type)),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True
                    rows.append([])

                if field == "Waste to treatment":
                    techno_excs = get_technosphere_exchanges(activity)
                    techno_excs = list(filter(lambda x: is_blacklisted(x, database) is False, techno_excs))
                    techno_excs = check_exchanges_for_conversion(techno_excs, database)

                    for exc in filter(lambda x: is_a_waste_treatment(x["name"], database=database) is True, techno_excs):

                        # In SimaPro, waste inputs are positive numbers
                        if exc["amount"] < 0:
                            exc["amount"] *= -1

                        exchange_name = format_exchange_name(
                            exc["name"],
                            exc["reference product"],
                            exc.get("location", "GLO"),
                            exc["unit"],
                            database
                        )

                        u_type = get_simapro_uncertainty_type(exc.get("uncertainty type"))

                        rows.append(
                            [
                                f"{exchange_name}",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(convert_sd_to_sd2(exc.get("scale", 1), u_type)),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True

                    rows.append([])

            rows.append([])

        # Add metadata: system descriptions and literature references
        for field in (
                "System description",
                "Literature reference",
        ):
            if field.lower() in self.metadata:
                rows.extend([[field], []])
                for key, val in self.metadata[field.lower()].items():
                    rows.extend(
                        [
                            [key],
                            [val],
                            [],
                        ]
                    )
                rows.append([])
                rows.extend([["End"], []])

        # check that all exchanges of all activities
        # have been used
        # print it in a prettytable
        print_unused_exchanges(self.inventories)

        return rows

    def convert_to_simapro(self, database: str = "ecoinvent", format: str = "csv") -> [str, list]:
        """
        Convert the inventories to Simapro CSV files.
        :param database: Name of the database to link to. Default is `ecoinvent`, but can be `uvek`.
        """

        if database not in ("ecoinvent", "uvek"):
            raise ValueError("Database must be either `ecoinvent` or `uvek`")

        data = self.format_inventories_for_simapro(database)

        if format == "data":
            return data

        # check that export direct exists
        # otherwise we create it
        self.export_dir.mkdir(parents=True, exist_ok=True)

        filepath = self.export_dir / f"simapro_{database}_{datetime.datetime.today().strftime('%d-%m-%Y')}.csv"

        with open(filepath, "w", newline="", encoding="utf-8") as csvFile:
            writer = csv.writer(csvFile, delimiter=",")
            for row in data:
                writer.writerow(row)
        csvFile.close()

        return f"Inventories export to: {filepath}"
