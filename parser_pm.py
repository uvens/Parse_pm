import xml.etree.cElementTree as ET
import glob
import csv
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import pandas as pd

logger = logging.getLogger(__name__)
filter_column = None


def init_my_logging():
    """
    Configuring logging for a SON-like appearance when running locally
    """
    logger.setLevel(logging.DEBUG)
    log_file_name = os.path.splitext(os.path.realpath(__file__))[0] + ".log"
    handler = RotatingFileHandler(
        log_file_name, maxBytes=10 * pow(1024, 2), backupCount=3
    )
    log_format = "%(asctime)-15s [{}:%(name)s:%(lineno)s:%(funcName)s:%(levelname)s] %(message)s".format(
        os.getpid()
    )
    handler.setLevel(logging.DEBUG)
    try:
        from colorlog import ColoredFormatter

        formatter = ColoredFormatter(log_format)
    except ImportError:
        formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)


def get_tag(elem):
    return elem.tag.split("}")[-1]


def in_schema(PM_Name):
    # print(filter_column)
    return PM_Name in filter_column


def initialize_output(report_filename):
    with open(report_filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()


def add_data_to_csv(path, report_filename):
    with open(report_filename, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        for p in path:
            end_time = p["end_time"]
            duration = p["duration"][2:-1]
            for k, v in p.items():
                xpath = {}
                PM_Name = p['path_to_object'].split(':')[1]
                xpath["path_to_object"] = p['path_to_object'].split(':')[0]
                xpath["start_date"] = path[0]["begin_time"]
                xpath["end_date_time"] = end_time
                xpath["duration"] = duration
                xpath["PM_name"] = PM_Name
                xpath["index"] = ""
                xpath["value"] = v
                if v and len(v) > 1:
                    for i, va in enumerate(v.split(",")):
                        # print(str(i), va)
                        xpath["index"] = str(i)
                        xpath["value"] = va
                        writer.writerow(xpath)
                else:
                    writer.writerow(xpath)
        logger.info(f"Added row {len(path)} records to csv")


def parse_data(file_name):
    path = []
    data = {}
    path_to_object = None
    begin_time = None
    for event, elem in ET.iterparse(file_name, events=("start", "end")):
        tag = get_tag(elem)
        if tag == 'managedObject':
            result = ''
        if event == "start":
            if tag == "measCollec":
                begin_time = elem.get("beginTime")
            elif tag == "granPeriod":
                data = {"begin_time": begin_time, "duration": elem.get("duration"), "end_time": elem.get("endTime")}
            elif tag == "measType":
                data['measType'] = elem.text
            elif tag == "measValue":
                data['path_to_object'] = elem.get("measObjLdn")
            elif tag == "r":
                data[f"{path_to_object}:{elem.get('p')}"] = elem.text
        elif event == "end":
            if tag == "measInfo":
                path.append(data)
                data = {}
                path_to_object = None
                begin_time = None
    return path


def initialize():
    global filter_column
    logger.info(f"Fetching Schema...")
    # df = pd.read_csv("mini_pm_parser_schema.csv")
    # filter_column = [c for c in df.PM_NAME]
    file = 'A20230620.2300-0400-0000-0400_S01MTGDN01.xml'
    xml_file_names = file
    logger.info(f"Working on {len(xml_file_names)} files in ...")
    path = parse_data(xml_file_names)
    report_filename = xml_file_names + ".csv"
    initialize_output(report_filename)
    add_data_to_csv(path, report_filename)


if __name__ == "__main__":

    fieldnames = [
        "path_to_object",
        "start_date",
        "end_date_time",
        "duration",
        "PM_name",
        "index",
        "value",
    ]
    init_my_logging()
    initialize()
