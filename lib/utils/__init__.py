from .DagJsonParser import DagParser


def parser(data: dict):
    return DagParser.parse(data)
