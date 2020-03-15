import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict
from math import sin, cos, sqrt, atan2, radians
import argparse

R = 6373.0
DELAY = 4.83

def node_executor():
    pass

@dataclass
class NodeInfo():
    lon: float
    lat: float
    name: str
    id_: int
    internal_id: int
    edges: list

def parse(filename: str) -> Dict[int, NodeInfo]:
    tree = ET.parse(filename)
    root = tree.getroot()
    for child in root:
        if child.tag == "{http://graphml.graphdrawing.org/xmlns}graph":
            graph = child

    nodes = []
    edges = []
    for child in graph:
        tag = child.tag
        if tag.endswith("node"):
            keys = {}
            for child2 in child:
                keys[child2.attrib['key']] = child2.text
            if "d32" not in keys or "d29" not in keys or "d33" not in keys:
                continue

            nodes.append(NodeInfo(lon=float(keys["d32"]), lat=float(keys["d29"]), name=keys["d33"], id_=int(child.attrib['id']), edges=[], internal_id=None))
    
    node_map = {}
    for i in nodes:
        node_map[i.id_] = i

    for child in graph:
        tag = child.tag
        if tag.endswith("edge"):
            src, dest = int(child.attrib["source"]), int(child.attrib["target"])
            if src not in node_map or dest not in node_map:
                continue
            node_map[src].edges.append(dest)
            node_map[dest].edges.append(src)
    
    return node_map


def dist(src: NodeInfo, dest: NodeInfo) -> float:
    """
    Source: https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
    """
    lat1 = radians(src.lat)
    lat2 = radians(dest.lat)
    lon1 = radians(src.lon)
    lon2 = radians(dest.lon)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c



def write_node_info(node_map: Dict[int, NodeInfo], filename: str):
    with open(filename, "w") as fout:
        print("Node 1 (id)", "Node 1 (label)", "Node 1 (longitude)", "Node 1 (latitude)", "Node 2 (id)", "Node 2 (label)", "Node 2 (longitude)", "Node 2 (latitude)", "Distance (km)", "Delay (mks)", file=fout, sep=",")
        ids = [i for i in node_map.keys()]
        ids.sort()

        for i in ids:
            edges = sorted(node_map[i].edges)
            src = node_map[i]
            for e in edges:
                dst = node_map[e]
                print(src.id_, src.name, src.lon, src.lat, dst.id_, dst.name, dst.lon, dst.lat, dist(src, dst), dist(src, dst) * DELAY, sep=',', file=fout)


def getPath(src, dst, next_edge):
    path = []
    while src != dst:
        path.append(src)
        src = next_edge[src][dst]
    path.append(dst)
    return path


def floyd(node_map: Dict[int, NodeInfo], filename: str):
    cur_id = 0
    id_to_internal = {}
    internal_to_id = {}
    nodes = {}

    for i in node_map.keys():
        id_to_internal[i] = cur_id
        internal_to_id[cur_id] = i
        cur_id += 1

    for i in node_map.values():
        int_id = id_to_internal[i.id_]
        i.internal_id = int_id
        new_edges = [id_to_internal[e] for e in i.edges]
        i.edges = new_edges
        nodes[int_id] = i
    
    count = cur_id
    matrix_networks = [[count * 2 * R] * count for _ in range(count)]
    next_edge = [[count * 2] * count for _ in range(count)]
    
    for src in nodes.values():
        for dst_ind in src.edges:
            dst = nodes[dst_ind]
            matrix_networks[src.internal_id][dst.internal_id] = dist(src, dst)
            next_edge[src.internal_id][dst.internal_id] = dst.internal_id

    for k in range(count):
        for i in range(count):
            for j in range(count):
                if matrix_networks[i][j] > matrix_networks[i][k] + matrix_networks[k][j]:
                    matrix_networks[i][j] = matrix_networks[i][k] + matrix_networks[k][j]
                    next_edge[i][j] = next_edge[i][k]

    
    with open(filename, "w") as fout:
        print("Node 1 (id)", "Node 2 (id)", "Path type", "Path", "Delay", sep=',', file=fout)
        for i in node_map.values():
            for j in node_map.values():
                if i.id_ == j.id_:
                    continue
                if matrix_networks[i.internal_id][j.internal_id] == count * 2 * R:
                    continue
                print(i.id_, j.id_, "main", [internal_to_id[k] for k in getPath(i.internal_id, j.internal_id, next_edge)], matrix_networks[i.internal_id][j.internal_id] * DELAY, sep=',', file=fout)


def parse_args():
    parser = argparse.ArgumentParser(description="Nice cli tool for find delays between internet nodes")
    parser.add_argument("-s", type=str, required=False, help="Source node for calc delay")
    parser.add_argument("-d", type=str, required=False, help="Destination node for calc delay")
    parser.add_argument("-t", type=str, required=True, help="Path to file in graphml format")
    return parser.parse_args()
    

def main():
    args = parse_args()
    t_name = args.t
    data = parse(args.t)
    write_node_info(data, t_name.split('.')[0] + "_topo.csv")
    floyd(data, t_name.split('.')[0] + "_routes.csv")


main()
