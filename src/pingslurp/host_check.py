import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pingslurp.config import Config

from urllib.parse import urlparse
import socket

import logging

LOG = logging.getLogger(__name__)


def parse_mongodb_connection_string(connection_string: str) -> list:
    # Parse the connection string
    parsed_url = urlparse(connection_string)

    # Extract the netloc part which contains the hosts and ports
    netloc = parsed_url.netloc

    # Remove the username and password if present
    if "@" in netloc:
        netloc = netloc.split("@")[1]

    # Split the netloc into individual host:port pairs
    hosts_ports = netloc.split(",")

    # Parse each host:port pair
    parsed_hosts_ports = []
    for host_port in hosts_ports:
        if ":" in host_port:
            host, port = host_port.split(":")
            parsed_hosts_ports.append((host, int(port)))
        else:
            parsed_hosts_ports.append((host_port, None))

    return parsed_hosts_ports


def check_host_connection(host: str, port: int, timeout=2) -> bool:
    try:
        # Attempt to create a connection to the host and port
        s = socket.create_connection((host, port), timeout)
        s.close()
        return True
    except Exception as e:
        LOG.warning(f"Connection to {host}:{port} failed: {e}")
        return False


async def check_hosts() -> int:
    # Create the client using the connection string
    client = AsyncIOMotorClient(Config.DB_CONNECTION)
    await client.admin.command("ping")
    # Get the list of nodes from the client
    nodes = list(client.nodes)
    nodes += parse_mongodb_connection_string(Config.DB_CONNECTION)
    LOG.info(f"Checking connection to {len(nodes)} nodes.")
    # Iterate through the nodes and check the connection to each one
    for host, port in nodes:
        if check_host_connection(host, port):
            LOG.info(f"Connection to {host}:{port} successful.")
        else:
            LOG.warning(f"Connection to {host}:{port} failed.")

    return len(nodes)
