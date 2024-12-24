import json
import logging
import requests
import pandas as pd

logger = logging.getLogger(__name__)


def get_scraped_data(container_id: str, base_url: str, api_key: str) -> pd.DataFrame:
    """
    Fetches data from the RESTful API of the specified Phantom container.

    Args:
        container_id (str): The ID of the container to fetch data from.
        base_url (str): The base URL of the API.
        api_key (str): API key for authentication.

    Returns:
        pd.DataFrame: DataFrame with the fetched data.
    """
    try:
        response = _send_get_request(
            url="/containers/fetch-result-object",
            base_url=base_url,
            api_key=api_key,
            params={"id": container_id},
        )

        if response.get("resultObject"):
            logger.info(f"Data found in container with ID {container_id}")
            res_obj = json.loads(response.get("resultObject"))

            if isinstance(res_obj, dict):
                # Fetch large data from the URL provided in the response
                response = _send_get_request(
                    url=res_obj.get("jsonUrl"),
                    base_url=base_url,
                    api_key=api_key,
                    is_phantom_endpoint=False,
                )
                df = pd.DataFrame(response)
            else:
                # Parse the data directly if it's not too large
                df = pd.DataFrame(res_obj)
            return df
        else:
            logger.info(f"No data found in container with ID {container_id}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error while processing container {container_id}: {e}")
        raise


def get_all_agent_containers(agent_id: str, base_url: str, api_key: str) -> list:
    """
    Fetches all containers from the specified agent.

    Args:
        agent_id (str): The ID of the agent to fetch containers for.
        base_url (str): The base URL of the API.
        api_key (str): API key for authentication.

    Returns:
        list or None: List of containers or None if the request fails.
    """
    try:
        response = _send_get_request(
            url="/containers/fetch-all",
            base_url=base_url,
            api_key=api_key,
            params={"agentId": agent_id},
        )

        return response.get("containers", [])
    except Exception as e:
        logger.error(f"Error while fetching containers for agent {agent_id}: {e}")
        raise


def get_saved_ids(file_path: str) -> list:
    """
    Reads all container IDs from a csv.

    Args:
        file_path (str): Path to the file containing container IDs.

    Returns:
        list: A list of container IDs.
    """
    try:
        df = pd.read_csv(file_path)
        ids = df["container_id"].tolist()
        return ids
    except:
        return []


def save_ids(container_ids: list[int], file_path: str) -> None:
    """
    Saves a list of container IDs to a CSV file.

    Args:
        container_ids (list[int]): The list of container IDs to save.
        file_path (str): The path to the CSV file.

    Returns:
        None
    """
    try:
        df = pd.DataFrame(container_ids, columns=["container_id"])
        df.to_csv(file_path, index=False)
    except Exception as ex:
        logger.error(f"Error saving container IDs to CSV file {file_path}: {ex}")
        raise


def _send_get_request(
    url: str,
    base_url: str,
    api_key: str,
    params: dict = None,
    is_phantom_endpoint: bool = True,
) -> dict:
    """
    Sends an HTTP GET request to the specified URL.

    Args:
        url (str): The API endpoint URL.
        base_url (str): The base URL of the API.
        api_key (str): API key for authentication.
        params (dict, optional): Query parameters to include in the request.
        is_phantom_endpoint (bool, optional): Whether the URL is a Phantom API endpoint.

    Returns:
        dict or None: Parsed JSON response if the request is successful, or None otherwise.
    """
    try:
        headers = {
            "accept": "application/json",
            "X-Phantombuster-Key": api_key,
        }
        base_url = base_url
        url = base_url + url if is_phantom_endpoint else url
        response = requests.get(url=url, headers=headers, params=params)

        if response.status_code == 200:
            return json.loads(response.text)
        else:
            logger.error(
                f"Request failed with status code {response.status_code}: {response.text}"
            )
            raise
    except Exception as e:
        logger.error(f"Error during GET request to {url}: {e}")
        raise
