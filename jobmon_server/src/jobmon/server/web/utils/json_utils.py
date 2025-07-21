"""TODO: DELETE AFTER DATABASE ETL OF EDGES TABLE"""

import json
from typing import List, Optional, Union


def parse_node_ids(node_ids: Union[str, List[int], None]) -> Optional[List[int]]:
    """Parse node IDs that might be stored as strings or actual JSON.
    
    This is a shim function to handle the transition from string-cast JSON
    to actual JSON storage in the database.
    
    Args:
        node_ids: The node IDs value from the database - can be:
                 - None
                 - A string like "[276911034, 278607788]" 
                 - An actual list like [276911034, 278607788]
    
    Returns:
        List of integers, or None if input was None/empty
    """
    if node_ids is None:
        return None
    
    if isinstance(node_ids, str):
        if not node_ids.strip():
            return None
        try:
            return json.loads(node_ids)
        except (json.JSONDecodeError, ValueError):
            # Fallback for malformed strings
            return None
    
    # Already a list/array
    return node_ids
