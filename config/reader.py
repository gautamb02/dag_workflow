from .models import Config,Error
import yaml
from typing import Union
from pathlib import Path
from pydantic import ValidationError

_config: Config | None = None 

def GetConfig() -> Config:
    global _config
    if _config is None:
        raise ValueError("Configuration not loaded. Please load config first.")
    return _config


def load_config(file_path: Path) -> Union[Config, Error]:
    try:
        # Load the YAML file
        with open(file_path, 'r') as file:
            config_data = yaml.safe_load(file)
        
        # Attempt to parse the configuration into the Config model
        _config = Config.parse_obj(config_data)
        return _config
    
    except ValidationError as e:
        # Return a validation error with details if validation fails
        error_message = f"Validation error: {e}"
        return Error(error_message)
    
    except Exception as e:
        # Handle unexpected errors (e.g., file not found, parsing issues)
        error_message = f"Unexpected error: {e}"
        return Error(error_message)