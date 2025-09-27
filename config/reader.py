from .models import Config
import yaml
from pathlib import Path
from pydantic import ValidationError

def load_config(file_path: Path) -> Config:
    with open(file_path, 'r') as file:
        config_data = yaml.safe_load(file)

    try:
        return Config.parse_obj(config_data)
    except ValidationError as e:
        print(f"Validation error: {e}")
        raise
