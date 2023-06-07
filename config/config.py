import yaml
from pathlib import Path


class Config:
    def __init__(self):
        path = "config\config.yaml"
        try:
            self.conf = yaml.safe_load(Path(path).read_text())
            print("Successfully load config")
        except:
            print("Failed to load config")