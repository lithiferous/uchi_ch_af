import os
import re

from datetime import datetime
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator


class ArchiveFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self, filepath, archivepath, task_name, *args, **kwargs):
        super(ArchiveFileOperator, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.archivepath = archivepath
        self.task_name = task_name

    def execute(self, context):
        file_name = context['task_instance'].xcom_pull(self.task_name, key='file_name')
        os.rename(self.filepath + file_name, self.archivepath + file_name)


class OmegaFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filepath, filepattern, *args, **kwargs):
        super(OmegaFileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern

    def poke(self, context):
        full_path = self.filepath
        file_pattern = re.compile(self.filepattern)

        directory = os.listdir(full_path)
        print(directory)
        for files in directory:
            if re.match(file_pattern, files):
                context['task_instance'].xcom_push('file_name', files)
                return True
        return False

class OmegaPlugin(AirflowPlugin):
    name = "omega_plugin"
    operators = [OmegaFileSensor, ArchiveFileOperator]
