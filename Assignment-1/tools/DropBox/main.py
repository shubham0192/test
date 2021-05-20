# Copyright (C) 2020 Alteryx, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example input tool."""
from ayx_plugin_sdk.core import (
    FieldType,
    InputConnectionBase,
    Metadata,
    Plugin,
    ProviderBase,
    RecordPacket,
    register_plugin,
)
from ayx_plugin_sdk.core.exceptions import WorkflowRuntimeError
from io import StringIO
import requests
import json


class DropBox(Plugin):
    """Concrete implementation of an AyxPlugin."""

    def __init__(self, provider: ProviderBase) -> None:
        """Construct a plugin."""
        self.provider = provider
        self.tool_config = provider.tool_config
        self.access_token = self.tool_config["AccessToken"]
        self.select_operation = self.tool_config["SelectOperation"]
        self.file_name = self.tool_config["FileName"]
        self.folder_path = self.tool_config["FolderPath"]
        self.file_path = self.tool_config["FilePath"]
        self.upload_file_path = self.tool_config["UploadFP"]

        self.output_anchor = self.provider.get_output_anchor("Output")

        self.output_metadata = Metadata()
        self.output_metadata.add_field("Employee Id", FieldType.int64)
        self.output_metadata.add_field("Employee Name", FieldType.v_wstring, size=100)
        self.output_metadata.add_field("Salary", FieldType.string,size =100)

        self.output_anchor.open(self.output_metadata)

        # if float(self.config_value) > 0.5:
        #     raise WorkflowRuntimeError("Values greater than 0.5 are not allowed.")

        self.provider.io.info("Plugin initialized.")

    def on_input_connection_opened(self, input_connection: InputConnectionBase) -> None:
        """Initialize the Input Connections of this plugin."""
        raise NotImplementedError("Input tools don't have input connections.")

    def on_record_packet(self, input_connection: InputConnectionBase) -> None:
        """Handle the record packet received through the input connection."""
        raise NotImplementedError("Input tools don't receive packets.")

    def on_complete(self) -> None:
        """Create all records."""
        import pandas as pd

        if self.select_operation == "Download":
            df = self.download_file()
            packet = RecordPacket.from_dataframe(self.output_metadata, df)
            self.output_anchor.write(packet)

        if self.select_operation == "ListFolder":
            df = self.list_folder()
            packet = RecordPacket.from_dataframe(self.output_metadata, df)
            self.output_anchor.write(packet)
        self.provider.io.info("Completed processing records.")

    def download_file(self) -> None:
        import pandas as pd
        from io import StringIO

        url = "https://content.dropboxapi.com/2/files/download"

        headers = {
            "Authorization": "Bearer" + " " + self.access_token,
            "Dropbox-API-Arg": '{"path":' + '"' + self.file_path + '"' + "}",
        }
        r = requests.post(url, headers=headers)
        string_data = StringIO(r.text)
        df = pd.read_csv(string_data, sep=",")
        print(df)
        self.provider.io.info(f"Downloaded file successfully")
        return df

    def list_folder(self) -> None:

        url = "https://api.dropboxapi.com/2/files/list_folder"

        headers = {
            "Authorization": "Bearer" + " " + self.access_token,
            "Content-Type": "application/json",
        }
        data = {"path": "/Apps/Test"}
        r = requests.post(url, headers=headers, data=json.dumps(data))
        print(r.json())

    def upload_file(self) -> None:
        """
        upload file to dropbox
        """

        url = "https://content.dropboxapi.com/2/files/upload"

        headers = {
            "Authorization": "Bearer" + " " + self.access_token,
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": '{"path":' + '"' + self.upload_file_path + '"' + "}",
        }

        data = open("new.xlsx", "rb").read()

        r = requests.post(url, headers=headers, data=data)
        print(r.status_code)
        self.provider.io.info(f"File upload process completed successfully")


AyxPlugin = register_plugin(DropBox)
