import argparse
import bz2
import json
import os
import sqlite3
from typing import Any
from datetime import datetime

import requests

from __init__ import __version__
from argfmt import CustomHelpFormatter


class SDEConn:
    """SQLite connection class"""

    def __init__(self, db_name: str):
        self.conn = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_COLNAMES)
        self.cursor = self.conn.cursor()
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __get_col_names(self) -> list:
        """
        Gets column names for the current table.
        """
        return list(map(lambda x: x[0], self.cursor.description))

    def execute_named(self, stmt: str) -> list[dict[str, Any]]:
        """
        Executes the provided statement and returns a named result.
        """
        c = self.cursor
        t = c.execute(stmt)
        col_names = self.__get_col_names()
        all = t.fetchall()
        named_result = []
        for row in all:
            named = {}
            for idx, col in enumerate(col_names):
                named[col] = row[idx]
            named_result.append(named)
        return named_result

    def execute_raw(self, stmt: str) -> list[tuple[Any]]:
        """
        Executes the provided statement and returns a raw result.
        """
        c = self.cursor
        t = c.execute(stmt)
        all = t.fetchall()
        return all

    def close(self):
        """
        Closes the connection.
        """
        return self.conn.close()


def get_real_version(version: str) -> str:
    """
    Ensures that the version provided is a valid format.
    """
    if version == "sqlite-latest":
        return "sqlite-latest"
    else:
        if "sde-" in version:
            if "-TRANQUILITY" in version:
                return f"{version}"
            else:
                return f"{version}-TRANQUILITY"
        else:
            if "-TRANQUILITY" in version:
                return f"sde-{version}"
            else:
                return f"sde-{version}-TRANQUILITY"


def build_sde_url(version: str) -> str:
    """
    Builds the url to be used when requesting the SDE from fuzzwork.
    """
    base_url = "https://www.fuzzwork.co.uk/dump"
    version = get_real_version(version)

    if version == "sqlite-latest":
        url_path = "/sqlite-latest.sqlite.bz2"
    else:
        url_path = f"/{version}/eve.db.bz2"
    
    return base_url + url_path


def get_sde(version: str, work_dir: str):
    """
    Downloads the specified SDE version from Fuzzwork to the working directory.
    """
    url = build_sde_url(version)
    print(f"Downloading SDE: {version} from {url}...")
    r = requests.get(url)

    # Download zipped SDE
    with open(work_dir+"sde.db.bz2", "wb") as dbzip:
        dbzip.write(r.content)
    
    with open(work_dir+"sde.db", "wb") as db:
        db.write(bz2.open(work_dir+"sde.db.bz2", 'rb').read())


def check_latest(output_dir: str) -> bool:
    """
    Checks if the local version of the SDE is up to date.
        Returns False if the local version is our of date.
    """
    hash_url = "https://www.fuzzwork.co.uk/dump/sqlite-latest.sqlite.bz2.md5"
    if os.path.exists(output_dir+"latest/hash.md5"):
        with open(output_dir+"latest/hash.md5", "r") as f:
            local_hash = f.read()
        r = requests.get(hash_url)
        web_hash = r.text
        return local_hash == web_hash
    return False


def save_hash(output_dir: str):
    """
    Gets and saves the latest hash file from Fuzzwork to the output directory.
    """
    hash_url = "https://www.fuzzwork.co.uk/dump/sqlite-latest.sqlite.bz2.md5"
    r = requests.get(hash_url)
    with open(output_dir+"latest/hash.md5", "w") as f:
        f.write(r.text)


def build_tables_indexes(output_dir: str, host_base_url: str, force: bool=False):
    """
    Builds any and all missing table indexes.
    """
    versions = [f for f in os.scandir(output_dir) if f.is_dir()]
    for version in versions:
        v_path = version.path
        if not os.path.exists(str(v_path)+"/tables.json") or force:
            files = [f for f in os.scandir(version.path) if not f.is_dir()]
            tables = []
            for file in files:
                if file.name == "hash.md5":
                    continue
                tables.append({
                    "name": file.name,
                    "href": f"{host_base_url}/{version.name}/{file.name}"
                })
            with open(v_path+"/tables.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(tables))


def build_versions_index(output_dir: str, host_base_url: str):
    """
    Builds the jsonfile that shows what versions are available on the server.
    """
    versions = []
    for f in os.scandir(output_dir):
        if f.is_dir():
            versions.append({
                'version': f.name,
                'href': f"{host_base_url}/{f.name}/tables.json"
            })
    with open(output_dir+"versions.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(versions))
    return


def build_global_indexes(output_dir: str, host_base_url: str, force: bool):
    latest = False
    if os.path.exists(output_dir + "latest/"):
        latest = True
        rel_hash_path = "latest/hash.md5"
        last_updated_stamp = os.path.getmtime(output_dir + "latest/hash.md5")
        last_updated = datetime.fromtimestamp(last_updated_stamp).strftime('%Y-%m-%d %H:%M')
    
    build_tables_indexes(output_dir, host_base_url, force)
    build_versions_index(output_dir, host_base_url)
    versions_index_rel = "versions.json"

    latest_data=""
    if latest:
        latest_data = f"""
        <br>
        Last Updated: {last_updated}
        <br>
        <a href="{rel_hash_path}">Current MD5</a>
        """

    html = f"""
    <html>
        <body>
            A simple SDE conversion to JSON files.
            <br>
            To see a list of available versions, see <a href="{versions_index_rel}">versions.json</a>
            <br>
            To access a table, visit /version/tablename.json.
            <br>
            Many thanks to FuzzySteve for the SDE conversions to SQLite and Squizz for the zzeve service.
            {latest_data}
            <br>
            <br>
            <small><a href="https://github.com/colcrunch/pysde2json">GitHub</a></small>
        </body>
    </html>
    """

    with open(output_dir + "/index.html", "w", encoding="utf-8") as f:
        f.write(html)


def run(
    sde_version: str="sqlite-latest",
    output_dir: str="/var/www/sde/",
    working_dir: str="/tmp/pysde/",
    force: bool=False,
    host_base_url=""
    ):

    ran = False

    # Create the working directory if it does not exist.
    if not os.path.exists(working_dir):
        os.mkdir(working_dir)

    # Check and download files.
    if sde_version == "sqlite-latest":
        json_output_dir = output_dir + "latest/"
        if not check_latest(output_dir) or force:
            if force:
                print("Forcing update of latest SDE version.")
            if not os.path.exists(json_output_dir):
                os.mkdir(json_output_dir)
            # Download latest SDE
            get_sde(sde_version, working_dir)

            # Save the latest hash
            save_hash(output_dir)
            ran = True
        else:
            print("local SDE version already up to date.")
    else:
        # Check if the version already exists at the output location
        version = get_real_version(sde_version)
        json_output_dir = output_dir + f"{version}/"
        if not os.path.exists(json_output_dir) or force:
            if force:
                print("Forcing update of existing SDE version!")
            if not os.path.exists(json_output_dir):
                os.mkdir(json_output_dir)
            # Fownload the versioned SDE
            get_sde(version, working_dir)
            ran = True
        else:
            print(f"{version} already exists.")

    if ran:
        # Process SDE
        print("Processing SDE tables!")
        with SDEConn(working_dir+"sde.db") as s:
            sqlite_minor = int(sqlite3.sqlite_version.split(".")[1])
            if sqlite_minor < 36:
                schema_table = "sqlite_master"
            else:
                schema_table = "sqlite_schema"

            tables = s.execute_raw(f"SELECT name FROM {schema_table} WHERE type='table' ORDER BY name;")

            for table in tables:
                name = table[0]
                items = s.execute_named(f"SELECT * FROM {name};")
                with open(json_output_dir+f"{name}.json", "w", encoding="utf-8") as f:
                    f.write(json.dumps(items))
        
        print("Updating indexes...")
        build_global_indexes(output_dir, host_base_url, force)


def main():
    """
    Entry point used when running `pysde` from the terminal.
    """
    parser = argparse.ArgumentParser(
        prog="pysde", 
        usage="%(prog)s [OPTIONS]",
        description="pysde2json: A simple converter for Fuzzwork SQLite SDE to JSON.",
        formatter_class=CustomHelpFormatter)
    
    parser.add_argument(
        '-v',
        '--version',
        action='version',
        version=f'pysde2json v{__version__}'
    )
    parser.add_argument(
        '-s',
        '--sde-version',
        metavar="s",
        dest="sde_version",
        help="define the version of the SDE to convert. (Default: sqlite-latest)"
    )
    parser.add_argument(
        '-o',
        '--output-dir',
        metavar="d",
        dest="output_dir",
        help="specifies the directory to output json files. (Default: /var/www/sde/)"
    )
    parser.add_argument(
        '-w',
        '--working-dir',
        metavar="d",
        dest="working_dir",
        help="specifies the working directory to use. (Default: /tmp/pysde/)"
    )
    parser.add_argument(
        '-b',
        '--base_url',
        metavar="url",
        dest="host_base_url",
        help="set the base url that your files will be available at. (Default: None)"
    )
    parser.add_argument(
        '-f',
        '--force',
        action='store_true',
        dest="force",
        help="force processing the version"
    )

    args = parser.parse_args()
    
    # Remove unused args from parsed args
    print(args)
    args = {k:v for k, v in vars(args).items() if not v is None}
    run(**args)

if __name__ == "__main__":
    main()
