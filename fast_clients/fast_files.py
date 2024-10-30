# 1.0.dev304.7

import os, sys
from abc import ABC, abstractmethod
from logging import getLogger
from pathlib import Path
import logging
import asyncio
from typing import Union, Tuple, Optional, Any
from urllib.parse import urlparse
from collections import defaultdict

import tarfile

import numpy as np
import pandas as pd
import smart_open
import ast
from fastapi import UploadFile, HTTPException
from pydantic import BaseModel, HttpUrl

from app.loggers import logger_f          # Import
# logger_f = logging.getLogger()  # Declare
# logger_f.setLevel(logging.DEBUG)  # Declare


import boto3
from botocore.exceptions import ClientError

try:
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(maxsize=None)


#   ___        _   ___ _ _
#  | __|_ _ __| |_| __(_) |___ ___
#  | _/ _` (_-<  _| _|| | / -_|_-<
#  |_|\__,_/__/\__|_| |_|_\___/__/


class FileData(BaseModel):
    """
    Represents the result of an upload operation

    Attributes:
        file (Bytes): File saved to memory
        path (Path | str): Path to file in local storage
        url (HttpUrl | str): A URL for accessing the object.
        size (int): Size of the file in bytes.
        filename (str): Name of the file.
        status (bool): True if the upload is successful else False.
        error (str): Error message for failed upload.
        message: Response Message
    """

    file: bytes = b""
    path: Path | str = ""
    url: HttpUrl | str = ""
    size: int = 0
    filename: str = ""
    content_type: str = ""
    status: bool = True
    error: str = ""
    message: str = ""


class CloudUpload(ABC):
    """Source: https://github.com/Ichinga-Samuel/fastfiles

    Methods:
        upload: Uploads a single object to the cloud
        multi_upload: Upload multiple objects to the cloud

    Attributes:
        config: A config dict
    """

    def __init__(self, config: dict | None = None):
        """
        Keyword Args:
            config (dict): A dictionary of config settings
        """
        self.config = config or {}

    # async def __call__(
    #     self, file: UploadFile | None = None, files: list[UploadFile] | None = None
    # ) -> FileData | list[FileData]:
    #     try:
    #         if file:
    #             return await self.upload(file=file)

    #         elif files:
    #             return await self.multi_upload(files=files)
    #         else:
    #             return FileData(
    #                 status=False,
    #                 error="No file or files provided",
    #                 message="No file or files provided",
    #             )
    #     except Exception as err:
    #         return FileData(
    #             status=False, error=str(err), message="File upload was unsuccessful"
    #         )

    @abstractmethod
    async def upload(self, *, file: UploadFile) -> FileData:
        """"""

    @abstractmethod
    async def multi_upload(self, *, files: list[UploadFile]) -> list[FileData]:
        """"""


#        ____
#   ___ |_  /
#  (_-<_/_ <
# /___/____/


class S3(CloudUpload):
    def __init__(self, config: dict | None = None):
        """
        Keyword Args:
            config (dict): A dictionary of config settings
        """
        self.config = config or {}
        # Add config check here, test against a pydantic model

    @property
    @cache
    def client(self):
        """An s3 client from boto3 library configured with the system's environments variables

        Returns:
            _type_: _description_
        """
        # key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        # access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        # endpoint_url = os.environ.get("AWS_ENDPOINT")
        # region_name = self.config.get('region') or os.environ.get('AWS_DEFAULT_REGION')
        # return boto3.client('s3', region_name=region_name, aws_access_key_id=key_id, aws_secret_access_key=access_key)

        try:
            return boto3.client(
                "s3",
                endpoint_url=self.config["s3_endpoint_url"],
                aws_access_key_id=self.config["s3_key_id"],
                aws_secret_access_key=self.config["s3_access_key"],
            )
        except Exception as e:
            logger_f.error(e)
            logger_f.error(f"Check S3 connexion, check config={self.config}")

    # ---------------------------------------------------------
    # UTILS Parse
    # ---------------------------------------------------------
    def parse_url_s3_as_localisation(self, s: str):
        """

        Args:
            s3_url (str): url of a s3 object formatted as `<my_s3>://<bucket_name>/<key>`

        Returns:
            Tuple[str, str]: the scheme, netloc and path of the ressource url.
        """
        o = urlparse(s, allow_fragments=False)
        return o.scheme, o.netloc, o.path

    def parse_url_s3_as_bucket_and_filename(self, s: str):
        """Parse a s3 url to bucket and key

        Args:
            s3_url (str): url of a s3 object formatted as `<my_s3>://<bucket_name>/<key>`

        Returns:
            Tuple[str, str]: the `s3 bucket` and `s3 key`
        """
        o = urlparse(s, allow_fragments=False)
        return o.netloc, o.path.lstrip("/")

    # ---------------------------------------------------------
    # UTILS Check
    # ---------------------------------------------------------
    def check_s3_folder_existence(self, s3_url: str) -> bool:
        bucket, key = self.parse_url_s3_as_bucket_and_filename(s3_url)
        if not key.endswith("/"):
            key = key + "/"
        resp = self.client.list_objects(
            Bucket=bucket, Prefix=key, Delimiter="/"
        )  # Maxkeys ? Avoid iterating over the whole folder, shit happens with nested folders
        print(resp)
        return "Contents" in resp  # Folder exists and is not empty
        # return 'CommonPrefixes' in resp # ALTERNATIVE, Folder exists and may be empty

    def check_s3_file_existence(self, s3_url: str) -> bool:
        bucket, key = self.parse_url_s3_as_bucket_and_filename(s3_url)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as e: # botocore.exceptions.ClientError
            if e.response['Error']['Code'] == "404":
                # The key does not exist.
                return False
            elif e.response['Error']['Code'] == "403":
                # Unauthorized, including invalid bucket
                return False
            else:
                raise 
        
        return True


    def check_s3_file_and_folder_coexistence(self, s3_url: str) -> bool:
        """From the s3 adress of an object, check if a folder with the same name exists a the same path
            WARNING: only works at bucket level. TODO: clean and expand to nested folders

        Args:
            s3_url (_type_): _description_
            s3_client (_type_): _description_
        """
        out = [False, False]
        bucket, key = self.parse_url_s3_as_bucket_and_filename(s3_url)
        folder_name_to_check = key.split('.')[0] + '/'
        l_folders, l_files =  self.list_s3_contents_at_folder(s3_url=f"s3://{bucket}") # WARNING

        logger_f.debug(f"key={key} and list={l_files}")
        if key in l_files:
            out[0] = True
        
        logger_f.debug(f"folder={folder_name_to_check} and list={l_folders}")
        if folder_name_to_check in l_folders:
            out[1] = True

        return out

    # ---------------------------------------------------------
    # UTILS List
    # ---------------------------------------------------------
    def list_s3_contents_at_folder(self, s3_url: str) -> Union[list[str], list[str]]:
        """Retrieve the name of all the objects and folder stored at a s3 path

        Args:
            s3_url (str): url of a s3 object, leading to a bucket or a folder, formatted as `<my_s3>://<bucket_name>/<key>`.

        Returns:
            Union[list[str], list[str]]: _description_
        """
        out_l_files = []
        out_l_folders = []

        bucket, key = self.parse_url_s3_as_bucket_and_filename(s3_url)
        my_paginator = self.client.get_paginator("list_objects_v2")
        pages = my_paginator.paginate(
            Bucket=bucket, Prefix=key + "/", Delimiter="/"
        )  # Add / at the end of key, escapes the Delimiter='/'
        for page in pages:
            try:
                for file in page["Contents"]:
                    out_l_files.append(file["Key"])
            except KeyError as ke:
                logging.warning(f"No files at s3_url={s3_url} |trace: {ke}")
            except Exception as e:
                logging.error(f"Unhandled error {e}")  # lol
            try:
                for folder in page["CommonPrefixes"]:
                    out_l_folders.append(folder["Prefix"])
            except KeyError as ke:
                logging.warning(f"No folders at s3_url={s3_url} |trace: {ke}")
            except Exception as e:
                logging.error(f"Unhandled error {e}")  # lol

        return out_l_folders, out_l_files


    # ---------------------------------------------------------
    # ARCHIVE
    # ---------------------------------------------------------
    def extract_archive(self, s3_url: str):
        """Extract an archived stored as an s3 object at the path `s3_url`
         Note: only tarballs are implemented

        Args:
            s3url (str): path to the tar file as an s3 formatted url "s3://{bucket}/{key}"
            client (Any): s3 client from the boto3 api
        """
        # def parse_s3_url(s3url: str) -> Tuple[str, str]:
        #     o = urlparse(s3url, allow_fragments=False)
        #     return o.netloc, o.path.lstrip('/')

        my_case = Path(s3_url).suffix # Defaulting to suffix
        match my_case:
            case '.tar.bz2':
                logger_f.info("(extract_archive) C'est une tarball")
            case '.zip':
                logger_f.info("(extract_archive) C'est une archive zip")
            case _:
                logger_f.info("(extract_archive) C'est pas géré")

        bucket, key = self.parse_url_s3_as_bucket_and_filename(s3_url)
        logging.debug(f"START TASK UT with url={s3_url}, bucket={bucket}")
        with smart_open.open(s3_url, 'rb', transport_params=dict(client=self.client), compression='disable') as fin:
            with tarfile.open(fileobj=fin) as tar:
                for member in tar.getmembers():
                    if member.isfile():
                        with smart_open.open(f's3://{bucket}/{member.name}', 'wb',transport_params=dict(client=self.client)) as fout:
                            logging.debug(f"Extracting file={member.name}")
                            with tar.extractfile(member) as ftarin:
                                fout.write(ftarin.read())
        logging.debug(f"END TASK UT")

    # ---------------------------------------------------------
    # UPLOAD
    # ---------------------------------------------------------
    async def upload(self, *, file: UploadFile, url_s3: str) -> FileData:
        """Upload a single file to an s3 filesystem located at `$AWS_ENDPOINT` with an s3 client

        Args:
            file (UploadFile): file to upload to s3

        Returns:
            FileData: A pydantic BaseModel representing the result of an UploadFile operation
        """
        try:
            filesystem_type, bucket_s3, key_s3 = self.parse_url_s3_as_localisation(
                s=url_s3
            )
            key_s3 = key_s3[1:]

        except Exception as e:
            logger_f.error(e)
            raise HTTPException(
                status_code=422,
                detail=f"File destination shall be written in a 'uri_ressource' field in json-params ; exception={e}",
            )

        try:
            extra_args = self.config.get("extra_args", {})
            await asyncio.to_thread(
                self.client.upload_fileobj,
                file.file,
                Bucket=bucket_s3,
                Key=key_s3,
                ExtraArgs=extra_args,
            )
            s3_file_head = self.client.head_object(Bucket=bucket_s3, Key=key_s3)
            uploaded_file_size = s3_file_head["ContentLength"]
            s3_displayed_delivery_url = f"s3://{bucket_s3}{key_s3}"
            return FileData(
                url=s3_displayed_delivery_url,
                message=f"{key_s3} uploaded successfully",
                filename=key_s3,
                content_type=str(file.content_type),
                size=uploaded_file_size,
            )
        except AttributeError as aerr:
            logger_f.warning(aerr)
            print(aerr)
            self.client.upload_fileobj(
                file.file, bucket_s3, key_s3, ExtraArgs=extra_args
            )
        except Exception as err:
            logger_f.error(err)
            print(err)
            return FileData(
                status=False, error=str(err), message="File upload was unsuccessful"
            )

    async def multi_upload(self, *, files: list[UploadFile]):
        tasks = [asyncio.create_task(self.upload(file=file)) for file in files]
        return await asyncio.gather(*tasks)

    # ---------------------------------------------------------
    # DOWNLOAD
    # ---------------------------------------------------------
    def create_presigned_url(self, s3_url: str, expiration: int = 3600):
        bucket_name, object_name = self.parse_url_s3_as_bucket_and_filename(s=s3_url)
        # return self.parse_url_s3(s=s3_url)

        try:
            response = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_name},
                ExpiresIn=expiration,
            )
        except ClientError as ce:
            logger_f.error(ce)
            return None
        except Exception as e:
            logger_f.error(e)
            return None

        # The response contains the presigned URL
        return response

    ## --------------------------------------------------------
    ## DOWNLOAD Geometries
    ## --------------------------------------------------------
    def smart_read_xyz(self, s3_url: str):
        try:
            s3_url_suffix = Path(s3_url).suffix
            # extra_args = self.config.get("extra_args", {})
            with smart_open.open(
                s3_url, "r", transport_params=dict(client=self.client)
            ) as fin:
                match s3_url_suffix:
                    case ".pts":
                        np_table = np.genfromtxt(fin, delimiter=" ")
                        return np_table[:, :3]
                    case _:
                        np_table = np.genfromtxt(fin, delimiter=",")
                        return np_table[:, :3]
        except AttributeError as aerr:
            logger_f.warning(aerr)
            print(aerr)
        except Exception as err:
            logger_f.error(err)
            print(err)
            return None

    ### PLY File Reader (source: xxxxxx)
    ### -------------------------------------------------------
    def smart_read_ply(self, s3_url, allow_bool=False):
        """Read a .ply (binary or ascii) file and store the elements in pandas DataFrame.

        Parameters
        ----------
        s3_url: str
            Path to the s3_url
        allow_bool: bool
            flag to allow bool as a valid PLY dtype. False by default to mirror original PLY specification.

        Returns
        -------
        data: dict
            Elements as pandas DataFrames; comments and ob_info as list of string
        """
        ply_dtypes = dict(
            [
                (b"int8", "i1"),
                (b"char", "i1"),
                (b"uint8", "u1"),
                (b"uchar", "b1"),
                (b"uchar", "u1"),
                (b"int16", "i2"),
                (b"short", "i2"),
                (b"uint16", "u2"),
                (b"ushort", "u2"),
                (b"int32", "i4"),
                (b"int", "i4"),
                (b"uint32", "u4"),
                (b"uint", "u4"),
                (b"float32", "f4"),
                (b"float", "f4"),
                (b"float64", "f8"),
                (b"double", "f8"),
            ]
        )

        valid_formats = {
            "ascii": "",
            "binary_big_endian": ">",
            "binary_little_endian": "<",
        }

        sys_byteorder = (">", "<")[sys.byteorder == "little"]

        if allow_bool:
            ply_dtypes[b"bool"] = "?"

        with smart_open.open(
            s3_url, "rb", transport_params=dict(client=self.client)
        ) as ply:
            # with smart_open.open(filename, 'rb') as ply:
            if b"ply" not in ply.readline():
                raise ValueError("The file does not start with the word ply")
            # get binary_little/big or ascii
            fmt = ply.readline().split()[1].decode()
            # get extension for building the numpy dtypes
            ext = valid_formats[fmt]

            line = []
            dtypes = defaultdict(list)
            count = 2
            points_size = None
            mesh_size = None
            has_texture = False
            comments = []
            while b"end_header" not in line and line != b"":
                line = ply.readline()

                if b"element" in line:
                    line = line.split()
                    name = line[1].decode()
                    size = int(line[2])
                    if name == "vertex":
                        points_size = size
                    elif name == "face":
                        mesh_size = size

                elif b"property" in line:
                    line = line.split()
                    # element mesh
                    if b"list" in line:
                        if b"vertex_indices" in line[-1] or b"vertex_index" in line[-1]:
                            mesh_names = ["n_points", "v1", "v2", "v3"]
                        else:
                            has_texture = True
                            mesh_names = ["n_coords"] + [
                                "v1_u",
                                "v1_v",
                                "v2_u",
                                "v2_v",
                                "v3_u",
                                "v3_v",
                            ]

                        if fmt == "ascii":
                            # the first number has different dtype than the list
                            dtypes[name].append((mesh_names[0], ply_dtypes[line[2]]))
                            # rest of the numbers have the same dtype
                            dt = ply_dtypes[line[3]]
                        else:
                            # the first number has different dtype than the list
                            dtypes[name].append(
                                (mesh_names[0], ext + ply_dtypes[line[2]])
                            )
                            # rest of the numbers have the same dtype
                            dt = ext + ply_dtypes[line[3]]

                        for j in range(1, len(mesh_names)):
                            dtypes[name].append((mesh_names[j], dt))
                    else:
                        if fmt == "ascii":
                            dtypes[name].append((line[2].decode(), ply_dtypes[line[1]]))
                        else:
                            dtypes[name].append(
                                (line[2].decode(), ext + ply_dtypes[line[1]])
                            )

                elif b"comment" in line:
                    line = line.split(b" ", 1)
                    comment = line[1].decode().rstrip()
                    comments.append(comment)

                count += 1

            # for bin
            end_header = ply.tell()

        data = {}

        if comments:
            data["comments"] = comments

        if fmt == "ascii":
            logger_f.debug(f"(smart_read_ply) ascii .ply file at url={s3_url}")
            top = count
            bottom = 0 if mesh_size is None else mesh_size

            names = [x[0] for x in dtypes["vertex"]]

            with smart_open.open(
                s3_url, "rb", transport_params=dict(client=self.client)
            ) as dply:
                data["points"] = pd.read_csv(
                    dply,
                    sep=" ",
                    header=None,
                    engine="python",
                    skiprows=top,
                    skipfooter=bottom,
                    usecols=names,
                    names=names,
                )

            for n, col in enumerate(data["points"].columns):
                data["points"][col] = data["points"][col].astype(dtypes["vertex"][n][1])

            if mesh_size:
                top = count + points_size

                names = np.array([x[0] for x in dtypes["face"]])
                usecols = [1, 2, 3, 5, 6, 7, 8, 9, 10] if has_texture else [1, 2, 3]
                names = names[usecols]

                data["mesh"] = pd.read_csv(
                    s3_url,
                    sep=" ",
                    header=None,
                    engine="python",
                    skiprows=top,
                    usecols=usecols,
                    names=names,
                )

                for n, col in enumerate(data["mesh"].columns):
                    data["mesh"][col] = data["mesh"][col].astype(
                        dtypes["face"][n + 1][1]
                    )

        else:
            # with open(s3_url, 'rb') as ply:
            logger_f.debug(f"(smart_read_ply) bin .ply file at url={s3_url}")
            with smart_open.open(
                s3_url, "rb", transport_params=dict(client=self.client)
            ) as ply:
                ply.seek(end_header)
                # points_np = np.fromfile(ply, dtype=dtypes["vertex"], count=points_size)
                points_np = np.frombuffer(
                    ply.read(), dtype=dtypes["vertex"], count=points_size
                )
                # points_np = np.loadtxt(ply, dtype=dtypes["vertex"],encoding='bytes',unpack=True)
                if ext != sys_byteorder:
                    points_np = points_np.byteswap().newbyteorder()
                data["points"] = pd.DataFrame(points_np)
                if mesh_size:
                    mesh_np = np.fromfile(ply, dtype=dtypes["face"], count=mesh_size)
                    if ext != sys_byteorder:
                        mesh_np = mesh_np.byteswap().newbyteorder()
                    data["mesh"] = pd.DataFrame(mesh_np)
                    data["mesh"].drop("n_points", axis=1, inplace=True)

        return data

    def describe_ply_element(name, df):
        """Takes the columns of the dataframe and builds a ply-like description

        Parameters
        ----------
        name: str
        df: pandas DataFrame

        Returns
        -------
        element: list[str]
        """
        property_formats = {"f": "float", "u": "uchar", "i": "int", "b": "bool"}
        element = ["element " + name + " " + str(len(df))]

        if name == "face":
            element.append("property list uchar int vertex_indices")

        else:
            for i in range(len(df.columns)):
                # get first letter of dtype to infer format
                f = property_formats[str(df.dtypes[i])[0]]
                element.append("property " + f + " " + df.columns.values[i])

        return element


#    __              __
#   / /__  _______ _/ /
#  / / _ \/ __/ _ `/ /
# /_/\___/\__/\_,_/_/


class Local(CloudUpload):
    """
    Local storage for FastAPI.
    """

    async def upload(self, *, file: UploadFile) -> FileData:
        """
        # Upload a file to the destination.

        # Args:
        #     file UploadFile: File to upload

        # Returns:
        #     FileData: Result of file upload
        #"""
        # try:
        #     dest = self.config.get("dest") or Path("uploads") / f"{key_s3}"
        #     file_object = await file.read()
        #     with open(f"{dest}", "wb") as fh:
        #         fh.write(file_object)
        #     await file.close()
        #     return FileData(
        #         path=dest,
        #         message=f"{key_s3} saved successfully",
        #         content_type=file.content_type,
        #         size=file.size,
        #         filename=key_s3,
        #     )
        # except Exception as err:
        #     logger_f.error(f"Error uploading file: {err} in {self.__class__.__name__}")
        #     return FileData(
        #         status=False, error=str(err), message=f"Unable to save file"
        #     )

    async def multi_upload(self, *, files: list[UploadFile]) -> list[FileData]:
        """
        Upload multiple files to the destination.

        Args:
            files (list[tuple[str, UploadFile]]): A list of tuples of field name and the file to upload.

        Returns:
            list[FileData]: A list of uploaded file data
        """
        res = await asyncio.gather(*[self.upload(file=file) for file in files])
        return list(res)
