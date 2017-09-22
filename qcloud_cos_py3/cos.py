import os
import aiohttp
from aiohttp import MultipartWriter
from aiohttp.hdrs import CONTENT_DISPOSITION, CONTENT_TYPE
from aiohttp.payload import StringPayload, BytesPayload
from collections import namedtuple
import requests
from io import BytesIO

from .cos_auth import CosAuth


CosConfig = namedtuple(
    'CosConfig',
    ['app_id', 'secret_id', 'secret_key', 'region', 'bucket']
)


class MyWriter(MultipartWriter):

    def __init__(self, subtype='mixed', boundary=None):
        super().__init__(subtype=subtype, boundary=boundary)
        self._content_type = self._content_type.replace('"', '')

    def append_payload(self, payload):
        """Adds a new body part to multipart writer."""
        if payload.content_type == 'application/octet-stream':
            payload.headers[CONTENT_TYPE] = payload.content_type

        # render headers
        headers = ''.join(
            [k + ': ' + v + '\r\n' for k, v in payload.headers.items()]
        ).encode('utf-8') + b'\r\n'

        self._parts.append((payload, headers, '', ''))


class CosBucket(object):

    def __init__(self, app_id, secret_id, secret_key, bucket_name, region='sh'):
        """初始化操作

        """
        self.config = CosConfig(app_id, secret_id, secret_key, region, bucket_name)
        self.signer = CosAuth(self.config)
        self.headers = {'Content-Type': 'application/json'}

    def _format_url(self, url_pattern, **extra):
        url_pattern = "http://{region}.file.myqcloud.com" + url_pattern
        return url_pattern.format(**self.config._asdict(), **extra)

    def create_folder(self, dir_name, biz_attr=''):
        """
        创建目录(https://www.qcloud.com/document/product/436/6061)
        """
        dir_name = dir_name.strip('/')
        url = self._format_url(
            "/files/v2/{app_id}/{bucket}/{dir_name}/",
            dir_name=dir_name
        )
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return requests.post(
            url, json={'op': 'create', 'biz_attr': biz_attr},
            headers=headers
        ).json()

    def list_folder(self, dir_name='', prefix=None, num=1000, context=None):
        """
        列出目录(https://www.qcloud.com/document/product/436/6062)

        :param dir_name:文件夹名称
        :param prefix:前缀
        :param num:查询的文件的数量，最大支持1000，默认查询数量为1000
        :param context:翻页标志，将上次查询结果的context的字段传入，即可实现翻页的功能
        :return 查询结果，为json格式
        """
        dir_name = dir_name.lstrip('/')
        url = self._format_url("/files/v2/{app_id}/{bucket}/")
        if dir_name:
            url += str(dir_name) + "/"
        if prefix:
            url += str(prefix)

        url += "?op=list&num=" + str(num)
        if context is not None:
            url += '&context=' + str(context)

        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return requests.get(url, headers=headers).json()

    def stat_folder(self, dir_name):
        """
        查询目录属性(https://www.qcloud.com/document/product/436/6063)
        """
        dir_name = dir_name.strip('/')
        url = self._format_url(
            "/files/v2/{app_id}/{bucket}/{dir_name}/?op=stat",
            dir_name=dir_name
        )
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return requests.get(url, headers=headers).json()

    def delete_folder(self, dir_name):
        """
        删除目录 https://www.qcloud.com/document/product/436/6064
        """
        dir_name = dir_name.strip('/')
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/{dir_name}/',
            dir_name=dir_name
        )
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.signer.sign_once(
                self.config.bucket, dir_name + '/'
            )
        }
        return requests.post(url, json={'op': 'delete'}, headers=headers).json()

    def upload_file(self, file_stream, upload_filename, dir_name="", biz_attr='',
                    replace=True, mime='application/octet-stream'):
        """简单上传文件(https://www.qcloud.com/document/product/436/6066)

        :param file_stream: 文件类似物
        :param upload_filename: 文件名称
        :param dir_name: 文件夹名称（可选）
        :param biz_attr: 业务属性（可选）
        :param replace: 是否覆盖（可选）
        :return:json数据串
        """
        insert = '0' if replace else '1'
        dir_name = dir_name.lstrip('/')
        url = self._format_url('/files/v2/{app_id}/{bucket}')
        if dir_name is not None:
            url += '/' + dir_name
        url += '/' + upload_filename
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return requests.post(
            url,
            data={'op': 'upload', 'biz_attr': biz_attr, 'insertOnly': insert},
            files={'filecontent': ('', file_stream, mime)},
            headers=headers
        ).json()

    async def async_upload_file(self, file_content, upload_filename, dir_name="",
                                biz_attr='', replace=True, mime='image/jpeg'):
        """
        由于COS不支持带引号的boundary，需要重写writer以生成不带引号的版本
        """
        TIMEOUT = 6
        insert = '0' if replace else '1'
        dir_name = dir_name.strip('/')
        url = self._format_url('/files/v2/{app_id}/{bucket}')
        if dir_name is not None:
            url += '/' + dir_name
        url += '/' + upload_filename
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        pl_op = StringPayload('upload')
        pl_op.set_content_disposition('form-data', name='op')
        pl_bz = StringPayload(biz_attr)
        pl_bz.set_content_disposition('form-data', name='biz_attr')
        pl_ir = StringPayload(insert)
        pl_ir.set_content_disposition('form-data', name='insertOnly')
        pl_fc = BytesPayload(file_content)
        pl_fc.set_content_disposition('form-data', name='filecontent', filename='')
        pl_fc._headers[CONTENT_DISPOSITION] = 'form-data; name="filecontent"; filename=""'
        with MyWriter('form-data') as writer:
            writer.append(pl_op)
            writer.append(pl_bz)
            writer.append(pl_ir)
            writer.append(pl_fc)

        conn = aiohttp.TCPConnector(verify_ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.post(url, data=writer, headers=headers,
                                    timeout=TIMEOUT) as resp:
                return await resp.json()

    def _upload_slice_control(self, file_size, slice_size, biz_attr, replace):
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        data = {
            'op': 'upload_slice_init',
            'filesize': str(file_size),
            'slice_size': str(slice_size),
            'biz_attr': biz_attr,
            'insertOnly': '0' if replace else '1',
        }
        r = requests.post(url=self.url, files=data, headers=headers).json()
        return r['data']['session']

    def _upload_slice_data(self, filecontent, session, offset):
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        data = {
            'op': 'upload_slice_data',
            'filecontent': filecontent,
            'session': session,
            'offset': str(offset)
        }
        r = requests.post(url=self.url, files=data, headers=headers).json()
        return r['data']

    def _upload_slice_finish(self, session, file_size):
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        data = {
            'op': 'upload_slice_finish',
            'session': session,
            'filesize': str(file_size)
        }
        r = requests.post(url=self.url, files=data, headers=headers).json()
        return r['data']

    def upload_slice_file(self, real_file_path, slice_size, upload_filename,
                          offset=0, dir_name='', biz_attr='', replace=True):
        """
        此分片上传代码由GitHub用户a270443177(https://github.com/a270443177)友情提供

        :param real_file_path:
        :param slice_size:
        :param upload_filename:
        :param offset:
        :param dir_name:
        :param biz_attr: 业务属性（可选）
        :param replace: 是否覆盖（可选）
        :return:
        """
        dir_name = dir_name.lstrip('/')
        self.url = self._format_url('/files/v2/{app_id}/{bucket}')
        if dir_name is not None:
            self.url += '/' + dir_name
        self.url += '/' + upload_filename
        file_size = os.path.getsize(real_file_path)
        session = self._upload_slice_control(
            file_size=file_size,
            slice_size=slice_size,
            biz_attr=biz_attr,
            replace=replace)

        with open(real_file_path, 'rb') as local_file:
            while offset < file_size:
                file_content = local_file.read(slice_size)
                self._upload_slice_data(filecontent=file_content,
                                        session=session, offset=offset)
                offset += slice_size
            r = self._upload_slice_finish(session=session, file_size=file_size)
        return r

    def upload_file_from_url(self, url, file_name, dir_name=''):
        """简单上传文件(https://www.qcloud.com/document/product/436/6066)

        :param url: 文件url地址
        :param file_name: 文件名称
        :param dir_name: 文件夹名称（可选）
        :return:json数据串
        """
        try:
            r = requests.get(url)
            r.raise_for_status()
        except:
            return None
        return self.upload_file(
            BytesIO(r.content), file_name, dir_name,
            mime=r.headers.get('content-type')
        )

    def move_file(self, source_file_path, dest_file_path):
        source_file_path = source_file_path.replace("\\", '/').lstrip('/')
        dest_file_path = dest_file_path.replace("\\", "/").lstrip('/')
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + source_file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(
                self.config.bucket, source_file_path
            )
        }
        return requests.post(
            url,
            data={'op': 'move', 'dest_fileid': dest_file_path,
                  'to_over_write': '0'},
            files={'filecontent': ('', '', 'application/octet-stream')},
            headers=headers
        ).json()

    def copy_file(self, source_file_path, dest_file_path):
        """
        复制文件https://www.qcloud.com/document/product/436/7419
        """
        source_file_path = source_file_path.replace("\\", '/').lstrip('/')
        dest_file_path = dest_file_path.replace("\\", "/").lstrip('/')
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + source_file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(
                self.config.bucket, source_file_path
            )
        }
        return requests.post(
            url,
            data={'op': 'copy', 'dest_fileid': source_file_path,
                  'to_over_write': '0'},
            files={'filecontent': ('', '', 'application/octet-stream')},
            headers=headers
        ).json()

    def delete_file(self, file_path):
        """
        删除文件 https://www.qcloud.com/document/product/436/6073
        """
        file_path = file_path.replace("\\", "/").lstrip('/')
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(self.config.bucket, file_path)
        }
        return requests.post(url, json={'op': 'delete'}, headers=headers).json()

    def stat_file(self, file_path):
        """
        查询文件属性 https://www.qcloud.com/document/api/436/6069
        """
        file_path = file_path.lstrip('/')
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/{file_path}?op=stat',
            file_path=file_path
        )
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return requests.get(url, headers=headers).json()

    def update_file_status(self, file_path, authority='eInvalid',
                           custom_headers=None):
        """
        修改文件属性 https://www.qcloud.com/document/api/436/6072
        """
        assert authority in (
            'eInvalid', # 空权限，此时系统会默认调取 Bucket 权限
            'eWRPrivate', # 私有读写
            'eWPrivateRPublic' # 公有读私有写
        )

        file_path = file_path.lstrip('/')
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(self.config.bucket, file_path)
        }
        payload = {
            'op': 'update',
            'authority': authority,
            'custom_headers': custom_headers or {}
        }
        return requests.post(url, json=payload, headers=headers).json()
